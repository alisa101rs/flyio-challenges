use std::{
    collections::{HashMap, HashSet},
    hash::{Hash, Hasher},
    ops,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use flyio_rs::{
    request::{Extension, Payload},
    response::IntoResponse,
    routing::Router,
    serve, setup_network,
    trace::setup_with_telemetry,
    NodeId, Rpc,
};
use futures::{stream::FuturesUnordered, StreamExt};
use parking_lot::Mutex;
use serde::{de::IgnoredAny, Deserialize, Serialize};
use smol_str::SmolStr;
use tracing::{instrument, Instrument};

#[derive(Debug, Clone, Deserialize)]
struct Add {
    delta: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Replicate {
    deltas: Vec<Delta>,
    last_commit: u64,
}

#[derive(Debug, Clone, Serialize)]
struct ReadResponse {
    value: u64,
}

#[derive(Debug, Copy, Clone, Deserialize, Serialize, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Identifier {
    src: u64,
    id: u64,
}

impl Identifier {
    pub fn new(src: u64, id: u64) -> Self {
        Self { src, id }
    }
}

#[derive(Debug, Copy, Clone, Deserialize, Serialize)]
pub struct Delta {
    id: Identifier,
    value: u64,
}

impl Delta {
    pub fn new(value: u64, id: Identifier) -> Self {
        Self { value, id }
    }
}

impl ops::Add<Delta> for Delta {
    type Output = u64;

    fn add(self, rhs: Delta) -> Self::Output {
        self.value + rhs.value
    }
}

impl PartialEq for Delta {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl PartialOrd for Delta {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.id.partial_cmp(&other.id)
    }
}

impl Ord for Delta {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id.cmp(&other.id)
    }
}

impl Eq for Delta {}

impl Hash for Delta {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let (network, rpc, messages) = setup_network().await?;
    setup_with_telemetry(format!("counter-{}", network.id))?;
    let counter = GrowCounter::create(&network.id, &network.other_nodes());

    let router = Router::new()
        .route("add", add)
        .route("read", read)
        .route("replicate", replicate)
        .layer(Extension(counter.clone()))
        .layer(Extension(network))
        .layer(Extension(rpc.clone()));

    tokio::spawn(start_replication(counter.clone(), rpc.clone()));
    tokio::spawn(compress(counter));

    serve(router, rpc, messages).await?;

    Ok(())
}

#[instrument(skip(counter), ret)]
async fn read(Extension(counter): Extension<GrowCounter>) -> impl IntoResponse {
    Payload(ReadResponse {
        value: counter.read(),
    })
}
#[instrument(skip(counter))]
async fn replicate(
    src: NodeId,
    Extension(counter): Extension<GrowCounter>,
    Payload(replicate): Payload<Replicate>,
) {
    let mut my_deltas = counter.deltas.lock();
    let last_replicate =
        counter.replications[&src].fetch_max(replicate.last_commit, Ordering::Relaxed);

    my_deltas.extend(
        replicate
            .deltas
            .iter()
            .filter(|it| it.id.id > last_replicate),
    );
}

#[instrument(skip(counter))]
async fn add(Extension(counter): Extension<GrowCounter>, Payload(add): Payload<Add>) {
    if add.delta != 0 {
        counter.add(add.delta);
    }
}

async fn start_replication(counter: GrowCounter, rpc: Rpc) {
    let mut ticker = tokio::time::interval(Duration::from_millis(500));
    loop {
        ticker.tick().await;
        send_replication(&counter, &rpc).await;
    }
}

async fn compress(counter: GrowCounter) {
    let mut ticker = tokio::time::interval(Duration::from_millis(1000));
    loop {
        ticker.tick().await;
        counter.compress();
    }
}

#[instrument(skip(counter, rpc))]
async fn send_replication(counter: &GrowCounter, rpc: &Rpc) {
    let mut futures = FuturesUnordered::new();
    for (node, last_commit) in &*counter.commits {
        let last_commit_from_n = last_commit.load(Ordering::Relaxed);

        let deltas: Vec<_> = {
            let deltas = counter.deltas.lock();

            deltas
                .iter()
                .filter(|&it| it.id.src == counter.myself && it.id.id > last_commit_from_n)
                .cloned()
                .collect()
        };
        if deltas.is_empty() {
            continue;
        }
        let last_commit = deltas.iter().max().unwrap().id.id;
        let span = tracing::info_span!(
            "Replication",
            ?last_commit_from_n,
            ?last_commit,
            ?deltas,
            ?node,
        );
        let dst = node.clone();
        let payload = Replicate {
            deltas,
            last_commit,
        };

        let send = rpc
            .send::<_, IgnoredAny>("replicate", dst.clone(), payload)
            .instrument(span);
        futures.push(async move {
            match send.await {
                Ok(_) => Some((dst, last_commit)),
                _ => None,
            }
        });
    }

    while let Some(response) = futures.next().await {
        if let Some((src, last_commit)) = response {
            counter
                .commits
                .get(&src)
                .unwrap()
                .fetch_max(last_commit, Ordering::Relaxed);
        }
    }
}

#[derive(Debug, Clone)]
struct GrowCounter {
    myself: u64,
    ids: Arc<AtomicU64>,
    deltas: Arc<Mutex<HashSet<Delta>>>,
    commits: Arc<HashMap<SmolStr, AtomicU64>>,
    replications: Arc<HashMap<SmolStr, AtomicU64>>,
}

impl GrowCounter {
    fn create(id: &NodeId, peers: &[NodeId]) -> Self {
        let commits = peers
            .iter()
            .map(|it| (it.clone(), AtomicU64::new(0)))
            .collect();

        let replications = peers
            .iter()
            .map(|it| (it.clone(), AtomicU64::new(0)))
            .collect();

        let myself = id.strip_prefix('n').unwrap().parse::<u64>().unwrap() + 1;

        Self {
            myself,
            ids: Arc::new(AtomicU64::new(1)),
            deltas: Arc::new(Mutex::new(Default::default())),
            commits: Arc::new(commits),
            replications: Arc::new(replications),
        }
    }
    fn next_id(&self) -> Identifier {
        Identifier {
            src: self.myself,
            id: self.ids.fetch_add(1, Ordering::Relaxed),
        }
    }

    fn add(&self, delta: u64) {
        self.deltas.lock().insert(Delta::new(delta, self.next_id()));
    }

    fn read(&self) -> u64 {
        self.deltas.lock().iter().map(|it| it.value).sum()
    }

    #[instrument(skip(self))]
    fn compress(&self) {
        let last_commit = self
            .commits
            .values()
            .map(|it| it.load(Ordering::Relaxed))
            .min()
            .unwrap();

        let mut values = self.deltas.lock();

        let mut compressed = 0;
        values.retain(|delta| {
            let should_remove = delta.id.src != self.myself || delta.id.id <= last_commit;
            compressed += delta.value * should_remove as u64;
            !should_remove
        });

        values.insert(Delta::new(compressed, Identifier::new(0, 0)));
    }
}
