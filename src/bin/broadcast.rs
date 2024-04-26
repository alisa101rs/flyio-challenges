use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
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
use futures_util::{stream::FuturesUnordered, StreamExt};
use parking_lot::Mutex;
use rand::Rng;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use tracing::{info_span, instrument, Instrument};

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let (network, rpc, messages) = setup_network().await?;
    setup_with_telemetry(format!("broadcast-{}", network.id))?;
    let storage = Arc::new(NodeStorage::create(&network.other_nodes()));

    let router = Router::new()
        .route("broadcast", broadcast)
        .route("topology", topology)
        .route("read", read)
        .route("gossip", gossip)
        .layer(Extension(storage.clone()))
        .layer(Extension(network))
        .layer(Extension(rpc.clone()));

    tokio::spawn(start_gossiping(storage, rpc.clone()));

    serve(router, rpc, messages).await?;

    Ok(())
}

async fn start_gossiping(storage: Arc<NodeStorage>, rpc: Rpc) {
    let mut ticker = tokio::time::interval(Duration::from_millis(250));
    loop {
        ticker.tick().await;
        send_gossips(storage.clone(), rpc.clone()).await;
    }
}

#[derive(Debug, Deserialize)]
struct Topology {
    #[allow(dead_code)]
    topology: HashMap<SmolStr, Vec<SmolStr>>,
}

#[instrument]
async fn topology(Payload(_topology): Payload<Topology>) {}

#[derive(Debug, Deserialize)]
struct Broadcast {
    message: u64,
}

#[instrument(skip(storage))]
async fn broadcast(
    Extension(storage): Extension<Arc<NodeStorage>>,
    Payload(message): Payload<Broadcast>,
) -> () {
    storage.seen.lock().insert(message.message);

    ()
}

#[derive(Debug, Serialize)]
struct ReadResponse {
    messages: HashSet<u64>,
}

#[instrument(skip(storage), ret)]
async fn read(Extension(storage): Extension<Arc<NodeStorage>>) -> impl IntoResponse {
    Payload(ReadResponse {
        messages: storage.seen.lock().clone(),
    })
}

#[derive(Debug, Serialize, Deserialize)]
struct GossipRequest {
    notify: HashSet<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct GossipResponse {
    acknowledged: HashSet<u64>,
}

#[instrument(skip(storage), ret)]
async fn gossip(
    src: NodeId,
    Extension(storage): Extension<Arc<NodeStorage>>,
    Payload(gossip): Payload<GossipRequest>,
) -> Payload<GossipResponse> {
    storage.seen.lock().extend(gossip.notify.iter());

    let response = if let Some(known) = storage.known.get(&src) {
        let known = &mut *known.lock();
        known.extend(gossip.notify.iter());
        GossipResponse {
            acknowledged: gossip.notify,
        }
    } else {
        panic!("gossip from non-neighbor")
    };

    Payload(response)
}

#[instrument(skip(storage, rpc))]
async fn send_gossips(storage: Arc<NodeStorage>, rpc: Rpc) {
    let mut futures = FuturesUnordered::new();

    for (dst, maybe_known_to) in storage.known.iter() {
        let payload = {
            let (known_to, mut notify): (HashSet<_>, HashSet<_>) = {
                let seen = storage.seen.lock();
                let known_to = &*maybe_known_to.lock();

                seen.iter()
                    .copied()
                    .partition(|seen| known_to.contains(seen))
            };

            let mut rng = rand::thread_rng();
            let additional_cap = (10 * notify.len() / 100) as u32;
            notify.extend(known_to.iter().filter(|_| {
                rng.gen_ratio(
                    additional_cap.min(known_to.len() as u32),
                    known_to.len() as u32,
                )
            }));

            if notify.is_empty() {
                continue;
            }

            GossipRequest { notify }
        };
        let rpc = rpc.clone();
        let span = info_span!("Gossiping");
        let dst = dst.clone();

        futures.push(
            async move {
                if let Ok(response) = rpc
                    .send::<_, GossipResponse>("gossip", dst.clone(), payload)
                    .await
                {
                    Some((dst, response))
                } else {
                    tracing::debug!("Gossip has not reached destination node");
                    None
                }
            }
            .instrument(span),
        );
    }

    while let Some(response) = futures.next().await {
        let Some((dst, response)) = response else {
            continue;
        };
        let known = storage.known.get(&dst).unwrap();
        known.lock().extend(response.acknowledged.into_iter());
    }
}

#[derive(Debug)]
struct NodeStorage {
    seen: Mutex<HashSet<u64>>,
    known: HashMap<NodeId, Mutex<HashSet<u64>>>,
}

impl NodeStorage {
    pub fn create(peers: &[NodeId]) -> Self {
        Self {
            seen: Mutex::new(HashSet::new()),
            known: peers
                .iter()
                .map(|it| (it.clone(), Mutex::new(HashSet::new())))
                .collect(),
        }
    }
}
