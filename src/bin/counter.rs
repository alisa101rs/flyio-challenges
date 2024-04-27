use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use flyio_rs::{
    network::Network,
    request::{Extension, Payload},
    response::IntoResponse,
    routing::Router,
    serve, setup_network,
    trace::setup_with_telemetry,
    NodeId, Rpc,
};
use futures::StreamExt;
use serde::{de::IgnoredAny, Deserialize, Serialize};
use tracing::instrument;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let (network, rpc, messages) = setup_network().await?;
    setup_with_telemetry(format!("counter-{}", network.id))?;
    let counter = Counters::create(&network.id, &network.other_nodes());

    let router = Router::new()
        .route("add", add)
        .route("read", read)
        .route("replicate", replicate)
        .layer(Extension(counter.clone()))
        .layer(Extension(rpc.clone()));

    tokio::spawn(start_replication(
        counter.clone(),
        rpc.clone(),
        network.clone(),
    ));

    serve(router, rpc, messages).await?;

    Ok(())
}

#[derive(Debug, Clone, Serialize)]
struct ReadResponse {
    value: i64,
}

#[instrument(skip(counter), ret)]
async fn read(Extension(counter): Extension<Counters>) -> impl IntoResponse {
    Payload(ReadResponse {
        value: (counter.positive.read() as i64) - counter.negative.read() as i64,
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Replicate {
    positive: Vec<(NodeId, u64)>,
    negative: Vec<(NodeId, u64)>,
}
#[instrument(skip(counter))]
async fn replicate(
    Extension(counter): Extension<Counters>,
    Payload(replicate): Payload<Replicate>,
) {
    counter.positive.merge(replicate.positive);
    counter.negative.merge(replicate.negative);
}

#[derive(Debug, Clone, Deserialize)]
struct Add {
    delta: i64,
}

#[instrument(skip(counter))]
async fn add(Extension(counter): Extension<Counters>, Payload(add): Payload<Add>) {
    if add.delta > 0 {
        counter.positive.add(add.delta as u64);
    }
    if add.delta < 0 {
        counter.negative.add(add.delta.abs() as _)
    }
}

async fn start_replication(counter: Counters, rpc: Rpc, network: Arc<Network>) {
    let mut ticker = tokio::time::interval(Duration::from_millis(500));
    loop {
        ticker.tick().await;
        send_replication(&counter, &rpc, &network).await;
    }
}

#[instrument(skip(counter, rpc))]
async fn send_replication(counter: &Counters, rpc: &Rpc, network: &Network) {
    let positive = counter.positive.serialize();
    let negative = counter.negative.serialize();
    let _ = rpc
        .broadcast::<_, IgnoredAny>("replicate", network, Replicate { positive, negative })
        .count()
        .await;
}

#[derive(Debug, Clone)]
struct Counters {
    positive: GrowCounter,
    negative: GrowCounter,
}

impl Counters {
    fn create(id: &NodeId, peers: &[NodeId]) -> Self {
        Self {
            positive: GrowCounter::create(id, peers),
            negative: GrowCounter::create(id, peers),
        }
    }
}

#[derive(Debug, Clone)]
struct GrowCounter {
    id: NodeId,
    values: Arc<HashMap<NodeId, AtomicU64>>,
}

impl GrowCounter {
    fn create(id: &NodeId, peers: &[NodeId]) -> Self {
        let me = [(id.clone(), AtomicU64::new(0))].into_iter();
        let peers = peers.iter().map(|it| (it.clone(), AtomicU64::new(0)));
        let values = Arc::new(me.chain(peers).collect());

        Self {
            id: id.clone(),
            values,
        }
    }

    fn add(&self, delta: u64) {
        self.values
            .get(&self.id)
            .unwrap()
            .fetch_add(delta, Ordering::SeqCst);
    }

    fn read(&self) -> u64 {
        self.values
            .iter()
            .map(|(_, it)| it.load(Ordering::Relaxed))
            .sum()
    }

    fn serialize(&self) -> Vec<(NodeId, u64)> {
        self.values
            .iter()
            .map(|(k, v)| (k.clone(), v.load(Ordering::Relaxed)))
            .collect()
    }

    fn merge(&self, others: impl IntoIterator<Item = (NodeId, u64)>) {
        for (node, value) in others {
            if node == self.id {
                continue;
            }
            self.values
                .get(&node)
                .unwrap()
                .fetch_max(value, Ordering::SeqCst);
        }
    }
}
