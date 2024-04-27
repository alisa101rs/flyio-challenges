use std::{collections::BTreeSet, sync::Arc, time::Duration};

use flyio_rs::{
    network::Network,
    request::{Extension, Payload},
    response::IntoResponse,
    routing::Router,
    serve, setup_network,
    trace::setup_with_telemetry,
    Rpc,
};
use futures::StreamExt;
use parking_lot::Mutex;
use serde::{de::IgnoredAny, Deserialize, Serialize};
use tracing::instrument;

#[derive(Debug, Clone, Deserialize)]
struct Add {
    element: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Replicate {
    values: Vec<u64>,
}

#[derive(Debug, Clone, Serialize)]
struct ReadResponse {
    value: Vec<u64>,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let (network, rpc, messages) = setup_network().await?;
    setup_with_telemetry(format!("gset-{}", network.id))?;
    let set = GSet::create();

    let router = Router::new()
        .route("add", add)
        .route("read", read)
        .route("replicate", replicate)
        .layer(Extension(set.clone()))
        .layer(Extension(network.clone()))
        .layer(Extension(rpc.clone()));

    tokio::spawn(start_replication(set.clone(), rpc.clone(), network));

    serve(router, rpc, messages).await?;

    Ok(())
}

#[instrument(skip(set), ret)]
async fn read(Extension(set): Extension<GSet>) -> impl IntoResponse {
    Payload(ReadResponse { value: set.read() })
}
#[instrument(skip(set))]
async fn replicate(Extension(set): Extension<GSet>, Payload(replicate): Payload<Replicate>) {
    set.add_all(replicate.values);
}

#[instrument(skip(set))]
async fn add(Extension(set): Extension<GSet>, Payload(add): Payload<Add>) {
    set.add(add.element);
}

async fn start_replication(set: GSet, rpc: Rpc, network: Arc<Network>) {
    let mut ticker = tokio::time::interval(Duration::from_millis(500));
    loop {
        ticker.tick().await;
        send_replication(&set, &rpc, &network).await;
    }
}

#[instrument(skip(set, rpc, network))]
async fn send_replication(set: &GSet, rpc: &Rpc, network: &Network) {
    let values = set.read();
    if values.is_empty() {
        return;
    }
    let _ = rpc
        .broadcast::<_, IgnoredAny>("replicate", network, Replicate { values })
        .count()
        .await;
}

#[derive(Debug, Clone)]
struct GSet {
    values: Arc<Mutex<BTreeSet<u64>>>,
}

impl GSet {
    fn create() -> Self {
        Self {
            values: Arc::new(Mutex::new(Default::default())),
        }
    }

    fn add(&self, value: u64) {
        self.values.lock().insert(value);
    }

    fn add_all(&self, values: impl IntoIterator<Item = u64>) {
        self.values.lock().extend(values);
    }

    fn read(&self) -> Vec<u64> {
        self.values.lock().iter().cloned().collect()
    }
}
