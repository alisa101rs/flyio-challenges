use std::{sync::Arc, time::Duration};

use flyio_rs::{
    network::Network,
    request::{Extension, Payload},
    NodeId, Rpc,
};
use futures_util::StreamExt;
use parking_lot::Mutex;
use serde::{de::IgnoredAny, Deserialize, Serialize};
use tracing::instrument;
use vclock::VClock64;

pub type Clock = VClock64<NodeId>;

#[derive(Debug, Clone)]
pub struct NodeClock {
    id: NodeId,
    clock: Arc<Mutex<Clock>>,
}

impl NodeClock {
    pub fn new(id: NodeId) -> Self {
        Self {
            clock: Arc::new(Mutex::new(VClock64::new(id.clone()))),
            id,
        }
    }

    pub fn next(&self) -> VClock64<NodeId> {
        let mut clock = self.clock.lock();

        clock.incr(&self.id);

        clock.clone()
    }

    pub fn merge(&self, other: &VClock64<NodeId>) {
        let mut clock = self.clock.lock();
        clock.incr(&self.id);
        clock.merge(other);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tick {
    clock: Clock,
}

pub async fn handler(Extension(clock): Extension<NodeClock>, Payload(tick): Payload<Tick>) {
    clock.merge(&tick.clock);
}

pub async fn start_ticking(clock: NodeClock, rpc: Rpc, network: Arc<Network>) {
    #[instrument(skip(clock, rpc, network))]
    async fn do_tick(clock: &NodeClock, rpc: &Rpc, network: &Network) {
        let clock = clock.next();

        let mut iter = rpc.broadcast::<_, IgnoredAny>("tick", network, Tick { clock });

        while let Some(_m) = iter.next().await {
            tracing::info!("Received Tock")
        }
    }

    let mut ticker = tokio::time::interval(Duration::from_millis(50));
    loop {
        ticker.tick().await;

        do_tick(&clock, &rpc, &network).await;
    }
}
