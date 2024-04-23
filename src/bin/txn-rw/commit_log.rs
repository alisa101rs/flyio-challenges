use std::{collections::VecDeque, sync::Arc, time::Instant};

use flyio_rs::{network::Network, Rpc};
use parking_lot::Mutex;
use tracing::instrument;

use crate::TransactionId;

#[derive(Clone)]
pub struct CommitLog {
    network: Arc<Network>,
    queue: Arc<Mutex<VecDeque<Transaction>>>,
}

impl CommitLog {
    pub fn new(network: Arc<Network>) -> Self {
        Self {
            network,
            queue: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    pub fn transaction(&self, operations: Vec<(u64, u64)>) -> eyre::Result<()> {
        let id = TransactionId::new();
        let at = Instant::now();

        self.queue
            .lock()
            .push_back(Transaction { id, at, operations });

        Ok(())
    }

    #[instrument(skip(self, storage, rpc))]
    pub async fn process(&self, storage: &Storage, rpc: &Rpc) {
        // let batch = {
        //     let mut queue = self.queue.lock();
        //     queue.drain(..).collect::<Vec<_>>()
        // };
        // //let message = RequestPayload::Broadcast { ops: batch };
        //
        // for node in self.network.other_nodes() {}

        todo!()
    }
}

#[derive(Debug, Clone)]
pub struct Transaction {
    id: TransactionId,
    at: Instant,
    operations: Vec<(u64, u64)>,
}
