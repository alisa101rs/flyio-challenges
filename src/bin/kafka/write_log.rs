use std::{
    cmp::max,
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use flyio_rs::{azync::Rpc, Message, Request};
use parking_lot::Mutex;
use rand::random;
use tracing::instrument;

use crate::{network::Network, Key, Offset, RequestPayload, ResponsePayload};

#[derive(Debug)]
pub struct WriteLog {
    network: Arc<Network>,
    queue: Mutex<Vec<(Key, Offset, u64)>>,
}

impl WriteLog {
    pub fn new(network: Arc<Network>) -> Self {
        Self {
            queue: Mutex::new(Default::default()),
            network,
        }
    }

    #[instrument(skip(self))]
    pub fn insert_write(&self, key: Key, offset: Offset, message: u64) {
        self.queue.lock().push((key, offset, message))
    }

    #[instrument(skip(self, rpc))]
    pub async fn propagate_writes(
        &self,
        rpc: Rpc<ResponsePayload>,
    ) -> HashMap<Key, BTreeSet<(Offset, u64)>> {
        let mut messages: HashMap<Key, BTreeSet<(Offset, u64)>> = HashMap::new();
        let mut offsets: HashMap<Key, Offset> = HashMap::new();

        {
            let mut queue = self.queue.lock();
            if queue.is_empty() {
                return HashMap::new();
            }
            for (key, offset, message) in queue.drain(..) {
                let value = offsets.entry(key.clone()).or_default();
                *value = max(offset, *value);
                messages.entry(key).or_default().insert((offset, message));
            }
        }

        for node in self.network.other_nodes() {
            let message = Message {
                id: 0,
                src: self.network.id.clone(),
                dst: node,
                body: Request {
                    message_id: random(),
                    payload: RequestPayload::PropagateWrites {
                        messages: messages.clone(),
                    },
                    traceparent: None,
                },
            };

            if let Ok(_response) = rpc.send(message).await {
            } else {
                unimplemented!("Unacknowledged write")
            }
        }

        for node in self.network.other_nodes() {
            let message = Message {
                id: 0,
                src: self.network.id.clone(),
                dst: node,
                body: Request {
                    message_id: random(),
                    payload: RequestPayload::AcknowledgeWrites {
                        offsets: offsets.clone(),
                    },
                    traceparent: None,
                },
            };

            if let Ok(_response) = rpc.send(message).await {
            } else {
                unimplemented!("Unacknowledged acknowledge")
            }
        }

        messages
    }
}
