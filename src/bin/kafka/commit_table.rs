use std::{collections::HashMap, sync::Arc};

use flyio_rs::{
    azync::Rpc,
    network::{Network, NodeId},
    Message, Request,
};
use futures::{stream::FuturesUnordered, StreamExt};
use parking_lot::Mutex;
use tracing::{info_span, instrument, Instrument};

use crate::{Key, Offset, RequestPayload, ResponsePayload};

#[derive(Debug)]
pub struct CommitTable {
    network: Arc<Network>,
    commits: Mutex<HashMap<Key, Offset>>,
    known: HashMap<NodeId, Arc<Mutex<HashMap<Key, Offset>>>>,
}

impl CommitTable {
    pub fn new(network: Arc<Network>) -> Self {
        let known = network
            .other_nodes()
            .iter()
            .map(|node| (node.clone(), Arc::new(Mutex::new(Default::default()))))
            .collect();
        Self {
            commits: Mutex::new(Default::default()),
            network,
            known,
        }
    }

    pub fn commit(&self, commits: impl IntoIterator<Item = (Key, Offset)>) {
        self.commits.lock().extend(commits);
    }

    pub fn list_commits(&self, keys: &[Key]) -> HashMap<Key, Offset> {
        let commits = self.commits.lock();
        keys.iter()
            .filter_map(|it| Some((it.clone(), commits.get(it).cloned()?)))
            .collect()
    }

    #[instrument(skip(self))]
    pub fn accept_gossip_commits(&self, from: &NodeId, incoming_commits: HashMap<Key, Offset>) {
        let mut commits = self.commits.lock();
        let known = self.known.get(from).unwrap();
        let mut known = known.lock();

        for (key, offset) in incoming_commits {
            let value = commits.entry(key.clone()).or_default();
            *value = std::cmp::max(*value, offset);

            let value = known.entry(key).or_default();
            *value = std::cmp::max(*value, offset);
        }
    }

    #[instrument(skip(self, rpc))]
    pub fn gossip_commits(self: Arc<Self>, rpc: &Rpc<ResponsePayload>) {
        let mut futures = FuturesUnordered::new();

        for (dst, maybe_known_to) in self.known.iter() {
            let (message, notify) = {
                let (notify, _known_to): (HashMap<_, _>, HashMap<_, _>) = {
                    let commits = self.commits.lock();
                    let known_offsets = maybe_known_to.lock();

                    commits
                        .iter()
                        .map(|(k, v)| (k.clone(), *v))
                        .partition(|(key, offset)| {
                            known_offsets.get(key).copied().unwrap_or_default() < *offset
                        })
                };

                // let mut rng = rand::thread_rng();
                // let additional_cap = (10 * notify.len() / 100) as u32;
                // notify.extend(known_to.iter().filter(|_| {
                //     rng.gen_ratio(
                //         additional_cap.min(known_to.len() as u32),
                //         known_to.len() as u32,
                //     )
                // }));

                if notify.is_empty() {
                    continue;
                }

                tracing::debug!(gossips = ?notify, "Sending gossip");
                let message_id = rand::random();
                let message = Message {
                    id: message_id,
                    src: self.network.id.clone(),
                    dst: dst.clone(),
                    body: Request {
                        message_id,
                        traceparent: None,
                        payload: RequestPayload::GossipCommits {
                            commits: notify.clone(),
                        },
                    },
                };

                (message, notify)
            };

            let rpc = rpc.clone();
            let span = info_span!("Gossiping Commits");
            let known_to = maybe_known_to.clone();
            futures.push(
                async move {
                    if let Ok(response) = rpc.send(message).await {
                        match response.body.payload {
                            ResponsePayload::GossipCommitsOk => {
                                let mut known_to = known_to.lock();
                                known_to.extend(notify.into_iter());
                            }
                            _ => unreachable!(),
                        }
                    }
                }
                .instrument(span),
            );
        }

        tokio::task::spawn(async move { while (futures.next().await).is_some() {} });
    }
}
