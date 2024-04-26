use std::{collections::HashMap, sync::Arc};

use flyio_rs::{network::NodeId, Rpc};
use futures::{stream::FuturesUnordered, StreamExt};
use parking_lot::Mutex;
use serde::de::IgnoredAny;
use tracing::{info_span, instrument, Instrument};

use crate::{GossipCommits, Key, Offset};

#[derive(Debug)]
pub struct CommitTable {
    commits: Mutex<HashMap<Key, Offset>>,
    known: HashMap<NodeId, Arc<Mutex<HashMap<Key, Offset>>>>,
}

impl CommitTable {
    pub fn new(peers: &[NodeId]) -> Self {
        let known = peers
            .iter()
            .map(|node| (node.clone(), Arc::new(Mutex::new(Default::default()))))
            .collect();
        Self {
            commits: Mutex::new(Default::default()),
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
    pub async fn gossip_commits(&self, rpc: &Rpc) {
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

                let message = GossipCommits {
                    commits: notify.clone(),
                };

                (message, notify)
            };
            let span = info_span!("Gossiping Commits", gossips = ?notify, %dst);

            let dst = dst.clone();
            let rpc = rpc.clone();

            let known_to = maybe_known_to.clone();
            futures.push(
                async move {
                    if let Ok(_) = rpc
                        .send::<_, IgnoredAny>("gossip_commits", dst, message)
                        .await
                    {
                        let mut known_to = known_to.lock();
                        known_to.extend(notify.into_iter());
                    }
                }
                .instrument(span),
            );
        }

        while (futures.next().await).is_some() {}
    }
}
