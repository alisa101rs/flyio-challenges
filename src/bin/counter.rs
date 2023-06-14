#![feature(async_fn_in_trait)]
#![feature(hash_drain_filter)]
#![feature(integer_atomics)]

use std::{
    collections::{HashMap, HashSet},
    hash::{Hash, Hasher},
    ops::Add,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use flyio_rs::{
    azync::{event_loop, periodic_injection, Event, Node, Rpc},
    setup_with_telemetry, Message, Request, Response,
};
use futures::{stream::FuturesUnordered, StreamExt};
use parking_lot::Mutex;
use rand::random;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use tokio::sync::mpsc;
use tracing::{instrument, Instrument};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RequestPayload {
    Add {
        delta: u64,
    },
    Read,
    Replicate {
        deltas: Vec<Delta>,
        last_commit: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResponsePayload {
    AddOk,
    ReadOk { value: u64 },
    ReplicateOk,
}

#[derive(Debug, Clone)]
pub enum Injected {
    Replicate,
    Compress,
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

impl Add<Delta> for Delta {
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
    event_loop::<GrowCounter, RequestPayload, ResponsePayload>().await?;

    Ok(())
}

#[derive(Debug, Default, Clone)]
struct GrowCounter {
    node_id: SmolStr,
    myself: u64,
    ids: Arc<AtomicU64>,
    deltas: Arc<Mutex<HashSet<Delta>>>,
    commits: Arc<HashMap<SmolStr, AtomicU64>>,
    replications: Arc<HashMap<SmolStr, AtomicU64>>,
}

impl GrowCounter {
    fn next_id(&self) -> Identifier {
        Identifier {
            src: self.myself,
            id: self.ids.fetch_add(1, Ordering::Relaxed),
        }
    }

    #[instrument(skip(self, rpc), err)]
    async fn replicate(&self, rpc: &Rpc<ResponsePayload>) -> eyre::Result<()> {
        let mut futures = FuturesUnordered::new();
        for (node, last_commit) in &*self.commits {
            let last_commit_from_n = last_commit.load(Ordering::Relaxed);

            let deltas: Vec<_> = {
                let deltas = self.deltas.lock();

                deltas
                    .iter()
                    .filter(|&it| it.id.src == self.myself && it.id.id > last_commit_from_n)
                    .cloned()
                    .collect()
            };
            if deltas.is_empty() {
                continue;
            }
            let last_commit = deltas.iter().max().unwrap().id.id;

            tracing::info!(
                ?last_commit_from_n,
                ?last_commit,
                ?deltas,
                ?node,
                "Replicating deltas to other node"
            );

            let message = Message {
                id: 0,
                src: self.node_id.clone(),
                dst: node.clone(),
                body: Request {
                    message_id: random(),
                    traceparent: None,
                    payload: RequestPayload::Replicate {
                        deltas,
                        last_commit,
                    },
                },
            };
            let rpc = rpc.clone();
            let span = tracing::info_span!("Replication");
            futures.push(async move {
                match rpc.send(message).instrument(span).await {
                    Ok(Message {
                        src,
                        body:
                            Response {
                                payload: ResponsePayload::ReplicateOk,
                                ..
                            },
                        ..
                    }) => Some((src, last_commit)),
                    _ => None,
                }
            });
        }

        let commits = self.commits.clone();
        while let Some(response) = futures.next().await {
            if let Some((src, last_commit)) = response {
                commits
                    .get(&src)
                    .unwrap()
                    .fetch_max(last_commit, Ordering::Relaxed);
            }
        }

        Ok(())
    }

    #[instrument(skip(self), ret)]
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

        let compressed: u64 = values
            .drain_filter(|delta| delta.id.src != self.myself || delta.id.id <= last_commit)
            .map(|it| it.value)
            .sum();

        values.insert(Delta::new(compressed, Identifier::new(0, 0)));
    }
}

impl Node for GrowCounter {
    type Injected = Injected;
    type Request = RequestPayload;
    type Response = ResponsePayload;

    fn from_init(
        node_id: SmolStr,
        node_ids: Vec<SmolStr>,
        tx: mpsc::Sender<Event<Self::Request, Self::Injected>>,
    ) -> eyre::Result<Self> {
        setup_with_telemetry(format!("counter-{node_id}"))?;

        periodic_injection(tx.clone(), Duration::from_millis(300), Injected::Replicate);
        periodic_injection(tx, Duration::from_millis(1500), Injected::Compress);

        let commits = node_ids
            .clone()
            .into_iter()
            .filter(|it| it != &node_id)
            .map(|it| (it, AtomicU64::new(0)))
            .collect();

        let replications = node_ids
            .into_iter()
            .filter(|it| it != &node_id)
            .map(|it| (it, AtomicU64::new(0)))
            .collect();

        let myself = node_id.strip_prefix('n').unwrap().parse::<u64>().unwrap() + 1;

        Ok(Self {
            node_id,
            myself,
            ids: Arc::new(AtomicU64::new(1)),
            deltas: Arc::new(Mutex::new(Default::default())),
            commits: Arc::new(commits),
            replications: Arc::new(replications),
        })
    }

    #[instrument(skip(self, rpc), err)]
    async fn process_event(
        &mut self,
        event: Event<Self::Request, Self::Injected>,
        rpc: Rpc<Self::Response>,
    ) -> eyre::Result<()> {
        match event {
            Event::Injected(Injected::Replicate) => {
                self.replicate(&rpc).await?;
                Ok(())
            }
            Event::Injected(Injected::Compress) => {
                self.compress();
                Ok(())
            }
            Event::Request(message) => {
                let payload = match message.body.payload {
                    RequestPayload::Add { delta } => {
                        if delta != 0 {
                            self.deltas.lock().insert(Delta::new(delta, self.next_id()));
                        }
                        ResponsePayload::AddOk
                    }
                    RequestPayload::Read => ResponsePayload::ReadOk { value: self.read() },
                    RequestPayload::Replicate {
                        ref deltas,
                        last_commit,
                    } => {
                        let mut my_deltas = self.deltas.lock();
                        let last_replicate = self.replications[&message.src]
                            .fetch_max(last_commit, Ordering::Relaxed);

                        my_deltas.extend(deltas.iter().filter(|it| it.id.id > last_replicate));

                        ResponsePayload::ReplicateOk
                    }
                };
                let response = message.into_reply(|_| payload);
                rpc.respond(response);

                Ok(())
            }
            Event::EOF => Ok(()),
        }
    }
}
