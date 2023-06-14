#![feature(async_fn_in_trait)]
#![feature(hash_drain_filter)]
#![feature(integer_atomics)]

use std::{
    collections::{BTreeSet, HashMap},
    hash::{Hasher},
    sync::{
        Arc,
    },
    time::Duration,
};



use flyio_rs::{
    azync::{event_loop, periodic_injection, Event, Node, Rpc},
    setup_with_telemetry, Message, Request, Response,
};

use futures::{StreamExt};
use parking_lot::Mutex;
use rand::random;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use tokio::sync::mpsc;
use tracing::{instrument, Instrument, Span};

use crate::{
    bucket::LogBucket, commit_table::CommitTable, leaders::TopicLeaders, network::Network,
    write_log::WriteLog,
};

pub type Key = SmolStr;
pub type NodeId = SmolStr;
pub type Offset = u64;
pub type Log = (Offset, u64);

mod bucket;
mod commit_table;
mod leaders;
mod network;
mod partitions;
mod write_log;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    event_loop::<KafkaLogNode, RequestPayload, ResponsePayload>().await?;

    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RequestPayload {
    Send {
        key: SmolStr,
        #[serde(rename = "msg")]
        message: u64,
    },
    Poll {
        offsets: HashMap<SmolStr, Offset>,
    },
    CommitOffsets {
        offsets: HashMap<SmolStr, Offset>,
    },
    ListCommittedOffsets {
        keys: Vec<SmolStr>,
    },
    GossipCommits {
        commits: HashMap<SmolStr, Offset>,
    },
    PropagateWrites {
        messages: HashMap<Key, BTreeSet<(Offset, u64)>>,
    },
    AcknowledgeWrites {
        offsets: HashMap<Key, Offset>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResponsePayload {
    SendOk {
        offset: u64,
    },
    PollOk {
        #[serde(rename = "msgs")]
        messages: HashMap<SmolStr, Vec<Log>>,
    },
    CommitOffsetsOk,
    ListCommittedOffsetsOk {
        offsets: HashMap<SmolStr, Offset>,
    },
    GossipCommitsOk,
    AcknowledgeWritesOk,
    PropagateWritesOk,
}

#[derive(Debug, Clone)]
pub enum Injected {
    GossipCommits,
    PropagateWrites,
}

/// KafkaLogNode
///
/// - commits are replicated on each node
/// - logs buckets have a leader and replicas on all other nodes
/// - `poll` can read from any replica
/// - `send` will be redirected to the leader
#[derive(Debug, Clone)]
struct KafkaLogNode {
    network: Arc<Network>,
    topic_leaders: Arc<TopicLeaders>,
    buckets: Arc<Mutex<HashMap<SmolStr, LogBucket>>>,
    commits: Arc<CommitTable>,
    write_log: Arc<WriteLog>,
}

impl KafkaLogNode {
    #[instrument(skip(self,))]
    fn propagate_writes(&self, messages: HashMap<Key, BTreeSet<(Offset, u64)>>) -> ResponsePayload {
        for (message, writes) in messages {
            self.buckets
                .lock()
                .entry(message)
                .or_insert_with(LogBucket::new)
                .insert(writes.into_iter());
        }

        ResponsePayload::PropagateWritesOk
    }

    #[instrument(skip(self), ret)]
    async fn create_write(&self, key: Key, message: u64) -> Offset {
        let offset = self
            .buckets
            .lock()
            .entry(key.clone())
            .or_insert_with(LogBucket::new)
            .next_offset();

        self.write_log.insert_write(key, offset, message);

        offset
    }

    #[instrument(skip(self, rpc), ret)]
    async fn send(&self, key: Key, message: u64, rpc: &Rpc<ResponsePayload>) -> ResponsePayload {
        let node = self.topic_leaders.get_or_insert(&key).await;
        if node == self.network.id {
            let offset = self.create_write(key, message).await;

            ResponsePayload::SendOk { offset }
        } else {
            let request = Message {
                id: 0,
                src: self.network.id.clone(),
                dst: node,
                body: Request {
                    payload: RequestPayload::Send { key, message },
                    message_id: random(),
                    traceparent: None,
                },
            };

            match rpc.send(request).await {
                Ok(Message {
                    body: Response { payload, .. },
                    ..
                }) => payload,
                _ => {
                    panic!("can't handle")
                }
            }
        }
    }

    #[instrument(skip(self,), ret)]
    fn poll(&self, offsets: &HashMap<SmolStr, Offset>) -> ResponsePayload {
        let mut messages = HashMap::new();

        let buckets = self.buckets.lock();
        for (key, &offset) in offsets {
            if let Some(bucket) = buckets.get(key) {
                messages.insert(key.clone(), bucket.poll(offset));
            }
        }

        ResponsePayload::PollOk { messages }
    }

    #[instrument(skip(self,), ret)]
    fn commit_offsets(&self, offsets: HashMap<SmolStr, Offset>) -> ResponsePayload {
        self.commits.commit(offsets.into_iter());

        ResponsePayload::CommitOffsetsOk
    }

    #[instrument(skip(self,), ret)]
    fn list_committed_offsets(&self, keys: &[SmolStr]) -> ResponsePayload {
        ResponsePayload::ListCommittedOffsetsOk {
            offsets: self.commits.list_commits(keys),
        }
    }

    #[instrument(skip(self, rpc))]
    async fn init_propagate_writes(&self, rpc: Rpc<ResponsePayload>) {
        let messages = self.write_log.propagate_writes(rpc.clone()).await;
        let mut buckets = self.buckets.lock();
        for (key, logs) in messages {
            buckets.get_mut(&key).expect("not possible").insert(logs);
        }
    }

    #[instrument(skip(self))]
    fn acknowledge_writes(&self, offsets: HashMap<Key, Offset>) {
        let buckets = self.buckets.lock();
        for (key, offset) in offsets {
            buckets.get(&key).expect("not possible").set_offset(offset);
        }
    }
}

impl Node for KafkaLogNode {
    type Injected = Injected;
    type Request = RequestPayload;
    type Response = ResponsePayload;

    fn from_init(
        node_id: NodeId,
        node_ids: Vec<NodeId>,
        tx: mpsc::Sender<Event<Self::Request, Self::Injected>>,
    ) -> eyre::Result<Self> {
        setup_with_telemetry(format!("kafka-{node_id}"))?;
        let network = Arc::new(Network::create(node_id, node_ids));

        periodic_injection(
            tx.clone(),
            Duration::from_millis(500),
            Injected::GossipCommits,
        );
        periodic_injection(tx, Duration::from_millis(1000), Injected::PropagateWrites);

        let topic_leaders = TopicLeaders::simple(network.clone());
        let commits = CommitTable::new(network.clone());
        let write_log = WriteLog::new(network.clone());

        Ok(Self {
            topic_leaders: Arc::new(topic_leaders),
            buckets: Arc::new(Mutex::new(Default::default())),
            commits: Arc::new(commits),
            write_log: Arc::new(write_log),
            network,
        })
    }

    #[instrument(skip(self, rpc), err)]
    async fn process_event(
        &self,
        event: Event<Self::Request, Self::Injected>,
        rpc: &Rpc<Self::Response>,
    ) -> eyre::Result<()> {
        match event {
            Event::Injected(Injected::GossipCommits) => {
                self.commits.clone().gossip_commits(rpc);
            }
            Event::Injected(Injected::PropagateWrites) => {
                let this = self.clone();
                let rpc = rpc.clone();
                let span = Span::current();
                tokio::task::spawn(
                    async move { this.init_propagate_writes(rpc).await }.instrument(span),
                );
            }
            Event::EOF => {}
            Event::Request(message) => {
                let span = Span::current();
                let node = self.clone();
                let rpc = rpc.clone();

                tokio::task::spawn(
                    async move {
                        let payload = match message.body.payload {
                            RequestPayload::Send { ref key, message } => {
                                node.send(key.clone(), message, &rpc).await
                            }
                            RequestPayload::Poll { ref offsets } => node.poll(offsets),
                            RequestPayload::CommitOffsets { offsets } => {
                                node.commit_offsets(offsets)
                            }
                            RequestPayload::ListCommittedOffsets { ref keys } => {
                                node.list_committed_offsets(keys)
                            }
                            RequestPayload::GossipCommits { commits } => {
                                node.commits.accept_gossip_commits(&message.src, commits);
                                ResponsePayload::GossipCommitsOk
                            }
                            RequestPayload::PropagateWrites { messages } => {
                                node.propagate_writes(messages)
                            }
                            RequestPayload::AcknowledgeWrites { offsets, .. } => {
                                node.acknowledge_writes(offsets);
                                ResponsePayload::AcknowledgeWritesOk
                            }
                        };

                        let response = Message {
                            id: message.id,
                            src: message.dst,
                            dst: message.src,
                            body: Response {
                                in_reply_to: message.body.message_id,
                                payload,
                            },
                        };
                        rpc.respond(response);
                    }
                    .instrument(span),
                );
            }
        }

        Ok(())
    }
}
