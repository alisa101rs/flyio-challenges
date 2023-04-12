#![feature(async_fn_in_trait)]
#![feature(hash_drain_filter)]
#![feature(integer_atomics)]

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use flyio_rs::{
    azync::{event_loop, Event, Node, Rpc},
    setup_with_telemetry,
};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use tokio::sync::mpsc;
use tracing::instrument;

type Offset = u64;
type Log = (Offset, u64);

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
}

#[derive(Debug, Clone)]
pub enum Injected {}

#[derive(Debug)]
struct LogBucket {
    last_commit: AtomicU64,
    offset: AtomicU64,
    logs: Mutex<Vec<Log>>,
}

impl LogBucket {
    fn new() -> Self {
        Self {
            last_commit: AtomicU64::new(0),
            offset: AtomicU64::new(1),
            logs: Mutex::new(Vec::new()),
        }
    }

    fn next_offset(&self) -> Offset {
        self.offset.fetch_add(1, Ordering::Relaxed)
    }

    fn insert(&self, message: u64) -> Offset {
        let offset = self.next_offset();
        self.logs.lock().push((offset, message));
        offset
    }

    fn commit(&self, offset: Offset) {
        self.last_commit.fetch_max(offset, Ordering::Relaxed);
    }

    fn get_commit(&self) -> u64 {
        self.last_commit.load(Ordering::Relaxed)
    }

    fn poll(&self, offset: Offset) -> Vec<Log> {
        self.logs
            .lock()
            .iter()
            .skip_while(|&&(it, _)| it < offset)
            .cloned()
            .collect()
    }
}

#[derive(Debug, Default)]
struct KafkaLogNode {
    node_id: SmolStr,
    buckets: Arc<Mutex<HashMap<SmolStr, LogBucket>>>,
}

impl KafkaLogNode {
    #[instrument(skip(self), ret)]
    fn send(&self, key: SmolStr, message: u64) -> ResponsePayload {
        let offset = self
            .buckets
            .lock()
            .entry(key)
            .or_insert_with(LogBucket::new)
            .insert(message);

        ResponsePayload::SendOk { offset }
    }

    #[instrument(skip(self), ret)]
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

    #[instrument(skip(self), ret)]
    fn commit_offsets(&self, offsets: &HashMap<SmolStr, Offset>) -> ResponsePayload {
        let buckets = self.buckets.lock();
        for (key, &offset) in offsets {
            if let Some(bucket) = buckets.get(key) {
                bucket.commit(offset);
            }
        }
        ResponsePayload::CommitOffsetsOk
    }

    #[instrument(skip(self), ret)]
    fn list_committed_offsets(&self, keys: &[SmolStr]) -> ResponsePayload {
        let mut offsets = HashMap::new();
        let buckets = self.buckets.lock();
        for key in keys {
            if let Some(bucket) = buckets.get(key) {
                offsets.insert(key.clone(), bucket.get_commit());
            }
        }
        ResponsePayload::ListCommittedOffsetsOk { offsets }
    }
}

impl Node for KafkaLogNode {
    type Injected = Injected;
    type Request = RequestPayload;
    type Response = ResponsePayload;

    fn from_init(
        node_id: SmolStr,
        _node_ids: Vec<SmolStr>,
        _tx: mpsc::Sender<Event<Self::Request, Self::Injected>>,
    ) -> eyre::Result<Self> {
        setup_with_telemetry(format!("kafka-{node_id}"))?;

        Ok(Self {
            node_id,
            buckets: Arc::new(Mutex::new(Default::default())),
        })
    }

    #[instrument(skip(self, rpc), err)]
    async fn process_event(
        &self,
        event: Event<Self::Request, Self::Injected>,
        rpc: &Rpc<Self::Response>,
    ) -> eyre::Result<()> {
        match event {
            Event::Injected(_) => Ok(()),
            Event::EOF => Ok(()),
            Event::Request(message) => {
                let payload = match message.body.payload {
                    RequestPayload::Send { ref key, message } => self.send(key.clone(), message),
                    RequestPayload::Poll { ref offsets } => self.poll(offsets),
                    RequestPayload::CommitOffsets { ref offsets } => self.commit_offsets(offsets),
                    RequestPayload::ListCommittedOffsets { ref keys } => {
                        self.list_committed_offsets(keys)
                    }
                };

                let response = message.into_reply(|_| payload);
                rpc.respond(response);

                Ok(())
            }
        }
    }
}
