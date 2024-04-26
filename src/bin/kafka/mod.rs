use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
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
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use tracing::instrument;

use crate::{
    bucket::LogBucket, commit_table::CommitTable, leaders::TopicLeaders, write_log::WriteLog,
};

pub type Key = SmolStr;
pub type Offset = u64;
pub type Log = (Offset, u64);

mod bucket;
mod commit_table;
mod leaders;
mod write_log;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let (network, rpc, messages) = setup_network().await?;
    setup_with_telemetry(format!("kafka-{}", network.id))?;
    let kafka = KafkaLogNode::create(network.clone());

    let router = Router::new()
        .route("send", send)
        .route("poll", poll)
        .route("commit_offsets", commit)
        .route("list_committed_offsets", list_committed)
        .route("gossip_commits", gossip_commits)
        .route("propagate_writes", propagate_writes)
        .route("acknowledge_writes", acknowledge_writes)
        .layer(Extension(kafka.clone()))
        .layer(Extension(network))
        .layer(Extension(rpc.clone()));

    tokio::spawn(start_gossiping_commits(kafka.clone(), rpc.clone()));
    tokio::spawn(start_write_propagation(kafka, rpc.clone()));

    serve(router, rpc, messages).await?;

    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SendRequest {
    key: SmolStr,
    #[serde(rename = "msg")]
    message: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SendResponse {
    offset: u64,
}

#[instrument(skip(kafka, rpc), ret)]
async fn send(
    Extension(kafka): Extension<KafkaLogNode>,
    Extension(rpc): Extension<Rpc>,
    Payload(message): Payload<SendRequest>,
) -> impl IntoResponse {
    let offset = kafka.send(message.key, message.message, rpc).await;
    Payload(SendResponse { offset })
}

#[derive(Debug, Clone, Deserialize)]
struct PollRequest {
    offsets: HashMap<SmolStr, Offset>,
}

#[derive(Debug, Clone, Serialize)]
struct PollResponse {
    #[serde(rename = "msgs")]
    messages: HashMap<SmolStr, Vec<Log>>,
}

#[instrument(skip(kafka), ret)]
async fn poll(
    Extension(kafka): Extension<KafkaLogNode>,
    Payload(offsets): Payload<PollRequest>,
) -> impl IntoResponse {
    Payload(PollResponse {
        messages: kafka.poll(&offsets.offsets),
    })
}

#[derive(Debug, Clone, Deserialize)]
struct CommitOffsets {
    offsets: HashMap<SmolStr, Offset>,
}

#[instrument(skip(kafka))]
async fn commit(
    Extension(kafka): Extension<KafkaLogNode>,
    Payload(offsets): Payload<CommitOffsets>,
) {
    kafka.commit_offsets(offsets.offsets);
}

#[derive(Debug, Clone, Deserialize)]
struct ListCommittedOffsets {
    keys: Vec<SmolStr>,
}

#[derive(Debug, Clone, Serialize)]
struct CommittedOffsets {
    offsets: HashMap<SmolStr, Offset>,
}

#[instrument(skip(kafka), ret)]
async fn list_committed(
    Extension(kafka): Extension<KafkaLogNode>,
    Payload(keys): Payload<ListCommittedOffsets>,
) -> impl IntoResponse {
    Payload(CommittedOffsets {
        offsets: kafka.list_committed_offsets(keys.keys.as_slice()),
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GossipCommits {
    commits: HashMap<SmolStr, Offset>,
}

#[instrument(skip(kafka))]
async fn gossip_commits(
    from: NodeId,
    Extension(kafka): Extension<KafkaLogNode>,
    Payload(gossips): Payload<GossipCommits>,
) {
    kafka.commits.accept_gossip_commits(&from, gossips.commits);
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PropagateWrites {
    messages: HashMap<Key, BTreeSet<(Offset, u64)>>,
}

#[instrument(skip(kafka))]
async fn propagate_writes(
    Extension(kafka): Extension<KafkaLogNode>,
    Payload(writes): Payload<PropagateWrites>,
) {
    kafka.propagate_writes(writes.messages);
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AcknowledgeWrites {
    offsets: HashMap<Key, Offset>,
}

#[instrument(skip(kafka))]
async fn acknowledge_writes(
    Extension(kafka): Extension<KafkaLogNode>,
    Payload(writes): Payload<AcknowledgeWrites>,
) {
    kafka.acknowledge_writes(writes.offsets);
}

async fn start_gossiping_commits(kafka: KafkaLogNode, rpc: Rpc) {
    let mut ticker = tokio::time::interval(Duration::from_millis(500));
    loop {
        ticker.tick().await;
        kafka.commits.gossip_commits(&rpc).await;
    }
}

async fn start_write_propagation(kafka: KafkaLogNode, rpc: Rpc) {
    let mut ticker = tokio::time::interval(Duration::from_millis(1000));
    loop {
        ticker.tick().await;
        kafka.init_propagate_writes(&rpc).await;
    }
}

/// KafkaLogNode
///
/// - commits are replicated on each node
/// - logs buckets have a leader and replicas on all other nodes
/// - `poll` can read from any replica
/// - `send` will be redirected to the leader
#[derive(Debug, Clone)]
struct KafkaLogNode {
    id: NodeId,
    topic_leaders: Arc<TopicLeaders>,
    buckets: Arc<Mutex<HashMap<SmolStr, LogBucket>>>,
    commits: Arc<CommitTable>,
    write_log: Arc<WriteLog>,
}

impl KafkaLogNode {
    fn create(network: Arc<Network>) -> Self {
        let topic_leaders = TopicLeaders::simple(network.clone());
        let commits = CommitTable::new(&network.other_nodes());
        let write_log = WriteLog::new(network.clone());

        Self {
            id: network.id.clone(),
            topic_leaders: Arc::new(topic_leaders),
            buckets: Arc::new(Mutex::new(Default::default())),
            commits: Arc::new(commits),
            write_log: Arc::new(write_log),
        }
    }

    fn propagate_writes(&self, messages: HashMap<Key, BTreeSet<(Offset, u64)>>) {
        for (message, writes) in messages {
            self.buckets
                .lock()
                .entry(message)
                .or_insert_with(LogBucket::new)
                .insert(writes.into_iter());
        }
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

    async fn send(&self, key: Key, message: u64, rpc: Rpc) -> Offset {
        let node = self.topic_leaders.get_or_insert(&key).await;
        if node == self.id {
            let offset = self.create_write(key, message).await;

            offset
        } else {
            let request = SendRequest { key, message };

            match rpc.send::<_, SendResponse>("send", node, request).await {
                Ok(payload) => payload.offset,
                _ => {
                    panic!("can't handle")
                }
            }
        }
    }

    fn poll(&self, offsets: &HashMap<SmolStr, Offset>) -> HashMap<Key, Vec<Log>> {
        let mut messages = HashMap::new();

        let buckets = self.buckets.lock();
        for (key, &offset) in offsets {
            if let Some(bucket) = buckets.get(key) {
                messages.insert(key.clone(), bucket.poll(offset));
            }
        }

        messages
    }

    fn commit_offsets(&self, offsets: HashMap<SmolStr, Offset>) {
        self.commits.commit(offsets.into_iter());
    }

    fn list_committed_offsets(&self, keys: &[SmolStr]) -> HashMap<Key, Offset> {
        self.commits.list_commits(keys)
    }

    #[instrument(skip(self))]
    async fn init_propagate_writes(&self, rpc: &Rpc) {
        let messages = self.write_log.propagate_writes(rpc.clone()).await;
        let mut buckets = self.buckets.lock();
        for (key, logs) in messages {
            buckets.get_mut(&key).expect("not possible").insert(logs);
        }
    }

    fn acknowledge_writes(&self, offsets: HashMap<Key, Offset>) {
        let buckets = self.buckets.lock();
        for (key, offset) in offsets {
            buckets.get(&key).expect("not possible").set_offset(offset);
        }
    }
}
