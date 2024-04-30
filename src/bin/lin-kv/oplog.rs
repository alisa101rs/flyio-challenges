use std::{cmp::max, collections::VecDeque, sync::Arc};

use eyre::bail;
use flyio_rs::{
    error::ErrorCode,
    network::Network,
    request::{Extension, Payload},
    storage::{log, log::Storage as _},
    NodeId, Rpc,
};
use futures_util::StreamExt;
use serde::{de::IgnoredAny, Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tracing::{instrument, Span};

use crate::{node::State, storage::Storage, CasRequest, WriteRequest};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum Operation {
    Write { key: u64, value: u64 },
    Cas { key: u64, from: u64, to: u64 },
}

#[derive(Debug, Clone)]
pub enum ReplicationEvent {
    Commit { offset: u64 },
    Append { offset: u64, op: Operation },
}

pub type Sync = oneshot::Sender<Result<(), ErrorCode>>;
pub type OperationQueue = mpsc::Sender<(Operation, Sync)>;
pub type ReplicationQueue = mpsc::Sender<ReplicationEvent>;

pub struct OperationHandler {
    state: State,
    log: log::Memory<(u64, Operation)>,
    queue: mpsc::Receiver<(Operation, Sync)>,
    replication: mpsc::Receiver<ReplicationEvent>,
    rpc: Rpc,
    network: Arc<Network>,
}

impl OperationHandler {
    pub fn create(
        state: State,
        rpc: Rpc,
        network: Arc<Network>,
    ) -> (Self, OperationQueue, ReplicationQueue) {
        let (txo, queue) = mpsc::channel(1);
        let (txr, replication) = mpsc::channel(1);
        let handler = Self {
            state,
            queue,
            rpc,
            network,
            replication,
            log: Default::default(),
        };

        (handler, txo, txr)
    }

    #[instrument(name = "oplog_job", skip(self, storage))]
    pub async fn start(mut self, storage: Storage) {
        let OperationHandler {
            mut state,
            mut log,
            mut queue,
            rpc,
            network,
            mut replication,
        } = self;

        let mut current_state = state.borrow_and_update().clone();

        let mut local_queue: VecDeque<(_, oneshot::Sender<Result<(), ErrorCode>>)> =
            VecDeque::new();

        tracing::info!(?current_state, "Starting oplog job");

        loop {
            tokio::select! {
                _ = state.changed() => {
                    current_state = state.borrow().clone();
                    tracing::info!(new_state = ?current_state, local_queue_size = local_queue.len(), "Changing current state");

                    if local_queue.is_empty() { continue }

                    if current_state.is_leader() {
                        while let Some((op, sync)) = local_queue.pop_front() {
                            let result = Self::append_entry(op, &rpc, &network, &mut log, &storage).await;

                            let _ = sync.send(result);
                        }
                    } else {
                        let Some(leader) = current_state.leader() else { continue };

                        for (op, sync) in local_queue.drain(..) {
                            tokio::spawn(Self::proxy(leader.clone(), op, rpc.clone(), sync));
                        }
                    }
                }
                Some((op, sync)) = queue.recv() => {
                    tracing::info!(?op,?current_state, "Received operation");
                    if current_state.is_leader() {
                        let result = Self::append_entry(op, &rpc, &network, &mut log, &storage).await;

                        let _ = sync.send(result);
                    } else {
                        let leader = current_state.leader();
                        if let Some(leader) = current_state.leader() {
                            tokio::spawn(Self::proxy(leader, op, rpc.clone(), sync));
                        } else {
                            local_queue.push_back((op, sync));
                        }
                    }
                }
                Some(replication) = replication.recv() => {
                    if current_state.is_leader() {
                        unimplemented!()
                    }
                    Self::replicate(&mut log, replication, &storage);
                }
            }
        }
    }

    #[instrument(skip(rpc, network, log, storage), fields(majority))]
    async fn append_entry(
        op: Operation,
        rpc: &Rpc,
        network: &Arc<Network>,
        log: &mut log::Memory<(u64, Operation)>,
        storage: &Storage,
    ) -> Result<(), ErrorCode> {
        let majority = (network.peer_count() + 1) / 2 + 1;
        let mut accepted = 1;

        Span::current().record("majority", majority);

        let offset = log.append((0, op.clone()));

        let mut requests = rpc.broadcast::<_, IgnoredAny>(
            "replicate",
            network,
            ReplicateRequest {
                op: op.clone(),
                term: 0,
                offset,
            },
        );

        while let Some(request) = requests.next().await {
            if request.is_err() {
                continue;
            }

            accepted += 1;
        }

        let reached_majority = accepted >= majority;

        if reached_majority {
            tracing::info!(%accepted, "Have reached majority");
            let result = Self::execute(&op, storage);
            log.commit(offset);

            {
                let rpc = rpc.clone();
                let network = Arc::clone(network);
                tokio::spawn(async move {
                    let mut requests = rpc.broadcast::<_, IgnoredAny>(
                        "commit",
                        &network,
                        CommitRequest { term: 0, offset },
                    );
                    let _ = requests.count().await;
                });
            }

            result
        } else {
            tracing::error!(%accepted, "Could not reach majority");
            return Err(ErrorCode::TemporaryUnavailable);
        }
    }

    #[instrument(skip(rpc, notify))]
    async fn proxy(
        leader: NodeId,
        op: Operation,
        rpc: Rpc,
        notify: oneshot::Sender<Result<(), ErrorCode>>,
    ) {
        let res = match op {
            Operation::Write { key, value } => {
                rpc.send::<_, IgnoredAny>("write", leader, WriteRequest { key, value })
                    .await
            }
            Operation::Cas { key, from, to } => {
                rpc.send::<_, IgnoredAny>("cas", leader, CasRequest { key, from, to })
                    .await
            }
        };

        match res.map_err(|er| er.code) {
            Ok(_) => {
                let _ = notify.send(Ok(()));
            }
            Err(ErrorCode::KeyDoesNotExist) => {
                let _ = notify.send(Err(ErrorCode::KeyDoesNotExist));
            }
            Err(ErrorCode::PreconditionFailed) => {
                let _ = notify.send(Err(ErrorCode::PreconditionFailed));
            }
            Err(_) => unimplemented!(),
        }
    }

    #[instrument(skip(log, storage))]
    fn replicate(
        log: &mut log::Memory<(u64, Operation)>,
        request: ReplicationEvent,
        storage: &Storage,
    ) {
        let last_commit = log.last_committed();
        match request {
            ReplicationEvent::Commit { offset, .. } => {
                log.commit(offset);

                for (_, (_, op)) in log.scan(last_commit.unwrap_or(0)..offset) {
                    let _ = Self::execute(&op, storage);
                }
            }
            ReplicationEvent::Append { offset, op } => {
                if log.len() > offset {
                    log.truncate(offset.saturating_sub(1));
                }
                let _ = log.append((0, op));
            }
        }
    }

    #[instrument(skip(storage), ret, err)]
    fn execute(operation: &Operation, storage: &Storage) -> Result<(), ErrorCode> {
        match *operation {
            Operation::Write { key, value, .. } => {
                storage.write(key, value);
            }
            Operation::Cas { key, from, to } => {
                storage.cas(key, from, to)?;
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicateRequest {
    op: Operation,
    term: u64,
    offset: u64,
}

#[instrument(skip(state))]
pub async fn replicate(
    from: NodeId,
    Extension(state): Extension<State>,
    Extension(queue): Extension<ReplicationQueue>,
    Payload(replicate): Payload<ReplicateRequest>,
) {
    let current_leader = state.borrow().leader();
    if current_leader != Some(from) {
        tracing::info!("Got replicate from non-leader");
        return;
    }

    let _ = queue
        .send(ReplicationEvent::Append {
            offset: replicate.offset,
            op: replicate.op,
        })
        .await;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitRequest {
    offset: u64,
    term: u64,
}

#[instrument(skip(state, queue))]
pub async fn commit(
    from: NodeId,
    Extension(state): Extension<State>,
    Extension(queue): Extension<ReplicationQueue>,
    Payload(commit): Payload<CommitRequest>,
) {
    let current_leader = state.borrow().leader();
    if current_leader != Some(from) {
        tracing::info!("Got replicate from non-leader");
        return;
    }

    let _ = queue
        .send(ReplicationEvent::Commit {
            offset: commit.offset,
        })
        .await;
}
