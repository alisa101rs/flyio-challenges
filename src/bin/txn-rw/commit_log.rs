use std::{future::ready, sync::Arc, time::Duration};

use flyio_rs::{
    network::Network,
    request::{Extension, Payload},
    response::IntoResponse,
    storage::{
        log,
        log::Storage as _,
        vector::{Storage as _, Transaction},
    },
    Rpc,
};
use futures::Stream;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::{select, sync::mpsc};
use tracing::instrument;

use crate::{clock::Clock, Operation};

pub type ReplicationItem = (Clock, Vec<Operation>);
pub type OperationReplication = mpsc::UnboundedSender<ReplicationItem>;

pub fn setup_replication(
    storage: log::Memory<ReplicationItem>,
    rpc: Rpc,
    network: Arc<Network>,
) -> OperationReplication {
    let (tx, rx) = mpsc::unbounded_channel();

    tokio::spawn(replication_loop(
        tokio_stream::wrappers::UnboundedReceiverStream::new(rx),
        storage,
        rpc,
        network,
    ));

    tx
}

async fn replication_loop(
    mut items: impl Stream<Item = ReplicationItem> + Unpin,
    mut storage: log::Memory<ReplicationItem>,
    rpc: Rpc,
    network: Arc<Network>,
) {
    let mut ticker = tokio::time::interval(Duration::from_millis(500));
    loop {
        select! {
            Some(item) = items.next() => {
                storage.append(item);
            }
            _ = ticker.tick() => {
                replicate_writes(&mut storage, &rpc, &network).await;
            }
        }
    }
}

#[instrument(skip(storage, rpc, network))]
async fn replicate_writes(
    storage: &mut log::Memory<ReplicationItem>,
    rpc: &Rpc,
    network: &Network,
) {
    let commit = storage.last_committed().unwrap_or(0);
    let ops = storage.scan(commit..).map(|(_, it)| it).collect::<Vec<_>>();

    let broadcast =
        rpc.broadcast::<_, ReplicationResponse>("replicate", &network, ReplicationRequest { ops });

    let c = broadcast.filter(|it| ready(it.is_ok())).count().await;

    if network.peer_count() == c {
        storage.commit(commit);
        tracing::info!(%commit, "Successfully replicated writes")
    } else {
        tracing::warn!(%commit, "Failed to replicate writes")
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReplicationRequest {
    ops: Vec<ReplicationItem>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReplicationResponse {
    items: usize,
}

#[instrument(name = "replicate", skip(storage), ret)]
pub async fn handler(
    Extension(storage): Extension<super::Storage>,
    Payload(replication): Payload<ReplicationRequest>,
) -> impl IntoResponse {
    let items = replication.ops.len();
    for (at, ops) in replication.ops {
        let mut ops = ops.into_iter();
        let mut transact = storage.begin(at);
        while let Some(Operation::Write { key, value }) = ops.next() {
            transact.write(key, value);
        }
        transact.commit();
    }

    Payload(ReplicationResponse { items })
}
