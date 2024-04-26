use std::{sync::Arc, time::Duration};

use derive_more::Display;
use flyio_rs::{
    network::Network,
    request::{Extension, Payload},
    response::{IntoResponse, Response},
    routing::Router,
    serve, setup_network,
    storage::vector::{DashVector, Storage, Transaction},
    trace::setup_with_telemetry,
    Rpc,
};
use futures::StreamExt;
use rand::random;
use serde::{de::IgnoredAny, Deserialize, Serialize};
use thiserror::Error;
use tracing::instrument;

use crate::clock::{Clock, NodeClock};

mod clock;
// mod commit_log;
//
// mod write_log;

#[derive(
    Copy, Clone, Debug, Display, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct TransactionId(u128);

impl TransactionId {
    pub fn new() -> TransactionId {
        TransactionId(random())
    }
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let (network, rpc, messages) = setup_network().await?;
    setup_with_telemetry(format!("txn-{}", network.id))?;
    let node = TxnNode::create(network.clone());

    let router = Router::new()
        .route("txn", transaction)
        .route("broadcast", broadcast)
        .route("tick", tick)
        .route("ping", ping)
        .layer(Extension(node.clone()))
        .layer(Extension(network.clone()))
        .layer(Extension(rpc.clone()));

    tokio::spawn(start_ticking(node.clone(), rpc.clone(), network.clone()));
    tokio::spawn(start_processing_commits(node, rpc.clone()));

    serve(router, rpc, messages).await?;

    Ok(())
}

#[derive(Clone)]
pub struct TxnNode {
    clock: NodeClock,
    storage: DashVector<u64, u64>,
}

impl TxnNode {
    fn create(network: Arc<Network>) -> Self {
        let storage = DashVector::new();

        Self {
            clock: NodeClock::new(network.id.clone()),
            storage,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TransactionRequest {
    txn: Vec<Operation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TransactionResponse {
    txn: Vec<OperationResult>,
}

async fn transaction(
    Extension(mut node): Extension<TxnNode>,
    Payload(transaction): Payload<TransactionRequest>,
) -> Result<impl IntoResponse, Error> {
    let txn = node
        .transaction(transaction.txn)
        .await
        .map_err(|er| Error {
            code: ErrorCode::TxnConflict,
            text: er.to_string(),
        })?;

    Ok(Payload(TransactionResponse { txn }))
}

async fn broadcast() {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Tick {
    clock: Clock,
}

async fn tick(Extension(node): Extension<TxnNode>, Payload(tick): Payload<Tick>) {
    node.clock.merge(&tick.clock);
}

async fn ping() {}

async fn start_ticking(node: TxnNode, rpc: Rpc, network: Arc<Network>) {
    #[instrument(skip(node, rpc, network))]
    async fn do_tick(node: &TxnNode, rpc: &Rpc, network: &Network) {
        let clock = node.clock.next();

        let mut iter = rpc.broadcast::<_, IgnoredAny>("tick", network, Tick { clock });

        while let Some(_m) = iter.next().await {
            tracing::info!("Received Tock")
        }
    }

    let mut ticker = tokio::time::interval(Duration::from_millis(10));
    loop {
        ticker.tick().await;

        do_tick(&node, &rpc, &network).await;
    }
}

async fn start_processing_commits(node: TxnNode, _rpc: Rpc) {
    let mut ticker = tokio::time::interval(Duration::from_millis(400));
    loop {
        ticker.tick().await;
        node.process_commits().await;
    }
}

#[derive(Debug, Clone, Error, Serialize, Deserialize)]
#[error("Error")]
struct Error {
    code: ErrorCode,
    text: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Copy, Clone)]
#[repr(u8)]
pub enum ErrorCode {
    TxnConflict = 30,
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        Response::Error(serde_json::to_value(self).unwrap())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(from = "RawOp")]
pub enum Operation {
    Read { key: u64 },
    Write { key: u64, value: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(into = "RawOp")]
pub enum OperationResult {
    Read { key: u64, value: Option<u64> },
    Write { key: u64, value: u64 },
}

impl From<RawOp> for Operation {
    fn from(RawOp(op, key, v): RawOp) -> Self {
        match op {
            OpType::Write => Self::Write {
                key,
                value: v.unwrap(),
            },
            OpType::Read => Self::Read { key },
        }
    }
}

impl From<OperationResult> for RawOp {
    fn from(val: OperationResult) -> Self {
        match val {
            OperationResult::Read { key, value } => RawOp(OpType::Read, key, value),
            OperationResult::Write { key, value } => RawOp(OpType::Write, key, Some(value)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawOp(OpType, u64, Option<u64>);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OpType {
    #[serde(rename = "r")]
    Read,
    #[serde(rename = "w")]
    Write,
}

impl TxnNode {
    #[instrument(skip(self,), ret, err)]
    pub async fn transaction(
        &mut self,
        ops: Vec<Operation>,
    ) -> Result<Vec<OperationResult>, eyre::Report> {
        let mut transaction = self.storage.begin(self.clock.next());
        let mut res = vec![];

        for op in ops {
            match op {
                Operation::Read { key } => res.push(OperationResult::Read {
                    key,
                    value: transaction.read(&key),
                }),
                Operation::Write { key, value } => {
                    transaction.write(key, value);
                    res.push(OperationResult::Write { key, value })
                }
            }
        }

        transaction.commit();
        //self.commit_log.transaction(writes)?;

        Ok(res)
    }

    #[instrument(skip(self,))]
    pub async fn process_commits(&self) {
        //
    }
}
