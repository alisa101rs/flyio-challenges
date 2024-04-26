use flyio_rs::{
    request::Extension,
    response::{IntoResponse, Response},
    routing::Router,
    serve, setup_network,
    storage::{log, vector::DashVector},
    trace::setup_with_telemetry,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::clock::{Clock, NodeClock};

mod clock;
mod commit_log;
mod transaction;
// mod write_log;

pub type Storage = DashVector<u64, u64>;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let (network, rpc, messages) = setup_network().await?;
    setup_with_telemetry(format!("txn-{}", network.id))?;

    let clock = NodeClock::new(network.id.clone());
    let storage = DashVector::<u64, u64>::new();
    let logs = log::Memory::<(Clock, Vec<Operation>)>::default();

    let replication = commit_log::setup_replication(logs, rpc.clone(), network.clone());

    let router = Router::new()
        .route("txn", transaction::handler)
        .route("tick", clock::handler)
        .route("ping", ping)
        .route("replicate", commit_log::handler)
        .layer(Extension(clock.clone()))
        .layer(Extension(network.clone()))
        .layer(Extension(storage.clone()))
        .layer(Extension(replication))
        .layer(Extension(rpc.clone()));

    tokio::spawn(clock::start_ticking(clock.clone(), rpc.clone(), network));

    serve(router, rpc, messages).await?;

    Ok(())
}

async fn ping() {}

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
#[serde(from = "RawOp", into = "RawOp")]
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

impl Into<RawOp> for Operation {
    fn into(self) -> RawOp {
        match self {
            Operation::Read { .. } => unimplemented!(),
            Operation::Write { key, value } => RawOp(OpType::Write, key, Some(value)),
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
