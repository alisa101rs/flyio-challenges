#![feature(async_fn_in_trait)]
#![feature(integer_atomics)]

use std::{sync::Arc, time::Duration};

use derive_more::Display;
use flyio_rs::{
    event::Event,
    event_loop,
    network::Network,
    periodic_injection,
    storage::{
        vector,
        vector::{DashVector, Storage, Transaction},
    },
    trace::setup_with_telemetry,
    Node, Rpc,
};
use futures::StreamExt;
use rand::random;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Sender;
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
    event_loop::<TxnNode, RequestPayload>(None).await?;

    Ok(())
}

#[derive(Clone)]
pub struct TxnNode {
    clock: NodeClock,
    network: Arc<Network>,
    storage: vector::DashVector<u64, u64>,
    rpc: Rpc,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RequestPayload {
    Txn { txn: Vec<Operation> },
    //Broadcast { ops: Vec<Transaction> },
    Tick { clock: Clock },
    Ping,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResponsePayload {
    TxnOk { txn: Vec<OperationResult> },
    Error { code: ErrorCode, text: String },
    BroadcastOk,
    Tock,
    Pong,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Copy, Clone)]
#[repr(u8)]
pub enum ErrorCode {
    TxnConflict = 30,
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

#[derive(Debug, Clone)]
pub enum Injected {
    ProcessCommits,
    Tick,
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
    pub async fn process_commits(&mut self) {
        //
    }
}

impl Node for TxnNode {
    type Injected = Injected;
    type Request = RequestPayload;

    fn from_init(
        network: Arc<Network>,
        tx: Sender<Event<Self::Request, Self::Injected>>,
        rpc: Rpc,
    ) -> eyre::Result<Self> {
        setup_with_telemetry(format!("txn-{}", network.id))?;
        periodic_injection(tx.clone(), Duration::from_millis(10), Injected::Tick);
        periodic_injection(tx, Duration::from_millis(400), Injected::ProcessCommits);

        let storage = DashVector::new();

        Ok(Self {
            clock: NodeClock::new(network.id.clone()),
            network,
            storage,
            rpc,
        })
    }

    #[instrument(skip(self), err)]
    async fn process_event(
        &mut self,
        event: Event<Self::Request, Self::Injected>,
    ) -> eyre::Result<()> {
        match event {
            Event::Request {
                src,
                message_id,
                payload,
            } => {
                let payload = match payload {
                    RequestPayload::Txn { txn } => match self.transaction(txn).await {
                        Ok(txn) => ResponsePayload::TxnOk { txn },
                        Err(er) => ResponsePayload::Error {
                            code: ErrorCode::TxnConflict,
                            text: er.to_string(),
                        },
                    },
                    RequestPayload::Ping => ResponsePayload::Pong,
                    //RequestPayload::Broadcast { .. } => ResponsePayload::Pong,
                    RequestPayload::Tick { clock } => {
                        self.clock.merge(&clock);
                        ResponsePayload::Tock
                    }
                };

                self.rpc.respond(src, message_id, payload);
            }
            Event::Injected(Injected::ProcessCommits) => {
                self.process_commits().await;
            }
            Event::Injected(Injected::Tick) => {
                let clock = self.clock.next();

                let mut iter = self
                    .rpc
                    .broadcast::<_, ResponsePayload>(&self.network, RequestPayload::Tick { clock });

                while let Some(_m) = iter.next().await {
                    tracing::info!("Received Tock")
                }
            }
            Event::EOF => {}
        }

        Ok(())
    }
}
