#![feature(async_fn_in_trait)]
#![feature(hash_drain_filter)]
#![feature(integer_atomics)]

use std::sync::Arc;

use flyio_rs::{
    azync::{event_loop, Event, Node, Rpc},
    network::Network,
    setup_with_telemetry, Message, Response,
};
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use tokio::sync::mpsc::Sender;
use tracing::instrument;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    event_loop::<TxnNode, RequestPayload, ResponsePayload>().await?;

    Ok(())
}

#[derive(Clone)]
pub struct TxnNode {
    network: Arc<Network>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RequestPayload {
    Txn { txn: Vec<Operation> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResponsePayload {
    TxnOk { txn: Vec<OperationResult> },
    Error { code: ErrorCode, text: String },
}

#[derive(Serialize_repr, Deserialize_repr, PartialEq, Debug, Copy, Clone)]
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
pub enum Injected {}

impl Node for TxnNode {
    type Injected = Injected;
    type Request = RequestPayload;
    type Response = ResponsePayload;

    fn from_init(
        network: Arc<Network>,
        _tx: Sender<Event<Self::Request, Self::Injected>>,
    ) -> eyre::Result<Self> {
        setup_with_telemetry(format!("txn-{}", network.id))?;
        Ok(Self { network })
    }

    #[instrument(skip(self, rpc), err)]
    async fn process_event(
        &mut self,
        event: Event<Self::Request, Self::Injected>,
        rpc: Rpc,
    ) -> eyre::Result<()> {
        match event {
            Event::Request(message) => {
                let payload = match message.body.payload {
                    RequestPayload::Txn { txn: _ } => ResponsePayload::TxnOk { txn: vec![] },
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
            Event::Injected(_) => unreachable!(),
            Event::EOF => {}
        }

        Ok(())
    }
}
