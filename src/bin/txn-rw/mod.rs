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
use tokio::sync::mpsc::Sender;

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
    TxnOk { txn: Vec<Operation> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Operation(OpType, u64, Option<u64>);

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

    async fn process_event(
        &mut self,
        event: Event<Self::Request, Self::Injected>,
        rpc: Rpc<Self::Response>,
    ) -> eyre::Result<()> {
        match event {
            Event::Request(message) => {
                let payload = match message.body.payload {
                    RequestPayload::Txn { txn } => ResponsePayload::TxnOk { txn },
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
