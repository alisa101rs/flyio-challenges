#![feature(async_fn_in_trait)]

use flyio_rs::{
    azync::{event_loop, Event, Node, Rpc},
    setup_tracing,
};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use tokio::sync::mpsc::Sender;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RequestPayload {
    Echo { echo: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResponsePayload {
    EchoOk { echo: String },
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    setup_tracing()?;

    event_loop::<EchoNode, RequestPayload, ResponsePayload>().await?;

    Ok(())
}

struct EchoNode;

impl Node for EchoNode {
    type Injected = ();
    type Request = RequestPayload;
    type Response = ResponsePayload;

    fn from_init(
        _node_id: SmolStr,
        _node_ids: Vec<SmolStr>,
        _tx: Sender<Event<Self::Request, Self::Injected>>,
    ) -> eyre::Result<Self> {
        Ok(Self)
    }

    async fn process_event(
        &self,
        event: Event<Self::Request, Self::Injected>,
        rpc: &Rpc<Self::Response>,
    ) -> eyre::Result<()> {
        match event {
            Event::Request(message) => {
                let response = message.into_reply(|payload| match payload {
                    RequestPayload::Echo { echo } => ResponsePayload::EchoOk { echo },
                });

                rpc.respond(response);
            }
            Event::Injected(_) => {}
            Event::EOF => {}
        }

        Ok(())
    }
}
