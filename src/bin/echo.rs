#![feature(async_fn_in_trait)]

use std::sync::Arc;

use flyio_rs::{
    event::Event, event_loop, network::Network, trace::setup_tracing, Node, Rpc,
};
use serde::{Deserialize, Serialize};
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

    event_loop::<EchoNode, RequestPayload>(None).await?;

    Ok(())
}

#[derive(Clone, Debug)]
struct EchoNode(Rpc);

impl Node for EchoNode {
    type Injected = ();
    type Request = RequestPayload;

    fn from_init(
        _network: Arc<Network>,
        _tx: Sender<Event<Self::Request, Self::Injected>>,
        rpc: Rpc,
    ) -> eyre::Result<Self> {
        Ok(Self(rpc))
    }

    async fn process_event(
        &mut self,
        event: Event<Self::Request, Self::Injected>,
    ) -> eyre::Result<()> {
        match event {
            Event::Request {
                src,
                message_id,
                payload: RequestPayload::Echo { echo },
            } => {
                self.0
                    .respond(src, message_id, ResponsePayload::EchoOk { echo });
            }
            Event::Injected(_) => {}
            Event::EOF => {}
        }

        Ok(())
    }
}
