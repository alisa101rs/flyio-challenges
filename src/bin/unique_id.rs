#![feature(async_fn_in_trait)]

use std::sync::Arc;

use flyio_rs::{event::Event, event_loop, network::Network, trace::setup_tracing, Node, Rpc};
use rand::random;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RequestPayload {
    Generate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResponsePayload {
    GenerateOk { id: u64 },
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    setup_tracing()?;

    event_loop::<UniqueIdNode, RequestPayload>(None).await?;

    Ok(())
}

#[derive(Debug, Clone)]
struct UniqueIdNode {
    rpc: Rpc,
}

impl UniqueIdNode {
    pub fn new(rpc: Rpc) -> Self {
        Self { rpc }
    }

    pub fn next_id(&mut self) -> u64 {
        random()
    }
}

impl Node for UniqueIdNode {
    type Injected = ();
    type Request = RequestPayload;

    fn from_init(
        _network: Arc<Network>,
        _tx: mpsc::Sender<Event<Self::Request, Self::Injected>>,
        rpc: Rpc,
    ) -> eyre::Result<Self> {
        Ok(Self::new(rpc))
    }

    async fn process_event(
        &mut self,
        event: Event<Self::Request, Self::Injected>,
    ) -> eyre::Result<()> {
        match event {
            Event::Request {
                src, message_id, ..
            } => {
                let id = self.next_id();
                self.rpc
                    .respond(src, message_id, ResponsePayload::GenerateOk { id });
                Ok(())
            }
            _ => Ok(()),
        }
    }
}
