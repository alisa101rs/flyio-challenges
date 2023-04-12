use std::sync::mpsc::SyncSender;

use flyio_rs::{
    setup_tracing,
    sync::{event_loop, Event, Node, Rpc},
};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

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

fn main() -> eyre::Result<()> {
    setup_tracing()?;

    event_loop::<UniqueIdNode, RequestPayload, ResponsePayload>()?;

    Ok(())
}

#[derive(Debug)]
struct UniqueIdNode {
    rng: rand::prelude::ThreadRng,
}

impl UniqueIdNode {
    pub fn new() -> Self {
        Self {
            rng: rand::thread_rng(),
        }
    }

    pub fn next_id(&mut self) -> u64 {
        self.rng.next_u64()
    }
}

impl Node for UniqueIdNode {
    type Injected = ();
    type Request = RequestPayload;
    type Response = ResponsePayload;

    fn from_init(
        _node_id: SmolStr,
        _node_ids: Vec<SmolStr>,
        _tx: SyncSender<Event<Self::Request, Self::Response, Self::Injected>>,
    ) -> eyre::Result<Self> {
        Ok(Self::new())
    }

    fn event(
        &mut self,
        event: Event<Self::Request, Self::Response, Self::Injected>,
        rpc: &mut Rpc,
    ) -> eyre::Result<()> {
        match event {
            Event::Request(request) => {
                let response =
                    request.into_reply(|_| ResponsePayload::GenerateOk { id: self.next_id() });

                rpc.respond(response);
                Ok(())
            }
            Event::Response(_) => {
                unreachable!("No request sended")
            }
            Event::Injected(_) => {
                unreachable!("No injected events")
            }
        }
    }
}
