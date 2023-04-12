use std::{
    collections::{HashMap, HashSet},
    sync::mpsc::SyncSender,
    time::Duration,
};

use eyre::eyre;
use flyio_rs::{
    setup_tracing,
    sync::{event_loop, Event, Node, Rpc},
    Message, Request,
};
use rand::{thread_rng, Rng, RngCore};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RequestPayload {
    Broadcast {
        message: u64,
    },
    Read,
    Topology {
        topology: HashMap<SmolStr, Vec<SmolStr>>,
    },
    Gossip {
        notify: HashSet<u64>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResponsePayload {
    BroadcastOk,
    ReadOk { messages: HashSet<u64> },
    TopologyOk,
    GossipOk { acknowledged: HashSet<u64> },
}

#[derive(Debug, Clone)]
pub enum Injected {
    Gossip,
}

fn main() -> eyre::Result<()> {
    setup_tracing()?;

    event_loop::<BroadcastNode, _, _>()?;

    Ok(())
}

#[derive(Debug, Default)]
struct BroadcastNode {
    node_id: SmolStr,
    neighbors: Vec<SmolStr>,
    seen: HashSet<u64>,
    known: HashMap<SmolStr, HashSet<u64>>,
}

impl BroadcastNode {
    fn topology(&mut self, neighbors: Vec<SmolStr>) {
        self.neighbors = neighbors;

        for neighbor in &self.neighbors {
            self.known.insert(neighbor.clone(), Default::default());
        }
    }

    fn gossip(&mut self, rpc: &mut Rpc) {
        for (dst, known_to) in &self.known {
            let (known_to, mut notify): (HashSet<_>, HashSet<_>) = self
                .seen
                .iter()
                .copied()
                .partition(|seen| known_to.contains(seen));

            let mut rng = rand::thread_rng();
            let additional_cap = (10 * notify.len() / 100) as u32;
            notify.extend(known_to.iter().filter(|_| {
                rng.gen_ratio(
                    additional_cap.min(known_to.len() as u32),
                    known_to.len() as u32,
                )
            }));

            if notify.is_empty() {
                continue;
            }

            let message_id = thread_rng().next_u64();

            let message = Message {
                id: message_id,
                src: self.node_id.clone(),
                dst: dst.clone(),
                body: Request {
                    message_id,
                    payload: RequestPayload::Gossip { notify },
                    traceparent: None,
                },
            };
            rpc.send(message).unwrap();
        }
    }
}

impl Node for BroadcastNode {
    type Injected = Injected;
    type Request = RequestPayload;
    type Response = ResponsePayload;

    fn from_init(
        node_id: SmolStr,
        _node_ids: Vec<SmolStr>,
        tx: SyncSender<Event<Self::Request, Self::Response, Self::Injected>>,
    ) -> eyre::Result<Self> {
        std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_millis(300));
            if let Err(_) = tx.send(Event::Injected(Injected::Gossip)) {
                return;
            }
        });

        Ok(Self {
            node_id,
            neighbors: Default::default(),
            seen: Default::default(),
            known: Default::default(),
        })
    }

    fn event(
        &mut self,
        event: Event<Self::Request, Self::Response, Self::Injected>,
        rpc: &mut Rpc,
    ) -> eyre::Result<()> {
        match event {
            Event::Injected(Injected::Gossip) => {
                self.gossip(rpc);
                Ok(())
            }
            Event::Request(message) => {
                let from = message.src.clone();
                let response = message.into_reply(|payload| match payload {
                    RequestPayload::Topology { mut topology } => {
                        self.topology(
                            topology
                                .remove(&self.node_id)
                                .ok_or(eyre!("topoly missing self"))
                                .unwrap(),
                        );
                        ResponsePayload::TopologyOk
                    }
                    RequestPayload::Broadcast { message } => {
                        self.seen.insert(message);
                        ResponsePayload::BroadcastOk
                    }
                    RequestPayload::Read => ResponsePayload::ReadOk {
                        messages: self.seen.clone(),
                    },
                    RequestPayload::Gossip { notify } => {
                        self.seen.extend(notify.iter());
                        self.known.get_mut(&from).unwrap().extend(notify.iter());
                        ResponsePayload::GossipOk {
                            acknowledged: notify,
                        }
                    }
                });

                rpc.respond(response);
                Ok(())
            }
            Event::Response(message) => {
                let from = message.src;

                match message.body.payload {
                    ResponsePayload::GossipOk { acknowledged } => {
                        self.known
                            .get_mut(&from)
                            .unwrap()
                            .extend(acknowledged.into_iter());
                    }
                    _ => unreachable!(),
                }

                Ok(())
            }
        }
    }
}
