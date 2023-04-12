#![feature(async_fn_in_trait)]

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use eyre::eyre;
use flyio_rs::{
    azync::{event_loop, Event, Node, Rpc},
    setup_with_telemetry, Message, Request,
};
use futures::{stream::FuturesUnordered, StreamExt};
use parking_lot::Mutex;
use rand::Rng;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use tokio::sync::mpsc;
use tracing::{info_span, instrument, Instrument};

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

#[tokio::main]
async fn main() -> eyre::Result<()> {
    event_loop::<BroadcastNode, _, _>().await?;

    Ok(())
}

#[derive(Debug, Default)]
struct BroadcastNode {
    node_id: SmolStr,
    seen: Mutex<HashSet<u64>>,
    known: HashMap<SmolStr, Arc<Mutex<Option<HashSet<u64>>>>>,
}

impl BroadcastNode {
    fn topology(&self, neighbors: Vec<SmolStr>) {
        for neighbor in neighbors {
            let Some(known) = self.known.get(&neighbor) else {
                unreachable!("unknown node")
            };
            *known.lock() = Some(Default::default());
        }
    }

    #[instrument(skip(self, rpc))]
    fn gossip(&self, rpc: &Rpc<ResponsePayload>) {
        let mut futures = FuturesUnordered::new();

        for (dst, maybe_known_to) in self.known.iter() {
            let message = {
                let (known_to, mut notify): (HashSet<_>, HashSet<_>) = {
                    let seen = self.seen.lock();
                    let Some(known_to) = &*maybe_known_to.lock() else {
                            continue
                        };

                    seen.iter()
                        .copied()
                        .partition(|seen| known_to.contains(seen))
                };

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

                let message_id = rand::random();
                Message {
                    id: message_id,
                    src: self.node_id.clone(),
                    dst: dst.clone(),
                    body: Request {
                        message_id,
                        traceparent: None,
                        payload: RequestPayload::Gossip { notify },
                    },
                }
            };

            let known_to = maybe_known_to.clone();
            let rpc = rpc.clone();
            let span = info_span!("Gossiping");
            futures.push(async move {
                if let Ok(response) = rpc.send(message).instrument(span).await {
                    match response.body.payload {
                        ResponsePayload::GossipOk { acknowledged } => {
                            let mut known_to = known_to.lock();
                            if let Some(known_to) = &mut *known_to {
                                known_to.extend(acknowledged.into_iter());
                            }
                        }
                        _ => unreachable!(),
                    }
                }
            });
        }

        tokio::task::spawn(async move { while (futures.next().await).is_some() {} });
    }
}

impl Node for BroadcastNode {
    type Injected = Injected;
    type Request = RequestPayload;
    type Response = ResponsePayload;

    fn from_init(
        node_id: SmolStr,
        node_ids: Vec<SmolStr>,
        tx: mpsc::Sender<Event<Self::Request, Self::Injected>>,
    ) -> eyre::Result<Self> {
        setup_with_telemetry(format!("broadcast-{node_id}"))?;
        tokio::task::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(300));
            loop {
                interval.tick().await;
                if let Err(_) = tx.send(Event::Injected(Injected::Gossip)).await {
                    return;
                }
            }
        });
        let known = node_ids
            .into_iter()
            .map(|node| (node, Arc::new(Mutex::new(None))))
            .collect();

        Ok(Self {
            node_id,
            seen: Mutex::new(Default::default()),
            known,
        })
    }

    #[instrument(skip(self, rpc), err)]
    async fn process_event(
        &self,
        event: Event<Self::Request, Self::Injected>,
        rpc: &Rpc<Self::Response>,
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
                                .ok_or(eyre!("topology missing self"))
                                .unwrap(),
                        );
                        ResponsePayload::TopologyOk
                    }
                    RequestPayload::Broadcast { message } => {
                        self.seen.lock().insert(message);
                        ResponsePayload::BroadcastOk
                    }
                    RequestPayload::Read => ResponsePayload::ReadOk {
                        messages: self.seen.lock().clone(),
                    },
                    RequestPayload::Gossip { notify } => {
                        self.seen.lock().extend(notify.iter());
                        if let Some(known) = self.known.get(&from) {
                            if let Some(known) = &mut *known.lock() {
                                known.extend(notify.iter());
                                ResponsePayload::GossipOk {
                                    acknowledged: notify,
                                }
                            } else {
                                panic!("gossip_ok from non-neighbor")
                            }
                        } else {
                            panic!("gossip_ok from non-neighbor")
                        }
                    }
                });

                rpc.respond(response);
                Ok(())
            }
            Event::EOF => Ok(()),
        }
    }
}
