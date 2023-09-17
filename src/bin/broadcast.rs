#![feature(async_fn_in_trait)]

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use eyre::eyre;
use flyio_rs::{
    event::Event, event_loop, network::Network, periodic_injection, trace::setup_with_telemetry, Node, Rpc,
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
    event_loop::<BroadcastNode, _>(None).await?;

    Ok(())
}

#[derive(Debug, Clone)]
struct BroadcastNode {
    network: Arc<Network>,
    seen: Arc<Mutex<HashSet<u64>>>,
    known: Arc<HashMap<SmolStr, Arc<Mutex<Option<HashSet<u64>>>>>>,
    rpc: Rpc,
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

    #[instrument(skip(self))]
    fn gossip(&self) {
        let mut futures = FuturesUnordered::new();

        for (dst, maybe_known_to) in self.known.iter() {
            let payload = {
                let (known_to, mut notify): (HashSet<_>, HashSet<_>) = {
                    let seen = self.seen.lock();
                    let Some(known_to) = &*maybe_known_to.lock() else {
                        continue;
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

                RequestPayload::Gossip { notify }
            };

            let known_to = maybe_known_to.clone();
            let rpc = self.rpc.clone();
            let span = info_span!("Gossiping");
            let dst = dst.clone();
            futures.push(async move {
                if let Ok(response) = rpc
                    .send::<_, ResponsePayload>(dst, payload)
                    .instrument(span)
                    .await
                {
                    match response {
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

    fn from_init(
        network: Arc<Network>,
        tx: mpsc::Sender<Event<Self::Request, Self::Injected>>,
        rpc: Rpc,
    ) -> eyre::Result<Self> {
        setup_with_telemetry(format!("broadcast-{}", network.id))?;
        periodic_injection(tx, Duration::from_millis(150), Injected::Gossip);

        let known = network
            .all_nodes()
            .into_iter()
            .map(|node| (node, Arc::new(Mutex::new(None))))
            .collect();

        Ok(Self {
            network,
            seen: Arc::new(Mutex::new(Default::default())),
            known: Arc::new(known),
            rpc,
        })
    }

    #[instrument(skip(self), err)]
    async fn process_event(
        &mut self,
        event: Event<Self::Request, Self::Injected>,
    ) -> eyre::Result<()> {
        match event {
            Event::Injected(Injected::Gossip) => {
                self.gossip();
            }
            Event::Request {
                src,
                message_id,
                payload,
            } => {
                let payload = match payload {
                    RequestPayload::Topology { mut topology } => {
                        self.topology(
                            topology
                                .remove(&self.network.id)
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
                        if let Some(known) = self.known.get(&src) {
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
                };

                self.rpc.respond(src, message_id, payload);
            }
            Event::EOF => {}
        }

        Ok(())
    }
}
