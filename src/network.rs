#![allow(dead_code, unused_imports, unused_variables)]

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use tokio::sync::mpsc::{channel, Sender};
use tracing::Instrument;

use crate::Rpc;

pub type NodeId = SmolStr;

#[derive(Debug)]
pub struct Network {
    pub id: NodeId,
    nodes: RwLock<Vec<Node>>,
}

impl Network {
    pub fn create(id: NodeId, all_nodes: Vec<NodeId>) -> Arc<Self> {
        Arc::new(Self {
            nodes: RwLock::new(all_nodes.into_iter().map(Node::new).collect()),
            id,
        })
    }

    pub fn other_nodes(&self) -> Vec<NodeId> {
        self.nodes
            .read()
            .iter()
            .filter(|&it| it.id != self.id && it.is_alive())
            .map(|it| it.id.clone())
            .collect()
    }

    pub fn all_nodes(&self) -> Vec<NodeId> {
        self.nodes.read().iter().map(|it| it.id.clone()).collect()
    }
}

#[derive(Debug, Clone)]
pub struct Node {
    id: NodeId,

    // TODO: implement later
    #[allow(dead_code)]
    health: HealthStatus,
    last_pong: Option<Instant>,
}

impl Node {
    pub fn new(id: NodeId) -> Self {
        Self {
            id,
            health: HealthStatus::Alive,
            last_pong: None,
        }
    }

    fn is_alive(&self) -> bool {
        matches!(self.health, HealthStatus::Alive)
    }

    fn set_alive(&mut self, last_pong: Instant) {
        self.health = HealthStatus::Alive;
        match self.last_pong {
            Some(v) if v > last_pong => {}
            _ => {
                self.last_pong = Some(last_pong);
            }
        }
    }

    fn no_pong_received(&mut self, timeout_received_at: Instant) {
        match (self.health, self.last_pong) {
            (HealthStatus::Disconnected, _) => (),
            (_, Some(last_pong)) if timeout_received_at - last_pong > Duration::from_secs(5) => {
                self.health = HealthStatus::Disconnected
            }
            _ => self.health = HealthStatus::Unavailable,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum HealthStatus {
    Alive,
    Unavailable,
    Disconnected,
}

pub async fn heartbeat_loop(period: Duration, network: Arc<Network>, rpc: Rpc) -> eyre::Result<()> {
    Ok(())
}
//
// #[derive(Debug, Clone, Serialize, Deserialize)]
// #[serde(tag = "type", rename_all = "snake_case")]
// enum HeartbeatRequest {
//     Ping,
// }
//
// #[derive(Debug, Clone, Serialize, Deserialize)]
// #[serde(tag = "type", rename_all = "snake_case")]
// enum HeartbeatResponse {
//     Pong,
// }
//
// pub async fn heartbeat_loop(period: Duration, network: Arc<Network>, rpc: Rpc) -> eyre::Result<()> {
//     let (tx, mut rv) = channel(256);
//
//     let with_node = |_me: NodeId,
//                      node: NodeId,
//                      rpc: Rpc,
//                      status: Sender<(NodeId, Option<Instant>)>| async move {
//         let mut interval = tokio::time::interval(period);
//         loop {
//             interval.tick().await;
//
//             let span = tracing::info_span!("Ping", %node);
//             async {
//                 match rpc
//                     .send::<_, HeartbeatResponse>(node.clone(), HeartbeatRequest::Ping)
//                     .await
//                 {
//                     Ok(_) => {
//                         let last_pong = Instant::now();
//                         let _ = status.send((node.clone(), Some(last_pong))).await;
//                     }
//                     Err(_) => {
//                         // request timed-out
//                         let _ = status.send((node.clone(), None)).await;
//                     }
//                 }
//             }
//             .instrument(span)
//             .await;
//         }
//     };
//
//     let mut set = tokio::task::JoinSet::new();
//     for node in network.other_nodes() {
//         set.spawn(with_node(
//             network.id.clone(),
//             node.clone(),
//             rpc.clone(),
//             tx.clone(),
//         ));
//     }
//
//     loop {
//         let (node, received_at) = tokio::select! {
//             Some(r) = rv.recv() => {
//                 r
//             },
//             None = set.join_next() => {
//                 break;
//             }
//         };
//
//         let mut nodes = network.nodes.write();
//         let Some(pos) = nodes.iter().position(|n| n.id == node) else {
//             tracing::error!(?node, "Received health check from non-existent node");
//             continue;
//         };
//         if let Some(pong_received_at) = received_at {
//             nodes[pos].set_alive(pong_received_at);
//         } else {
//             nodes[pos].no_pong_received(Instant::now());
//         }
//     }
//
//     Ok(())
// }
