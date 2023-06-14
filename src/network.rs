use std::sync::Arc;

use parking_lot::RwLock;
use smol_str::SmolStr;

pub type NodeId = SmolStr;

#[derive(Debug)]
pub struct Network {
    pub id: SmolStr,
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
            .filter(|&it| it.id != self.id)
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
}

impl Node {
    pub fn new(id: NodeId) -> Self {
        Self {
            id,
            health: HealthStatus::Alive,
        }
    }
}

#[derive(Debug, Clone)]
pub enum HealthStatus {
    Alive,
}