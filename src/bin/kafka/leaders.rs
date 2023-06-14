use std::{
    collections::HashMap,
    hash::{BuildHasherDefault, Hasher},
    sync::Arc,
};

use derivative::Derivative;
use flyio_rs::network::{Network, NodeId};
use fnv::FnvHasher;
use parking_lot::Mutex;

use crate::Key;

#[derive(Debug)]
pub struct TopicLeaders {
    network: Arc<Network>,
    leaders: Mutex<HashMap<Key, NodeId>>,
    elections: LeaderElectionProcess,
}

impl TopicLeaders {
    pub fn simple(network: Arc<Network>) -> Self {
        Self {
            network,
            leaders: Mutex::new(Default::default()),
            elections: LeaderElectionProcess::hasher(),
        }
    }
}

impl TopicLeaders {
    pub async fn get_or_insert(&self, key: &Key) -> NodeId {
        {
            let leaders = self.leaders.lock();
            if let Some(leader) = leaders.get(key) {
                return leader.clone();
            }
        }

        let new_leader = self.elect_leader(key).await;
        self.leaders.lock().insert(key.clone(), new_leader.clone());
        new_leader
    }

    pub async fn elect_leader(&self, key: &Key) -> NodeId {
        let nodes = self.network.all_nodes();
        self.elections.elect(key, &nodes).await
    }
}

#[derive(Debug, Derivative)]
#[derivative(Clone(bound = ""))]
pub enum LeaderElectionProcess {
    /// Only works when all nodes are always available
    Hasher(HasherElections<FnvHasher>),
}

impl LeaderElectionProcess {
    pub fn hasher() -> Self {
        Self::Hasher(Default::default())
    }

    pub async fn elect(&self, key: &Key, nodes: &[NodeId]) -> NodeId {
        match self {
            LeaderElectionProcess::Hasher(hasher) => hasher.choose_leader(key, nodes),
        }
    }
}

#[derive(Derivative, Default)]
#[derivative(Debug, Clone(bound = ""))]
pub struct HasherElections<H> {
    #[derivative(Debug = "ignore")]
    hasher: BuildHasherDefault<H>,
}

impl<H: Hasher + Default> HasherElections<H> {
    pub fn choose_leader(&self, key: &Key, nodes: &[NodeId]) -> NodeId {
        use std::hash::{BuildHasher, Hash};

        let mut hasher = self.hasher.build_hasher();
        key.hash(&mut hasher);
        nodes[(hasher.finish() % nodes.len() as u64) as usize].clone()
    }
}
