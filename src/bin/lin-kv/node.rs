use std::ops::Deref;

use flyio_rs::NodeId;
use tokio::sync::watch;

pub type State = watch::Receiver<NodeState>;

pub struct Node {
    pub id: NodeId,
    pub term: u64,
    state: watch::Sender<NodeState>,
    casted_vote: Option<NodeId>,
}

impl Node {
    pub fn start(id: NodeId) -> (Self, State) {
        let (state, rv) = watch::channel(NodeState::Follower { of: None });
        let node = Self {
            id,
            term: 0,
            state,
            casted_vote: None,
        };

        (node, rv)
    }

    pub fn state(&self) -> impl Deref<Target = NodeState> + '_ {
        self.state.borrow()
    }

    pub fn become_candidate(&mut self) -> u64 {
        self.term += 1;
        self.state.send(NodeState::Candidate {}).unwrap();
        self.casted_vote = Some(self.id.clone());
        self.term
    }

    pub fn become_leader(&mut self) {
        assert_eq!(&*self.state(), &NodeState::Candidate);

        self.state.send(NodeState::Leader).unwrap();
    }

    pub fn become_follower(&mut self, of: Option<NodeId>) {
        self.state.send(NodeState::Follower { of }).unwrap();
    }

    pub fn cast_vote(&mut self, to: NodeId, term: u64) -> Result<(), ()> {
        if !matches!(&*self.state(), NodeState::Follower { .. }) {
            return Err(());
        }

        if self.casted_vote.is_some() && term <= self.term {
            return Err(());
        }
        if term < self.term {
            return Err(());
        }

        self.casted_vote = Some(to);
        self.term = term;
        self.state.send(NodeState::Follower { of: None }).unwrap();
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum NodeState {
    Follower { of: Option<NodeId> },
    Leader,
    Candidate,
}

impl NodeState {
    pub fn is_leader(&self) -> bool {
        matches!(self, NodeState::Leader)
    }
    pub fn leader(&self) -> Option<NodeId> {
        match self {
            NodeState::Follower { of: Some(of) } => Some(of.clone()),
            _ => None,
        }
    }
}
