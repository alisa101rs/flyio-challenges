use std::sync::Arc;

use flyio_rs::NodeId;
use parking_lot::Mutex;
use vclock::VClock64;

pub type Clock = VClock64<NodeId>;

#[derive(Debug, Clone)]
pub struct NodeClock {
    id: NodeId,
    clock: Arc<Mutex<Clock>>,
}

impl NodeClock {
    pub fn new(id: NodeId) -> Self {
        Self {
            clock: Arc::new(Mutex::new(VClock64::new(id.clone()))),
            id,
        }
    }

    pub fn next(&self) -> VClock64<NodeId> {
        let mut clock = self.clock.lock();

        *clock = std::mem::replace(&mut *clock, VClock64::default()).next(&self.id);

        clock.clone()
    }

    pub fn merge(&self, other: &VClock64<NodeId>) {
        let mut clock = self.clock.lock();
        *clock = std::mem::replace(&mut *clock, VClock64::default()).next(&self.id);
        clock.merge(other);
    }
}
