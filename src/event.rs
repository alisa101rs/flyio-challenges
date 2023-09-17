use crate::{NodeId};

#[derive(Debug, Clone)]
pub enum Event<Req, Inj> {
    Request {
        src: NodeId,
        message_id: u64,
        payload: Req,
    },
    Injected(Inj),
    EOF,
}
