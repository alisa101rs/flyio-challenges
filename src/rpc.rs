use std::time::Duration;

use derivative::Derivative;
use eyre::{eyre, Result};
use futures::{stream::FuturesUnordered, Stream};
use rand::random;
use serde::{de::DeserializeOwned, Serialize};
use smol_str::SmolStr;
use tracing::instrument;

pub use super::Mailbox;
use crate::{
    message::{Message, RequestPayload, ResponsePayload},
    network::Network,
    trace::inject_trace,
    NodeId,
};

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct Rpc {
    node: NodeId,
    peers: Vec<NodeId>,
    #[derivative(Debug = "ignore")]
    mailbox: Mailbox,
}

impl Rpc {
    pub fn node_id(&self) -> NodeId {
        self.node.clone()
    }
    pub fn peers(&self) -> &[NodeId] {
        self.peers.as_slice()
    }
    pub fn new(node: NodeId, peers: Vec<NodeId>, mailbox: Mailbox) -> Self {
        Self {
            node,
            peers,
            mailbox,
        }
    }

    pub fn respond<Res>(&self, to: NodeId, ty: String, message_id: u64, payload: Res)
    where
        Res: Serialize + std::fmt::Debug,
    {
        let message = Message {
            id: random(),
            src: self.node.clone(),
            dst: to,
            body: ResponsePayload {
                in_reply_to: message_id,
                ty,
                payload: serde_json::to_value(payload).expect("Could not serialize payload"),
            },
        };
        tracing::debug!(?message, "Responding");
        self.mailbox.respond(message);
    }

    pub fn broadcast<'a, Req, Res>(
        &'a self,
        ty: impl Into<SmolStr>,
        network: &Network,
        payload: Req,
    ) -> impl Stream<Item = Result<(NodeId, Res)>> + Send + Sync + 'a
    where
        Req: Serialize + Clone + std::fmt::Debug + Sync + Send + 'static,
        Res: DeserializeOwned + std::fmt::Debug + Sync + Send + 'static,
    {
        let futures = FuturesUnordered::new();
        let ty = ty.into();

        for dst in network.other_nodes() {
            let response = self.send(ty.clone(), dst.clone(), payload.clone());

            futures.push(async move { Ok((dst, response.await?)) });
        }

        futures
    }

    pub async fn send<Req, Res>(
        &self,
        ty: impl Into<SmolStr> + std::fmt::Debug,
        dst: NodeId,
        payload: Req,
    ) -> Result<Res>
    where
        Req: Serialize + std::fmt::Debug,
        Res: DeserializeOwned + std::fmt::Debug,
    {
        let id = random();
        let response = self
            .send_message(Message {
                id,
                src: self.node.clone(),
                dst,
                body: RequestPayload {
                    message_id: id,
                    traceparent: None,
                    payload: serde_json::to_value(payload)?,
                    ty: ty.into(),
                },
            })
            .await?;

        serde_json::from_value(response.body.payload).map_err(|er| eyre!("Invalid response {er}"))
    }

    #[instrument(skip(self), err)]
    async fn send_message(
        &self,
        mut req: Message<RequestPayload>,
    ) -> Result<Message<ResponsePayload>> {
        inject_trace(&mut req);
        let rv = self.mailbox.send(req);

        match tokio::time::timeout(Duration::from_millis(250), rv).await {
            Ok(Ok(res)) => Ok(res),
            Ok(Err(er)) => {
                tracing::error!(?er, "Could not receive response");
                Err(eyre!("Error: {er:?}"))
            }
            Err(timeout) => {
                tracing::error!("Timeout. Could not receive response in time");
                Err(eyre!("Timeout: {timeout:?}"))
            }
        }
    }
}
