use std::time::Duration;

use derivative::Derivative;
use eyre::{eyre, Context, Result};
use futures::{stream::FuturesUnordered, Stream};
use rand::random;
use serde::{de::DeserializeOwned, Serialize};
use tracing::instrument;

pub use super::Mailbox;
use crate::{network::Network, trace::inject_trace, Message, NodeId, Request, Response};

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct Rpc {
    node: NodeId,
    peers: Vec<NodeId>,
    #[derivative(Debug = "ignore")]
    mailbox: Mailbox,
}

impl Rpc {
    pub fn new(node: NodeId, peers: Vec<NodeId>, mailbox: Mailbox) -> Self {
        Self {
            node,
            peers,
            mailbox,
        }
    }

    pub fn respond<Res>(&self, to: NodeId, message_id: u64, payload: Res)
    where
        Res: Serialize + std::fmt::Debug,
    {
        let message = Message {
            id: random(),
            src: self.node.clone(),
            dst: to,
            body: Response {
                in_reply_to: message_id,
                payload: serde_json::to_value(payload).expect("Could not serialize payload"),
            },
        };
        self.mailbox.output.write(Some(message));
    }

    pub fn broadcast<'a, Req, Res>(
        &'a self,
        network: &Network,
        payload: Req,
    ) -> impl Stream<Item = Result<(NodeId, Res)>> + Send + Sync + 'a
    where
        Req: Serialize + Clone + std::fmt::Debug + Sync + Send + 'static,
        Res: DeserializeOwned + std::fmt::Debug + Sync + Send + 'static,
    {
        let mut futures = FuturesUnordered::new();

        for dst in network.other_nodes() {
            let response = self.send(dst.clone(), payload.clone());

            futures.push(async move { Ok((dst, response.await?)) });
        }

        futures
    }

    #[instrument(skip(self, payload), err)]
    pub async fn send<Req, Res>(&self, dst: NodeId, payload: Req) -> Result<Res>
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
                body: Request {
                    message_id: id,
                    traceparent: None,
                    payload: serde_json::to_value(payload)?,
                },
            })
            .await?;

        serde_json::from_value(response.body.payload).map_err(|er| eyre!("Invalid response {er}"))
    }

    #[instrument(skip(self, req))]
    async fn send_message(&self, mut req: Message<Request>) -> Result<Message<Response>> {
        inject_trace(&mut req);
        tracing::debug!(request = ?req, "Sending request");
        let rv = self.mailbox.send(req);

        match tokio::time::timeout(Duration::from_millis(250), rv).await {
            Ok(Ok(res)) => res
                .try_map_payload(serde_json::from_value)
                .wrap_err(eyre!("could not deserialize response")),
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
