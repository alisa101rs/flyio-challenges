use std::sync::mpsc::{sync_channel, SyncSender};

use eyre::{Report, Result, WrapErr};
use serde::{de::DeserializeOwned, Serialize};
use smol_str::SmolStr;
use tracing::instrument;

use crate::{Message, Output, Request, RequestOrResponse, Response};

#[derive(Debug)]
pub enum Event<Req, Res, Inj> {
    Request(Message<Request<Req>>),
    Response(Message<Response<Res>>),
    Injected(Inj),
}

pub trait Node
where
    Self: Sized,
{
    type Injected;
    type Request;
    type Response;

    fn from_init(
        node_id: SmolStr,
        node_ids: Vec<SmolStr>,
        tx: SyncSender<Event<Self::Request, Self::Response, Self::Injected>>,
    ) -> eyre::Result<Self>;

    fn event(
        &mut self,
        event: Event<Self::Request, Self::Response, Self::Injected>,
        rpc: &mut Rpc,
    ) -> eyre::Result<()>;
}

pub struct Rpc {
    output: Output,
}

impl Rpc {
    pub fn new() -> Self {
        Self {
            output: Output::lock(),
        }
    }
}

impl Rpc {
    pub fn send<Rq>(&mut self, request: Message<Request<Rq>>) -> Result<()>
    where
        Rq: Serialize + std::fmt::Debug + Send + 'static,
    {
        tracing::info!(?request, "Sending request");
        self.output.write(Some(request));
        // match self.rv.recv_timeout(Duration::from_millis(1000)) {
        //     Ok(resp) => Ok(resp),
        //     Err(_) => Err(eyre!("timeout"))?,
        // }
        Ok(())
    }

    pub fn respond<Rs>(&mut self, response: Message<Response<Rs>>)
    where
        Rs: Serialize + std::fmt::Debug + Send + 'static,
    {
        tracing::info!(?response, "Sending response");
        self.output.write(Some(response));
    }
}

#[instrument(err)]
pub fn event_loop<N, Rq, Rs>() -> eyre::Result<()>
where
    N: Node<Request = Rq, Response = Rs>,
    Rq: Serialize + DeserializeOwned + Send + 'static + std::fmt::Debug,
    Rs: Serialize + DeserializeOwned + Send + 'static + std::fmt::Debug,
    N::Injected: Send + 'static,
{
    let (node_id, node_ids) = crate::initialize()?;
    let (tx, rx) = sync_channel(16);
    let mut node = N::from_init(node_id, node_ids, tx.clone())?;

    let jh = {
        std::thread::spawn(move || {
            let stdin = std::io::stdin();
            for line in stdin.lines() {
                let line = line.wrap_err("got invalid stdin")?;
                tracing::debug!(?line, "Received line");
                let message: Message<RequestOrResponse<Rq, Rs>> = serde_json::de::from_str(&line)
                    .wrap_err("failed to deserialize message")
                    .unwrap();
                let Message {
                    id, src, dst, body, ..
                } = message;
                let event = match body {
                    RequestOrResponse::Response(res) => {
                        let message = Message {
                            id,
                            src,
                            dst,
                            body: res,
                        };
                        tracing::info!(?message, "Received response");
                        Event::Response(message)
                    }
                    RequestOrResponse::Request(req) => {
                        let message = Message {
                            id,
                            src,
                            dst,
                            body: req,
                        };
                        tracing::info!(?message, "Received request");
                        Event::Request(message)
                    }
                };
                if let Err(_er) = tx.send(event) {
                    tracing::error!("Channel closed");
                    return Ok::<(), Report>(());
                }
            }

            eyre::Result::Ok(())
        })
    };

    let mut rpc = Rpc::new();

    for message in rx {
        node.event(message, &mut rpc).expect("error in process");
    }

    jh.join()
        .expect("failed to join")
        .expect("error in read loop");

    Ok(())
}
