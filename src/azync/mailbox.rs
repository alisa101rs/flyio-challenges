use std::{collections::HashMap, sync::Arc};

use eyre::{Context, Report};
use parking_lot::Mutex;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};

use crate::{Message, Output, Request, RequestOrResponse, Response};

#[derive(Clone)]
pub struct Mailbox {
    letters: Arc<Mutex<HashMap<u64, oneshot::Sender<Message<Response<Value>>>>>>,
    pub(crate) output: Output,
}

impl Mailbox {
    pub fn send<Req>(
        &self,
        message: Message<Request<Req>>,
    ) -> oneshot::Receiver<Message<Response<Value>>>
    where
        Req: Serialize + std::fmt::Debug,
    {
        let (tx, rv) = oneshot::channel();
        let mut letters = self.letters.lock();
        letters.insert(message.body.message_id, tx);
        self.output.write(Some(message));
        rv
    }
}

pub fn launch_mailbox_loop<Req>() -> (Mailbox, mpsc::Receiver<Message<Request<Req>>>)
where
    Req: Serialize + DeserializeOwned + Send + 'static + std::fmt::Debug,
{
    let (tx, rv) = mpsc::channel(128);
    let letters = Arc::new(Mutex::new(Default::default()));
    let mailbox = Mailbox {
        letters,
        output: Output::lock(),
    };

    {
        let letters = mailbox.letters.clone();
        std::thread::spawn(move || {
            let stdin = std::io::stdin();
            for line in stdin.lines() {
                let line = line.wrap_err("got invalid stdin")?;
                tracing::debug!(%line, "Received line");
                let message: Message<RequestOrResponse<Req, Value>> =
                    serde_json::de::from_str(&line)
                        .wrap_err("failed to deserialize message")
                        .unwrap();
                let Message { id, src, dst, body } = message;
                match body {
                    RequestOrResponse::Response(res) => {
                        let in_reply_to = res.in_reply_to;
                        let message = Message {
                            id,
                            src,
                            dst,
                            body: res,
                        };
                        tracing::info!(?message, "Received response");
                        let mut letters = letters.lock();
                        if let Some(tx) = letters.remove(&in_reply_to) {
                            if tx.send(message).is_err() {
                                tracing::warn!(%in_reply_to, "Reply lost")
                            }
                        } else {
                            tracing::warn!(?message, "Response without waiter");
                        }
                    }
                    RequestOrResponse::Request(req) => {
                        let message = Message {
                            id,
                            src,
                            dst,
                            body: req,
                        };
                        tracing::info!(?message, "Received request");
                        if let Err(_er) = tx.blocking_send(message) {
                            tracing::error!("Channel closed");
                            return Ok::<(), Report>(());
                        }
                    }
                }
            }
            tracing::warn!("Closing message loop");
            drop(tx);
            Result::Ok(())
        })
    };

    (mailbox, rv)
}
