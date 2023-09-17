use std::{collections::HashMap, sync::Arc};

use eyre::{Context, Report};
use parking_lot::Mutex;


use tokio::sync::{mpsc, oneshot};

use crate::{Message, Output, Request, RequestOrResponse, Response};

type LetterBox = Arc<Mutex<HashMap<u64, oneshot::Sender<Message<Response>>>>>;

#[derive(Clone)]
pub struct Mailbox {
    letters: LetterBox,
    pub(crate) output: Output,
}

impl Mailbox {
    pub fn send(&self, message: Message<Request>) -> oneshot::Receiver<Message<Response>> {
        let (tx, rv) = oneshot::channel();
        let mut letters = self.letters.lock();
        letters.insert(message.body.message_id, tx);
        self.output.write(Some(message));
        rv
    }
}

pub fn launch_mailbox_loop() -> (Mailbox, mpsc::Receiver<Message<Request>>) {
    let (tx, rv) = mpsc::channel(128);
    let letters = Arc::new(Mutex::new(Default::default()));
    let mailbox = Mailbox {
        letters: letters.clone(),
        output: Output::lock(),
    };

    std::thread::spawn(move || mailbox_thread(tx, letters));

    (mailbox, rv)
}

fn mailbox_thread(tx: mpsc::Sender<Message<Request>>, letters: LetterBox) -> eyre::Result<()> {
    let stdin = std::io::stdin();
    for line in stdin.lines() {
        let line = line.wrap_err("got invalid stdin")?;
        tracing::debug!(%line, "Received line");
        let message: Message<RequestOrResponse> = serde_json::de::from_str(&line)
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
    Ok(())
}
