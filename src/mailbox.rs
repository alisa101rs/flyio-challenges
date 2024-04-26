use std::{collections::HashMap, sync::Arc};

use eyre::{Context, Report};
use futures::Stream;
use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    message::{Message, RequestOrResponse, RequestPayload, ResponsePayload},
    output::Output,
};

type LetterBox = Arc<Mutex<HashMap<u64, oneshot::Sender<Message<ResponsePayload>>>>>;

#[derive(Clone)]
pub struct Mailbox {
    letters: LetterBox,
    pub(crate) output: Output,
}

impl Mailbox {
    pub fn respond(&self, message: Message<ResponsePayload>) {
        self.output.write(Some(message));
    }

    pub fn send(
        &self,
        message: Message<RequestPayload>,
    ) -> oneshot::Receiver<Message<ResponsePayload>> {
        let (tx, rv) = oneshot::channel();
        let mut letters = self.letters.lock();
        letters.insert(message.body.message_id, tx);
        self.output.write(Some(message));
        rv
    }

    fn send_reply(&self, in_reply_to: u64, message: Message<ResponsePayload>) {
        let mut letters = self.letters.lock();
        if let Some(tx) = letters.remove(&in_reply_to) {
            if tx.send(message).is_err() {
                tracing::warn!(%in_reply_to, "Reply lost")
            }
        } else {
            tracing::warn!(?message, "Response without waiter");
        }
    }
}

pub fn launch_mailbox_loop() -> (Mailbox, impl Stream<Item = Message<RequestPayload>> + Unpin) {
    let (tx, rv) = mpsc::channel(128);
    let letters = Arc::new(Mutex::new(Default::default()));
    let mailbox = Mailbox {
        letters,
        output: Output::lock(),
    };

    std::thread::spawn({
        let mailbox = mailbox.clone();
        move || mailbox_thread(tx, mailbox)
    });

    (mailbox, ReceiverStream::new(rv))
}

fn mailbox_thread(tx: mpsc::Sender<Message<RequestPayload>>, mailbox: Mailbox) -> eyre::Result<()> {
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
                mailbox.send_reply(in_reply_to, message);
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
