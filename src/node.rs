use std::{fmt, sync::Arc, time::Duration};

use eyre::{Result};

use serde::{de::DeserializeOwned, Serialize};

use tokio::{select, sync::mpsc, task::JoinHandle};
use tracing::{instrument, Instrument};

pub use crate::Mailbox;
use crate::{
    event::Event,
    initialize, launch_mailbox_loop,
    network::{heartbeat_loop, Network},
    trace::get_span,
    Rpc,
};

pub trait Node: Sized + Clone {
    type Injected: Send + 'static;
    type Request: DeserializeOwned + Send + fmt::Debug + 'static;

    fn from_init(
        network: Arc<Network>,
        tx: mpsc::Sender<Event<Self::Request, Self::Injected>>,
        rpc: Rpc,
    ) -> eyre::Result<Self>;

    async fn process_event(
        &mut self,
        event: Event<Self::Request, Self::Injected>,
    ) -> eyre::Result<()>;
}

#[instrument(err)]
pub async fn event_loop<N, Req>(heartbeat_period: Option<Duration>) -> eyre::Result<()>
where
    N: Node<Request = Req> + 'static,
    Req: Serialize + DeserializeOwned + Send + 'static + std::fmt::Debug,
{
    let (node_id, node_ids) = initialize()?;
    let (mailbox, mut messages) = launch_mailbox_loop();
    let (tx, mut rv) = mpsc::channel(128);
    let network = Network::create(node_id.clone(), node_ids.clone());
    let rpc = Rpc::new(node_id, node_ids, mailbox);
    let mut node = N::from_init(network.clone(), tx, rpc.clone())?;
    let guard = if let Some(heartbeat_period) = heartbeat_period {
        tokio::task::spawn(heartbeat_loop(heartbeat_period, network, rpc.clone()))
    } else {
        tokio::task::spawn(async move { Ok(()) })
    };

    tokio::task::LocalSet::new()
        .run_until(async move {
            loop {
                let (event, traceparent) = select!(
                    maybe_message = messages.recv() => {
                        if let Some(message) = maybe_message {
                            (
                                Event::Request {
                                    src: message.src,
                                    message_id: message.body.message_id,
                                    payload: serde_json::from_value(message.body.payload)?,
                                },
                                message.body.traceparent
                            )
                        } else {
                            node.process_event(Event::EOF).await.unwrap();
                            break;
                        }
                    },
                    Some(event) = rv.recv() => {
                        (event, None)
                    }
                );

                let mut node = node.clone();
                let span = get_span(&event, traceparent);
                tokio::task::spawn_local(
                    async move {
                        tracing::debug!("Started processing");
                        if let Err(_er) = node.process_event(event).await {
                            todo!()
                        }
                    }
                    .instrument(span),
                );
            }

            Result::<()>::Ok(())
        })
        .await?;
    guard.abort();

    Ok(())
}

pub fn periodic_injection<R, I>(
    channel: mpsc::Sender<Event<R, I>>,
    period: Duration,
    injection: I,
) -> JoinHandle<()>
where
    R: Send + 'static,
    I: Sync + Send + Clone + 'static,
{
    tokio::task::spawn(async move {
        let mut interval = tokio::time::interval(period);
        loop {
            interval.tick().await;
            if let Err(_) = channel.send(Event::Injected(injection.clone())).await {
                tracing::error!("Channel closed");
                break;
            }
        }
    })
}
