use std::time::Duration;

use eyre::{eyre, Result};
use opentelemetry::propagation::Injector;
use serde::{de::DeserializeOwned, Serialize};
use smol_str::SmolStr;
use tokio::{select, sync::mpsc, task::JoinHandle};
use tracing::{info_span, instrument, Span};

mod mailbox;
pub use self::mailbox::Mailbox;
use crate::{initialize, Message, Request, Response};

#[derive(Debug, Clone)]
pub enum Event<Req, Inj> {
    Request(Message<Request<Req>>),
    Injected(Inj),
    EOF,
}

#[derive(Clone)]
pub struct Rpc<Res> {
    mailbox: Mailbox<Res>,
}

impl<Res> Rpc<Res>
where
    Res: Serialize + std::fmt::Debug,
{
    pub fn respond(&self, response: Message<Response<Res>>) {
        self.mailbox.output.write(Some(response));
    }

    #[instrument(skip(self, req))]
    pub async fn send<Req>(&self, mut req: Message<Request<Req>>) -> Result<Message<Response<Res>>>
    where
        Req: Serialize + std::fmt::Debug,
    {
        inject_trace(&mut req);
        tracing::debug!(request = ?req, "Sending request");
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

pub trait Node: Sized {
    type Injected;
    type Request;
    type Response;

    fn from_init(
        node_id: SmolStr,
        node_ids: Vec<SmolStr>,
        tx: mpsc::Sender<Event<Self::Request, Self::Injected>>,
    ) -> eyre::Result<Self>;

    async fn process_event(
        &self,
        event: Event<Self::Request, Self::Injected>,
        rpc: &Rpc<Self::Response>,
    ) -> eyre::Result<()>;
}

#[instrument(err)]
pub async fn event_loop<N, Req, Res>() -> eyre::Result<()>
where
    N: Node<Request = Req, Response = Res>,
    Req: Serialize + DeserializeOwned + Send + 'static + std::fmt::Debug,
    Res: Serialize + DeserializeOwned + Send + 'static + std::fmt::Debug,
    N::Injected: Send + 'static,
{
    let (node_id, node_ids) = initialize()?;
    let (mailbox, mut messages) = mailbox::launch_mailbox_loop::<Req, Res>();
    let (tx, mut rv) = mpsc::channel(128);
    let node = N::from_init(node_id, node_ids, tx)?;
    let rpc = Rpc { mailbox };

    loop {
        let event = select!(
            maybe_message = messages.recv() => {
                if let Some(message) = maybe_message {
                     Event::Request(message)
                } else {
                    node.process_event(Event::EOF, &rpc).await?;
                    break
                }
            },
            Some(event) = rv.recv() => {
                event
            }
        );
        let span = get_span(&event);

        {
            let _guard = span.enter();
            node.process_event(event, &rpc).await?;
        }
    }

    Ok(())
}

fn get_span<R, I>(event: &Event<R, I>) -> Span {
    let span = match event {
        Event::Request(_) => info_span!("Request event"),
        Event::Injected(_) => info_span!("Injected event"),
        Event::EOF => info_span!("EOF event"),
    };
    tracing_opentelemetry::OpenTelemetrySpanExt::set_parent(&span, extract_remote_context(event));
    span
}

fn extract_remote_context<R, I>(event: &Event<R, I>) -> opentelemetry::Context {
    struct EventExtractor<'a, R, I>(&'a Event<R, I>);

    impl<'a, R, I> opentelemetry::propagation::Extractor for EventExtractor<'a, R, I> {
        fn get(&self, key: &str) -> Option<&str> {
            if key == "traceparent" {
                match self.0 {
                    Event::Request(message) => message.body.traceparent.as_deref(),
                    Event::Injected(_) => None,
                    Event::EOF => None,
                }
            } else {
                None
            }
        }

        fn keys(&self) -> Vec<&str> {
            self.get("traceparent").into_iter().collect()
        }
    }

    let extractor = EventExtractor(event);
    opentelemetry::global::get_text_map_propagator(|propagator| propagator.extract(&extractor))
}

fn inject_trace<R>(message: &mut Message<Request<R>>) {
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    struct MessageInjector<'a, R>(&'a mut Message<Request<R>>);

    let mut inj = MessageInjector(message);

    impl<'a, R> Injector for MessageInjector<'a, R> {
        fn set(&mut self, key: &str, value: String) {
            if key == "traceparent" {
                self.0.body.traceparent = Some(value);
            }
        }
    }

    opentelemetry::global::get_text_map_propagator(|prop| {
        prop.inject_context(&tracing::Span::current().context(), &mut inj)
    });
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
