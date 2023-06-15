#![feature(async_fn_in_trait)]

pub mod azync;
pub mod network;
pub mod sync;

use std::{
    io::{stdin, stdout, Write},
    time::Duration,
};

use eyre::Result;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Message<Body> {
    pub id: u64,
    pub src: SmolStr,
    #[serde(default, rename = "dest")]
    pub dst: SmolStr,
    pub body: Body,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Request<Payload> {
    #[serde(rename = "msg_id")]
    pub message_id: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,
    #[serde(flatten)]
    pub payload: Payload,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Response<Payload> {
    pub in_reply_to: u64,
    #[serde(flatten)]
    pub payload: Payload,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum RequestOrResponse<Req, Res> {
    Request(Request<Req>),
    Response(Response<Res>),
}

impl<Rq> Message<Request<Rq>> {
    pub fn into_reply<Rs, F>(self, f: F) -> Message<Response<Rs>>
    where
        F: FnOnce(Rq) -> Rs,
    {
        let Message {
            id, src, dst, body, ..
        } = self;
        let Request {
            message_id,
            payload,
            ..
        } = body;

        Message {
            id,
            src: dst,
            dst: src,
            body: Response {
                in_reply_to: message_id,
                payload: f(payload),
            },
        }
    }
}

impl<Res> Message<Response<Res>> {
    pub fn try_map_payload<NewRes, F, E>(self, f: F) -> Result<Message<Response<NewRes>>, E>
    where
        F: FnOnce(Res) -> Result<NewRes, E>,
    {
        let Message {
            id, src, dst, body, ..
        } = self;
        let Response {
            in_reply_to,
            payload,
        } = body;

        Ok(Message {
            id,
            src: dst,
            dst: src,
            body: Response {
                in_reply_to,
                payload: f(payload)?,
            },
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InitRequestPayload {
    Init {
        node_id: SmolStr,
        node_ids: Vec<SmolStr>,
    },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InitResponsePayload {
    InitOk,
}

fn initialize() -> Result<(SmolStr, Vec<SmolStr>)> {
    let mut init = String::new();
    stdin().read_line(&mut init)?;

    let message: Message<Request<InitRequestPayload>> = serde_json::de::from_str(&init)?;

    tracing::debug!(?message, "Initialize message");

    let Message { body, .. } = &message;

    let selv = match &body.payload {
        InitRequestPayload::Init { node_id, node_ids } => (node_id.clone(), node_ids.clone()),
    };
    let response = message.into_reply(|_payload| InitResponsePayload::InitOk);
    Output::lock().write(Some(response));

    tracing::info!(node = ?selv, "Node initialized");

    Ok(selv)
}

#[derive(Copy, Clone)]
pub struct Output {}

impl Output {
    pub fn lock() -> Self {
        Self {}
    }

    pub fn write<R>(&self, iter: impl IntoIterator<Item = R>)
    where
        R: Serialize + std::fmt::Debug,
    {
        let mut stdout = stdout().lock();
        for message in iter {
            serde_json::ser::to_writer(&mut stdout, &message).expect("failed to write to stdout");
            stdout.write(b"\n").expect("failed to write new line");
        }
    }
}

pub fn setup_tracing() -> eyre::Result<()> {
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_writer(std::io::stderr);
    let filter_layer = tracing_subscriber::EnvFilter::try_from_default_env()
        .or_else(|_| tracing_subscriber::EnvFilter::try_new("debug"))?;

    tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer)
        .init();

    Ok(())
}

pub fn setup_with_telemetry(node: String) -> eyre::Result<()> {
    use opentelemetry::{
        runtime::Tokio,
        sdk::{
            propagation::TraceContextPropagator,
            trace,
            trace::{RandomIdGenerator, Sampler},
            Resource,
        },
        KeyValue,
    };
    use opentelemetry_otlp::WithExportConfig;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_writer(std::io::stderr);

    let filter_layer = tracing_subscriber::EnvFilter::try_from_default_env()
        .or_else(|_| tracing_subscriber::EnvFilter::try_new("debug"))?;

    let registry = tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer);

    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
    let endpoint = "http://0.0.0.0:4317";

    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint(endpoint)
                .with_timeout(Duration::from_secs(3)),
        )
        .with_trace_config(
            trace::config()
                .with_sampler(Sampler::AlwaysOn)
                .with_id_generator(RandomIdGenerator::default())
                .with_max_events_per_span(64)
                .with_max_attributes_per_span(32)
                .with_max_events_per_span(16)
                .with_max_attributes_per_link(512)
                .with_resource(Resource::new(vec![KeyValue::new("service.name", node)])),
        )
        .install_batch(Tokio)?;

    registry
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .try_init()?;

    Ok(())
}
