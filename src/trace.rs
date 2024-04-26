use std::time::Duration;

use opentelemetry::propagation::Injector;
use tracing::Span;

use crate::message::{Message, RequestPayload};

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

pub(crate) fn inject_parent(span: &Span, parent: Option<String>) {
    tracing_opentelemetry::OpenTelemetrySpanExt::set_parent(span, extract_remote_context(parent));
}

pub(crate) fn extract_remote_context(parent: Option<String>) -> opentelemetry::Context {
    struct EventExtractor(Option<String>);

    impl opentelemetry::propagation::Extractor for EventExtractor {
        fn get(&self, key: &str) -> Option<&str> {
            if key == "traceparent" {
                self.0.as_deref()
            } else {
                None
            }
        }

        fn keys(&self) -> Vec<&str> {
            self.get("traceparent").into_iter().collect()
        }
    }

    let extractor = EventExtractor(parent);
    opentelemetry::global::get_text_map_propagator(|propagator| propagator.extract(&extractor))
}

pub(crate) fn inject_trace(message: &mut Message<RequestPayload>) {
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    struct MessageInjector<'a>(&'a mut Message<RequestPayload>);

    let mut inj = MessageInjector(message);

    impl<'a> Injector for MessageInjector<'a> {
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
