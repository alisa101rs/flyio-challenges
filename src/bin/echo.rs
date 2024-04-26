use flyio_rs::{request::Payload, routing::Router, serve, setup_network, trace::setup_tracing};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Echo {
    echo: String,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    setup_tracing()?;
    let (_, rpc, messages) = setup_network().await?;

    let router = Router::new().route("echo", echo);

    serve(router, rpc, messages).await?;

    Ok(())
}

async fn echo(Payload(echo): Payload<Echo>) -> Payload<Echo> {
    tracing::debug!("Got an echo");

    Payload(echo)
}
