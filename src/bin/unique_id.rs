use flyio_rs::{
    request::Payload, response::IntoResponse, routing::Router, serve, setup_network,
    trace::setup_tracing,
};
use rand::random;
use serde::{Deserialize, Serialize};

#[tokio::main]
async fn main() -> eyre::Result<()> {
    setup_tracing()?;

    let (_, rpc, messages) = setup_network().await?;

    let router = Router::new().route("generate", generate);

    serve(router, rpc, messages).await?;

    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GenerateResponse {
    id: u64,
}

async fn generate() -> impl IntoResponse {
    Payload(GenerateResponse { id: random() })
}
