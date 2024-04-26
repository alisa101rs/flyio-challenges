use std::sync::Arc;

use eyre::bail;
use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

use crate::{
    launch_mailbox_loop,
    message::{Message, RequestPayload},
    network::Network,
    Rpc,
};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InitRequestPayload {
    node_id: SmolStr,
    node_ids: Vec<SmolStr>,
}

pub async fn setup_network() -> eyre::Result<(
    Arc<Network>,
    Rpc,
    impl Stream<Item = Message<RequestPayload>> + Unpin,
)> {
    let (rpc, messages) = initialize().await?;
    let network = Network::create(rpc.node_id(), rpc.peers().to_vec());

    Ok((network, rpc, messages))
}

async fn initialize() -> eyre::Result<(Rpc, impl Stream<Item = Message<RequestPayload>> + Unpin)> {
    let (mailbox, mut messages) = launch_mailbox_loop();
    let Some(init) = messages.next().await else {
        bail!("Expected to receive initialization message")
    };

    let Ok(InitRequestPayload { node_id, node_ids }) =
        serde_json::value::from_value(init.body.payload)
    else {
        bail!("Expected to receive initialization message")
    };
    let rpc = Rpc::new(node_id, node_ids, mailbox);

    rpc.respond(init.src, "init_ok".to_owned(), init.body.message_id, ());

    tracing::info!(node_id = %rpc.node_id(), node_ids = ?rpc.peers(), "Node initialized");

    Ok((rpc, messages))
}
