pub mod error;
mod init;
mod mailbox;
mod message;
pub mod network;
mod output;
pub mod request;
pub mod response;
pub mod routing;
mod rpc;
pub mod storage;
pub mod trace;
mod util;

use futures::Stream;
use futures_util::StreamExt;
use message::{Message, RequestPayload};
use serde_json::json;
use tower::ServiceExt;
use tracing::{info_span, instrument, Instrument};

pub use self::{
    init::setup_network,
    mailbox::{launch_mailbox_loop, Mailbox},
    network::NodeId,
    rpc::Rpc,
};
use crate::{
    error::ErrorCode,
    request::Request,
    response::{IntoResponse, Response},
    routing::Router,
    trace::inject_parent,
};

#[instrument(skip(router, rpc, messages), err)]
pub async fn serve(
    router: Router,
    rpc: Rpc,
    mut messages: impl Stream<Item = Message<RequestPayload>> + Unpin,
) -> eyre::Result<()> {
    while let Some(message) = messages.next().await {
        tokio::spawn(handle(message, router.clone(), rpc.clone()));
    }

    Ok(())
}

async fn handle(
    mut message: Message<RequestPayload>,
    router: Router,
    rpc: Rpc,
) -> eyre::Result<()> {
    let span = info_span!(
        "Incoming request",
        otel.name=%message.body.ty,
        otel.kind="server",
        r#type=%message.body.ty,
        from = %message.src,
        message_id = %message.body.message_id
    );
    inject_parent(&span, message.body.traceparent.take());

    async {
        let request = Request::from_message(&message);
        let response = router
            .oneshot(request)
            .await
            .map_err(|it| ErrorCode::Crash)
            .into_response();
        match response {
            Response::None => {}
            Response::Message(m) => {
                rpc.respond(
                    message.src,
                    message.body.response_type(),
                    message.body.message_id,
                    m,
                );
            }
            Response::Error(e) => {
                rpc.respond(message.src, "error".to_string(), message.body.message_id, e);
            }
        }
    }
    .instrument(span)
    .await;

    Ok(())
}
