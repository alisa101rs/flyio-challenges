#![feature(async_fn_in_trait)]

pub mod event;
mod mailbox;
pub mod network;
mod node;
mod output;
mod routing;
mod rpc;
pub mod storage;
pub mod trace;

use std::io::stdin;

use eyre::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use smol_str::SmolStr;

pub use self::{
    mailbox::{launch_mailbox_loop, Mailbox},
    network::NodeId,
    node::{event_loop, periodic_injection, Node},
    output::Output,
    rpc::Rpc,
};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Message<Body> {
    pub id: u64,
    pub src: SmolStr,
    #[serde(default, rename = "dest")]
    pub dst: SmolStr,
    pub body: Body,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Request {
    #[serde(rename = "msg_id")]
    pub message_id: u64,
    #[serde(flatten)]
    pub payload: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Response {
    pub in_reply_to: u64,
    #[serde(flatten)]
    pub payload: Value,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum RequestOrResponse {
    Request(Request),
    Response(Response),
}

impl Message<Request> {
    pub fn into_reply<F, E>(self, f: F) -> Result<Message<Response>, E>
    where
        F: FnOnce(Value) -> Result<Value, E>,
    {
        let Message {
            id, src, dst, body, ..
        } = self;
        let Request {
            message_id,
            payload,
            ..
        } = body;

        Ok(Message {
            id,
            src: dst,
            dst: src,
            body: Response {
                in_reply_to: message_id,
                payload: f(payload)?,
            },
        })
    }
}

impl Message<Response> {
    pub fn try_map_payload<F, E>(self, f: F) -> Result<Message<Response>, E>
    where
        F: FnOnce(Value) -> Result<Value, E>,
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

    let message: Message<Request> = serde_json::de::from_str(&init)?;

    tracing::debug!(?message, "Initialize message");

    let Message {
        id,
        body: Request {
            message_id,
            payload,
            ..
        },
        src,
        ..
    } = message;

    let payload: InitRequestPayload = serde_json::value::from_value(payload)?;

    let selv = match payload {
        InitRequestPayload::Init { node_id, node_ids } => (node_id, node_ids),
    };
    let response = Message {
        id,
        dst: src,
        src: selv.0.clone(),
        body: Response {
            in_reply_to: message_id,
            payload: serde_json::to_value(InitResponsePayload::InitOk)?,
        },
    };
    Output::lock().write(Some(response));

    tracing::info!(node = ?selv, "Node initialized");

    Ok(selv)
}
