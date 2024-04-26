use serde::{Deserialize, Serialize};
use serde_json::Value;
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
pub struct RequestPayload {
    #[serde(rename = "msg_id")]
    pub message_id: u64,
    #[serde(rename = "type")]
    pub ty: SmolStr,
    #[serde(flatten)]
    pub payload: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,
}

impl RequestPayload {
    pub(crate) fn response_type(&self) -> String {
        format!("{}_ok", self.ty)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ResponsePayload {
    pub in_reply_to: u64,
    #[serde(rename = "type")]
    pub ty: String,
    #[serde(flatten)]
    pub payload: Value,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum RequestOrResponse {
    Request(RequestPayload),
    Response(ResponsePayload),
}
