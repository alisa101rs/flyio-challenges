use eyre::WrapErr;
use serde::de::DeserializeOwned;
use serde_json::Value;
use smol_str::SmolStr;

mod extension;

pub use self::extension::Extension;
use crate::{
    message::{Message, RequestPayload},
    util::Extensions,
    NodeId,
};

#[derive(Debug, Clone)]
pub struct Request {
    pub(crate) method: SmolStr,
    body: Value,
    pub(crate) from: NodeId,
    extensions: Extensions,
}

impl Request {
    pub fn from_message(message: &Message<RequestPayload>) -> Self {
        Self {
            method: message.body.ty.clone(),
            body: message.body.payload.clone(),
            from: message.src.clone(),
            extensions: Default::default(),
        }
    }
    pub fn extensions(&self) -> &Extensions {
        &self.extensions
    }
    pub fn extensions_mut(&mut self) -> &mut Extensions {
        &mut self.extensions
    }
}

pub trait FromRequest: Sized {
    fn from_request(request: &Request) -> eyre::Result<Self>;
}

#[derive(Debug, Clone)]
pub struct Payload<T>(pub T);

impl<T: DeserializeOwned> FromRequest for Payload<T> {
    fn from_request(request: &Request) -> eyre::Result<Self> {
        Ok(Payload(
            serde_json::from_value(request.body.clone())
                .wrap_err("Failed to deserialize payload")?,
        ))
    }
}

impl FromRequest for NodeId {
    fn from_request(request: &Request) -> eyre::Result<Self> {
        Ok(request.from.clone())
    }
}
