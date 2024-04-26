use std::fmt;

use serde::Serialize;
use serde_json::{json, Value};

use crate::request::Payload;

#[derive(Debug)]
pub enum Response {
    None,
    Message(Value),
    Error(Value),
}

pub trait IntoResponse: fmt::Debug + Sized {
    fn into_response(self) -> Response;
}

impl IntoResponse for Response {
    fn into_response(self) -> Response {
        self
    }
}
impl IntoResponse for () {
    fn into_response(self) -> Response {
        Response::Message(Value::Null)
    }
}

impl<T: IntoResponse, E: IntoResponse> IntoResponse for Result<T, E> {
    fn into_response(self) -> Response {
        match self {
            Ok(v) => v.into_response(),
            Err(er) => er.into_response(),
        }
    }
}

impl IntoResponse for eyre::Report {
    fn into_response(self) -> Response {
        Response::Error(json!({"error": self.to_string() }))
    }
}

impl<T: Serialize + fmt::Debug> IntoResponse for Payload<T> {
    fn into_response(self) -> Response {
        Response::Message(serde_json::to_value(self.0).expect("should not fail"))
    }
}
