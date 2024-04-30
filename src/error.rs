use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use strum::FromRepr;
use thiserror::Error;

#[derive(Debug, Clone, Error, Serialize, Deserialize)]
#[error("{}", text)]
pub struct Error {
    text: String,
    pub code: ErrorCode,
}

impl From<ErrorCode> for Error {
    fn from(code: ErrorCode) -> Self {
        Self {
            text: code.to_string(),
            code,
        }
    }
}

#[derive(Debug, Clone, Copy, Error, FromRepr, Serialize_repr, Deserialize_repr)]
#[repr(u8)]
pub enum ErrorCode {
    #[error("Timeout")]
    Timeout = 0,
    #[error("todo")]
    NodeNotFound = 1,
    #[error("todo")]
    NotSupported = 10,
    #[error("Temporary unavailable")]
    TemporaryUnavailable = 11,
    #[error("todo")]
    MalformedRequest = 12,
    #[error("todo")]
    Crash = 13,
    #[error("todo")]
    Abort = 14,
    #[error("Key does not exist")]
    KeyDoesNotExist = 20,
    #[error("Key already exists")]
    KeyAlreadyExists = 21,
    #[error("Precondition failed")]
    PreconditionFailed = 22,
    #[error("Conflict")]
    TxnConflict = 30,
}
