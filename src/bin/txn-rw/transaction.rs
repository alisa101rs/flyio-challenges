use flyio_rs::{
    request::{Extension, Payload},
    response::IntoResponse,
    storage::vector::{Storage, Transaction},
};
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::{
    clock::NodeClock, commit_log::OperationReplication, Error, Operation, OperationResult,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionRequest {
    txn: Vec<Operation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionResponse {
    txn: Vec<OperationResult>,
}

#[instrument(name = "txn", skip(storage, clock, replication), ret)]
pub async fn handler(
    Extension(storage): Extension<super::Storage>,
    Extension(clock): Extension<NodeClock>,
    Extension(replication): Extension<OperationReplication>,
    Payload(txn): Payload<TransactionRequest>,
) -> Result<impl IntoResponse, Error> {
    let now = clock.next();
    let mut transaction = storage.begin(now.clone());
    let mut res = vec![];
    let mut ops = vec![];

    for op in txn.txn {
        match op {
            Operation::Read { key } => res.push(OperationResult::Read {
                key,
                value: transaction.read(&key),
            }),
            Operation::Write { key, value } => {
                transaction.write(key, value);
                res.push(OperationResult::Write { key, value });
                ops.push(Operation::Write { key, value });
            }
        }
    }

    transaction.commit();

    let _ = replication.send((now, ops));

    Ok(Payload(TransactionResponse { txn: res }))
}
