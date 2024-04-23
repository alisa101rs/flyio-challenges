use std::{sync::Arc, time::Instant};

use dashmap::DashMap;

use crate::TransactionId;

pub type Key = u64;
pub type Value = u64;

#[derive(Clone)]
pub struct Storage {
    s: Arc<DashMap<Key, Entry>>,
}

impl Storage {
    pub fn new() -> Self {
        Self {
            s: Arc::new(DashMap::new()),
        }
    }

    pub fn read(&self, key: Key) -> Option<Value> {
        self.s.get(&key).map(|it| it.value)
    }
}

struct Entry {
    value: Value,
    last_modified: (TransactionId, Instant),
}
