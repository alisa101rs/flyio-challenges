use std::{
    collections::BTreeSet,
    sync::atomic::{AtomicU64, Ordering},
};

use derivative::Derivative;
use derive_more::{From, Into};
use parking_lot::Mutex;

use crate::{Log, Offset};

#[derive(Debug, Copy, Clone, Derivative, PartialEq, Eq, PartialOrd, Ord, From, Into)]
#[derivative(Hash)]
struct LogEntry {
    offset: Offset,
    #[derivative(Hash = "ignore")]
    log: u64,
}

#[derive(Debug)]
pub struct LogBucket {
    offset: AtomicU64,
    logs: Mutex<BTreeSet<LogEntry>>,
}

impl LogBucket {
    pub fn new() -> Self {
        Self {
            offset: AtomicU64::new(1),
            logs: Mutex::new(Default::default()),
        }
    }

    pub fn next_offset(&self) -> Offset {
        self.offset.fetch_add(1, Ordering::SeqCst)
    }

    pub fn insert(&self, messages: impl IntoIterator<Item = (Offset, u64)>) {
        let mut logs = self.logs.lock();
        logs.extend(messages.into_iter().map(LogEntry::from));
    }

    pub fn set_offset(&self, offset: Offset) {
        self.offset.store(offset, Ordering::SeqCst);
    }

    pub fn poll(&self, offset: Offset) -> Vec<Log> {
        self.logs
            .lock()
            .iter()
            .skip_while(|&entry| entry.offset < offset)
            .cloned()
            .map(|it| it.into())
            .collect()
    }
}
