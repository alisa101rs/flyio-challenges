use std::{collections::BTreeMap, sync::Arc};

use flyio_rs::error::ErrorCode;
use parking_lot::Mutex;

#[derive(Debug, Clone)]
pub struct Storage {
    raw: Arc<Mutex<BTreeMap<u64, u64>>>,
}

impl Storage {
    pub fn create() -> Self {
        Self {
            raw: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn read(&self, key: u64) -> Option<u64> {
        self.raw.lock().get(&key).cloned()
    }

    pub fn write(&self, key: u64, value: u64) {
        let _ = self.raw.lock().insert(key, value);
    }

    pub fn cas(&self, key: u64, from: u64, to: u64) -> Result<(), ErrorCode> {
        let mut s = self.raw.lock();
        let Some(v) = s.get_mut(&key) else {
            return Err(ErrorCode::KeyDoesNotExist);
        };
        if *v != from {
            return Err(ErrorCode::PreconditionFailed);
        }
        *v = to;

        Ok(())
    }
}
