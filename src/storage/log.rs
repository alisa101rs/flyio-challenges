use std::{collections::BTreeMap, ops::RangeBounds};

pub trait Storage<V: Clone> {
    fn append(&mut self, value: V) -> u64;
    fn scan<'a>(&'a self, range: impl RangeBounds<u64>) -> Box<dyn Iterator<Item = (u64, V)> + 'a>;
    fn commit(&mut self, index: u64);
    fn last_committed(&self) -> Option<u64>;
    fn truncate(&mut self, index: u64);
    fn len(&self) -> u64;
}

#[derive(Debug)]
pub struct Memory<V> {
    log: BTreeMap<u64, V>,
    committed: Option<u64>,
}

impl<V> Default for Memory<V> {
    fn default() -> Self {
        Self {
            log: BTreeMap::new(),
            committed: None,
        }
    }
}

impl<V: Clone> Storage<V> for Memory<V> {
    fn append(&mut self, value: V) -> u64 {
        self.log.insert(self.log.len() as _, value);
        self.log.len() as _
    }

    fn scan<'a>(&'a self, range: impl RangeBounds<u64>) -> Box<dyn Iterator<Item = (u64, V)> + 'a> {
        Box::new(self.log.range(range).map(|(k, v)| (*k, v.clone())))
    }

    fn commit(&mut self, index: u64) {
        assert!(index <= self.len());

        self.committed = Some(index);
    }

    fn last_committed(&self) -> Option<u64> {
        self.committed
    }

    fn truncate(&mut self, index: u64) {
        if let Some(commit) = self.committed {
            assert!(index > commit);
        }

        let _ = self.log.split_off(&index);
    }

    fn len(&self) -> u64 {
        self.log.len() as _
    }
}
