use std::{collections::Bound, ops::RangeBounds};

pub trait Storage<V: Clone> {
    fn append(&mut self, value: V) -> u64;
    fn scan<'a>(&'a self, range: impl RangeBounds<u64>) -> Box<dyn Iterator<Item = (u64, V)> + 'a>;
    fn commit(&mut self, index: u64);
    fn last_committed(&self) -> Option<u64>;
    fn truncate(&mut self, index: u64);
    fn len(&self) -> u64;
}

#[derive(Debug, Default)]
pub struct Memory<V> {
    log: Vec<V>,
    committed: Option<u64>,
}

impl<V: Clone> Storage<V> for Memory<V> {
    fn append(&mut self, value: V) -> u64 {
        self.log.push(value);
        self.log.len() as _
    }

    fn scan<'a>(&'a self, range: impl RangeBounds<u64>) -> Box<dyn Iterator<Item = (u64, V)> + 'a> {
        let start = match range.start_bound() {
            Bound::Included(s) => *s as usize,
            Bound::Excluded(s) => *s as usize + 1,
            Bound::Unbounded => 0,
        };

        let items = match range.end_bound() {
            Bound::Included(e) => (*e as usize) - start,
            Bound::Excluded(e) => (*e as usize - 1) - start,
            Bound::Unbounded => usize::MAX,
        };

        Box::new(
            self.log
                .iter()
                .enumerate()
                .map(|(k, v)| (k as u64, v.clone()))
                .skip(start)
                .take(items),
        )
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

        self.log.drain((index as usize)..);
    }

    fn len(&self) -> u64 {
        self.log.len() as _
    }
}
