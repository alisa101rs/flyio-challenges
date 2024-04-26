use std::{
    collections::{HashMap, HashSet, VecDeque},
    hash::Hash,
    marker::PhantomData,
    sync::Arc,
};

use dashmap::{mapref::entry::Entry, DashMap};
use parking_lot::Mutex;
use vclock::VClock64;

use crate::NodeId;

type TransactionId = VClock64<NodeId>;
type Clock = VClock64<NodeId>;

pub trait Storage<K: Hash + Eq + PartialEq, V: Clone> {
    type Transaction<'a>: Transaction<'a, K, V>
    where
        Self: 'a;

    fn begin<'a>(&'a self, at: Clock) -> Self::Transaction<'a>;
}

pub trait Transaction<'a, K: Hash + Eq + PartialEq, V: Clone> {
    fn write(&mut self, key: K, value: V);
    fn read(&mut self, key: &K) -> Option<V>;
    fn commit(self);
}

#[derive(Clone)]
pub struct DashVector<K, V> {
    storage: Arc<DashMap<K, KeyEntries<V>>>,
    #[allow(dead_code)]
    active_transactions: Arc<Mutex<HashSet<TransactionId>>>,
}

impl<K: Eq + PartialEq + Hash, V: Clone> DashVector<K, V> {
    pub fn new() -> Self {
        Self {
            storage: Arc::new(DashMap::new()),
            active_transactions: Arc::new(Mutex::new(HashSet::new())),
        }
    }
}

pub struct KeyEntries<V>(VecDeque<KeyEntry<V>>);

impl<V> KeyEntries<V> {
    pub fn new(value: V, at: Clock) -> Self {
        let mut vec = VecDeque::new();
        vec.push_front(KeyEntry { value, at });
        Self(vec)
    }

    pub fn add(&mut self, value: V, at: Clock) {
        if let Some(pos) = self.0.iter().rposition(|e| at > e.at) {
            self.0.insert(pos, KeyEntry { value, at });
        } else {
            self.0.push_front(KeyEntry { value, at });
        };
    }
}

impl<V> KeyEntries<V> {
    fn not_before(&self, time: &Clock) -> Option<&V> {
        for entry in self.0.iter().rev() {
            if &entry.at <= time {
                return Some(&entry.value);
            }
        }
        None
    }
}

pub struct KeyEntry<V> {
    value: V,
    at: Clock,
}

pub struct DashTransaction<'a, K, V> {
    storage: Arc<DashMap<K, KeyEntries<V>>>,
    at: Clock,
    writes: HashMap<K, V>,
    lifetime: PhantomData<&'a ()>,
}

impl<K: Hash + Eq + PartialEq, V: Clone> Storage<K, V> for DashVector<K, V> {
    type Transaction<'a> = DashTransaction<'a, K, V> where K: 'a, V: 'a;

    fn begin<'a>(&'a self, at: Clock) -> Self::Transaction<'a> {
        DashTransaction {
            storage: self.storage.clone(),
            at,
            writes: HashMap::new(),
            lifetime: Default::default(),
        }
    }
}

impl<'a, K: Hash + Eq + PartialEq, V: Clone> Transaction<'a, K, V> for DashTransaction<'a, K, V> {
    fn write(&mut self, key: K, value: V) {
        self.writes.insert(key, value);
    }

    fn read(&mut self, key: &K) -> Option<V> {
        if let Some(v) = self.writes.get(key) {
            return Some(v.clone());
        }
        let entries = self.storage.get(key)?;

        let v = entries
            .try_map(|e| e.not_before(&self.at))
            .ok()
            .map(|e| e.value().clone());

        v
    }

    fn commit(mut self) {
        for (key, value) in self.writes.drain() {
            match self.storage.entry(key) {
                Entry::Occupied(mut o) => {
                    o.get_mut().add(value, self.at.clone());
                }
                Entry::Vacant(v) => {
                    v.insert(KeyEntries::new(value, self.at.clone()));
                }
            }
        }
    }
}
