//! Attr cache: cache-until-poked (RFC-018).
//!
//! S1 wires only the TTL backstop. /watch-driven invalidation lands in S4,
//! at which point the poke path is the freshness mechanism and this TTL is
//! a safety net, not the contract.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::transport::{Item, ItemId};

/// Backstop TTL until S4's invalidation wiring makes freshness poke-driven.
pub const DEFAULT_TTL: Duration = Duration::from_secs(60);

pub struct AttrCache {
    ttl: Duration,
    inner: Mutex<HashMap<ItemId, Entry>>,
}

struct Entry {
    item: Item,
    at: Instant,
}

impl AttrCache {
    pub fn new(ttl: Duration) -> Self {
        AttrCache {
            ttl,
            inner: Mutex::new(HashMap::new()),
        }
    }

    /// Fresh cached state for `id`, if present and within TTL.
    pub fn get(&self, id: &ItemId) -> Option<Item> {
        let inner = self.inner.lock().expect("attr cache poisoned");
        inner
            .get(id)
            .filter(|e| e.at.elapsed() < self.ttl)
            .map(|e| e.item.clone())
    }

    pub fn insert(&self, item: Item) {
        let mut inner = self.inner.lock().expect("attr cache poisoned");
        inner.insert(
            item.id.clone(),
            Entry {
                item,
                at: Instant::now(),
            },
        );
    }

    pub fn invalidate(&self, id: &ItemId) {
        self.inner.lock().expect("attr cache poisoned").remove(id);
    }
}
