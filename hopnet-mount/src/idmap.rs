//! u64 ⇄ ItemId allocation for FUSE inode numbers.
//!
//! Daemon-lifetime, monotonic, never reused (generation stays 0);
//! FUSE_ROOT_ID (1) is pre-seeded to `ItemId::Root`. Rebuilt from scratch
//! on daemon restart — inode numbers are not stable across restarts, which
//! FUSE permits.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::transport::ItemId;

pub const ROOT_INO: u64 = 1;

pub struct IdMap {
    inner: Mutex<Inner>,
}

struct Inner {
    by_ino: HashMap<u64, ItemId>,
    by_id: HashMap<ItemId, u64>,
    next: u64,
}

impl IdMap {
    pub fn new() -> Self {
        let mut by_ino = HashMap::new();
        let mut by_id = HashMap::new();
        by_ino.insert(ROOT_INO, ItemId::Root);
        by_id.insert(ItemId::Root, ROOT_INO);
        IdMap {
            inner: Mutex::new(Inner {
                by_ino,
                by_id,
                next: ROOT_INO + 1,
            }),
        }
    }

    /// The inode number for `id`, allocating one on first sight.
    pub fn ino(&self, id: &ItemId) -> u64 {
        let mut inner = self.inner.lock().expect("idmap poisoned");
        if let Some(&ino) = inner.by_id.get(id) {
            return ino;
        }
        let ino = inner.next;
        inner.next += 1;
        inner.by_ino.insert(ino, id.clone());
        inner.by_id.insert(id.clone(), ino);
        ino
    }

    /// The id previously allocated for `ino`, if any.
    pub fn get(&self, ino: u64) -> Option<ItemId> {
        self.inner
            .lock()
            .expect("idmap poisoned")
            .by_ino
            .get(&ino)
            .cloned()
    }

    /// The inode number for `id` ONLY if one was already allocated.
    /// Kernel invalidation must not mint numbers for items the kernel has
    /// never seen (RFC-018 S4).
    pub fn peek(&self, id: &ItemId) -> Option<u64> {
        self.inner
            .lock()
            .expect("idmap poisoned")
            .by_id
            .get(id)
            .copied()
    }
}

impl Default for IdMap {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hopnet_common::CustomUUID;

    // Should: hand back the same inode number for the same item id on
    // every call, and resolve that number back to the id.
    // Should not: reuse a number for a different id.
    #[test]
    fn allocation_is_stable_and_bijective() {
        let map = IdMap::new();
        let a = ItemId::Inode(CustomUUID::new(None));
        let b = ItemId::Inode(CustomUUID::new(None));

        let ino_a = map.ino(&a);
        let ino_b = map.ino(&b);
        assert_ne!(ino_a, ino_b);
        assert_eq!(map.ino(&a), ino_a);
        assert_eq!(map.get(ino_a), Some(a));
        assert_eq!(map.get(ino_b), Some(b));
    }

    // Should: pre-seed FUSE_ROOT_ID (1) to the drive root without any
    // allocation call.
    #[test]
    fn root_is_preseeded() {
        let map = IdMap::new();
        assert_eq!(map.get(ROOT_INO), Some(ItemId::Root));
        assert_eq!(map.ino(&ItemId::Root), ROOT_INO);
    }

    // Should: return None for an inode number that was never allocated.
    #[test]
    fn unknown_ino_is_none() {
        let map = IdMap::new();
        assert_eq!(map.get(42), None);
    }
}
