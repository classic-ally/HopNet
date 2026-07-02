//! Keyed async locks: per-`(library_id, content_hash)` finalize exclusivity
//! (spec §blobs notes — at most one inflight materialization per blob).
//! The hash is only known post-stream, so the lock brackets the
//! finish → finalize window, preventing two temps racing one rename target.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::Mutex as AsyncMutex;

use crate::ids::{ContentHash, LibraryId};

type LockMap = HashMap<(LibraryId, ContentHash), Arc<AsyncMutex<()>>>;

#[derive(Default)]
pub struct KeyedLocks {
    inner: Mutex<LockMap>,
}

impl KeyedLocks {
    pub fn new() -> Self {
        Self::default()
    }

    /// The lock for a key, created on first use. Entries are never removed —
    /// bounded by distinct blobs touched in one drain, negligible.
    pub fn lock_for(&self, library: &LibraryId, hash: &ContentHash) -> Arc<AsyncMutex<()>> {
        self.inner
            .lock()
            .expect("keyed locks mutex")
            .entry((library.clone(), hash.clone()))
            .or_default()
            .clone()
    }
}
