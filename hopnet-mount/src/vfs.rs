//! MountCore: fuser-free namespace logic — the tested layer.
//!
//! The FUSE adapter (`fuse` module) is a thin errno-mapping shim over
//! this; everything behavioural lives here so it runs against the mock
//! transport with no kernel and no node. (Module is named `vfs` rather
//! than `core` to avoid shadowing the `core` prelude crate.)

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::attrs::AttrCache;
use crate::idmap::{IdMap, ROOT_INO};
use crate::transport::{Item, ItemId, ItemKind, NodeTransport, TransportError};

#[derive(Debug)]
pub enum CoreError {
    /// Unknown inode number, unknown name, or item gone. (ENOENT)
    NotFound,
    /// Directory op on a non-directory. (ENOTDIR)
    NotADirectory,
    /// File open() on a folder. (EISDIR)
    IsADirectory,
    /// Directory handle not (or no longer) open. (EBADF)
    StaleHandle,
    /// Transport failure — node unreachable, protocol error. (EIO)
    Transport(TransportError),
    /// Content cache failure. (EIO)
    Cache(crate::cache::CacheError),
}

impl std::fmt::Display for CoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CoreError::NotFound => write!(f, "not found"),
            CoreError::NotADirectory => write!(f, "not a directory"),
            CoreError::IsADirectory => write!(f, "is a directory"),
            CoreError::StaleHandle => write!(f, "stale handle"),
            CoreError::Transport(e) => write!(f, "transport: {e}"),
            CoreError::Cache(e) => write!(f, "cache: {e}"),
        }
    }
}

impl std::error::Error for CoreError {}

impl From<TransportError> for CoreError {
    fn from(e: TransportError) -> Self {
        CoreError::Transport(e)
    }
}

/// An item paired with its allocated inode number — what the FUSE
/// adapter turns into a `FileAttr`.
#[derive(Debug, Clone)]
pub struct NodeAttr {
    pub ino: u64,
    pub item: Item,
}

#[derive(Debug, Clone)]
pub struct DirEntry {
    pub ino: u64,
    pub name: String,
    pub is_dir: bool,
}

/// Snapshot taken at open(): the blob and size the handle serves for its
/// whole life, regardless of concurrent remote modifies (RFC-018
/// snapshot-at-open — the reason downloads are blob-addressed).
///
/// Scope note: this pins what the DAEMON serves per handle. The kernel
/// page cache is per-inode, so after an inval_inode a concurrent fresh
/// read can populate pages an older fd will then see — the same
/// visibility semantics as overwriting a local file in place. What the
/// snapshot rules out is the daemon mixing blobs under one handle (and
/// the GC race on a displaced blob, issue #26).
struct OpenFile {
    blob: Option<hopnet_common::CustomUUID>,
    size: u64,
}

pub struct MountCore {
    transport: Arc<dyn NodeTransport>,
    ids: IdMap,
    attrs: AttrCache,
    dirs: Mutex<HashMap<u64, Arc<Vec<DirEntry>>>>,
    next_dir_handle: AtomicU64,
    cache: Option<Arc<crate::cache::CacheManager>>,
    files: Mutex<HashMap<u64, OpenFile>>,
    next_file_handle: AtomicU64,
}

impl MountCore {
    pub fn new(transport: Arc<dyn NodeTransport>, attr_ttl: Duration) -> Self {
        MountCore {
            transport,
            ids: IdMap::new(),
            attrs: AttrCache::new(attr_ttl),
            dirs: Mutex::new(HashMap::new()),
            next_dir_handle: AtomicU64::new(1),
            cache: None,
            files: Mutex::new(HashMap::new()),
            next_file_handle: AtomicU64::new(1),
        }
    }

    /// Attach the content cache (S5). Reads fail with EIO until attached.
    pub fn with_cache(mut self, cache: Arc<crate::cache::CacheManager>) -> Self {
        self.cache = Some(cache);
        self
    }

    pub async fn lookup(&self, parent_ino: u64, name: &str) -> Result<NodeAttr, CoreError> {
        let parent = self.ids.get(parent_ino).ok_or(CoreError::NotFound)?;
        match self
            .transport
            .lookup(parent, name.to_string())
            .await?
        {
            None => Err(CoreError::NotFound),
            Some(item) => {
                self.attrs.insert(item.clone());
                Ok(NodeAttr {
                    ino: self.ids.ino(&item.id),
                    item,
                })
            }
        }
    }

    pub async fn getattr(&self, ino: u64) -> Result<NodeAttr, CoreError> {
        let id = self.ids.get(ino).ok_or(CoreError::NotFound)?;
        if let Some(item) = self.attrs.get(&id) {
            return Ok(NodeAttr { ino, item });
        }
        match self.transport.item(id).await? {
            None => Err(CoreError::NotFound),
            Some(item) => {
                self.attrs.insert(item.clone());
                Ok(NodeAttr { ino, item })
            }
        }
    }

    /// Open a directory: snapshot the full listing (walking all enumerate
    /// pages) into a handle. readdir then serves offsets from the
    /// immutable snapshot, so concurrent mutation cannot skip or
    /// duplicate entries mid-listing; a fresh opendir observes changes.
    pub async fn opendir(&self, ino: u64) -> Result<u64, CoreError> {
        let attr = self.getattr(ino).await?;
        if !matches!(attr.item.kind, ItemKind::Folder) {
            return Err(CoreError::NotADirectory);
        }

        let parent_ino = if ino == ROOT_INO {
            ROOT_INO
        } else {
            self.ids.ino(&attr.item.parent)
        };
        let mut entries = vec![
            DirEntry {
                ino,
                name: ".".to_string(),
                is_dir: true,
            },
            DirEntry {
                ino: parent_ino,
                name: "..".to_string(),
                is_dir: true,
            },
        ];

        let dir_id = attr.item.id;
        let mut cursor = None;
        loop {
            let page = self.transport.enumerate(dir_id.clone(), cursor).await?;
            for item in page.items {
                self.attrs.insert(item.clone());
                entries.push(DirEntry {
                    ino: self.ids.ino(&item.id),
                    name: item.name.clone(),
                    is_dir: matches!(item.kind, ItemKind::Folder),
                });
            }
            match page.next {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }

        let fh = self.next_dir_handle.fetch_add(1, Ordering::Relaxed);
        self.dirs
            .lock()
            .expect("dir table poisoned")
            .insert(fh, Arc::new(entries));
        Ok(fh)
    }

    /// The snapshot behind an open directory handle. The FUSE adapter
    /// slices it from the kernel-supplied offset.
    pub fn dir_entries(&self, fh: u64) -> Result<Arc<Vec<DirEntry>>, CoreError> {
        self.dirs
            .lock()
            .expect("dir table poisoned")
            .get(&fh)
            .cloned()
            .ok_or(CoreError::StaleHandle)
    }

    pub fn releasedir(&self, fh: u64) {
        self.dirs.lock().expect("dir table poisoned").remove(&fh);
    }

    /// Open a file for reading (RFC-018 S5): captures (blob, size) at
    /// open — the handle's snapshot. Folders are EISDIR.
    pub async fn open(&self, ino: u64) -> Result<u64, CoreError> {
        let attr = self.getattr(ino).await?;
        let size = match attr.item.kind {
            ItemKind::Folder => return Err(CoreError::IsADirectory),
            ItemKind::File { size } => size,
        };
        let fh = self.next_file_handle.fetch_add(1, Ordering::Relaxed);
        self.files.lock().expect("file table poisoned").insert(
            fh,
            OpenFile {
                blob: attr.item.blob.clone(),
                size,
            },
        );
        Ok(fh)
    }

    /// Read from an open handle. Short only at EOF; empty files (no
    /// blob) are all-EOF.
    pub async fn read(&self, fh: u64, offset: u64, len: u64) -> Result<Vec<u8>, CoreError> {
        let (blob, size) = {
            let files = self.files.lock().expect("file table poisoned");
            let open = files.get(&fh).ok_or(CoreError::StaleHandle)?;
            (open.blob.clone(), open.size)
        };
        let Some(blob) = blob else {
            return Ok(Vec::new());
        };
        let cache = self.cache.as_ref().ok_or_else(|| {
            CoreError::Cache(crate::cache::CacheError::Io("no cache attached".to_string()))
        })?;
        cache
            .read(&blob, size, offset, len)
            .await
            .map_err(CoreError::Cache)
    }

    pub fn release(&self, fh: u64) {
        self.files.lock().expect("file table poisoned").remove(&fh);
    }

    /// Cached-bytes gauge (tests, future metrics). None without a cache.
    pub fn cache_stats(&self) -> Option<u64> {
        self.cache.as_ref().map(|c| c.cached_bytes())
    }

    /// Poke-driven invalidation entry point (wired to /watch in S4; used
    /// directly by tests now).
    pub fn invalidate(&self, id: &ItemId) {
        self.attrs.invalidate(id);
    }

    /// Apply a /changes delta (RFC-018 S4): refresh the attr cache and
    /// compute the kernel invalidations the watch loop must fire.
    ///
    /// Kernel state is only busted for items the kernel has seen (idmap
    /// peek — never allocates); everything else is cache-refresh only,
    /// since directory listings are re-enumerated per opendir anyway.
    pub fn apply_changes(&self, changes: &crate::transport::Changes) -> Vec<Invalidation> {
        let mut invalidations = Vec::new();

        for item in &changes.items {
            let old = self.attrs.get_stale(&item.id);
            if let Some(ino) = self.ids.peek(&item.id) {
                invalidations.push(Invalidation::Inode { ino });
                // Entry invalidation on the (parent, name) pairs the
                // kernel may hold: the old location (if we knew it and it
                // moved) and the current one.
                if let Some(old) = &old {
                    let moved = old.parent != item.parent || old.name != item.name;
                    if moved {
                        if let Some(old_parent_ino) = self.ids.peek(&old.parent) {
                            invalidations.push(Invalidation::Entry {
                                parent_ino: old_parent_ino,
                                name: old.name.clone(),
                            });
                        }
                    }
                }
                if let Some(parent_ino) = self.ids.peek(&item.parent) {
                    invalidations.push(Invalidation::Entry {
                        parent_ino,
                        name: item.name.clone(),
                    });
                }
            }
            // Fresh state into the cache — this is what makes the TTL a
            // backstop rather than the freshness mechanism.
            self.attrs.insert(item.clone());
        }

        for gone in &changes.deleted {
            let id = ItemId::Inode(gone.clone());
            if let Some(old) = self.attrs.get_stale(&id) {
                if let Some(parent_ino) = self.ids.peek(&old.parent) {
                    invalidations.push(Invalidation::Entry {
                        parent_ino,
                        name: old.name.clone(),
                    });
                }
            }
            if let Some(ino) = self.ids.peek(&id) {
                invalidations.push(Invalidation::Inode { ino });
            }
            self.attrs.invalidate(&id);
        }

        invalidations
    }
}

/// A kernel cache bust the watch loop must fire after applying changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Invalidation {
    Entry { parent_ino: u64, name: String },
    Inode { ino: u64 },
}
