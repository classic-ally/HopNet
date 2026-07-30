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
    /// Directory handle not (or no longer) open. (EBADF)
    StaleHandle,
    /// Transport failure — node unreachable, protocol error. (EIO)
    Transport(TransportError),
}

impl std::fmt::Display for CoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CoreError::NotFound => write!(f, "not found"),
            CoreError::NotADirectory => write!(f, "not a directory"),
            CoreError::StaleHandle => write!(f, "stale directory handle"),
            CoreError::Transport(e) => write!(f, "transport: {e}"),
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

pub struct MountCore {
    transport: Arc<dyn NodeTransport>,
    ids: IdMap,
    attrs: AttrCache,
    dirs: Mutex<HashMap<u64, Arc<Vec<DirEntry>>>>,
    next_dir_handle: AtomicU64,
}

impl MountCore {
    pub fn new(transport: Arc<dyn NodeTransport>, attr_ttl: Duration) -> Self {
        MountCore {
            transport,
            ids: IdMap::new(),
            attrs: AttrCache::new(attr_ttl),
            dirs: Mutex::new(HashMap::new()),
            next_dir_handle: AtomicU64::new(1),
        }
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

    /// Poke-driven invalidation entry point (wired to /watch in S4; used
    /// directly by tests now).
    pub fn invalidate(&self, id: &ItemId) {
        self.attrs.invalidate(id);
    }
}
