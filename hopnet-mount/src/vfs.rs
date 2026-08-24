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
use crate::transport::{Height, Item, ItemId, ItemKind, NodeTransport, TransportError};

/// statfs numbers refresh at most this often (node-side full-table scan).
const STATFS_TTL: Duration = Duration::from_secs(15);

#[derive(Debug)]
pub enum CoreError {
    /// Unknown inode number, unknown name, or item gone. (ENOENT)
    NotFound,
    /// Directory op on a non-directory. (ENOTDIR)
    NotADirectory,
    /// File open() on a folder. (EISDIR)
    IsADirectory,
    /// Name already taken under the parent. (EEXIST)
    AlreadyExists,
    /// rmdir of a non-empty folder. (ENOTEMPTY)
    NotEmpty,
    /// Directory handle not (or no longer) open. (EBADF)
    StaleHandle,
    /// Staging IO failure. (EIO)
    Staging(String),
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
            CoreError::AlreadyExists => write!(f, "already exists"),
            CoreError::NotEmpty => write!(f, "not empty"),
            CoreError::StaleHandle => write!(f, "stale handle"),
            CoreError::Staging(why) => write!(f, "staging: {why}"),
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
    /// Item identity (files only reach this table, never Root).
    id: ItemId,
    ino: u64,
    blob: Option<hopnet_common::CustomUUID>,
    size: u64,
    /// Height of the item state this session is based on (conflict
    /// detection at upload).
    base_height: Height,
    /// Write-back state (S7): staging appears on first mutation
    /// (copy-up); `dirty` tracks unuploaded bytes.
    write: tokio::sync::Mutex<Option<crate::staging::StagedFile>>,
    dirty: std::sync::atomic::AtomicBool,
}

pub struct MountCore {
    transport: Arc<dyn NodeTransport>,
    ids: IdMap,
    attrs: AttrCache,
    dirs: Mutex<HashMap<u64, Arc<Vec<DirEntry>>>>,
    next_dir_handle: AtomicU64,
    cache: Option<Arc<crate::cache::CacheManager>>,
    staging: Option<Arc<crate::staging::Staging>>,
    files: Mutex<HashMap<u64, Arc<OpenFile>>>,
    next_file_handle: AtomicU64,
    /// Serializes uploads per inode across write sessions.
    upload_locks: Mutex<HashMap<hopnet_common::CustomUUID, Arc<tokio::sync::Mutex<()>>>>,
    /// LWW conflicts observed at upload (issue #26 owns resolution).
    conflicts: AtomicU64,
    /// statfs TTL cache (S8): file managers hammer statfs, and the
    /// node-side number is a full-table scan. Also the last-known store
    /// — a transport blip must not turn `df` into an error.
    statfs: Mutex<StatfsState>,
    /// Daemon-mediated read count (S9 signal: passthrough bypasses it).
    reads: AtomicU64,
}

#[derive(Default)]
struct StatfsState {
    info: Option<crate::transport::StatfsInfo>,
    /// tokio Instant, not std: the paused-clock tests advance it.
    fetched_at: Option<tokio::time::Instant>,
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
            staging: None,
            files: Mutex::new(HashMap::new()),
            next_file_handle: AtomicU64::new(1),
            upload_locks: Mutex::new(HashMap::new()),
            conflicts: AtomicU64::new(0),
            statfs: Mutex::new(StatfsState::default()),
            reads: AtomicU64::new(0),
        }
    }

    /// Attach the content cache (S5). Reads fail with EIO until attached.
    pub fn with_cache(mut self, cache: Arc<crate::cache::CacheManager>) -> Self {
        self.cache = Some(cache);
        self
    }

    /// Attach durable write staging (S7). Writes fail until attached.
    pub fn with_staging(mut self, staging: Arc<crate::staging::Staging>) -> Self {
        self.staging = Some(staging);
        self
    }

    /// LWW conflicts observed at upload time (test/metrics gauge).
    pub fn conflicts(&self) -> u64 {
        self.conflicts.load(Ordering::Relaxed)
    }

    /// Mesh capacity for the statfs arm, TTL-cached and never failing:
    /// within the TTL the cached numbers answer directly; past it we
    /// refetch, and a transport error falls back to the last-known
    /// numbers (zeros before the first success). Concurrent expiry may
    /// double-fetch — harmless, the route is a read.
    pub async fn statfs(&self) -> crate::transport::StatfsInfo {
        let cached = {
            let state = self.statfs.lock().expect("statfs poisoned");
            match (state.info, state.fetched_at) {
                (Some(info), Some(at)) if at.elapsed() < STATFS_TTL => return info,
                (info, _) => info,
            }
        };
        match self.transport.statfs().await {
            Ok(info) => {
                let mut state = self.statfs.lock().expect("statfs poisoned");
                state.info = Some(info);
                state.fetched_at = Some(tokio::time::Instant::now());
                info
            }
            Err(e) => {
                tracing::debug!("statfs fetch failed, serving last-known: {e}");
                cached.unwrap_or(crate::transport::StatfsInfo {
                    total_bytes: 0,
                    used_bytes: 0,
                })
            }
        }
    }

    pub async fn lookup(&self, parent_ino: u64, name: &str) -> Result<NodeAttr, CoreError> {
        let parent = self.ids.get(parent_ino).ok_or(CoreError::NotFound)?;
        match self.transport.lookup(parent, name.to_string()).await? {
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
        self.open_inner(ino, false).await
    }

    /// Open with write intent (S7); `truncate` = O_TRUNC (staging starts
    /// empty, no copy-up).
    pub async fn open_rw(&self, ino: u64, truncate: bool) -> Result<u64, CoreError> {
        let fh = self.open_inner(ino, truncate).await?;
        if truncate {
            let state = self.handle(fh)?;
            let mut write = state.write.lock().await;
            self.ensure_staged(&state, &mut write, true).await?;
        }
        Ok(fh)
    }

    async fn open_inner(&self, ino: u64, _truncate: bool) -> Result<u64, CoreError> {
        let attr = self.getattr(ino).await?;
        let size = match attr.item.kind {
            ItemKind::Folder => return Err(CoreError::IsADirectory),
            ItemKind::File { size } => size,
        };
        let fh = self.next_file_handle.fetch_add(1, Ordering::Relaxed);
        self.files.lock().expect("file table poisoned").insert(
            fh,
            Arc::new(OpenFile {
                id: attr.item.id.clone(),
                ino,
                blob: attr.item.blob.clone(),
                size,
                base_height: attr.item.height,
                write: tokio::sync::Mutex::new(None),
                dirty: std::sync::atomic::AtomicBool::new(false),
            }),
        );
        Ok(fh)
    }

    fn handle(&self, fh: u64) -> Result<Arc<OpenFile>, CoreError> {
        self.files
            .lock()
            .expect("file table poisoned")
            .get(&fh)
            .cloned()
            .ok_or(CoreError::StaleHandle)
    }

    /// Daemon-mediated reads served so far — the passthrough proof
    /// signal (S9): with a backing fd registered, this must not move.
    pub fn read_calls(&self) -> u64 {
        self.reads.load(Ordering::Relaxed)
    }

    /// Passthrough eligibility (S9): the handle's snapshot blob, IF the
    /// cache holds it complete right now. `None` for empty files (no
    /// blob), dirty handles (staged bytes must stay daemon-served), or
    /// any missing/in-flight segment. No hydration here — the RFC's
    /// "subsequent open" semantics: a cold first open stays
    /// daemon-mediated and completes the bitmap as it reads.
    pub fn backing_for(&self, fh: u64) -> Option<crate::cache::Backing> {
        let state = self.handle(fh).ok()?;
        if state.dirty.load(Ordering::Acquire) {
            return None;
        }
        let blob = state.blob.as_ref()?;
        self.cache.as_ref()?.backing(blob)
    }

    /// Read from an open handle. Dirty handles serve staged bytes
    /// (read-your-writes); otherwise the snapshot blob via the cache.
    pub async fn read(&self, fh: u64, offset: u64, len: u64) -> Result<Vec<u8>, CoreError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        let state = self.handle(fh)?;
        {
            let write = state.write.lock().await;
            if let Some(staged) = write.as_ref() {
                return staged
                    .read_at(offset, len)
                    .await
                    .map_err(|e| CoreError::Staging(e.to_string()));
            }
        }
        let Some(blob) = state.blob.clone() else {
            return Ok(Vec::new());
        };
        let cache = self.cache.as_ref().ok_or_else(|| {
            CoreError::Cache(crate::cache::CacheError::Io(
                "no cache attached".to_string(),
            ))
        })?;
        cache
            .read(&blob, state.size, offset, len)
            .await
            .map_err(CoreError::Cache)
    }

    /// Ensure the handle has a staging file, copying up existing content
    /// on first touch (unless truncating).
    async fn ensure_staged(
        &self,
        state: &OpenFile,
        write: &mut Option<crate::staging::StagedFile>,
        truncate: bool,
    ) -> Result<(), CoreError> {
        if write.is_some() {
            return Ok(());
        }
        let staging = self
            .staging
            .as_ref()
            .ok_or_else(|| CoreError::Staging("no staging attached".to_string()))?;
        let ItemId::Inode(inode_id) = &state.id else {
            return Err(CoreError::IsADirectory);
        };
        let staged = staging
            .begin(crate::staging::StagedMeta {
                inode_id: inode_id.clone(),
                base_height: state.base_height,
            })
            .map_err(|e| CoreError::Staging(e.to_string()))?;

        // Copy-up: whole-file (the substrate's whole-blob-rewrite
        // semantic; deltas are issue #25).
        if !truncate && state.size > 0 {
            if let Some(blob) = &state.blob {
                let cache = self.cache.as_ref().ok_or_else(|| {
                    CoreError::Cache(crate::cache::CacheError::Io(
                        "no cache attached".to_string(),
                    ))
                })?;
                const COPY_CHUNK: u64 = 4 * 1024 * 1024;
                let mut offset = 0u64;
                while offset < state.size {
                    let chunk = cache
                        .read(blob, state.size, offset, COPY_CHUNK)
                        .await
                        .map_err(CoreError::Cache)?;
                    if chunk.is_empty() {
                        break;
                    }
                    let advanced = chunk.len() as u64;
                    staged
                        .write_at(offset, chunk)
                        .await
                        .map_err(|e| CoreError::Staging(e.to_string()))?;
                    offset += advanced;
                }
            }
        }
        *write = Some(staged);
        Ok(())
    }

    /// Write through an open handle (write-back: staged locally, uploaded
    /// on release/fsync).
    pub async fn write(&self, fh: u64, offset: u64, data: &[u8]) -> Result<u32, CoreError> {
        let state = self.handle(fh)?;
        let mut write = state.write.lock().await;
        self.ensure_staged(&state, &mut write, false).await?;
        write
            .as_ref()
            .expect("staged above")
            .write_at(offset, data.to_vec())
            .await
            .map_err(|e| CoreError::Staging(e.to_string()))?;
        state
            .dirty
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(data.len() as u32)
    }

    /// Truncate via a handle (ftruncate / setattr with fh).
    pub async fn truncate(&self, fh: u64, size: u64) -> Result<(), CoreError> {
        let state = self.handle(fh)?;
        let mut write = state.write.lock().await;
        // Truncate-to-zero needs no copy-up; anything else does.
        self.ensure_staged(&state, &mut write, size == 0).await?;
        write
            .as_ref()
            .expect("staged above")
            .set_len(size)
            .await
            .map_err(|e| CoreError::Staging(e.to_string()))?;
        state
            .dirty
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    /// Staged-size overlay for getattr: the freshest size the kernel
    /// should see while a write session is dirty.
    pub async fn staged_size(&self, ino: u64) -> Option<u64> {
        let handles: Vec<Arc<OpenFile>> = self
            .files
            .lock()
            .expect("file table poisoned")
            .values()
            .filter(|f| f.ino == ino)
            .cloned()
            .collect();
        for state in handles {
            let write = state.write.lock().await;
            if let Some(staged) = write.as_ref() {
                if let Ok(size) = staged.size() {
                    return Some(size);
                }
            }
        }
        None
    }

    /// The strict tier: upload the staged content NOW and return only on
    /// decided success. No-op if clean.
    pub async fn fsync(&self, fh: u64) -> Result<(), CoreError> {
        let state = self.handle(fh)?;
        if !state.dirty.load(std::sync::atomic::Ordering::Acquire) {
            return Ok(());
        }
        let write = state.write.lock().await;
        let Some(staged) = write.as_ref().cloned() else {
            return Ok(());
        };
        drop(write);
        self.upload_staged(&staged).await?;
        state
            .dirty
            .store(false, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    /// Release: drop the handle; dirty content uploads in the background
    /// (durable staging means a crash before upload loses nothing).
    pub fn release(self: &Arc<Self>, fh: u64) {
        let Some(state) = self.files.lock().expect("file table poisoned").remove(&fh) else {
            return;
        };
        if !state.dirty.load(std::sync::atomic::Ordering::Acquire) {
            // Clean session: discard any staging (fsync already uploaded).
            tokio::spawn(async move {
                let write = state.write.lock().await;
                if let Some(staged) = write.as_ref() {
                    staged.finish();
                }
            });
            return;
        }
        let this = self.clone();
        tokio::spawn(async move {
            let write = state.write.lock().await;
            let Some(staged) = write.as_ref().cloned() else {
                return;
            };
            drop(write);
            // Bounded retries; failures leave the durable pair for the
            // next recovery scan.
            for attempt in 0..3u32 {
                match this.upload_staged(&staged).await {
                    Ok(()) => {
                        staged.finish();
                        return;
                    }
                    Err(e) => {
                        tracing::warn!("background upload attempt {attempt} failed: {e}");
                        tokio::time::sleep(std::time::Duration::from_secs(1 << attempt)).await;
                    }
                }
            }
            tracing::error!("background upload gave up; staging retained for recovery");
        });
    }

    /// Upload one staged file's content (shared by fsync, release, and
    /// recovery). Per-inode lock serializes concurrent sessions.
    async fn upload_staged(&self, staged: &crate::staging::StagedFile) -> Result<(), CoreError> {
        let inode_id = staged.meta.inode_id.clone();
        let lock = {
            let mut locks = self.upload_locks.lock().expect("upload locks poisoned");
            locks
                .entry(inode_id.clone())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .clone()
        };
        let _guard = lock.lock().await;

        // Conflict detection (LWW per RFC; rollback is issue #26).
        match self.transport.item(ItemId::Inode(inode_id.clone())).await {
            Ok(Some(current)) if current.height > staged.meta.base_height => {
                self.conflicts.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    inode = %inode_id,
                    base = staged.meta.base_height,
                    current = current.height,
                    "remote modification during local write session — last writer wins (issue #26)"
                );
            }
            Ok(None) => {
                return Err(CoreError::NotFound);
            }
            _ => {}
        }

        let size = staged
            .size()
            .map_err(|e| CoreError::Staging(e.to_string()))?;
        let mutated = self
            .transport
            .update_content(inode_id, size, staged.byte_source())
            .await
            .map_err(CoreError::Transport)?;
        if let Some(item) = mutated.item {
            self.attrs.insert(item);
        }
        Ok(())
    }

    /// Startup recovery (S7): re-run the upload path for staging pairs a
    /// previous daemon run left behind. Target gone → park under
    /// orphaned/ (never delete user bytes).
    pub async fn recover(self: &Arc<Self>) {
        let Some(staging) = self.staging.clone() else {
            return;
        };
        for recovered in staging.scan() {
            let staged = recovered.staged;
            match self.upload_staged(&staged).await {
                Ok(()) => {
                    tracing::info!(inode = %staged.meta.inode_id, "recovered staged upload");
                    staged.finish();
                }
                Err(CoreError::NotFound) => {
                    tracing::error!(
                        inode = %staged.meta.inode_id,
                        "staged content's inode is gone; parking under orphaned/"
                    );
                    staged.orphan();
                }
                Err(e) => {
                    tracing::warn!("recovery upload failed (kept for next run): {e}");
                }
            }
        }
    }

    // ---- namespace mutations (S7): strict-synchronous ----

    fn map_create_err(e: TransportError) -> CoreError {
        match e {
            TransportError::Conflict(_) => CoreError::AlreadyExists,
            other => CoreError::Transport(other),
        }
    }

    /// Rename's conflict mapping differs from create's: the node's coded
    /// verdict discriminates the POSIX errnos, and a bare 409 under
    /// replace-allowed is a consensus rejection — POSIX forbids EEXIST
    /// there, so it surfaces as a transport error (EIO), logged.
    fn map_rename_err(replace: bool, e: TransportError) -> CoreError {
        use hopnet_common::mount::MountConflictCode as Code;
        match e {
            TransportError::Conflict(Some(Code::NotEmpty)) => CoreError::NotEmpty,
            TransportError::Conflict(Some(Code::IsDirectory)) => CoreError::IsADirectory,
            TransportError::Conflict(Some(Code::NotDirectory)) => CoreError::NotADirectory,
            TransportError::Conflict(code @ (Some(Code::Occupied) | None)) => {
                if replace {
                    // Occupied despite replace=true is a server bug or a
                    // pre-replace node; bare is a consensus rejection.
                    // Either way the kernel must not hear EEXIST.
                    tracing::warn!(
                        ?code,
                        "conflict on a replace-allowed rename; reporting EIO, not EEXIST"
                    );
                    CoreError::Transport(TransportError::Conflict(code))
                } else {
                    CoreError::AlreadyExists
                }
            }
            other => CoreError::Transport(other),
        }
    }

    /// mkdir: strict — the folder exists mesh-wide when this returns.
    pub async fn mkdir(&self, parent_ino: u64, name: &str) -> Result<NodeAttr, CoreError> {
        let parent = self.ids.get(parent_ino).ok_or(CoreError::NotFound)?;
        let mutated = self
            .transport
            .create_folder(parent, name.to_string())
            .await
            .map_err(Self::map_create_err)?;
        let item = mutated.item.ok_or(CoreError::NotFound)?;
        self.attrs.insert(item.clone());
        Ok(NodeAttr {
            ino: self.ids.ino(&item.id),
            item,
        })
    }

    /// create: strict empty file on the node, then a dirty write session
    /// (content follows on release/fsync).
    pub async fn create(
        self: &Arc<Self>,
        parent_ino: u64,
        name: &str,
    ) -> Result<(NodeAttr, u64), CoreError> {
        let parent = self.ids.get(parent_ino).ok_or(CoreError::NotFound)?;
        let empty: crate::transport::ByteSource =
            Box::pin(tokio_stream::empty::<std::io::Result<bytes::Bytes>>());
        let mutated = self
            .transport
            .create_file(parent, name.to_string(), 0, empty)
            .await
            .map_err(Self::map_create_err)?;
        let item = mutated.item.ok_or(CoreError::NotFound)?;
        self.attrs.insert(item.clone());
        let ino = self.ids.ino(&item.id);
        let fh = self.open_rw(ino, true).await?;
        Ok((NodeAttr { ino, item }, fh))
    }

    /// rename/move: strict. `replace` = POSIX semantics (false is
    /// RENAME_NOREPLACE).
    pub async fn rename(
        &self,
        parent_ino: u64,
        name: &str,
        new_parent_ino: u64,
        new_name: &str,
        replace: bool,
    ) -> Result<(), CoreError> {
        let parent = self.ids.get(parent_ino).ok_or(CoreError::NotFound)?;
        let new_parent = self.ids.get(new_parent_ino).ok_or(CoreError::NotFound)?;
        let child = self
            .transport
            .lookup(parent, name.to_string())
            .await?
            .ok_or(CoreError::NotFound)?;
        let ItemId::Inode(uuid) = &child.id else {
            return Err(CoreError::NotFound);
        };
        // Best-effort destination pre-lookup: a replacing rename kills the
        // occupant, whose cached attrs must not outlive it. The /changes
        // deletion delta is the backstop for every other daemon; this
        // closes the response→delta window locally. Failure is ignored —
        // it is an optimization, not a correctness gate.
        let occupant_id = if replace {
            match self
                .transport
                .lookup(new_parent.clone(), new_name.to_string())
                .await
            {
                Ok(Some(dest)) if dest.id != child.id => Some(dest.id),
                _ => None,
            }
        } else {
            None
        };
        let mutated = self
            .transport
            .rename(
                uuid.clone(),
                Some(new_parent),
                Some(new_name.to_string()),
                replace,
            )
            .await
            .map_err(|e| Self::map_rename_err(replace, e))?;
        self.attrs.invalidate(&child.id);
        if let Some(occupant_id) = occupant_id {
            self.attrs.invalidate(&occupant_id);
        }
        if let Some(item) = mutated.item {
            self.attrs.insert(item);
        }
        Ok(())
    }

    /// unlink (file) / rmdir (folder): strict. `dir` picks the
    /// non-empty-conflict mapping (ENOTEMPTY vs EEXIST-shaped).
    pub async fn remove(&self, parent_ino: u64, name: &str, dir: bool) -> Result<(), CoreError> {
        let parent = self.ids.get(parent_ino).ok_or(CoreError::NotFound)?;
        let child = self
            .transport
            .lookup(parent, name.to_string())
            .await?
            .ok_or(CoreError::NotFound)?;
        match (&child.kind, dir) {
            (ItemKind::Folder, false) => return Err(CoreError::IsADirectory),
            (ItemKind::File { .. }, true) => return Err(CoreError::NotADirectory),
            _ => {}
        }
        let ItemId::Inode(uuid) = &child.id else {
            return Err(CoreError::NotFound);
        };
        self.transport
            .delete(uuid.clone(), false)
            .await
            .map_err(|e| match e {
                TransportError::Conflict(_) => CoreError::NotEmpty,
                other => CoreError::Transport(other),
            })?;
        self.attrs.invalidate(&child.id);
        Ok(())
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
