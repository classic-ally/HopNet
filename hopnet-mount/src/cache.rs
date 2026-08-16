//! Sparse content cache (RFC-018 S5): per-blob decode buffers with
//! segment-granular hydration, single-flight fetches, and disk-pressure
//! hole-punch eviction.
//!
//! Discipline (per the RFC): the cache has NO correctness role — any
//! segment may be punched at any moment and the only cost is a refetch;
//! logical file sizes never change (punch, never truncate); the whole
//! directory is wiped at startup (blob content is immutable per id, so
//! persistence via SEEK_DATA is a safe future optimization, not built).
//! Segment size defaults to the storage substrate's 40 MB chunk: the node
//! reconstructs whole chunks regardless of requested range, so sub-chunk
//! segments would save loopback bytes only, not mesh traffic.

use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use hopnet_common::CustomUUID;

use crate::transport::{NodeTransport, TransportError};

/// Storage-chunk-aligned production segment size.
pub const DEFAULT_SEGMENT_SIZE: u64 = 40 * 1024 * 1024;

/// Default cap on concurrently open master cache-file descriptors.
/// Coldest fds past this are closed and lazily reopened on demand —
/// well under the stock 1024 soft RLIMIT_NOFILE with headroom for
/// sockets, passthrough clones, and staging.
pub const DEFAULT_MAX_OPEN_FILES: usize = 256;

/// Default cap on live per-blob cache states. Coldest quiescent blobs
/// past this are fully evicted (state dropped, file unlinked), so
/// neither memory nor the cache directory grows with every distinct
/// blob ever read in the daemon's lifetime.
pub const DEFAULT_MAX_BLOBS: usize = 4096;

#[derive(Debug, Clone)]
pub enum EvictionPolicy {
    /// Keep at least this much free on the cache filesystem (production;
    /// checked via statvfs before each segment write).
    MinFree { bytes: u64 },
    /// Hard cap on total cached bytes (tests — deterministic).
    MaxBytes { bytes: u64 },
}

impl EvictionPolicy {
    /// Production default headroom: room for several concurrent segment
    /// fetches — a multiple of the working unit, not a disk quota.
    pub fn default_min_free(segment_size: u64) -> Self {
        EvictionPolicy::MinFree {
            bytes: 4 * segment_size,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CacheConfig {
    pub root: PathBuf,
    pub segment_size: u64,
    pub policy: EvictionPolicy,
    /// Tier-1 descriptor bound; see [`DEFAULT_MAX_OPEN_FILES`].
    pub max_open_files: usize,
    /// Tier-2 blob-state bound; see [`DEFAULT_MAX_BLOBS`].
    pub max_blobs: usize,
}

#[derive(Debug)]
pub enum CacheError {
    Transport(TransportError),
    Io(String),
}

impl std::fmt::Display for CacheError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CacheError::Transport(e) => write!(f, "transport: {e}"),
            CacheError::Io(why) => write!(f, "cache io: {why}"),
        }
    }
}

impl std::error::Error for CacheError {}

impl From<TransportError> for CacheError {
    fn from(e: TransportError) -> Self {
        CacheError::Transport(e)
    }
}

/// Per-segment fetch state: absent, being fetched (single-flight), or
/// present on disk.
struct SegState {
    present: Vec<bool>,
    inflight: HashMap<u32, tokio::sync::watch::Receiver<Option<bool>>>,
}

struct BlobState {
    /// Master handle, closed by the descriptor LRU under fd pressure
    /// and lazily reopened. Every consumer of the file takes a
    /// `try_clone` — kernel-duplicated fds are independent — so a
    /// closed master never invalidates in-flight I/O or a passthrough
    /// backing fd; the only cost is a local reopen.
    file: Mutex<Option<File>>,
    size: u64,
    segs: Mutex<SegState>,
    /// Passthrough pins (S9): while > 0 the kernel may read ANY offset
    /// of this blob's file without the daemon observing it, so eviction
    /// must not punch holes — a punched page would fault in as zeros.
    /// Whole-blob by necessity, not per-segment.
    pins: AtomicUsize,
    /// Access-clock stamp of the last touch; both descriptor closure
    /// and blob-state eviction pick victims coldest-first by this.
    last_touch: AtomicU64,
}

/// A complete blob's cache file plus its eviction pin (S9 passthrough).
/// The fd is registered with the kernel as a backing file; the pin
/// keeps every byte on disk until the guard drops at release.
pub struct Backing {
    pub file: File,
    pub pin: PinGuard,
}

/// RAII eviction pin; dropping re-exposes the blob to eviction.
pub struct PinGuard {
    state: Arc<BlobState>,
}

impl Drop for PinGuard {
    fn drop(&mut self) {
        self.state.pins.fetch_sub(1, Ordering::Release);
    }
}

pub struct CacheManager {
    config: CacheConfig,
    transport: Arc<dyn NodeTransport>,
    blobs: Mutex<HashMap<CustomUUID, Arc<BlobState>>>,
    /// (blob, segment) → last-access stamp; the eviction scan's ground
    /// truth for what is cached and how cold it is.
    cached: Mutex<HashMap<(CustomUUID, u32), u64>>,
    cached_bytes: AtomicU64,
    access_clock: AtomicU64,
}

impl CacheManager {
    /// Wipes and recreates the cache root (ephemeral by design).
    pub fn new(config: CacheConfig, transport: Arc<dyn NodeTransport>) -> Result<Self, CacheError> {
        let _ = std::fs::remove_dir_all(&config.root);
        std::fs::create_dir_all(&config.root)
            .map_err(|e| CacheError::Io(format!("create cache root: {e}")))?;
        Ok(CacheManager {
            config,
            transport,
            blobs: Mutex::new(HashMap::new()),
            cached: Mutex::new(HashMap::new()),
            cached_bytes: AtomicU64::new(0),
            access_clock: AtomicU64::new(0),
        })
    }

    pub fn cached_bytes(&self) -> u64 {
        self.cached_bytes.load(Ordering::Relaxed)
    }

    /// Hand out the blob's cache file for kernel passthrough (S9), if —
    /// and only if — every segment is present and none is mid-fetch.
    /// The fd is a `try_clone` of the master handle (reopened here if
    /// the descriptor LRU closed it), and the pin is taken under the
    /// `segs` lock so eviction cannot slip between the completeness
    /// check and the pin.
    pub fn backing(&self, blob: &CustomUUID) -> Option<Backing> {
        let state = self
            .blobs
            .lock()
            .expect("cache blobs poisoned")
            .get(blob)
            .cloned()?;
        let segs = state.segs.lock().expect("segstate poisoned");
        if !segs.inflight.is_empty() || !segs.present.iter().all(|p| *p) {
            return None;
        }
        self.stamp(&state);
        let file = match self.blob_file_inner(blob, &state) {
            Ok(file) => file,
            Err(e) => {
                tracing::debug!("backing fd unavailable: {e}");
                return None;
            }
        };
        state.pins.fetch_add(1, Ordering::AcqRel);
        drop(segs);
        Some(Backing {
            file,
            pin: PinGuard {
                state: state.clone(),
            },
        })
    }

    fn seg_len(&self, size: u64, seg: u32) -> u64 {
        let start = seg as u64 * self.config.segment_size;
        (size - start).min(self.config.segment_size)
    }

    fn blob_path(&self, blob: &CustomUUID) -> PathBuf {
        self.config.root.join(blob.to_string())
    }

    fn stamp(&self, state: &BlobState) {
        state.last_touch.store(
            self.access_clock.fetch_add(1, Ordering::Relaxed),
            Ordering::Relaxed,
        );
    }

    /// Log and wrap a cache-file `open()` failure. This path was once
    /// silent, which let a descriptor-limit wedge masquerade as data
    /// loss — the open must always leave a diagnosis in the journal.
    fn open_failed(&self, blob: &CustomUUID, e: &std::io::Error) -> CacheError {
        let why = describe_open_error(e);
        tracing::error!(%blob, "cache file open failed: {why}");
        CacheError::Io(format!("open cache file: {why}"))
    }

    fn blob_state(&self, blob: &CustomUUID, size: u64) -> Result<Arc<BlobState>, CacheError> {
        let state = {
            let mut blobs = self.blobs.lock().expect("cache blobs poisoned");
            if let Some(state) = blobs.get(blob) {
                self.stamp(state);
                return Ok(state.clone());
            }
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(self.blob_path(blob))
                .map_err(|e| self.open_failed(blob, &e))?;
            file.set_len(size)
                .map_err(|e| CacheError::Io(format!("set_len: {e}")))?;
            let seg_count = size.div_ceil(self.config.segment_size).max(1) as usize;
            let state = Arc::new(BlobState {
                file: Mutex::new(Some(file)),
                size,
                segs: Mutex::new(SegState {
                    present: vec![false; seg_count],
                    inflight: HashMap::new(),
                }),
                pins: AtomicUsize::new(0),
                last_touch: AtomicU64::new(self.access_clock.fetch_add(1, Ordering::Relaxed)),
            });
            blobs.insert(blob.clone(), state.clone());
            self.prune_blob_states(&mut blobs, blob);
            state
        };
        self.enforce_fd_cap();
        Ok(state)
    }

    /// A cloned handle to the blob's cache file, reopening a master fd
    /// the descriptor LRU closed. `create(false)` on the reopen: a
    /// vanished file must fail loudly, never silently rematerialize as
    /// zero-filled bytes the `present` bitmap claims are hydrated.
    ///
    /// Inner variant only — no cap enforcement — so it is safe under
    /// the `segs` lock (enforcement takes `blobs`, which is above
    /// `segs` in the lock order).
    fn blob_file_inner(&self, blob: &CustomUUID, state: &BlobState) -> Result<File, CacheError> {
        let mut slot = state.file.lock().expect("blob file poisoned");
        if slot.is_none() {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(false)
                .open(self.blob_path(blob))
                .map_err(|e| self.open_failed(blob, &e))?;
            *slot = Some(file);
        }
        slot.as_ref()
            .expect("slot just filled")
            .try_clone()
            .map_err(|e| CacheError::Io(e.to_string()))
    }

    /// `blob_file_inner` plus recency stamping and descriptor-cap
    /// enforcement — the entry point for the read and fetch paths.
    fn blob_file(&self, blob: &CustomUUID, state: &BlobState) -> Result<File, CacheError> {
        self.stamp(state);
        let had_fd = state.file.lock().expect("blob file poisoned").is_some();
        let file = self.blob_file_inner(blob, state)?;
        if !had_fd {
            self.enforce_fd_cap();
        }
        Ok(file)
    }

    /// Tier-1 bound: close the coldest unpinned master fds until at
    /// most `max_open_files` remain open. Snapshot-then-act (never
    /// holding `blobs` while touching a file slot's neighbours'
    /// stamps): a victim re-touched mid-scan just reopens on demand —
    /// benign, never incorrect, since all I/O runs on clones.
    fn enforce_fd_cap(&self) {
        let snapshot: Vec<(CustomUUID, Arc<BlobState>)> = {
            let blobs = self.blobs.lock().expect("cache blobs poisoned");
            blobs.iter().map(|(b, s)| (b.clone(), s.clone())).collect()
        };
        let mut open: Vec<&Arc<BlobState>> = snapshot
            .iter()
            .filter(|(_, s)| s.file.lock().expect("blob file poisoned").is_some())
            .map(|(_, s)| s)
            .collect();
        if open.len() <= self.config.max_open_files {
            return;
        }
        open.sort_by_key(|s| s.last_touch.load(Ordering::Relaxed));
        let excess = open.len() - self.config.max_open_files;
        let mut closed = 0usize;
        for state in open {
            if closed >= excess {
                break;
            }
            // Pinned blobs keep their master fd as the conservative
            // choice, though passthrough correctness rests on the
            // clone's independence, not on this exemption.
            if state.pins.load(Ordering::Acquire) > 0 {
                continue;
            }
            *state.file.lock().expect("blob file poisoned") = None;
            closed += 1;
        }
    }

    /// Tier-2 bound: fully evict the coldest quiescent blobs (no pins,
    /// no inflight fetches) until the map fits `max_blobs` — state
    /// dropped, index purged, file unlinked. Runs with `blobs` held,
    /// which is what makes removal atomic against `blob_state` and
    /// keeps the documented blobs→segs→cached lock order. A reader
    /// still holding the old `Arc` keeps reading correct bytes from
    /// the orphaned inode via its clone: blob content is immutable
    /// per id, so every such race is benign.
    fn prune_blob_states(
        &self,
        blobs: &mut HashMap<CustomUUID, Arc<BlobState>>,
        keep: &CustomUUID,
    ) {
        if blobs.len() <= self.config.max_blobs {
            return;
        }
        let mut candidates: Vec<(CustomUUID, Arc<BlobState>)> = blobs
            .iter()
            .filter(|(b, s)| *b != keep && s.pins.load(Ordering::Acquire) == 0)
            .map(|(b, s)| (b.clone(), s.clone()))
            .collect();
        candidates.sort_by_key(|(_, s)| s.last_touch.load(Ordering::Relaxed));
        for (victim, state) in candidates {
            if blobs.len() <= self.config.max_blobs {
                return;
            }
            let mut segs = state.segs.lock().expect("segstate poisoned");
            // Authoritative re-checks under `segs` (pins are taken
            // under this lock; inflight means a fetch owns the file).
            if state.pins.load(Ordering::Acquire) > 0 || !segs.inflight.is_empty() {
                continue;
            }
            *state.file.lock().expect("blob file poisoned") = None;
            let mut freed = 0u64;
            {
                let mut cached = self.cached.lock().expect("cache index poisoned");
                for seg in 0..segs.present.len() as u32 {
                    if cached.remove(&(victim.clone(), seg)).is_some() {
                        freed += self.seg_len(state.size, seg);
                    }
                }
            }
            for present in segs.present.iter_mut() {
                *present = false;
            }
            drop(segs);
            self.cached_bytes.fetch_sub(freed, Ordering::Relaxed);
            if let Err(e) = std::fs::remove_file(self.blob_path(&victim)) {
                tracing::debug!(%victim, "cache file unlink failed: {e}");
            }
            blobs.remove(&victim);
            tracing::debug!(%victim, freed, "evicted quiescent blob state");
        }
    }

    /// Read `len` bytes at `offset` from `blob` (logical size `size`),
    /// hydrating any missing segments. Returns short reads only at EOF.
    pub async fn read(
        &self,
        blob: &CustomUUID,
        size: u64,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>, CacheError> {
        if offset >= size || len == 0 || size == 0 {
            return Ok(Vec::new());
        }
        let len = len.min(size - offset);

        let first_seg = (offset / self.config.segment_size) as u32;
        let last_seg = ((offset + len - 1) / self.config.segment_size) as u32;

        // A punched-under-us segment surfaces as a re-ensure; bounded.
        // State is re-resolved per attempt so a blob whose whole state
        // was pruned between attempts is simply recreated and refetched.
        for _attempt in 0..3 {
            let state = self.blob_state(blob, size)?;
            for seg in first_seg..=last_seg {
                self.ensure_segment(blob, &state, seg).await?;
            }

            let buf = {
                let file = self.blob_file(blob, &state)?;
                tokio::task::spawn_blocking(move || {
                    use std::os::unix::fs::FileExt;
                    let mut buf = vec![0u8; len as usize];
                    file.read_exact_at(&mut buf, offset).map(|_| buf)
                })
                .await
                .map_err(|e| CacheError::Io(e.to_string()))?
                .map_err(|e| CacheError::Io(e.to_string()))?
            };

            // Presence re-check: if an overlapping segment was punched
            // between ensure and pread, the buffer may contain hole
            // zeros — retry the ensure loop.
            let still_present = {
                let segs = state.segs.lock().expect("segstate poisoned");
                (first_seg..=last_seg).all(|s| segs.present[s as usize])
            };
            if still_present {
                self.touch(blob, first_seg, last_seg);
                return Ok(buf);
            }
        }
        Err(CacheError::Io(
            "segment evicted repeatedly under read".to_string(),
        ))
    }

    fn touch(&self, blob: &CustomUUID, first_seg: u32, last_seg: u32) {
        let stamp = self.access_clock.fetch_add(1, Ordering::Relaxed);
        let mut cached = self.cached.lock().expect("cache index poisoned");
        for seg in first_seg..=last_seg {
            if let Some(entry) = cached.get_mut(&(blob.clone(), seg)) {
                *entry = stamp;
            }
        }
    }

    /// Single-flight segment hydration.
    async fn ensure_segment(
        &self,
        blob: &CustomUUID,
        state: &Arc<BlobState>,
        seg: u32,
    ) -> Result<(), CacheError> {
        enum Role {
            Ready,
            Wait(tokio::sync::watch::Receiver<Option<bool>>),
            Fetch(tokio::sync::watch::Sender<Option<bool>>),
        }

        // One waiter retry if the fetch we waited on failed.
        for _attempt in 0..2 {
            let role = {
                let mut segs = state.segs.lock().expect("segstate poisoned");
                if segs.present[seg as usize] {
                    Role::Ready
                } else if let Some(rx) = segs.inflight.get(&seg) {
                    Role::Wait(rx.clone())
                } else {
                    let (tx, rx) = tokio::sync::watch::channel(None);
                    segs.inflight.insert(seg, rx);
                    Role::Fetch(tx)
                }
            };
            match role {
                Role::Ready => return Ok(()),
                Role::Fetch(tx) => return self.fetch_segment(blob, state, seg, tx).await,
                Role::Wait(mut rx) => {
                    let outcome = rx
                        .wait_for(|v| v.is_some())
                        .await
                        .map(|v| v.unwrap_or(false));
                    match outcome {
                        Ok(true) => return Ok(()),
                        // Fetcher failed or sender dropped — retry once,
                        // becoming the fetcher ourselves if still absent.
                        Ok(false) | Err(_) => continue,
                    }
                }
            }
        }
        Err(CacheError::Io(
            "segment fetch failed after retry".to_string(),
        ))
    }

    async fn fetch_segment(
        &self,
        blob: &CustomUUID,
        state: &Arc<BlobState>,
        seg: u32,
        tx: tokio::sync::watch::Sender<Option<bool>>,
    ) -> Result<(), CacheError> {
        let seg_start = seg as u64 * self.config.segment_size;
        let seg_len = self.seg_len(state.size, seg);

        let result = self
            .fetch_segment_inner(blob, state, seg_start, seg_len)
            .await;

        let mut segs = state.segs.lock().expect("segstate poisoned");
        segs.inflight.remove(&seg);
        match &result {
            Ok(()) => {
                segs.present[seg as usize] = true;
                // Index insert stays under `segs` (the documented
                // segs→cached order) so blob-state pruning — which
                // clears the index under this same lock — can never
                // interleave and strand an entry for a pruned blob.
                let stamp = self.access_clock.fetch_add(1, Ordering::Relaxed);
                self.cached
                    .lock()
                    .expect("cache index poisoned")
                    .insert((blob.clone(), seg), stamp);
                self.cached_bytes.fetch_add(seg_len, Ordering::Relaxed);
                drop(segs);
                let _ = tx.send(Some(true));
            }
            Err(_) => {
                drop(segs);
                let _ = tx.send(Some(false));
            }
        }
        result
    }

    async fn fetch_segment_inner(
        &self,
        blob: &CustomUUID,
        state: &Arc<BlobState>,
        seg_start: u64,
        seg_len: u64,
    ) -> Result<(), CacheError> {
        self.ensure_room(seg_len)?;

        let bytes = self
            .transport
            .read_blob(blob.clone(), seg_start, seg_len)
            .await?;
        if bytes.len() as u64 != seg_len {
            return Err(CacheError::Io(format!(
                "short blob read: wanted {seg_len}, got {}",
                bytes.len()
            )));
        }

        let file = self.blob_file(blob, state)?;
        tokio::task::spawn_blocking(move || {
            use std::os::unix::fs::FileExt;
            file.write_all_at(&bytes, seg_start)
        })
        .await
        .map_err(|e| CacheError::Io(e.to_string()))?
        .map_err(|e| CacheError::Io(format!("cache write: {e}")))?;
        Ok(())
    }

    /// Make room for an incoming segment per the eviction policy.
    fn ensure_room(&self, incoming: u64) -> Result<(), CacheError> {
        loop {
            let need_evict = match &self.config.policy {
                EvictionPolicy::MaxBytes { bytes } => {
                    self.cached_bytes.load(Ordering::Relaxed) + incoming > *bytes
                }
                EvictionPolicy::MinFree { bytes } => {
                    let free = free_bytes(&self.config.root)?;
                    free < bytes + incoming
                }
            };
            if !need_evict {
                return Ok(());
            }
            if !self.evict_coldest() {
                // Nothing evictable: for MaxBytes that's a cap smaller
                // than one segment (or everything left is pinned by
                // passthrough opens); for MinFree the disk is genuinely
                // full of other people's data. Proceed and let the write
                // fail loudly if it must.
                return Ok(());
            }
        }
    }

    /// Punch the least-recently-used cached segment of an UNPINNED
    /// blob. False if nothing is evictable (empty, or all pinned).
    fn evict_coldest(&self) -> bool {
        // Pinned blobs stay in the index untouched — they become
        // evictable again the instant their last pin drops, with their
        // stamps intact. Snapshot first, then scan: taking `blobs`
        // inside the `cached` lock would create a lock cycle with the
        // segs→cached order used below.
        let pinned: std::collections::HashSet<CustomUUID> = {
            let blobs = self.blobs.lock().expect("cache blobs poisoned");
            blobs
                .iter()
                .filter(|(_, s)| s.pins.load(Ordering::Acquire) > 0)
                .map(|(b, _)| b.clone())
                .collect()
        };
        let coldest = {
            let cached = self.cached.lock().expect("cache index poisoned");
            cached
                .iter()
                .filter(|((blob, _), _)| !pinned.contains(blob))
                .min_by_key(|(_, stamp)| **stamp)
                .map(|(key, _)| key.clone())
        };
        let Some((blob, seg)) = coldest else {
            return false;
        };
        let Some(state) = self
            .blobs
            .lock()
            .expect("cache blobs poisoned")
            .get(&blob)
            .cloned()
        else {
            self.cached
                .lock()
                .expect("cache index poisoned")
                .remove(&(blob, seg));
            return true;
        };

        let mut segs = state.segs.lock().expect("segstate poisoned");
        // A pin may have landed after the snapshot; the pin was taken
        // under this same segs lock, so this re-check is authoritative.
        // Keep the index entry — the next snapshot filters it out.
        if state.pins.load(Ordering::Acquire) > 0 {
            return true;
        }
        // Never punch a segment mid-fetch.
        if segs.inflight.contains_key(&seg) {
            // Drop it from the index so the scan doesn't spin on it; the
            // fetch completion re-inserts.
            self.cached
                .lock()
                .expect("cache index poisoned")
                .remove(&(blob, seg));
            return true;
        }
        segs.present[seg as usize] = false;
        let seg_len = self.seg_len(state.size, seg);
        // Reopen-to-punch if the descriptor LRU closed the master fd;
        // on failure the bytes just stay allocated, which is the same
        // never-incorrect outcome as a failed punch.
        match self.blob_file_inner(&blob, &state) {
            Ok(file) => punch_hole(&file, seg as u64 * self.config.segment_size, seg_len),
            Err(e) => tracing::debug!("punch skipped, cache file unavailable: {e}"),
        }
        drop(segs);

        tracing::debug!(%blob, seg, "evicted cache segment");
        // Conditional so a concurrent blob-state prune that already
        // purged this entry (and its bytes) is not double-subtracted.
        if self
            .cached
            .lock()
            .expect("cache index poisoned")
            .remove(&(blob, seg))
            .is_some()
        {
            self.cached_bytes.fetch_sub(seg_len, Ordering::Relaxed);
        }
        true
    }

    /// Currently open master cache-file descriptors (tests, metrics).
    pub fn open_file_count(&self) -> usize {
        self.blobs
            .lock()
            .expect("cache blobs poisoned")
            .values()
            .filter(|s| s.file.lock().expect("blob file poisoned").is_some())
            .count()
    }

    /// Live per-blob cache states (tests, metrics).
    pub fn blob_count(&self) -> usize {
        self.blobs.lock().expect("cache blobs poisoned").len()
    }
}

/// EMFILE/ENFILE get a diagnosis, not a bare errno: the operator must
/// learn this is a descriptor limit — recoverable, data intact — and
/// not corruption or loss.
pub(crate) fn describe_open_error(e: &std::io::Error) -> String {
    // 24 = EMFILE (per-process), 23 = ENFILE (system-wide); the values
    // are identical on Linux and macOS.
    match e.raw_os_error() {
        Some(23 | 24) => format!(
            "{e} — open file descriptor limit reached; \
             no data is lost, raise the daemon's LimitNOFILE"
        ),
        _ => e.to_string(),
    }
}

/// Free bytes on the cache filesystem.
#[cfg(unix)]
fn free_bytes(root: &std::path::Path) -> Result<u64, CacheError> {
    let vfs = rustix_statvfs(root)?;
    Ok(vfs)
}

#[cfg(target_os = "linux")]
fn rustix_statvfs(root: &std::path::Path) -> Result<u64, CacheError> {
    let vfs = rustix::fs::statvfs(root).map_err(|e| CacheError::Io(e.to_string()))?;
    Ok(vfs.f_bavail * vfs.f_frsize)
}

#[cfg(all(unix, not(target_os = "linux")))]
fn rustix_statvfs(_root: &std::path::Path) -> Result<u64, CacheError> {
    // Non-linux dev hosts (macOS tests) use MaxBytes policies; MinFree
    // reports "plenty" rather than linking another platform API.
    Ok(u64::MAX / 2)
}

/// `fallocate(PUNCH_HOLE | KEEP_SIZE)` — frees the blocks, keeps the
/// logical size. Errors are logged only: a failed punch means the bytes
/// stay cached, which is never incorrect.
#[cfg(target_os = "linux")]
fn punch_hole(file: &File, offset: u64, len: u64) {
    use rustix::fs::FallocateFlags;
    if let Err(e) = rustix::fs::fallocate(
        file,
        FallocateFlags::PUNCH_HOLE | FallocateFlags::KEEP_SIZE,
        offset,
        len,
    ) {
        tracing::debug!("punch_hole failed (leaving bytes cached): {e}");
    }
}

#[cfg(not(target_os = "linux"))]
fn punch_hole(_file: &File, _offset: u64, _len: u64) {
    // No-op off Linux: eviction accounting still drops the segment, it
    // just isn't returned to the filesystem. The daemon is linux-only.
}
