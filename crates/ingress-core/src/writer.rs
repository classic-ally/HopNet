//! Streaming blob writer (spec §Write path).
//!
//! Two layers: [`ResourceWrite`] is the synchronous byte layer (append+hash
//! into a `.partial` temp — no DB); [`finalize_resource`] is the async half
//! that runs the dedup decision and commits through the store. The ordering
//! invariant — filesystem durability strictly before the database commit —
//! lives in the seam between them.
//!
//! Inflight exclusivity per `(library_id, content_hash)` is the Phase 3
//! scheduler's concern; Phase 2 flows are strictly sequential.

use std::fs;
use std::io::Write as _;
use std::path::PathBuf;

use crate::error::{IngressError, Result};
use crate::ids::{ContentHash, LibraryId, PhotoId};
use crate::model::ResourceType;
use crate::paths::{BlobPaths, TempKey};
use crate::store::StateStore;

/// Facts about a completed stream, produced by [`ResourceWrite::finish`].
#[derive(Debug)]
pub struct FinishedStream {
    pub temp_path: PathBuf,
    pub hash: ContentHash,
    pub size_bytes: u64,
}

/// One in-flight resource stream: append-and-hash into a `.partial` temp.
///
/// Sync I/O by design — called from the FFI boundary with ~1 MiB chunks;
/// the BLAKE3 update is CPU-bound and fast.
pub struct ResourceWrite {
    temp_path: PathBuf,
    file: fs::File,
    hasher: blake3::Hasher,
    bytes: u64,
}

impl ResourceWrite {
    /// Open the temp for writing. Creates `.partial/` on demand; truncates
    /// any stale temp with the same key — re-entry after an abandoned
    /// stream is a fresh start.
    pub fn begin(paths: &BlobPaths, key: &TempKey) -> Result<Self> {
        let dir = paths.partial_dir();
        fs::create_dir_all(&dir).map_err(io_err)?;
        let temp_path = paths.temp_path(key);
        let file = fs::File::create(&temp_path).map_err(io_err)?;
        Ok(Self { temp_path, file, hasher: blake3::Hasher::new(), bytes: 0 })
    }

    pub fn append(&mut self, chunk: &[u8]) -> Result<()> {
        self.file.write_all(chunk).map_err(io_err)?;
        self.hasher.update(chunk);
        self.bytes += chunk.len() as u64;
        Ok(())
    }

    /// Flush and report the stream facts. Deliberately does NOT fsync or
    /// rename — that is [`finalize_resource`]'s job, because a dedup hit
    /// discards the temp instead of placing it.
    pub fn finish(mut self) -> Result<FinishedStream> {
        self.file.flush().map_err(io_err)?;
        Ok(FinishedStream {
            temp_path: self.temp_path,
            hash: ContentHash::from_hex(self.hasher.finalize().to_hex().to_string()),
            size_bytes: self.bytes,
        })
    }

    /// Best-effort temp removal.
    pub fn abort(self) {
        drop(self.file);
        let _ = fs::remove_file(&self.temp_path);
    }
}

fn io_err(e: std::io::Error) -> IngressError {
    IngressError::Invariant(format!("blob write io: {e}"))
}

/// Startup sweep (spec §Write path crash windows): delete every file under
/// `.partial/` — nothing ever references temps, so this is always safe.
/// Returns the number of files removed.
pub fn sweep_partials(paths: &BlobPaths) -> Result<u64> {
    let dir = paths.partial_dir();
    let mut removed = 0u64;
    match fs::read_dir(&dir) {
        Ok(entries) => {
            for entry in entries.flatten() {
                if fs::remove_file(entry.path()).is_ok() {
                    removed += 1;
                }
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(io_err(e)),
    }
    Ok(removed)
}

/// The filesystem half of a dedup miss (spec §Write path step 3): fsync the
/// temp, create the fan-out dirs, atomically rename into place, best-effort
/// fsync the parent dir. Separately callable so crash-window tests can
/// exercise "renamed but not committed" without fault injection.
pub fn place_blob(paths: &BlobPaths, finished: &FinishedStream, ext: &str) -> Result<PathBuf> {
    let file = fs::File::open(&finished.temp_path).map_err(io_err)?;
    file.sync_all().map_err(io_err)?;
    drop(file);

    let blob_path = paths.blob_path(&finished.hash, ext);
    let parent = blob_path.parent().expect("blob path has fan-out parents");
    fs::create_dir_all(parent).map_err(io_err)?;
    fs::rename(&finished.temp_path, &blob_path).map_err(io_err)?;
    if let Ok(dir) = fs::File::open(parent) {
        let _ = dir.sync_all();
    }
    Ok(blob_path)
}

/// Outcome of [`finalize_resource`].
#[derive(Debug, PartialEq, Eq)]
pub enum FinalizeOutcome {
    /// Dedup miss: bytes placed at `blob_path`.
    Written { blob_path: PathBuf, photo_completed: bool },
    /// Dedup hit: temp discarded, existing blob's refcount incremented.
    Deduped { blob_path: PathBuf, photo_completed: bool },
}

impl FinalizeOutcome {
    pub fn blob_path(&self) -> &PathBuf {
        match self {
            FinalizeOutcome::Written { blob_path, .. }
            | FinalizeOutcome::Deduped { blob_path, .. } => blob_path,
        }
    }

    pub fn photo_completed(&self) -> bool {
        match self {
            FinalizeOutcome::Written { photo_completed, .. }
            | FinalizeOutcome::Deduped { photo_completed, .. } => *photo_completed,
        }
    }

    pub fn deduped(&self) -> bool {
        matches!(self, FinalizeOutcome::Deduped { .. })
    }
}

/// Spec §Write path steps 3–4: dedup decision, placement, single-transaction
/// commit. Filesystem durability strictly precedes the database commit — a
/// committed row never references bytes that might not exist.
pub async fn finalize_resource(
    store: &StateStore,
    paths: &BlobPaths,
    library: &LibraryId,
    photo_id: &PhotoId,
    resource_type: ResourceType,
    finished: FinishedStream,
    ext: &str,
) -> Result<FinalizeOutcome> {
    let existing = store.blob(library, &finished.hash).await?;

    let (blob_path, deduped) = match existing {
        Some(blob) => {
            // Dedup hit: the bytes are already durably on disk. Discard the
            // temp; keep the first writer's extension for the path.
            fs::remove_file(&finished.temp_path).map_err(io_err)?;
            (paths.blob_path(&finished.hash, &blob.ext), true)
        }
        None => (place_blob(paths, &finished, ext)?, false),
    };

    // Single transaction: content_hash + written_at together (two-state
    // rule), refcount upsert (insert-at-1 or increment), materialized_at
    // stamp when this was the last pending resource.
    let photo_completed = store
        .mark_resource_written(photo_id, resource_type, &finished.hash, ext, finished.size_bytes as i64)
        .await?;

    Ok(if deduped {
        FinalizeOutcome::Deduped { blob_path, photo_completed }
    } else {
        FinalizeOutcome::Written { blob_path, photo_completed }
    })
}
