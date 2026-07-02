//! Library transitions — the hard move (spec §Asset migrating between
//! libraries): a photo's bytes are physically relocated so its full state
//! lives under the destination library's subtree. The `photo_id` is retained.
//!
//! Ordering deviates from the spec's step numbering deliberately: files are
//! copied BEFORE the refcount transaction (spec errata — a dst refcount
//! committed ahead of the copy could reference bytes that never arrived,
//! which fsck classifies as byte loss). Copy-first leaves only benign
//! orphans, matching the write path's durability-precedes-commit invariant.

use std::fs;
use std::io::{Read, Write as _};

use crate::error::{IngressError, Result};
use crate::ids::{ContentHash, LibraryId, PhotoId};
use crate::paths::{BlobPaths, DataDir};
use crate::sidecar_io::move_sidecar;
use crate::store::StateStore;

fn io_err(e: std::io::Error) -> IngressError {
    IngressError::Invariant(format!("transition io: {e}"))
}

/// Counters from one transition, for logging and daemon reporting.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct TransitionReport {
    /// Blobs physically copied to the destination root.
    pub blobs_copied: u64,
    /// Blobs already present at the destination (refcount-only increment).
    pub blobs_shared: u64,
    /// Source files deleted (refcount reached 0 at the source).
    pub src_files_deleted: u64,
}

/// Execute a hard move (spec's seven steps, copy-first):
///
/// 1. Pre-tx: copy every written blob absent at the destination
///    (`.partial` temp + fsync + rename — idempotent, skip when present).
/// 2. One transaction: per written resource, increment the dst refcount and
///    decrement the src (reaping the src row at 0); repoint
///    `photos.library_id`; append `library_transition {src, dst}`.
/// 3. Post-tx: delete reaped src files; relocate the sidecar (skipped when
///    the photo never materialized — its pending rows fetch into the dst
///    root automatically, since paths derive from `photos.library_id`).
pub async fn execute_transition(
    store: &StateStore,
    data_dir: &DataDir,
    photo_id: &PhotoId,
    src: &LibraryId,
    dst: &LibraryId,
) -> Result<TransitionReport> {
    let src_config = store
        .library(src)
        .await?
        .ok_or_else(|| IngressError::Invariant(format!("no library row {src}")))?;
    let dst_config = store
        .library(dst)
        .await?
        .ok_or_else(|| IngressError::Invariant(format!("no library row {dst}")))?;
    let src_paths = BlobPaths::new(&src_config.blob_root);
    let dst_paths = BlobPaths::new(&dst_config.blob_root);

    let written: Vec<(ContentHash, String, i64)> = store
        .resources_for_photo(photo_id)
        .await?
        .into_iter()
        .filter(|r| r.written_at.is_some())
        .filter_map(|r| Some((r.content_hash?, r.ext?, r.size_bytes?)))
        .collect();

    let mut report = TransitionReport::default();

    // Step 1 — pre-transaction filesystem copies. The dst `blobs` row is the
    // authority for "already there" (spec step 3's refcount check); the file
    // check on top makes re-runs after a partial crash idempotent.
    for (hash, ext, _) in &written {
        // First-writer-wins carries the SOURCE library's ext to the dst.
        let dst_present =
            store.blob(dst, hash).await?.is_some() || dst_paths.blob_path(hash, ext).is_file();
        if dst_present {
            report.blobs_shared += 1;
        } else {
            copy_blob(&src_paths, &dst_paths, hash, ext)?;
            report.blobs_copied += 1;
        }
    }

    // Step 2 — single transaction over all refcounts + the photo row.
    let mut reap: Vec<(ContentHash, String)> = Vec::new();
    let mut tx = store.pool().begin().await?;
    for (hash, ext, size_bytes) in &written {
        crate::store::blobs::upsert_increment(&mut *tx, dst, hash, ext, *size_bytes).await?;
        if let Some(reaped_ext) =
            crate::store::blobs::decrement_and_reap(&mut tx, src, hash).await?
        {
            reap.push((hash.clone(), reaped_ext));
        }
    }
    crate::store::photos::set_library(&mut *tx, photo_id, dst).await?;
    crate::store::log::append(
        &mut *tx,
        "library_transition",
        Some(photo_id),
        Some(serde_json::json!({ "src": src.to_string(), "dst": dst.to_string() })),
    )
    .await?;
    // T5: the sidecar relocates below (new library_id inside it) — the
    // remote copy under the DESTINATION root doesn't exist yet.
    crate::store::photos::mark_sidecar_dirty(&mut *tx, photo_id).await?;
    tx.commit().await?;

    // Step 3 — post-transaction cleanup. Failures here leave benign orphans.
    for (hash, ext) in &reap {
        if fs::remove_file(src_paths.blob_path(hash, ext)).is_ok() {
            report.src_files_deleted += 1;
        }
    }
    // Capture the rel-path BEFORE the move — the remote copy under the
    // SOURCE library's backup root must go too, or a sidecar-tree recovery
    // resurrects the photo in the wrong library (spec §Asset migrating
    // step 6 note). Best-effort: a mount-down failure leaves a stale
    // document for fsck.
    let src_rel =
        crate::sidecar_io::find_sidecar(&data_dir.sidecar_root(src), photo_id)?.and_then(|p| {
            p.strip_prefix(data_dir.sidecar_root(src))
                .ok()
                .map(|r| r.to_path_buf())
        });
    move_sidecar(data_dir, photo_id, src, dst)?;
    if let (Some(rel), Some(remote)) = (src_rel, &src_config.sidecar_root_remote) {
        let _ = fs::remove_file(std::path::Path::new(remote).join(rel));
    }

    Ok(report)
}

/// Copy one blob between library roots via the destination's `.partial/`
/// directory + fsync + atomic rename (same crash discipline as the write
/// path; renames cannot cross filesystems, so the temp lives on the dst).
fn copy_blob(src: &BlobPaths, dst: &BlobPaths, hash: &ContentHash, ext: &str) -> Result<()> {
    let src_path = src.blob_path(hash, ext);
    let dst_path = dst.blob_path(hash, ext);

    fs::create_dir_all(dst.partial_dir()).map_err(io_err)?;
    let tmp = dst.partial_dir().join(format!("move-{hash}.{ext}"));

    let mut reader = fs::File::open(&src_path).map_err(io_err)?;
    let mut writer = fs::File::create(&tmp).map_err(io_err)?;
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = reader.read(&mut buf).map_err(io_err)?;
        if n == 0 {
            break;
        }
        writer.write_all(&buf[..n]).map_err(io_err)?;
    }
    crate::writer::sync_file(&writer).map_err(io_err)?;
    drop(writer);

    let parent = dst_path.parent().expect("blob path has fan-out parents");
    fs::create_dir_all(parent).map_err(io_err)?;
    fs::rename(&tmp, &dst_path).map_err(io_err)?;
    if let Ok(dir) = fs::File::open(parent) {
        let _ = dir.sync_all();
    }
    Ok(())
}
