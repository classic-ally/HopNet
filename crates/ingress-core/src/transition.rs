//! Library transitions — the hard move (spec §Asset migrating between
//! libraries). With the process-global spool there are no per-library byte
//! trees: a hard move is a pure ledger operation — transfer the refcounts
//! between the libraries' `blobs` rows and repoint `photos.library_id`.
//! The file never moves; the `photo_id` is retained.

use crate::error::{IngressError, Result};
use crate::ids::{ContentHash, LibraryId, PhotoId};
use crate::store::StateStore;

/// Counters from one transition, for logging and daemon reporting.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct TransitionReport {
    /// Written resources whose refcounts transferred src → dst.
    pub blobs_transferred: u64,
}

/// Execute a hard move, in one transaction: per written resource, increment
/// the dst refcount and decrement the src (reaping the src row at 0 —
/// never the file: the dst row references it); repoint `photos.library_id`;
/// append `library_transition {src, dst}`. Metadata needs no relocation —
/// the publish capsule lives on the photo row. A spool-evicted src blob
/// passes its eviction stamp to the dst row post-tx (the bytes are
/// deliberately gone; HopNet holds them).
pub async fn execute_transition(
    store: &StateStore,
    photo_id: &PhotoId,
    src: &LibraryId,
    dst: &LibraryId,
) -> Result<TransitionReport> {
    for lib in [src, dst] {
        if store.library(lib).await?.is_none() {
            return Err(IngressError::Invariant(format!("no library row {lib}")));
        }
    }

    let written: Vec<(ContentHash, String, i64)> = store
        .resources_for_photo(photo_id)
        .await?
        .into_iter()
        .filter(|r| r.written_at.is_some())
        .filter_map(|r| Some((r.content_hash?, r.ext?, r.size_bytes?)))
        .collect();

    let mut report = TransitionReport::default();
    let mut inherit_eviction: Vec<ContentHash> = Vec::new();
    for (hash, _, _) in &written {
        if store
            .blob(src, hash)
            .await?
            .is_some_and(|b| b.evicted_at.is_some())
            && store.blob(dst, hash).await?.is_none()
        {
            inherit_eviction.push(hash.clone());
        }
    }

    let mut tx = store.pool().begin().await?;
    for (hash, ext, size_bytes) in &written {
        // First-writer-wins carries the SOURCE library's ext to the dst.
        crate::store::blobs::upsert_increment(&mut *tx, dst, hash, ext, *size_bytes).await?;
        crate::store::blobs::decrement_and_reap(&mut tx, src, hash).await?;
        report.blobs_transferred += 1;
    }
    crate::store::photos::set_library(&mut *tx, photo_id, dst).await?;
    crate::store::log::append(
        &mut *tx,
        "library_transition",
        Some(photo_id),
        Some(serde_json::json!({ "src": src.to_string(), "dst": dst.to_string() })),
    )
    .await?;
    tx.commit().await?;

    for hash in &inherit_eviction {
        store.stamp_blob_evicted(dst, hash).await?;
    }

    Ok(report)
}
