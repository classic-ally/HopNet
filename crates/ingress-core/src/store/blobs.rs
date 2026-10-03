//! `blobs` refcount bookkeeping. DB rows only — file I/O is Phase 2+.

use chrono::Utc;
use sqlx::Executor;
use sqlx::sqlite::Sqlite;

use crate::error::Result;
use crate::ids::{ContentHash, LibraryId};
use crate::model::BlobRecord;

use super::StateStore;

/// Does blob `b` have a referent resource `r` (photo `p`, library `l`)
/// that BLOCKS its eviction — the exact predicate of
/// [`StateStore::evictable_blobs`] — and is in the terminal state
/// `$terminal`? Per resource, like eviction itself: a reopened row that
/// kept its old hash as the superseded pointer is not blocking (the old
/// bytes are the mesh's), so its blob is evictable however the refetch
/// goes. Binds: `?1` = the fetch retry cap, `?2` = the publish/edit retry
/// cap.
macro_rules! blocked_by {
    ($terminal:expr) => {
        concat!(
            "EXISTS (SELECT 1 FROM photo_resources r \
               JOIN photos p ON p.photo_id = r.photo_id \
               JOIN libraries l ON l.library_id = p.library_id \
               WHERE p.library_id = b.library_id \
                 AND r.content_hash = b.content_hash \
                 AND (p.published_at IS NULL \
                   OR r.published_content_hash IS NOT r.content_hash) \
                 AND (",
            $terminal,
            "))"
        )
    };
}

/// Unevicted spool bytes that will not evict on their own, by cause —
/// ADVISORY, for `status` and the `spool_full` log line. Every one of
/// these bytes counts against the spool soft cap all the same: a photo in
/// an outage looks exactly like a stuck one (the publish ledger fills in
/// both), and the cap exists for the outage. One spool file (one content
/// hash, shared across libraries) is stuck while ANY unevicted row for it
/// has a blocking referent in a terminal state; `bytes` counts each file
/// once, the causes may overlap.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
pub struct SpoolStuck {
    /// Every stuck file, counted once.
    pub bytes: u64,
    /// A deleted photo: unpublished (waits on hard delete, never publish)
    /// or published with an edit owed (edits never propagate for a
    /// tombstone). Hard delete at retention reaps these.
    pub deleted: u64,
    /// A scope-bound library with no `mesh_library_id`: nothing claims it
    /// until an operator binds it.
    pub unbound_library: u64,
    /// Unpublished at the publish retry cap. The scan resets the ledger.
    pub publish_capped: u64,
    /// Published, at the edit retry cap. The scan resets the ledger.
    pub edit_capped: u64,
    /// A resource given up at the fetch retry cap, so the photo never
    /// completes. The scan resets the ledger.
    pub fetch_gave_up: u64,
}

impl StateStore {
    /// Largest blob seen in a library — the pessimistic size estimate for
    /// admission when a descriptor reports no expected size.
    pub async fn max_blob_size(&self, library_id: &LibraryId) -> Result<Option<i64>> {
        Ok(
            sqlx::query_scalar("SELECT MAX(size_bytes) FROM blobs WHERE library_id = ?")
                .bind(library_id)
                .fetch_one(self.pool())
                .await?,
        )
    }

    /// Bytes the spool holds materialized: one file per content hash with
    /// an unevicted row in any library (the spool is shared across
    /// libraries). Read fresh each time (eviction, hard deletes and fsck
    /// repairs all move it), so it stays right across crashes and other
    /// processes.
    pub async fn unevicted_bytes(&self) -> Result<u64> {
        let sum: Option<i64> = sqlx::query_scalar(
            "SELECT SUM(size_bytes) FROM ( \
               SELECT MAX(size_bytes) AS size_bytes FROM blobs \
               WHERE evicted_at IS NULL GROUP BY content_hash)",
        )
        .fetch_one(self.pool())
        .await?;
        Ok(sum.unwrap_or(0).max(0) as u64)
    }

    /// The stuck breakdown ([`SpoolStuck`]) — one file per content hash,
    /// each cause tested per referent resource. Five correlated EXISTS per
    /// blob row: run it for `status` and on a cap crossing, never per
    /// claim.
    pub async fn stuck_spool(
        &self,
        fetch_retry_cap: i64,
        publish_retry_cap: i64,
    ) -> Result<SpoolStuck> {
        let (bytes, deleted, unbound_library, publish_capped, edit_capped, fetch_gave_up): (
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
        ) = sqlx::query_as(concat!(
            "SELECT COALESCE(SUM(size_bytes), 0), \
                    COALESCE(SUM(CASE WHEN deleted THEN size_bytes ELSE 0 END), 0), \
                    COALESCE(SUM(CASE WHEN unbound THEN size_bytes ELSE 0 END), 0), \
                    COALESCE(SUM(CASE WHEN publish_capped THEN size_bytes ELSE 0 END), 0), \
                    COALESCE(SUM(CASE WHEN edit_capped THEN size_bytes ELSE 0 END), 0), \
                    COALESCE(SUM(CASE WHEN fetch_gave_up THEN size_bytes ELSE 0 END), 0) \
             FROM ( \
               SELECT MAX(b.size_bytes) AS size_bytes, \
                      MAX(",
            blocked_by!("p.deleted_at IS NOT NULL"),
            ") AS deleted, \
                      MAX(",
            blocked_by!("l.scope_binding IS NOT NULL AND l.mesh_library_id IS NULL"),
            ") AS unbound, \
                      MAX(",
            blocked_by!("p.published_at IS NULL AND p.publish_attempts >= ?2"),
            ") AS publish_capped, \
                      MAX(",
            blocked_by!("p.published_at IS NOT NULL AND p.edit_publish_attempts >= ?2"),
            ") AS edit_capped, \
                      MAX(",
            blocked_by!(
                "EXISTS (SELECT 1 FROM photo_resources g \
                         WHERE g.photo_id = p.photo_id AND g.written_at IS NULL \
                           AND g.retry_count >= ?1)"
            ),
            ") AS fetch_gave_up \
               FROM blobs b WHERE b.evicted_at IS NULL \
               GROUP BY b.content_hash) \
             WHERE deleted OR unbound OR publish_capped OR edit_capped OR fetch_gave_up"
        ))
        .bind(fetch_retry_cap)
        .bind(publish_retry_cap)
        .fetch_one(self.pool())
        .await?;
        let u = |n: i64| n.max(0) as u64;
        Ok(SpoolStuck {
            bytes: u(bytes),
            deleted: u(deleted),
            unbound_library: u(unbound_library),
            publish_capped: u(publish_capped),
            edit_capped: u(edit_capped),
            fetch_gave_up: u(fetch_gave_up),
        })
    }

    pub async fn blob(
        &self,
        library_id: &LibraryId,
        hash: &ContentHash,
    ) -> Result<Option<BlobRecord>> {
        Ok(
            sqlx::query_as("SELECT * FROM blobs WHERE library_id = ? AND content_hash = ?")
                .bind(library_id)
                .bind(hash)
                .fetch_optional(self.pool())
                .await?,
        )
    }

    /// Every blob row of one library — feeds fsck's missing-blob and
    /// orphan-file checks. fetch_all is fine at photo-library scale.
    pub async fn blobs_for_library(&self, library_id: &LibraryId) -> Result<Vec<BlobRecord>> {
        Ok(
            sqlx::query_as("SELECT * FROM blobs WHERE library_id = ? ORDER BY content_hash")
                .bind(library_id)
                .fetch_all(self.pool())
                .await?,
        )
    }

    /// Blobs whose every referencing photo is consensus-decided
    /// (`published_at` set — adoption sets it too) — the spool-eviction
    /// work queue. A single undecided referent keeps the blob.
    pub async fn evictable_blobs(&self, limit: i64) -> Result<Vec<BlobRecord>> {
        // The second disjunct is the edit guard. Eviction rides the end of
        // every publish pass, so a refetched edit whose propagation parked
        // (node unreachable, responsibility lost) would otherwise have its
        // new bytes deleted before the next pass could ever send them —
        // and PhotoKit will not re-deliver an unchanged asset. The bytes
        // stay until the mesh has them, which is the same trade the
        // hard-delete guard makes for tombstones.
        Ok(sqlx::query_as(
            "SELECT b.* FROM blobs b \
             WHERE b.evicted_at IS NULL \
               AND NOT EXISTS ( \
                 SELECT 1 FROM photo_resources r \
                 JOIN photos p ON p.photo_id = r.photo_id \
                 WHERE p.library_id = b.library_id \
                   AND r.content_hash = b.content_hash \
                   AND (p.published_at IS NULL \
                     OR r.published_content_hash IS NOT r.content_hash)) \
             ORDER BY b.written_at \
             LIMIT ?",
        )
        .bind(limit)
        .fetch_all(self.pool())
        .await?)
    }

    /// Stamp a blob evicted. Stamped BEFORE the unlink: a crash between
    /// leaves an evicted row with a lingering file, which fsck classifies
    /// as a benign orphan (the reverse order would read as byte loss).
    pub async fn stamp_blob_evicted(
        &self,
        library_id: &LibraryId,
        hash: &ContentHash,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE blobs SET evicted_at = ? \
             WHERE library_id = ? AND content_hash = ? AND evicted_at IS NULL",
        )
        .bind(Utc::now())
        .bind(library_id)
        .bind(hash)
        .execute(self.pool())
        .await?;
        Ok(())
    }

    /// Clear the eviction stamp — a new (undecided) photo re-referenced the
    /// hash and the write path re-placed the bytes.
    pub async fn clear_blob_eviction(
        &self,
        library_id: &LibraryId,
        hash: &ContentHash,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE blobs SET evicted_at = NULL WHERE library_id = ? AND content_hash = ?",
        )
        .bind(library_id)
        .bind(hash)
        .execute(self.pool())
        .await?;
        Ok(())
    }

    /// Whether ANY library's ledger row still expects this hash's bytes on
    /// disk (unevicted). The spool is process-global — one file can back
    /// rows in several libraries — so every unlink site must gate on this,
    /// not on its own row alone.
    pub async fn hash_is_live(&self, hash: &ContentHash) -> Result<bool> {
        let (n,): (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM blobs WHERE content_hash = ? AND evicted_at IS NULL",
        )
        .bind(hash)
        .fetch_one(self.pool())
        .await?;
        Ok(n > 0)
    }
}

/// Increment the refcount, creating the row at 1 if absent. On conflict the
/// first writer's `ext` wins (spec §blobs notes).
pub(crate) async fn upsert_increment<'e, E>(
    exec: E,
    library_id: &LibraryId,
    hash: &ContentHash,
    ext: &str,
    size_bytes: i64,
) -> Result<()>
where
    E: Executor<'e, Database = Sqlite>,
{
    sqlx::query(
        "INSERT INTO blobs (library_id, content_hash, ext, size_bytes, ref_count, written_at) \
         VALUES (?, ?, ?, ?, 1, ?) \
         ON CONFLICT (library_id, content_hash) \
         DO UPDATE SET ref_count = ref_count + 1",
    )
    .bind(library_id)
    .bind(hash)
    .bind(ext)
    .bind(size_bytes)
    .bind(Utc::now())
    .execute(exec)
    .await?;
    Ok(())
}

/// Decrement the refcount; at 0 the `blobs` row is deleted in the SAME
/// statement scope and the blob's `ext` is returned so the caller can delete
/// the file after its transaction commits.
///
/// Row-deletion-at-0 (rather than keeping a 0-count row) picks the benign
/// failure class: a deleted row with a lingering file is an orphan (swept by
/// recovery); a retained row whose file was deleted would read as byte loss
/// to fsck. Decrementing a missing row is an invariant violation and errors
/// loudly.
pub(crate) async fn decrement_and_reap(
    exec: &mut sqlx::SqliteConnection,
    library_id: &LibraryId,
    hash: &ContentHash,
) -> Result<Option<String>> {
    let row: Option<(i64, String)> = sqlx::query_as(
        "UPDATE blobs SET ref_count = ref_count - 1 \
         WHERE library_id = ? AND content_hash = ? \
         RETURNING ref_count, ext",
    )
    .bind(library_id)
    .bind(hash)
    .fetch_optional(&mut *exec)
    .await?;
    let (ref_count, ext) = row.ok_or_else(|| {
        crate::IngressError::Invariant(format!(
            "decrement of missing blob row ({library_id}, {hash})"
        ))
    })?;
    if ref_count == 0 {
        sqlx::query("DELETE FROM blobs WHERE library_id = ? AND content_hash = ?")
            .bind(library_id)
            .bind(hash)
            .execute(&mut *exec)
            .await?;
        return Ok(Some(ext));
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::store_with_personal;

    // Impact: fsck classifies a retained row with a missing file as byte loss
    // (loud) and a lingering file with no row as a benign orphan — reaping the
    // row at 0 picks the benign failure class for every crash window.
    // Should: delete the row at refcount 0 and return the ext for file reap.
    // Should not: reap while any reference remains.
    #[tokio::test]
    async fn decrement_reaps_row_only_at_zero() {
        let (store, lib) = store_with_personal().await;
        let hash = ContentHash::of_bytes(b"blob");
        {
            let mut conn = store.pool().acquire().await.unwrap();
            upsert_increment(&mut *conn, &lib, &hash, "heic", 42)
                .await
                .unwrap();
            upsert_increment(&mut *conn, &lib, &hash, "heic", 42)
                .await
                .unwrap();
            assert_eq!(
                decrement_and_reap(&mut conn, &lib, &hash).await.unwrap(),
                None
            );
        }
        assert_eq!(store.blob(&lib, &hash).await.unwrap().unwrap().ref_count, 1);

        {
            let mut conn = store.pool().acquire().await.unwrap();
            assert_eq!(
                decrement_and_reap(&mut conn, &lib, &hash).await.unwrap(),
                Some("heic".to_string())
            );
        }
        assert!(store.blob(&lib, &hash).await.unwrap().is_none());
    }

    // Impact: decrementing a blob that was never recorded means refcount
    // bookkeeping has already diverged — silence here would let it drift.
    // Should: error loudly on a missing row.
    #[tokio::test]
    async fn decrement_of_missing_row_errors() {
        let (store, lib) = store_with_personal().await;
        let mut conn = store.pool().acquire().await.unwrap();
        let err = decrement_and_reap(&mut conn, &lib, &ContentHash::of_bytes(b"ghost")).await;
        assert!(err.is_err());
    }
}
