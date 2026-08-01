//! `blobs` refcount bookkeeping. DB rows only — file I/O is Phase 2+.

use chrono::Utc;
use sqlx::Executor;
use sqlx::sqlite::Sqlite;

use crate::error::Result;
use crate::ids::{ContentHash, LibraryId};
use crate::model::BlobRecord;

use super::StateStore;

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
        Ok(sqlx::query_as(
            "SELECT b.* FROM blobs b \
             WHERE b.evicted_at IS NULL \
               AND NOT EXISTS ( \
                 SELECT 1 FROM photo_resources r \
                 JOIN photos p ON p.photo_id = r.photo_id \
                 WHERE p.library_id = b.library_id \
                   AND r.content_hash = b.content_hash \
                   AND p.published_at IS NULL) \
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
