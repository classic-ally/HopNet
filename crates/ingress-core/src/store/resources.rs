//! `photo_resources` table access.

use chrono::Utc;
use sqlx::sqlite::Sqlite;
use sqlx::Executor;

use crate::error::Result;
use crate::ids::{ContentHash, LibraryId, PhotoId};
use crate::model::{PhotoRecord, ResourceRecord, ResourceType};

use super::StateStore;

impl StateStore {
    pub async fn resources_for_photo(&self, id: &PhotoId) -> Result<Vec<ResourceRecord>> {
        resources_for_photo(self.pool(), id).await
    }

    /// Commit a materialized resource (the write path's DB half): in ONE
    /// transaction, set `content_hash`/`ext`/`size_bytes`/`written_at` on
    /// the resource row, upsert the blob refcount, and stamp
    /// `photos.materialized_at` if this was the photo's last pending
    /// resource. Returns whether the photo completed (materialized_at was
    /// stamped by this call).
    ///
    /// `content_hash` and `written_at` commit together or not at all
    /// (two-state rule, spec §Per-resource state machine); the refcount
    /// change rides the same transaction (spec §blobs notes).
    pub async fn mark_resource_written(
        &self,
        photo_id: &PhotoId,
        resource_type: ResourceType,
        hash: &ContentHash,
        ext: &str,
        size_bytes: i64,
    ) -> Result<bool> {
        let mut tx = self.pool().begin().await?;

        let library_id: Option<LibraryId> =
            sqlx::query_scalar("SELECT library_id FROM photos WHERE photo_id = ?")
                .bind(photo_id)
                .fetch_one(&mut *tx)
                .await?;
        let library_id = library_id.ok_or_else(|| {
            crate::IngressError::Invariant(format!(
                "cannot materialize resource for unmapped-library photo {photo_id}"
            ))
        })?;

        let now = Utc::now();
        sqlx::query(
            "UPDATE photo_resources \
             SET content_hash = ?, ext = ?, size_bytes = ?, written_at = ?, \
                 retry_count = 0, next_retry_at = NULL, last_error = NULL \
             WHERE photo_id = ? AND resource_type = ?",
        )
        .bind(hash)
        .bind(ext)
        .bind(size_bytes)
        .bind(now)
        .bind(photo_id)
        .bind(resource_type)
        .execute(&mut *tx)
        .await?;

        super::blobs::upsert_increment(&mut *tx, &library_id, hash, ext, size_bytes).await?;

        // Photo-level completion: same transaction as the final resource write.
        let completed = sqlx::query(
            "UPDATE photos SET materialized_at = ? \
             WHERE photo_id = ? AND materialized_at IS NULL \
               AND NOT EXISTS (SELECT 1 FROM photo_resources \
                               WHERE photo_id = ? AND written_at IS NULL)",
        )
        .bind(now)
        .bind(photo_id)
        .bind(photo_id)
        .execute(&mut *tx)
        .await?
        .rows_affected()
            > 0;

        tx.commit().await?;
        Ok(completed)
    }
}

/// Summary of resources awaiting retry (drain exit report).
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct RetrySummary {
    pub awaiting_retry: i64,
    pub gave_up: i64,
    pub earliest_next_retry_at: Option<chrono::DateTime<Utc>>,
}

impl StateStore {
    /// Record a per-resource fetch failure in ONE transaction: bump
    /// `retry_count`, stamp `next_retry_at` (exponential backoff computed by
    /// the caller) and `last_error`; when the bump reaches the cap, append
    /// the `resource_gave_up` ingest-log event in the same transaction —
    /// exactly once per give-up.
    pub async fn record_resource_failure(
        &self,
        photo_id: &PhotoId,
        resource_type: ResourceType,
        error: &str,
        next_retry_at: chrono::DateTime<Utc>,
        retry_cap: i64,
    ) -> Result<i64> {
        let mut tx = self.pool().begin().await?;
        let (retry_count,): (i64,) = sqlx::query_as(
            "UPDATE photo_resources \
             SET retry_count = retry_count + 1, next_retry_at = ?, last_error = ? \
             WHERE photo_id = ? AND resource_type = ? \
             RETURNING retry_count",
        )
        .bind(next_retry_at)
        .bind(error)
        .bind(photo_id)
        .bind(resource_type)
        .fetch_one(&mut *tx)
        .await?;

        if retry_count == retry_cap {
            super::log::append(
                &mut *tx,
                "resource_gave_up",
                Some(photo_id),
                Some(serde_json::json!({
                    "resource_type": resource_type.as_str(),
                    "final_error": error,
                    "retries": retry_count,
                })),
            )
            .await?;
        }
        tx.commit().await?;
        Ok(retry_count)
    }

    /// Retry-state summary for the drain exit report.
    pub async fn retry_summary(&self, retry_cap: i64) -> Result<RetrySummary> {
        Ok(sqlx::query_as(
            "SELECT \
               COUNT(*) FILTER (WHERE retry_count < ?) AS awaiting_retry, \
               COUNT(*) FILTER (WHERE retry_count >= ?) AS gave_up, \
               MIN(next_retry_at) FILTER (WHERE retry_count < ?) AS earliest_next_retry_at \
             FROM photo_resources \
             WHERE written_at IS NULL AND next_retry_at IS NOT NULL",
        )
        .bind(retry_cap)
        .bind(retry_cap)
        .bind(retry_cap)
        .fetch_one(self.pool())
        .await?)
    }
}

pub(crate) async fn resources_for_photo<'e, E>(exec: E, id: &PhotoId) -> Result<Vec<ResourceRecord>>
where
    E: Executor<'e, Database = Sqlite>,
{
    Ok(
        sqlx::query_as("SELECT * FROM photo_resources WHERE photo_id = ? ORDER BY resource_type")
            .bind(id)
            .fetch_all(exec)
            .await?,
    )
}

/// Mint a pending resource row (`content_hash`/`written_at` NULL).
pub(crate) async fn insert_pending_resource<'e, E>(
    exec: E,
    photo_id: &PhotoId,
    resource_type: ResourceType,
) -> Result<()>
where
    E: Executor<'e, Database = Sqlite>,
{
    sqlx::query("INSERT INTO photo_resources (photo_id, resource_type) VALUES (?, ?)")
        .bind(photo_id)
        .bind(resource_type)
        .execute(exec)
        .await?;
    Ok(())
}

/// Match-precedence rules 2a–2c: find the photo whose ORIGINAL resource has
/// this hash, scoped to one library (per-library dedup namespace — the join
/// through `photos` carries the scoping).
pub(crate) async fn photo_by_original_hash<'e, E>(
    exec: E,
    library_id: &LibraryId,
    hash: &ContentHash,
) -> Result<Option<PhotoRecord>>
where
    E: Executor<'e, Database = Sqlite>,
{
    Ok(sqlx::query_as(
        "SELECT p.* FROM photos p \
         JOIN photo_resources r ON r.photo_id = p.photo_id \
         WHERE r.resource_type = 0 AND r.content_hash = ? AND p.library_id = ?",
    )
    .bind(hash)
    .bind(library_id)
    .fetch_optional(exec)
    .await?)
}
