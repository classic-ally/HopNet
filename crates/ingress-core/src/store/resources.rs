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

    /// Commit a materialized resource (Phase 2 write path, and test seeding):
    /// in ONE transaction, set `content_hash`/`ext`/`size_bytes`/`written_at`
    /// on the resource row, upsert the blob refcount, and stamp
    /// `photos.materialized_at` if this was the photo's last pending resource.
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
    ) -> Result<()> {
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
                 next_retry_at = NULL, last_error = NULL \
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
        sqlx::query(
            "UPDATE photos SET materialized_at = ? \
             WHERE photo_id = ? \
               AND NOT EXISTS (SELECT 1 FROM photo_resources \
                               WHERE photo_id = ? AND written_at IS NULL)",
        )
        .bind(now)
        .bind(photo_id)
        .bind(photo_id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
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
