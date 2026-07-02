//! `photos` table access.

use chrono::{DateTime, Utc};
use sqlx::sqlite::Sqlite;
use sqlx::Executor;

use crate::error::Result;
use crate::ids::{LibraryId, PhotoId};
use crate::model::PhotoRecord;

use super::StateStore;

impl StateStore {
    /// Look up a photo by its iCloud identifier (match-precedence rule 1).
    pub async fn photo_by_cloud_id(&self, cloud_id: &str) -> Result<Option<PhotoRecord>> {
        photo_by_cloud_id(self.pool(), cloud_id).await
    }

    pub async fn photo(&self, id: &PhotoId) -> Result<Option<PhotoRecord>> {
        Ok(sqlx::query_as("SELECT * FROM photos WHERE photo_id = ?")
            .bind(id)
            .fetch_optional(self.pool())
            .await?)
    }

    /// Total photo rows — CLI status and test assertions.
    pub async fn count_photos(&self) -> Result<i64> {
        let (n,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM photos")
            .fetch_one(self.pool())
            .await?;
        Ok(n)
    }
}

pub(crate) async fn photo_by_cloud_id<'e, E>(exec: E, cloud_id: &str) -> Result<Option<PhotoRecord>>
where
    E: Executor<'e, Database = Sqlite>,
{
    Ok(sqlx::query_as("SELECT * FROM photos WHERE cloud_id = ?")
        .bind(cloud_id)
        .fetch_optional(exec)
        .await?)
}

/// Parameters for minting a new `photos` row. Pipeline-state columns start
/// NULL per the mint-before-materialize rule.
pub(crate) struct NewPhoto<'a> {
    pub photo_id: &'a PhotoId,
    pub library_id: Option<&'a LibraryId>,
    pub cloud_id: Option<&'a str>,
    pub local_id: &'a str,
    pub group_id: Option<&'a str>,
    pub group_type: Option<i64>,
    pub is_group_pick: bool,
    pub asset_modified_at: Option<DateTime<Utc>>,
}

pub(crate) async fn insert_photo<'e, E>(exec: E, p: NewPhoto<'_>) -> Result<()>
where
    E: Executor<'e, Database = Sqlite>,
{
    sqlx::query(
        "INSERT INTO photos \
         (photo_id, library_id, cloud_id, local_id, group_id, group_type, group_index, is_group_pick, \
          discovered_at, asset_modified_at) \
         VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?, ?)",
    )
    .bind(p.photo_id)
    .bind(p.library_id)
    .bind(p.cloud_id)
    .bind(p.local_id)
    .bind(p.group_id)
    .bind(p.group_type)
    .bind(p.is_group_pick)
    .bind(Utc::now())
    .bind(p.asset_modified_at)
    .execute(exec)
    .await?;
    Ok(())
}

pub(crate) async fn update_local_id<'e, E>(exec: E, id: &PhotoId, local_id: &str) -> Result<()>
where
    E: Executor<'e, Database = Sqlite>,
{
    sqlx::query("UPDATE photos SET local_id = ? WHERE photo_id = ?")
        .bind(local_id)
        .bind(id)
        .execute(exec)
        .await?;
    Ok(())
}

/// Late-binding (rule 2a): populate `cloud_id` on an existing row.
pub(crate) async fn set_cloud_id<'e, E>(exec: E, id: &PhotoId, cloud_id: &str) -> Result<()>
where
    E: Executor<'e, Database = Sqlite>,
{
    sqlx::query("UPDATE photos SET cloud_id = ? WHERE photo_id = ?")
        .bind(cloud_id)
        .bind(id)
        .execute(exec)
        .await?;
    Ok(())
}

/// Adopt-on-redelivery: populate `library_id` on a previously-unmapped row.
pub(crate) async fn adopt_library<'e, E>(exec: E, id: &PhotoId, library: &LibraryId) -> Result<()>
where
    E: Executor<'e, Database = Sqlite>,
{
    sqlx::query("UPDATE photos SET library_id = ? WHERE photo_id = ? AND library_id IS NULL")
        .bind(library)
        .bind(id)
        .execute(exec)
        .await?;
    Ok(())
}

/// Local-only seed guard: the live record for a cloud_id-less asset on this
/// device. NOT an identity lookup — a same-device double-mint guard only.
pub(crate) async fn photo_by_local_id_no_cloud<'e, E>(
    exec: E,
    local_id: &str,
) -> Result<Option<PhotoRecord>>
where
    E: Executor<'e, Database = Sqlite>,
{
    Ok(sqlx::query_as(
        "SELECT * FROM photos \
         WHERE local_id = ? AND cloud_id IS NULL AND deleted_at IS NULL",
    )
    .bind(local_id)
    .fetch_optional(exec)
    .await?)
}

/// Delete a photo row and its resource rows (late-binding merge of a
/// provisional photo; nothing on disk references it by construction).
pub(crate) async fn delete_photo(
    exec: &mut sqlx::SqliteConnection,
    id: &PhotoId,
) -> Result<()> {
    sqlx::query("DELETE FROM photo_resources WHERE photo_id = ?")
        .bind(id)
        .execute(&mut *exec)
        .await?;
    sqlx::query("DELETE FROM photos WHERE photo_id = ?")
        .bind(id)
        .execute(&mut *exec)
        .await?;
    Ok(())
}

/// The drain work queue (spec §Discovery: state.db IS the work queue):
/// FIFO by discovery time, photos with at least one fetchable resource.
pub(crate) async fn pending_photos<'e, E>(
    exec: E,
    retry_cap: i64,
    now: DateTime<Utc>,
    limit: i64,
) -> Result<Vec<PhotoRecord>>
where
    E: Executor<'e, Database = Sqlite>,
{
    Ok(sqlx::query_as(
        "SELECT p.* FROM photos p \
         WHERE p.library_id IS NOT NULL \
           AND p.materialized_at IS NULL \
           AND p.deleted_at IS NULL \
           AND EXISTS (SELECT 1 FROM photo_resources r \
                       WHERE r.photo_id = p.photo_id \
                         AND r.written_at IS NULL \
                         AND r.retry_count < ? \
                         AND (r.next_retry_at IS NULL OR r.next_retry_at <= ?)) \
         ORDER BY p.discovered_at, p.photo_id \
         LIMIT ?",
    )
    .bind(retry_cap)
    .bind(now)
    .bind(limit)
    .fetch_all(exec)
    .await?)
}

// NOTE: `set_asset_modified_at` (stamping the modification date after a
// successful metadata refresh) arrives with Phase 4's change classification —
// resolve only *reports* metadata_changed in Phase 1.
