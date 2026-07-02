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

// NOTE: `set_asset_modified_at` (stamping the modification date after a
// successful metadata refresh) arrives with Phase 4's change classification —
// resolve only *reports* metadata_changed in Phase 1.
