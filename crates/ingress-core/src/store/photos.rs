//! `photos` table access.

use chrono::{DateTime, Utc};
use sqlx::Executor;
use sqlx::sqlite::Sqlite;

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
pub(crate) async fn delete_photo(exec: &mut sqlx::SqliteConnection, id: &PhotoId) -> Result<()> {
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

/// Stamp the modification date after a successful metadata refresh. Ordered
/// AFTER the sidecar rewrite by the caller: a crash between the two re-delivers
/// the refresh (idempotent) rather than losing it.
pub(crate) async fn set_asset_modified_at<'e, E>(
    exec: E,
    id: &PhotoId,
    at: DateTime<Utc>,
) -> Result<()>
where
    E: Executor<'e, Database = Sqlite>,
{
    sqlx::query("UPDATE photos SET asset_modified_at = ? WHERE photo_id = ?")
        .bind(at)
        .bind(id)
        .execute(exec)
        .await?;
    Ok(())
}

/// Tombstone (spec §Deletion): set `deleted_at` only when active. Returns
/// whether this call did the tombstoning — the guard makes PhotoKit's
/// redundant event delivery (2–4 per action) idempotent, and the caller logs
/// `deletion_observed` exactly once off the `true` return.
pub(crate) async fn tombstone_photo<'e, E>(exec: E, id: &PhotoId, at: DateTime<Utc>) -> Result<bool>
where
    E: Executor<'e, Database = Sqlite>,
{
    Ok(
        sqlx::query("UPDATE photos SET deleted_at = ? WHERE photo_id = ? AND deleted_at IS NULL")
            .bind(at)
            .bind(id)
            .execute(exec)
            .await?
            .rows_affected()
            > 0,
    )
}

/// Restore inside the retention window: clear `deleted_at` only when
/// tombstoned. Same idempotency contract as [`tombstone_photo`].
pub(crate) async fn restore_photo<'e, E>(exec: E, id: &PhotoId) -> Result<bool>
where
    E: Executor<'e, Database = Sqlite>,
{
    Ok(sqlx::query(
        "UPDATE photos SET deleted_at = NULL WHERE photo_id = ? AND deleted_at IS NOT NULL",
    )
    .bind(id)
    .execute(exec)
    .await?
    .rows_affected()
        > 0)
}

/// Resolve an observer `removed` event: the live photo currently holding this
/// local_id. Ignores tombstoned rows so a delete arriving after a re-import
/// hits the live row, not the old tombstone. A miss is dropped by the caller —
/// the reconciliation scan is the deletion backstop.
pub(crate) async fn photo_by_local_id_active<'e, E>(
    exec: E,
    local_id: &str,
) -> Result<Option<PhotoRecord>>
where
    E: Executor<'e, Database = Sqlite>,
{
    Ok(sqlx::query_as(
        "SELECT * FROM photos WHERE local_id = ? AND deleted_at IS NULL \
         ORDER BY discovered_at DESC LIMIT 1",
    )
    .bind(local_id)
    .fetch_optional(exec)
    .await?)
}

/// Hard-move step 5: repoint the photo at the destination library. Runs
/// inside the transition transaction alongside the refcount updates.
pub(crate) async fn set_library<'e, E>(exec: E, id: &PhotoId, library: &LibraryId) -> Result<()>
where
    E: Executor<'e, Database = Sqlite>,
{
    sqlx::query("UPDATE photos SET library_id = ? WHERE photo_id = ?")
        .bind(library)
        .bind(id)
        .execute(exec)
        .await?;
    Ok(())
}

/// Re-enter the work queue after new/reopened resource rows: NULL
/// `materialized_at` in the same transaction as the resource-plan changes;
/// `mark_resource_written` re-stamps it when everything is written again.
pub(crate) async fn clear_materialized<'e, E>(exec: E, id: &PhotoId) -> Result<()>
where
    E: Executor<'e, Database = Sqlite>,
{
    sqlx::query("UPDATE photos SET materialized_at = NULL WHERE photo_id = ?")
        .bind(id)
        .execute(exec)
        .await?;
    Ok(())
}

/// Deletion-synthesis candidates for a reconciliation scan: active photos in
/// bound libraries discovered before the scan started. The caller filters
/// against its in-memory seen set (a giant `NOT IN` is the wrong tool).
pub(crate) async fn active_photo_ids_discovered_before<'e, E>(
    exec: E,
    cutoff: DateTime<Utc>,
) -> Result<Vec<PhotoId>>
where
    E: Executor<'e, Database = Sqlite>,
{
    Ok(sqlx::query_scalar(
        "SELECT photo_id FROM photos \
         WHERE library_id IS NOT NULL AND deleted_at IS NULL AND discovered_at < ?",
    )
    .bind(cutoff)
    .fetch_all(exec)
    .await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::{AssetDescriptorBuilder, store_with_personal};
    use crate::resolve::{SeedOutcome, seed_descriptor};

    async fn seed_one(store: &StateStore, desc: &crate::descriptor::AssetDescriptor) -> PhotoId {
        match seed_descriptor(store, desc).await.expect("seed") {
            SeedOutcome::MintedPending { photo_id, .. } => photo_id,
            other => panic!("expected MintedPending, got {other:?}"),
        }
    }

    // Impact: PhotoKit delivers 2–4 near-identical events per user action
    // (spike); an unguarded tombstone would log deletion_observed per event
    // and restart the retention clock on each.
    // Should: return true exactly once per state change.
    // Should not: touch an already-tombstoned (or already-active) row.
    #[tokio::test]
    async fn tombstone_and_restore_are_idempotent() {
        let (store, _) = store_with_personal().await;
        let desc = AssetDescriptorBuilder::simple_image().build();
        let id = seed_one(&store, &desc).await;

        let now = Utc::now();
        assert!(tombstone_photo(store.pool(), &id, now).await.unwrap());
        assert!(!tombstone_photo(store.pool(), &id, now).await.unwrap());
        assert!(
            store
                .photo(&id)
                .await
                .unwrap()
                .unwrap()
                .deleted_at
                .is_some()
        );

        assert!(restore_photo(store.pool(), &id).await.unwrap());
        assert!(!restore_photo(store.pool(), &id).await.unwrap());
        assert!(
            store
                .photo(&id)
                .await
                .unwrap()
                .unwrap()
                .deleted_at
                .is_none()
        );
    }

    // Impact: a removed event arriving after a delete+re-import must resolve
    // the live row — tombstoning the old tombstone would orphan the new photo.
    // Should: return the active row for a local_id shared with a tombstone.
    // Should not: return anything once every holder is tombstoned.
    #[tokio::test]
    async fn local_id_active_lookup_ignores_tombstoned() {
        let (store, _) = store_with_personal().await;
        let old = seed_one(
            &store,
            &AssetDescriptorBuilder::simple_image()
                .with_local_id("SAME/L0/001")
                .build(),
        )
        .await;
        tombstone_photo(store.pool(), &old, Utc::now())
            .await
            .unwrap();

        let new = seed_one(
            &store,
            &AssetDescriptorBuilder::simple_image()
                .with_local_id("SAME/L0/001")
                .build(),
        )
        .await;

        let found = photo_by_local_id_active(store.pool(), "SAME/L0/001")
            .await
            .unwrap();
        assert_eq!(found.map(|p| p.photo_id), Some(new.clone()));

        tombstone_photo(store.pool(), &new, Utc::now())
            .await
            .unwrap();
        assert!(
            photo_by_local_id_active(store.pool(), "SAME/L0/001")
                .await
                .unwrap()
                .is_none()
        );
    }

    // Impact: deletion synthesis must never tombstone photos minted during
    // the scan (observer inserts racing the enumeration) or already-tombstoned
    // rows — both would churn retention state.
    // Should: return only active, bound, pre-cutoff photos.
    #[tokio::test]
    async fn synthesis_candidates_respect_cutoff_and_state() {
        let (store, _) = store_with_personal().await;
        let before = seed_one(&store, &AssetDescriptorBuilder::simple_image().build()).await;
        let gone = seed_one(&store, &AssetDescriptorBuilder::simple_image().build()).await;
        tombstone_photo(store.pool(), &gone, Utc::now())
            .await
            .unwrap();

        let cutoff = Utc::now();
        let after = seed_one(&store, &AssetDescriptorBuilder::simple_image().build()).await;

        let ids = active_photo_ids_discovered_before(store.pool(), cutoff)
            .await
            .unwrap();
        assert!(ids.contains(&before));
        assert!(
            !ids.contains(&gone),
            "tombstoned row is not a synthesis candidate"
        );
        assert!(
            !ids.contains(&after),
            "post-cutoff mint is not a synthesis candidate"
        );
    }
}
