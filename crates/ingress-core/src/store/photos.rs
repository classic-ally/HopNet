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

    /// Persist the publish-metadata capsule (serialized
    /// [`crate::descriptor::DescriptorCapsule`]). Rides the same trigger
    /// points as materialization, metadata refresh, and heal — so the
    /// column tracks the live descriptor.
    pub async fn update_descriptor_capsule(&self, id: &PhotoId, json: &str) -> Result<()> {
        sqlx::query("UPDATE photos SET descriptor_json = ? WHERE photo_id = ?")
            .bind(json)
            .bind(id)
            .execute(self.pool())
            .await?;
        Ok(())
    }

    /// Persist the capsule straight from a live descriptor — the single
    /// metadata write every materialization/refresh/heal path funnels
    /// through (the successor of the sidecar-file write).
    pub async fn persist_descriptor(
        &self,
        id: &PhotoId,
        desc: &crate::descriptor::AssetDescriptor,
    ) -> Result<()> {
        let json = serde_json::to_string(&crate::descriptor::DescriptorCapsule::from(desc))?;
        self.update_descriptor_capsule(id, &json).await
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

/// Hard-delete candidates: tombstones past their retention cutoff. The
/// caller computes the cutoff per library (Rust-side, chrono-bound — house
/// pattern) so per-library `retention_days` applies fresh each run;
/// `library = None` selects unmapped tombstones (NULL library).
///
/// A published photo whose delete has NOT yet reached the mesh is held back
/// past its cutoff: reaping the row would destroy the only record that the
/// mesh still needs telling, stranding the photo in HopNet permanently with
/// nothing left to repair it. Retention is therefore "30 days, or until the
/// mesh knows, whichever is later". Unpublished tombstones are unaffected —
/// there is nothing to propagate. Only metadata lingers; the bytes follow
/// the ordinary eviction rule and are already gone.
///
/// The guard clause is repeated verbatim in both branches rather than
/// interpolated — sqlx rejects non-'static query strings outright.
pub(crate) async fn expired_tombstones<'e, E>(
    exec: E,
    library: Option<&LibraryId>,
    cutoff: DateTime<Utc>,
    limit: i64,
) -> Result<Vec<PhotoRecord>>
where
    E: Executor<'e, Database = Sqlite>,
{
    match library {
        Some(lib) => Ok(sqlx::query_as(
            "SELECT * FROM photos \
             WHERE deleted_at IS NOT NULL AND deleted_at < ? AND library_id = ? \
               AND (published_at IS NULL OR tombstone_published_at IS NOT NULL) \
             ORDER BY deleted_at LIMIT ?",
        )
        .bind(cutoff)
        .bind(lib)
        .bind(limit)
        .fetch_all(exec)
        .await?),
        None => Ok(sqlx::query_as(
            "SELECT * FROM photos \
             WHERE deleted_at IS NOT NULL AND deleted_at < ? AND library_id IS NULL \
               AND (published_at IS NULL OR tombstone_published_at IS NOT NULL) \
             ORDER BY deleted_at LIMIT ?",
        )
        .bind(cutoff)
        .bind(limit)
        .fetch_all(exec)
        .await?),
    }
}

/// The publish work queue: materialized, active, unpublished photos of any
/// library WITH a publish target — the personal partition (`scope_binding
/// IS NULL`) always has one, a scope-bound (shared) library only once an
/// operator sets `mesh_library_id` (an unbound shared library published as
/// personal-consensus photos would be exactly the dedup debt the old
/// personal-only gate existed to avoid). Tombstones are excluded:
/// tombstone propagation is out of scope, and a deleted-then-published
/// photo would be unreachable in HopNet anyway. Attempts at the cap are
/// terminal until an operator resets them.
pub(crate) async fn publishable_photos<'e, E>(
    exec: E,
    now: DateTime<Utc>,
    retry_cap: i64,
    limit: i64,
) -> Result<Vec<PhotoRecord>>
where
    E: Executor<'e, Database = Sqlite>,
{
    Ok(sqlx::query_as(
        "SELECT p.* FROM photos p \
         JOIN libraries l ON l.library_id = p.library_id \
         WHERE (l.scope_binding IS NULL OR l.mesh_library_id IS NOT NULL) \
           AND p.published_at IS NULL \
           AND p.materialized_at IS NOT NULL \
           AND p.deleted_at IS NULL \
           AND p.publish_attempts < ? \
           AND (p.publish_next_retry_at IS NULL OR p.publish_next_retry_at <= ?) \
         ORDER BY p.photo_id \
         LIMIT ?",
    )
    .bind(retry_cap)
    .bind(now)
    .bind(limit)
    .fetch_all(exec)
    .await?)
}

/// Terminal publish success: stamp once and clear the retry ledger. Guarded
/// on still-NULL so a duplicate mark (confirm race) is a no-op.
/// Stamp a first publish AND the edit-propagation baseline: the mesh now
/// holds exactly this metadata and these resource bytes.
///
/// The baseline is not bookkeeping — it is what stops the edit queue from
/// firing on every freshly published photo. With the markers left NULL,
/// "the mesh has never been told" and "the mesh is current" would be the
/// same state, and the next pass would re-upload the entire archive.
pub(crate) async fn mark_published(
    pool: &sqlx::SqlitePool,
    id: &PhotoId,
    at: DateTime<Utc>,
) -> Result<bool> {
    let mut tx = pool.begin().await?;
    let stamped = sqlx::query(
        "UPDATE photos SET published_at = ?, \
         published_asset_modified_at = asset_modified_at, publish_attempts = 0, \
         publish_next_retry_at = NULL, publish_last_error = NULL \
         WHERE photo_id = ? AND published_at IS NULL",
    )
    .bind(at)
    .bind(id)
    .execute(&mut *tx)
    .await?
    .rows_affected()
        > 0;
    if stamped {
        stamp_resource_baseline(&mut tx, id).await?;
    }
    tx.commit().await?;
    Ok(stamped)
}

/// Every written resource's bytes are now the mesh's. Runs inside the
/// publish/adopt stamp so there is no committed state where a photo reads
/// as published but its resources read as never told.
async fn stamp_resource_baseline(
    tx: &mut sqlx::Transaction<'_, Sqlite>,
    id: &PhotoId,
) -> Result<()> {
    sqlx::query(
        "UPDATE photo_resources SET published_content_hash = content_hash \
         WHERE photo_id = ? AND removed_at IS NULL AND written_at IS NOT NULL",
    )
    .bind(id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Remote adoption: the mesh already holds this asset (cloud-fingerprint
/// match), published by another device or a previous state.db. Stamp
/// published_at WITHOUT uploading and record the remote consensus id.
/// Same still-NULL guard as [`mark_published`].
pub(crate) async fn mark_adopted(
    pool: &sqlx::SqlitePool,
    id: &PhotoId,
    consensus_photo_id: &str,
    at: DateTime<Utc>,
) -> Result<bool> {
    let mut tx = pool.begin().await?;
    let stamped = sqlx::query(
        "UPDATE photos SET published_at = ?, consensus_photo_id = ?, \
         published_asset_modified_at = asset_modified_at, publish_attempts = 0, \
         publish_next_retry_at = NULL, publish_last_error = NULL \
         WHERE photo_id = ? AND published_at IS NULL",
    )
    .bind(at)
    .bind(consensus_photo_id)
    .bind(id)
    .execute(&mut *tx)
    .await?
    .rows_affected()
        > 0;
    if stamped {
        // Adoption uploads nothing, which is exactly why the baseline
        // matters most here: the mesh's copy came from another device, and
        // without the stamp this daemon would immediately "correct" it by
        // re-uploading every resource it holds.
        stamp_resource_baseline(&mut tx, id).await?;
    }
    tx.commit().await?;
    Ok(stamped)
}

/// Record one failed publish attempt. `attempts` is the caller-computed new
/// total (set to the cap for permanent rejections); `next_retry_at = None`
/// leaves the photo immediately claimable once attempts allow.
pub(crate) async fn record_publish_failure<'e, E>(
    exec: E,
    id: &PhotoId,
    attempts: i64,
    next_retry_at: Option<DateTime<Utc>>,
    error: &str,
) -> Result<()>
where
    E: Executor<'e, Database = Sqlite>,
{
    sqlx::query(
        "UPDATE photos SET publish_attempts = ?, publish_next_retry_at = ?, \
         publish_last_error = ? WHERE photo_id = ?",
    )
    .bind(attempts)
    .bind(next_retry_at)
    .bind(error)
    .bind(id)
    .execute(exec)
    .await?;
    Ok(())
}

/// Photos whose local tombstone state disagrees with what the mesh was
/// told (spec §Propagation to the mesh). Covers BOTH directions in one
/// query — a pending delete (`deleted_at` set, marker NULL) and a pending
/// restore (`deleted_at` NULL, marker set) — because they share a scope
/// partition, a responsibility gate and a retry ledger; the caller reads
/// `deleted_at` to pick the transaction.
///
/// `published_at IS NOT NULL` is load-bearing: the mesh never heard of an
/// unpublished photo, so there is nothing to tell it.
pub(crate) async fn tombstone_propagatable_photos<'e, E>(
    exec: E,
    now: DateTime<Utc>,
    retry_cap: i64,
    limit: i64,
) -> Result<Vec<PhotoRecord>>
where
    E: Executor<'e, Database = Sqlite>,
{
    Ok(sqlx::query_as(
        "SELECT * FROM photos \
         WHERE published_at IS NOT NULL \
           AND ((deleted_at IS NOT NULL AND tombstone_published_at IS NULL) \
             OR (deleted_at IS NULL AND tombstone_published_at IS NOT NULL)) \
           AND tombstone_publish_attempts < ? \
           AND (tombstone_publish_next_retry_at IS NULL \
                OR tombstone_publish_next_retry_at <= ?) \
         ORDER BY photo_id \
         LIMIT ?",
    )
    .bind(retry_cap)
    .bind(now)
    .bind(limit)
    .fetch_all(exec)
    .await?)
}

/// The mesh has been told this photo is deleted. Clears the retry ledger.
pub(crate) async fn mark_tombstone_published<'e, E>(
    exec: E,
    id: &PhotoId,
    at: DateTime<Utc>,
) -> Result<()>
where
    E: Executor<'e, Database = Sqlite>,
{
    sqlx::query(
        "UPDATE photos SET tombstone_published_at = ?, tombstone_publish_attempts = 0, \
         tombstone_publish_next_retry_at = NULL, tombstone_publish_last_error = NULL \
         WHERE photo_id = ?",
    )
    .bind(at)
    .bind(id)
    .execute(exec)
    .await?;
    Ok(())
}

/// The mesh has been told this photo is restored. Clearing the marker (not
/// stamping a second one) is what lets a later delete propagate again.
pub(crate) async fn clear_tombstone_published<'e, E>(exec: E, id: &PhotoId) -> Result<()>
where
    E: Executor<'e, Database = Sqlite>,
{
    sqlx::query(
        "UPDATE photos SET tombstone_published_at = NULL, tombstone_publish_attempts = 0, \
         tombstone_publish_next_retry_at = NULL, tombstone_publish_last_error = NULL \
         WHERE photo_id = ?",
    )
    .bind(id)
    .execute(exec)
    .await?;
    Ok(())
}

/// Record one failed propagation attempt. Same contract as
/// [`record_publish_failure`], against the tombstone ledger.
pub(crate) async fn record_tombstone_failure<'e, E>(
    exec: E,
    id: &PhotoId,
    attempts: i64,
    next_retry_at: Option<DateTime<Utc>>,
    error: &str,
) -> Result<()>
where
    E: Executor<'e, Database = Sqlite>,
{
    sqlx::query(
        "UPDATE photos SET tombstone_publish_attempts = ?, tombstone_publish_next_retry_at = ?, \
         tombstone_publish_last_error = ? WHERE photo_id = ?",
    )
    .bind(attempts)
    .bind(next_retry_at)
    .bind(error)
    .bind(id)
    .execute(exec)
    .await?;
    Ok(())
}

/// Photos whose bytes or metadata have moved on from what the mesh holds
/// (spec §Propagation of edits). Three kinds of divergence, one queue:
///
/// - a written resource whose hash no longer matches its marker (a re-edit)
/// - a resource minted after publish and never told (a first edit)
/// - a resource removed locally that the mesh still serves (a revert)
/// - `asset_modified_at` past the value the mesh's metadata was composed from
///
/// `materialized_at IS NOT NULL` gates the whole thing: a reopened resource
/// clears it, so a photo mid-refetch cannot submit half an edit and stamp it
/// converged. `deleted_at IS NULL` because the handler rejects an edit to a
/// tombstoned photo outright — those belong to the tombstone queue.
pub(crate) async fn editable_photos<'e, E>(
    exec: E,
    now: DateTime<Utc>,
    retry_cap: i64,
    limit: i64,
) -> Result<Vec<PhotoRecord>>
where
    E: Executor<'e, Database = Sqlite>,
{
    Ok(sqlx::query_as(
        "SELECT p.* FROM photos p \
         JOIN libraries l ON l.library_id = p.library_id \
         WHERE (l.scope_binding IS NULL OR l.mesh_library_id IS NOT NULL) \
           AND p.published_at IS NOT NULL \
           AND p.materialized_at IS NOT NULL \
           AND p.deleted_at IS NULL \
           AND p.edit_publish_attempts < ? \
           AND (p.edit_publish_next_retry_at IS NULL \
                OR p.edit_publish_next_retry_at <= ?) \
           AND (p.published_asset_modified_at IS NOT p.asset_modified_at \
             OR EXISTS (SELECT 1 FROM photo_resources r \
                        WHERE r.photo_id = p.photo_id \
                          AND ((r.removed_at IS NOT NULL \
                                AND r.published_content_hash IS NOT NULL) \
                            OR (r.removed_at IS NULL AND r.written_at IS NOT NULL \
                                AND r.published_content_hash IS NOT r.content_hash)))) \
         ORDER BY p.photo_id \
         LIMIT ?",
    )
    .bind(retry_cap)
    .bind(now)
    .bind(limit)
    .fetch_all(exec)
    .await?)
}

/// The mesh now holds this photo's metadata. Clears the edit ledger.
pub(crate) async fn mark_metadata_published<'e, E>(exec: E, id: &PhotoId) -> Result<()>
where
    E: Executor<'e, Database = Sqlite>,
{
    sqlx::query(
        "UPDATE photos SET published_asset_modified_at = asset_modified_at \
         WHERE photo_id = ?",
    )
    .bind(id)
    .execute(exec)
    .await?;
    Ok(())
}

/// Clear the edit retry ledger after a successful propagation.
pub(crate) async fn clear_edit_failure<'e, E>(exec: E, id: &PhotoId) -> Result<()>
where
    E: Executor<'e, Database = Sqlite>,
{
    sqlx::query(
        "UPDATE photos SET edit_publish_attempts = 0, edit_publish_next_retry_at = NULL, \
         edit_publish_last_error = NULL WHERE photo_id = ?",
    )
    .bind(id)
    .execute(exec)
    .await?;
    Ok(())
}

/// Record one failed edit propagation. Same contract as
/// [`record_publish_failure`], against the edit ledger.
pub(crate) async fn record_edit_failure<'e, E>(
    exec: E,
    id: &PhotoId,
    attempts: i64,
    next_retry_at: Option<DateTime<Utc>>,
    error: &str,
) -> Result<()>
where
    E: Executor<'e, Database = Sqlite>,
{
    sqlx::query(
        "UPDATE photos SET edit_publish_attempts = ?, edit_publish_next_retry_at = ?, \
         edit_publish_last_error = ? WHERE photo_id = ?",
    )
    .bind(attempts)
    .bind(next_retry_at)
    .bind(error)
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
    use chrono::Duration;

    async fn seed_one(store: &StateStore, desc: &crate::descriptor::AssetDescriptor) -> PhotoId {
        match seed_descriptor(store, desc).await.expect("seed") {
            SeedOutcome::MintedPending { photo_id, .. } => photo_id,
            other => panic!("expected MintedPending, got {other:?}"),
        }
    }

    /// Seed a photo and stamp it published, the precondition for every
    /// propagation queue test (an unpublished photo has nothing to tell).
    async fn seed_published(
        store: &StateStore,
        desc: &crate::descriptor::AssetDescriptor,
    ) -> PhotoId {
        let id = seed_one(store, desc).await;
        mark_published(store.pool(), &id, Utc::now()).await.unwrap();
        id
    }

    async fn propagatable(store: &StateStore) -> Vec<PhotoId> {
        tombstone_propagatable_photos(store.pool(), Utc::now(), 5, 100)
            .await
            .unwrap()
            .into_iter()
            .map(|p| p.photo_id)
            .collect()
    }

    // Impact: the two columns disagreeing IS the queue — there is no other
    // record of what the mesh has been told, so a wrong predicate here
    // either floods consensus or silently strands deletes.
    // Should: queue a published photo tombstoned but not yet propagated.
    // Should: queue a published photo restored while the marker is still set.
    // Should not: queue either converged state (both set, or both clear).
    #[tokio::test]
    async fn propagation_queue_is_the_disagreement_between_the_two_columns() {
        let (store, _) = store_with_personal().await;
        let id = seed_published(&store, &AssetDescriptorBuilder::simple_image().build()).await;

        // Live, mesh agrees.
        assert!(propagatable(&store).await.is_empty());

        // Deleted locally, mesh not told.
        tombstone_photo(store.pool(), &id, Utc::now())
            .await
            .unwrap();
        assert_eq!(propagatable(&store).await, vec![id.clone()]);

        // Converged.
        mark_tombstone_published(store.pool(), &id, Utc::now())
            .await
            .unwrap();
        assert!(propagatable(&store).await.is_empty());

        // Restored locally, mesh still tombstoned.
        restore_photo(store.pool(), &id).await.unwrap();
        assert_eq!(propagatable(&store).await, vec![id.clone()]);

        // Converged again — clearing, not re-stamping, is what closes it.
        clear_tombstone_published(store.pool(), &id).await.unwrap();
        assert!(propagatable(&store).await.is_empty());
    }

    // Impact: a delete → restore → delete cycle only converges because the
    // marker resets; were it set-once like published_at, the second delete
    // would read as already-converged and never reach the mesh.
    // Should: re-queue a second delete after a restore has cleared the marker.
    #[tokio::test]
    async fn second_delete_after_restore_queues_again() {
        let (store, _) = store_with_personal().await;
        let id = seed_published(&store, &AssetDescriptorBuilder::simple_image().build()).await;

        tombstone_photo(store.pool(), &id, Utc::now())
            .await
            .unwrap();
        mark_tombstone_published(store.pool(), &id, Utc::now())
            .await
            .unwrap();
        restore_photo(store.pool(), &id).await.unwrap();
        clear_tombstone_published(store.pool(), &id).await.unwrap();

        tombstone_photo(store.pool(), &id, Utc::now())
            .await
            .unwrap();
        assert_eq!(propagatable(&store).await, vec![id]);
    }

    // Should not: queue a tombstoned photo the mesh never received.
    #[tokio::test]
    async fn unpublished_tombstone_never_queues() {
        let (store, _) = store_with_personal().await;
        let id = seed_one(&store, &AssetDescriptorBuilder::simple_image().build()).await;
        tombstone_photo(store.pool(), &id, Utc::now())
            .await
            .unwrap();
        assert!(propagatable(&store).await.is_empty());
    }

    // Should not: queue a photo whose attempts have reached the cap.
    // Should not: queue one still inside its backoff window.
    #[tokio::test]
    async fn propagation_queue_honours_the_retry_ledger() {
        let (store, _) = store_with_personal().await;
        let id = seed_published(&store, &AssetDescriptorBuilder::simple_image().build()).await;
        tombstone_photo(store.pool(), &id, Utc::now())
            .await
            .unwrap();

        record_tombstone_failure(store.pool(), &id, 5, None, "boom")
            .await
            .unwrap();
        assert!(propagatable(&store).await.is_empty());

        let later = Utc::now() + Duration::hours(1);
        record_tombstone_failure(store.pool(), &id, 1, Some(later), "boom")
            .await
            .unwrap();
        assert!(propagatable(&store).await.is_empty());

        record_tombstone_failure(store.pool(), &id, 1, None, "boom")
            .await
            .unwrap();
        assert_eq!(propagatable(&store).await, vec![id]);
    }

    // Impact: this predicate is the only thing standing between a daemon
    // that was offline for the whole retention window and a photo stranded
    // in the mesh forever — reaping the row destroys the last record that
    // the mesh still needs telling.
    // Should: hold a published tombstone past its cutoff until propagated.
    // Should: reap it once the marker is set.
    // Should: reap an unpublished tombstone immediately — nothing to tell.
    #[tokio::test]
    async fn hard_delete_waits_for_propagation() {
        let (store, library) = store_with_personal().await;
        let long_ago = Utc::now() - Duration::days(60);
        let cutoff = Utc::now() - Duration::days(30);

        let published =
            seed_published(&store, &AssetDescriptorBuilder::simple_image().build()).await;
        let unpublished = seed_one(
            &store,
            &AssetDescriptorBuilder::simple_image()
                .with_local_id("OTHER/L0/002")
                .with_cloud_id("cloud-other")
                .build(),
        )
        .await;
        for id in [&published, &unpublished] {
            tombstone_photo(store.pool(), id, long_ago).await.unwrap();
        }

        let expired = |ids: Vec<PhotoRecord>| -> Vec<PhotoId> {
            ids.into_iter().map(|p| p.photo_id).collect()
        };

        // The published tombstone is held back; the unpublished one is not.
        let candidates = expired(
            expired_tombstones(store.pool(), Some(&library), cutoff, 100)
                .await
                .unwrap(),
        );
        assert_eq!(candidates, vec![unpublished]);

        mark_tombstone_published(store.pool(), &published, Utc::now())
            .await
            .unwrap();
        let candidates = expired(
            expired_tombstones(store.pool(), Some(&library), cutoff, 100)
                .await
                .unwrap(),
        );
        assert!(candidates.contains(&published));
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
