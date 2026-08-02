//! `photo_resources` table access.

use chrono::{DateTime, Utc};
use sqlx::Executor;
use sqlx::sqlite::Sqlite;

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
    /// the resource row, upsert the blob refcount, swap out a superseded
    /// blob if the row was reopened by a re-edit, and stamp
    /// `photos.materialized_at` if this was the photo's last pending
    /// resource.
    ///
    /// `content_hash` and `written_at` commit together or not at all
    /// (two-state rule, spec §Per-resource state machine); the refcount
    /// changes ride the same transaction (spec §blobs notes). A reopened row
    /// (`written_at` NULL, `content_hash` retained as the superseded
    /// pointer) has its old blob decremented here — atomically with the
    /// replacement bytes, so there is no committed state where the old
    /// render is gone and the new one isn't. When old == new (fileSize
    /// false positive), increment-then-decrement of the same key nets to a
    /// no-op and nothing is logged.
    pub async fn mark_resource_written(
        &self,
        photo_id: &PhotoId,
        resource_type: ResourceType,
        hash: &ContentHash,
        ext: &str,
        size_bytes: i64,
    ) -> Result<WriteCommit> {
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

        // Superseded pointer: a re-edit reopened this row keeping its old
        // hash so the swap could happen here, with the new bytes.
        let superseded: Option<ContentHash> = sqlx::query_scalar(
            "SELECT content_hash FROM photo_resources \
             WHERE photo_id = ? AND resource_type = ? AND written_at IS NULL",
        )
        .bind(photo_id)
        .bind(resource_type)
        .fetch_optional(&mut *tx)
        .await?
        .flatten();

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

        let mut reap_superseded = None;
        if let Some(old) = superseded {
            let reaped_ext = super::blobs::decrement_and_reap(&mut tx, &library_id, &old).await?;
            if old != *hash {
                super::log::append(
                    &mut *tx,
                    "blob_superseded",
                    Some(photo_id),
                    Some(serde_json::json!({
                        "resource_type": resource_type.as_str(),
                        "old": old.to_string(),
                        "new": hash.to_string(),
                    })),
                )
                .await?;
            }
            if let Some(ext) = reaped_ext {
                reap_superseded = Some((old, ext));
            }
        }

        // Photo-level completion: same transaction as the final resource
        // write. Completion triggers the capsule (re)write.
        let photo_completed = sqlx::query(
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
        Ok(WriteCommit {
            photo_completed,
            reap_superseded,
        })
    }
}

/// Outcome of [`StateStore::mark_resource_written`].
#[derive(Debug)]
pub struct WriteCommit {
    /// This write stamped `photos.materialized_at` (last pending resource).
    pub photo_completed: bool,
    /// A superseded blob's refcount hit 0 in the swap: the caller deletes
    /// `blobs/<aa>/<bb>/<hash>.<ext>` after this transaction has committed.
    pub reap_superseded: Option<(ContentHash, String)>,
}

/// Summary of resources awaiting retry (drain exit report).
#[derive(Debug, Clone, sqlx::FromRow, serde::Serialize)]
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

/// The photo's LIVE resources. Rows awaiting removal propagation are
/// excluded: locally the resource is already gone, and every consumer here
/// (sidecar compose, publish assembly, status) describes what Photos has,
/// not what the mesh has yet to be told.
pub(crate) async fn resources_for_photo<'e, E>(exec: E, id: &PhotoId) -> Result<Vec<ResourceRecord>>
where
    E: Executor<'e, Database = Sqlite>,
{
    Ok(sqlx::query_as(
        "SELECT * FROM photo_resources WHERE photo_id = ? AND removed_at IS NULL \
         ORDER BY resource_type",
    )
    .bind(id)
    .fetch_all(exec)
    .await?)
}

/// Kinds this photo has removed locally but the mesh still holds.
pub(crate) async fn pending_removals<'e, E>(exec: E, id: &PhotoId) -> Result<Vec<ResourceType>>
where
    E: Executor<'e, Database = Sqlite>,
{
    Ok(sqlx::query_scalar(
        "SELECT resource_type FROM photo_resources \
         WHERE photo_id = ? AND removed_at IS NOT NULL \
           AND published_content_hash IS NOT NULL \
         ORDER BY resource_type",
    )
    .bind(id)
    .fetch_all(exec)
    .await?)
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

/// Re-edit reopen: put a written row back in the work queue (`written_at`
/// NULL + fresh retry state) while KEEPING `content_hash`/`ext`/`size_bytes`
/// as the superseded pointer — `mark_resource_written` swaps the blob
/// refcounts atomically with the replacement bytes. The guard makes redundant
/// observer deliveries idempotent (an already-pending row is left alone).
pub(crate) async fn reopen_resource<'e, E>(
    exec: E,
    photo_id: &PhotoId,
    resource_type: ResourceType,
) -> Result<bool>
where
    E: Executor<'e, Database = Sqlite>,
{
    Ok(sqlx::query(
        "UPDATE photo_resources \
         SET written_at = NULL, retry_count = 0, next_retry_at = NULL, last_error = NULL \
         WHERE photo_id = ? AND resource_type = ? AND written_at IS NOT NULL",
    )
    .bind(photo_id)
    .bind(resource_type)
    .execute(exec)
    .await?
    .rows_affected()
        > 0)
}

/// Re-enqueue terminally-pending resources (spec §Failure Handling: "Terminal
/// resources are automatically re-enqueued by the next reconciliation scan").
/// Touches only pending rows at/over the cap — written rows and healthy
/// retry state are left alone.
pub(crate) async fn reset_gave_up<'e, E>(exec: E, retry_cap: i64) -> Result<u64>
where
    E: Executor<'e, Database = Sqlite>,
{
    Ok(sqlx::query(
        "UPDATE photo_resources SET retry_count = 0, next_retry_at = NULL \
         WHERE written_at IS NULL AND retry_count >= ?",
    )
    .bind(retry_cap)
    .execute(exec)
    .await?
    .rows_affected())
}

/// Delete one resource row (revert-to-original observed). The caller has
/// already read the row and handles the blob decrement + file reap for
/// written rows in the same transaction.
pub(crate) async fn delete_resource_row<'e, E>(
    exec: E,
    photo_id: &PhotoId,
    resource_type: ResourceType,
) -> Result<()>
where
    E: Executor<'e, Database = Sqlite>,
{
    sqlx::query("DELETE FROM photo_resources WHERE photo_id = ? AND resource_type = ?")
        .bind(photo_id)
        .bind(resource_type)
        .execute(exec)
        .await?;
    Ok(())
}

/// Retire a resource the MESH still holds: keep the row as a removal
/// marker until propagation clears it. Returns false when the mesh never
/// saw this resource, in which case the caller hard-deletes instead.
///
/// The content columns are cleared with the same write — the blob's
/// refcount was decremented alongside this call, so a row still naming the
/// hash would make `fsck` report drift and would keep the eviction guard
/// pinning bytes nothing references. What survives is
/// `published_content_hash`: the only record that the mesh is owed a
/// removal at all.
pub(crate) async fn soft_remove_resource<'e, E>(
    exec: E,
    photo_id: &PhotoId,
    resource_type: ResourceType,
    at: DateTime<Utc>,
) -> Result<bool>
where
    E: Executor<'e, Database = Sqlite>,
{
    Ok(sqlx::query(
        "UPDATE photo_resources \
         SET removed_at = ?, content_hash = NULL, ext = NULL, size_bytes = NULL, \
             retry_count = 0, next_retry_at = NULL, last_error = NULL \
         WHERE photo_id = ? AND resource_type = ? AND published_content_hash IS NOT NULL",
    )
    .bind(at)
    .bind(photo_id)
    .bind(resource_type)
    .execute(exec)
    .await?
    .rows_affected()
        > 0)
}

/// Stamp what the mesh now holds for one resource.
pub(crate) async fn mark_resource_edit_published<'e, E>(
    exec: E,
    photo_id: &PhotoId,
    resource_type: ResourceType,
    hash: &ContentHash,
) -> Result<()>
where
    E: Executor<'e, Database = Sqlite>,
{
    sqlx::query(
        "UPDATE photo_resources SET published_content_hash = ? \
         WHERE photo_id = ? AND resource_type = ?",
    )
    .bind(hash)
    .bind(photo_id)
    .bind(resource_type)
    .execute(exec)
    .await?;
    Ok(())
}

/// The removal reached the mesh; the marker row has no further job.
pub(crate) async fn finish_resource_removal<'e, E>(
    exec: E,
    photo_id: &PhotoId,
    resource_type: ResourceType,
) -> Result<()>
where
    E: Executor<'e, Database = Sqlite>,
{
    sqlx::query(
        "DELETE FROM photo_resources \
         WHERE photo_id = ? AND resource_type = ? AND removed_at IS NOT NULL",
    )
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::{AssetDescriptorBuilder, store_with_personal};
    use crate::resolve::{SeedOutcome, seed_descriptor};
    use crate::store::StateStore;

    async fn seeded_live_photo(store: &StateStore) -> PhotoId {
        let desc = AssetDescriptorBuilder::live_photo().build();
        match seed_descriptor(store, &desc).await.expect("seed") {
            SeedOutcome::MintedPending { photo_id, .. } => photo_id,
            other => panic!("expected MintedPending, got {other:?}"),
        }
    }

    async fn resource(
        store: &StateStore,
        id: &PhotoId,
        rt: ResourceType,
    ) -> crate::model::ResourceRecord {
        store
            .resources_for_photo(id)
            .await
            .unwrap()
            .into_iter()
            .find(|r| r.resource_type == rt)
            .expect("resource row")
    }

    // Impact: a re-edit that never re-enters the work queue is silent data
    // staleness — the archive keeps serving the old render forever.
    // Should: reopened row (written_at NULL, superseded hash KEPT) makes
    // pending_photos return the photo again after clear_materialized.
    // Should not: reopen an already-pending row (redundant event delivery).
    #[tokio::test]
    async fn reopen_reenters_work_queue_keeping_superseded_hash() {
        let (store, _) = store_with_personal().await;
        let id = seeded_live_photo(&store).await;
        let old = ContentHash::of_bytes(b"render-v1");
        store
            .mark_resource_written(
                &id,
                ResourceType::Original,
                &ContentHash::of_bytes(b"orig"),
                "heic",
                10,
            )
            .await
            .unwrap();
        let commit = store
            .mark_resource_written(&id, ResourceType::PairedVideo, &old, "mov", 20)
            .await
            .unwrap();
        assert!(commit.photo_completed);

        assert!(
            reopen_resource(store.pool(), &id, ResourceType::PairedVideo)
                .await
                .unwrap()
        );
        assert!(
            !reopen_resource(store.pool(), &id, ResourceType::PairedVideo)
                .await
                .unwrap(),
            "second reopen is a no-op on the now-pending row"
        );
        super::super::photos::clear_materialized(store.pool(), &id)
            .await
            .unwrap();

        let row = resource(&store, &id, ResourceType::PairedVideo).await;
        assert!(row.written_at.is_none());
        assert_eq!(
            row.content_hash,
            Some(old),
            "superseded pointer survives the reopen"
        );

        let queue = super::super::photos::pending_photos(store.pool(), 5, Utc::now(), 10)
            .await
            .unwrap();
        assert_eq!(queue.iter().filter(|p| p.photo_id == id).count(), 1);
    }

    // Impact: the swap is the byte-loss/leak boundary of re-edit — decrementing
    // the wrong side or skipping the log makes superseded bytes untraceable.
    // Should: commit new hash, decrement old blob (reap at 0), log
    // blob_superseded, all observable after one call.
    #[tokio::test]
    async fn written_swap_decrements_old_blob_and_logs() {
        let (store, lib) = store_with_personal().await;
        let id = seeded_live_photo(&store).await;
        let old = ContentHash::of_bytes(b"render-v1");
        let new = ContentHash::of_bytes(b"render-v2");
        store
            .mark_resource_written(
                &id,
                ResourceType::Original,
                &ContentHash::of_bytes(b"orig"),
                "heic",
                10,
            )
            .await
            .unwrap();
        store
            .mark_resource_written(&id, ResourceType::PairedVideo, &old, "mov", 20)
            .await
            .unwrap();

        reopen_resource(store.pool(), &id, ResourceType::PairedVideo)
            .await
            .unwrap();
        super::super::photos::clear_materialized(store.pool(), &id)
            .await
            .unwrap();

        let commit = store
            .mark_resource_written(&id, ResourceType::PairedVideo, &new, "mov", 22)
            .await
            .unwrap();
        assert!(
            commit.photo_completed,
            "photo completes again after the refetch"
        );
        assert_eq!(
            commit.reap_superseded,
            Some((old.clone(), "mov".to_string())),
            "old blob hit refcount 0 — caller must delete the file"
        );
        assert!(
            store.blob(&lib, &old).await.unwrap().is_none(),
            "reaped row is gone"
        );
        assert_eq!(store.blob(&lib, &new).await.unwrap().unwrap().ref_count, 1);
        assert_eq!(store.log_events("blob_superseded").await.unwrap().len(), 1);
    }

    // Impact: fileSize compare can false-positive (re-edit yielding an equal
    // byte count is detected, but an UNCHANGED render refetched must not
    // delete live bytes or spam the log).
    // Should not: log blob_superseded or reap when old == new.
    #[tokio::test]
    async fn same_hash_swap_is_silent_noop() {
        let (store, lib) = store_with_personal().await;
        let id = seeded_live_photo(&store).await;
        let hash = ContentHash::of_bytes(b"render-v1");
        store
            .mark_resource_written(
                &id,
                ResourceType::Original,
                &ContentHash::of_bytes(b"orig"),
                "heic",
                10,
            )
            .await
            .unwrap();
        store
            .mark_resource_written(&id, ResourceType::PairedVideo, &hash, "mov", 20)
            .await
            .unwrap();

        reopen_resource(store.pool(), &id, ResourceType::PairedVideo)
            .await
            .unwrap();
        super::super::photos::clear_materialized(store.pool(), &id)
            .await
            .unwrap();
        let commit = store
            .mark_resource_written(&id, ResourceType::PairedVideo, &hash, "mov", 20)
            .await
            .unwrap();

        assert!(
            commit.reap_superseded.is_none(),
            "increment+decrement of the same key nets out"
        );
        assert_eq!(store.blob(&lib, &hash).await.unwrap().unwrap().ref_count, 1);
        assert!(
            store
                .log_events("blob_superseded")
                .await
                .unwrap()
                .is_empty()
        );
    }

    // Impact: spec §Failure Handling — transient iCloud outages must self-heal
    // on the next scan without resurrecting healthy retry state.
    // Should: reset only pending rows at/over the cap.
    // Should not: touch written rows or sub-cap retry counts.
    #[tokio::test]
    async fn reset_gave_up_only_touches_terminal_pending() {
        let (store, _) = store_with_personal().await;
        let id = seeded_live_photo(&store).await;
        let cap = 3i64;
        for _ in 0..cap {
            store
                .record_resource_failure(&id, ResourceType::Original, "net down", Utc::now(), cap)
                .await
                .unwrap();
        }
        store
            .record_resource_failure(&id, ResourceType::PairedVideo, "net down", Utc::now(), cap)
            .await
            .unwrap();

        let reset = reset_gave_up(store.pool(), cap).await.unwrap();
        assert_eq!(reset, 1, "only the terminal original resets");

        let orig = resource(&store, &id, ResourceType::Original).await;
        assert_eq!(orig.retry_count, 0);
        assert!(orig.next_retry_at.is_none());
        let paired = resource(&store, &id, ResourceType::PairedVideo).await;
        assert_eq!(paired.retry_count, 1, "sub-cap retry state survives");
    }
}
