//! Read-only aggregate queries feeding the CLI `status` view.

use std::collections::HashMap;

use crate::error::Result;
use crate::ids::LibraryId;

use super::StateStore;

/// Per-library aggregate counters (one row per configured library; a library
/// with no photos yet reports zeros).
#[derive(Debug, Clone, serde::Serialize)]
pub struct LibraryStats {
    pub library_id: LibraryId,
    /// Photos with no tombstone.
    pub photos_active: i64,
    /// Photos awaiting hard-delete (`deleted_at` set).
    pub tombstones: i64,
    /// Active photos not yet fully materialized.
    pub photos_pending: i64,
    /// Photos whose local sidecar is newer than the remote copy.
    pub dirty_sidecars: i64,
    pub blob_count: i64,
    pub blob_bytes: i64,
}

impl StateStore {
    /// Aggregate photo/blob counters per configured library. Unmapped
    /// photos (`library_id IS NULL`) are excluded — they are reported
    /// separately via [`StateStore::count_unmapped_photos`].
    pub async fn library_stats(&self) -> Result<Vec<LibraryStats>> {
        let photo_rows: Vec<(LibraryId, i64, i64, i64, i64)> = sqlx::query_as(
            "SELECT library_id, \
                    COUNT(*) FILTER (WHERE deleted_at IS NULL), \
                    COUNT(*) FILTER (WHERE deleted_at IS NOT NULL), \
                    COUNT(*) FILTER (WHERE deleted_at IS NULL AND materialized_at IS NULL), \
                    COUNT(*) FILTER (WHERE sidecar_replicated_at IS NULL AND materialized_at IS NOT NULL) \
             FROM photos WHERE library_id IS NOT NULL GROUP BY library_id",
        )
        .fetch_all(self.pool())
        .await?;
        let blob_rows: Vec<(LibraryId, i64, i64)> = sqlx::query_as(
            "SELECT library_id, COUNT(*), COALESCE(SUM(size_bytes), 0) \
             FROM blobs GROUP BY library_id",
        )
        .fetch_all(self.pool())
        .await?;

        let mut photos: HashMap<LibraryId, (i64, i64, i64, i64)> = photo_rows
            .into_iter()
            .map(|(lib, active, tomb, pending, dirty)| (lib, (active, tomb, pending, dirty)))
            .collect();
        let mut blobs: HashMap<LibraryId, (i64, i64)> = blob_rows
            .into_iter()
            .map(|(lib, count, bytes)| (lib, (count, bytes)))
            .collect();

        let mut stats = Vec::new();
        for lib in self.libraries().await? {
            let (photos_active, tombstones, photos_pending, dirty_sidecars) =
                photos.remove(&lib.library_id).unwrap_or_default();
            let (blob_count, blob_bytes) = blobs.remove(&lib.library_id).unwrap_or_default();
            stats.push(LibraryStats {
                library_id: lib.library_id,
                photos_active,
                tombstones,
                photos_pending,
                dirty_sidecars,
                blob_count,
                blob_bytes,
            });
        }
        Ok(stats)
    }

    /// Photos whose PhotoKit scope has no configured binding (ingest
    /// blocked until the user binds it).
    pub async fn count_unmapped_photos(&self) -> Result<i64> {
        Ok(
            sqlx::query_scalar("SELECT COUNT(*) FROM photos WHERE library_id IS NULL")
                .fetch_one(self.pool())
                .await?,
        )
    }

    /// Unwritten resources that have never failed a fetch (no backoff
    /// deadline) on active, mapped photos — the "fresh work" complement to
    /// [`StateStore::retry_summary`]'s awaiting-retry/gave-up split.
    pub async fn count_pending_resources(&self) -> Result<i64> {
        Ok(sqlx::query_scalar(
            "SELECT COUNT(*) FROM photo_resources r \
             JOIN photos p ON p.photo_id = r.photo_id \
             WHERE r.written_at IS NULL AND r.next_retry_at IS NULL \
               AND p.deleted_at IS NULL AND p.library_id IS NOT NULL",
        )
        .fetch_one(self.pool())
        .await?)
    }
}
