//! The lifecycle job (spec §Hard-delete cleanup):
//! hard-delete tombstones past retention, prune the ingest log, and
//! spool-evict consensus-decided blobs.
//!
//! The daemon loop runs `run_cleanup` on a timer; the one-shot CLI
//! subcommand (`run_standalone`) runs it once under the exclusive run lock.

use std::fs;


use chrono::{DateTime, Duration, Utc};

use crate::error::Result;
use crate::ids::{ContentHash, PhotoId};
use crate::model::LibraryConfig;
use crate::paths::{DataDir, SpoolPaths};
use crate::store::StateStore;

/// Retention for unmapped tombstones (NULL library — no per-library config
/// to read). Matches the schema default.
pub const DEFAULT_RETENTION_DAYS: i64 = 30;

#[derive(Debug, Clone)]
pub struct CleanupConfig {
    /// Ingest-log rows older than this are pruned (spec: 180).
    pub log_retention_days: i64,
    /// Hard-delete cap per run — a whole-library expiry must not stall the
    /// daemon loop; the remainder processes on subsequent ticks.
    pub hard_delete_batch: usize,
}

impl Default for CleanupConfig {
    fn default() -> Self {
        Self {
            log_retention_days: 180,
            hard_delete_batch: 500,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct CleanupReport {
    pub photos_hard_deleted: u64,
    pub blob_files_deleted: u64,
    pub log_rows_pruned: u64,
    /// Spool-evicted blobs: local bytes deleted because every referencing
    /// photo is consensus-decided in HopNet (the crash-window sweep behind
    /// the publish pass's own eviction).
    pub spool_evicted: u64,
}

impl CleanupReport {
    pub fn absorb(&mut self, other: &CleanupReport) {
        self.photos_hard_deleted += other.photos_hard_deleted;
        self.blob_files_deleted += other.blob_files_deleted;
        self.log_rows_pruned += other.log_rows_pruned;
        self.spool_evicted += other.spool_evicted;
    }
}

/// The hourly job. `now` is injected (tests; also keeps one consistent
/// instant across the sub-steps).
pub async fn run_cleanup(
    store: &StateStore,
    data_dir: &DataDir,
    cfg: &CleanupConfig,
    now: DateTime<Utc>,
) -> Result<CleanupReport> {
    let spool = data_dir.spool();
    let mut report = CleanupReport::default();
    hard_delete_expired(store, &spool, cfg, now, &mut report).await?;
    report.log_rows_pruned =
        crate::store::log::prune_before(store.pool(), now - Duration::days(cfg.log_retention_days))
            .await?;
    report.spool_evicted =
        evict_published_blobs(store, &spool, cfg.hard_delete_batch as i64).await?;
    Ok(report)
}

// ---------------------------------------------------------------- spool eviction

/// Delete the local bytes of every blob whose referencing photos are all
/// consensus-decided (published or adopted): HopNet holds the archive copy,
/// so the spool entry's job is done. Stamp-then-unlink — a crash between
/// the two leaves an evicted row with a lingering file, which fsck
/// classifies as a benign orphan rather than byte loss. Runs at the end of
/// every publish pass (minimal residence) and on the cleanup tick (crash
/// windows).
pub async fn evict_published_blobs(
    store: &StateStore,
    spool: &SpoolPaths,
    limit: i64,
) -> Result<u64> {
    let candidates = store.evictable_blobs(limit).await?;
    if candidates.is_empty() {
        return Ok(0);
    }

    let mut evicted = 0u64;
    for blob in candidates {
        store
            .stamp_blob_evicted(&blob.library_id, &blob.content_hash)
            .await?;
        // The spool is process-global: unlink only once NO library's row
        // still expects the bytes (another library's undecided referent may
        // share the file).
        if !store.hash_is_live(&blob.content_hash).await? {
            let _ = fs::remove_file(spool.blob_path(&blob.content_hash, &blob.ext));
        }
        evicted += 1;
    }
    if evicted > 0 {
        store
            .append_log(
                "spool_evicted",
                None,
                Some(serde_json::json!({ "blobs": evicted })),
            )
            .await?;
    }
    Ok(evicted)
}

/// One-shot entry for the CLI subcommand: exclusive lock (errors while the
/// daemon holds it), Tier-1 repair on an unclean reclaim, then one cleanup
/// run.
pub async fn run_standalone(
    store: &StateStore,
    data_dir: &DataDir,
    cfg: &CleanupConfig,
    now: DateTime<Utc>,
) -> Result<CleanupReport> {
    let acquired = crate::runlock::DrainLock::acquire(data_dir)?;
    if acquired.unclean {
        crate::recovery::repair_refcounts(store).await?;
    }
    let cleanup = run_cleanup(store, data_dir, cfg, now).await?;
    drop(acquired.lock);
    Ok(cleanup)
}

// ---------------------------------------------------------------- hard delete

/// Spec §Hard-delete cleanup, with the in-tx `hard_delete` log errata: the
/// black-box row commits atomically with the row deletions — a crash between
/// tx and fs cleanup must never leave a vanished photo with a silent log.
async fn hard_delete_expired(
    store: &StateStore,
    spool: &SpoolPaths,
    cfg: &CleanupConfig,
    now: DateTime<Utc>,
    report: &mut CleanupReport,
) -> Result<()> {
    let libraries = store.libraries().await?;
    let mut budget = cfg.hard_delete_batch as i64;

    // Per-library passes (retention_days read fresh each run — a config
    // change applies from the next run, spec edge-case table), then the
    // unmapped pass under the fixed default.
    let mut passes: Vec<(Option<LibraryConfig>, i64)> = libraries
        .iter()
        .map(|l| (Some(l.clone()), l.retention_days))
        .collect();
    passes.push((None, DEFAULT_RETENTION_DAYS));

    for (library, retention_days) in passes {
        if budget <= 0 {
            break;
        }
        let cutoff = now - Duration::days(retention_days);
        let candidates = crate::store::photos::expired_tombstones(
            store.pool(),
            library.as_ref().map(|l| &l.library_id),
            cutoff,
            budget,
        )
        .await?;
        for photo in candidates {
            hard_delete_one(store, spool, library.as_ref(), &photo.photo_id, report).await?;
            budget -= 1;
        }
    }
    Ok(())
}

async fn hard_delete_one(
    store: &StateStore,
    spool: &SpoolPaths,
    library: Option<&LibraryConfig>,
    photo_id: &PhotoId,
    report: &mut CleanupReport,
) -> Result<()> {
    let mut reap: Vec<(ContentHash, String)> = Vec::new();
    let mut resources_detail: Vec<serde_json::Value> = Vec::new();

    let mut tx = store.pool().begin().await?;
    // Guarded re-check inside the tx: a restore may have raced the candidate
    // query (idempotency backstop; the daemon loop serializes event
    // application, so this is belt-and-braces).
    let still_tombstoned: bool =
        sqlx::query_scalar("SELECT deleted_at IS NOT NULL FROM photos WHERE photo_id = ?")
            .bind(photo_id)
            .fetch_optional(&mut *tx)
            .await?
            .unwrap_or(false);
    if !still_tombstoned {
        return Ok(());
    }

    let rows = crate::store::resources::resources_for_photo(&mut *tx, photo_id).await?;
    for row in &rows {
        // Hash-gate, not written_at: superseded-pending rows still hold a
        // refcount (same rule as the revert path).
        if let (Some(hash), Some(library)) = (&row.content_hash, library)
            && let Some(ext) =
                crate::store::blobs::decrement_and_reap(&mut tx, &library.library_id, hash).await?
        {
            reap.push((hash.clone(), ext));
        }
        resources_detail.push(serde_json::json!({
            "type": row.resource_type.as_str(),
            "hash": row.content_hash.as_ref().map(|h| h.to_string()),
            "ext": row.ext,
        }));
    }
    sqlx::query("DELETE FROM photo_resources WHERE photo_id = ?")
        .bind(photo_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM photos WHERE photo_id = ?")
        .bind(photo_id)
        .execute(&mut *tx)
        .await?;
    crate::store::log::append(
        &mut *tx,
        "hard_delete",
        Some(photo_id),
        Some(serde_json::json!({
            "library": library.map(|l| l.library_id.to_string()),
            "resources": resources_detail,
            "reaped": reap.iter().map(|(h, _)| h.to_string()).collect::<Vec<_>>(),
        })),
    )
    .await?;
    tx.commit().await?;
    report.photos_hard_deleted += 1;

    // Post-tx filesystem cleanup: crash here leaves benign orphans (spec's
    // crash-window rationale — the tx is authoritative). Liveness-guarded:
    // the spool is process-global, so another library's row may still back
    // the same file.
    for (hash, ext) in &reap {
        if !store.hash_is_live(hash).await?
            && fs::remove_file(spool.blob_path(hash, ext)).is_ok()
        {
            report.blob_files_deleted += 1;
        }
    }
    Ok(())
}

