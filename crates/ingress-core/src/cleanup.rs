//! The lifecycle job (spec §Hard-delete cleanup, §State snapshots, §photos
//! notes on `sidecar_replicated_at`): hard-delete tombstones past retention,
//! prune the ingest log, write daily state snapshots to the storage roots,
//! and drain the dirty-sidecar set to each library's remote backup root.
//!
//! Hourly work (`run_cleanup`) and the faster replication drain
//! (`replicate_dirty_sidecars`) are separate entry points: the daemon loop
//! runs them on independent timers, and the one-shot CLI subcommand
//! (`run_standalone`) runs both once under the exclusive run lock.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Datelike, Duration, Utc};

use crate::error::{IngressError, Result};
use crate::ids::{ContentHash, PhotoId};
use crate::model::LibraryConfig;
use crate::paths::{BlobPaths, DataDir};
use crate::sidecar_io::find_sidecar;
use crate::store::StateStore;

/// Retention for unmapped tombstones (NULL library — no per-library config
/// to read). Matches the schema default.
pub const DEFAULT_RETENTION_DAYS: i64 = 30;

#[derive(Debug, Clone)]
pub struct CleanupConfig {
    /// Ingest-log rows older than this are pruned (spec: 180).
    pub log_retention_days: i64,
    /// Snapshots kept per storage root (spec: newest 7).
    pub snapshot_keep: usize,
    /// Hard-delete cap per run — a whole-library expiry must not stall the
    /// daemon loop; the remainder processes on subsequent ticks.
    pub hard_delete_batch: usize,
    /// Sidecar replication cap per pass (same rationale).
    pub replication_batch: usize,
}

impl Default for CleanupConfig {
    fn default() -> Self {
        Self {
            log_retention_days: 180,
            snapshot_keep: 7,
            hard_delete_batch: 500,
            replication_batch: 500,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct CleanupReport {
    pub photos_hard_deleted: u64,
    pub blob_files_deleted: u64,
    pub log_rows_pruned: u64,
    pub snapshots_written: u64,
}

impl CleanupReport {
    pub fn absorb(&mut self, other: &CleanupReport) {
        self.photos_hard_deleted += other.photos_hard_deleted;
        self.blob_files_deleted += other.blob_files_deleted;
        self.log_rows_pruned += other.log_rows_pruned;
        self.snapshots_written += other.snapshots_written;
    }
}

#[derive(Debug, Default, Clone)]
pub struct ReplicationReport {
    pub replicated: u64,
    /// Dirty photos whose local sidecar is missing (crash between the
    /// completion tx and the first sidecar write) — skipped unstamped;
    /// fsck tier-2's class.
    pub missing: u64,
    /// Dirty photos the mount accepted the root of but refused the copy for
    /// (e.g. a destination clash, or a per-file fs error) — skipped unstamped
    /// and retried next pass, but NOT allowed to stall the whole drain. The
    /// poison-pill guard: one un-copyable photo at the head of the `photo_id`
    /// order must not freeze replication for every following photo/library.
    pub failed: u64,
    pub stalled: bool,
}

impl ReplicationReport {
    pub fn absorb(&mut self, other: &ReplicationReport) {
        self.replicated += other.replicated;
        self.missing += other.missing;
        self.failed += other.failed;
        self.stalled = other.stalled;
    }
}

/// Edge-trigger state for `mount_lost`/`mount_regained`, held by the caller
/// across ticks so a down mount logs once, not per pass.
#[derive(Debug, Default)]
pub struct ReplicationState {
    stalled: bool,
}

/// The hourly job. `now` is injected (tests; also keeps one consistent
/// instant across the sub-steps).
pub async fn run_cleanup(
    store: &StateStore,
    data_dir: &DataDir,
    cfg: &CleanupConfig,
    now: DateTime<Utc>,
) -> Result<CleanupReport> {
    let mut report = CleanupReport::default();
    hard_delete_expired(store, data_dir, cfg, now, &mut report).await?;
    report.log_rows_pruned =
        crate::store::log::prune_before(store.pool(), now - Duration::days(cfg.log_retention_days))
            .await?;
    report.snapshots_written = snapshot_if_due(store, data_dir, cfg, now).await?;
    Ok(report)
}

/// One-shot entry for the CLI subcommand: exclusive lock (errors while the
/// daemon holds it), Tier-1 repair on an unclean reclaim, then one cleanup
/// run + one replication pass (no daemon → empty skip set).
pub async fn run_standalone(
    store: &StateStore,
    data_dir: &DataDir,
    cfg: &CleanupConfig,
    now: DateTime<Utc>,
) -> Result<(CleanupReport, ReplicationReport)> {
    let acquired = crate::runlock::DrainLock::acquire(data_dir)?;
    if acquired.unclean {
        crate::recovery::repair_refcounts(store).await?;
    }
    let cleanup = run_cleanup(store, data_dir, cfg, now).await?;
    let mut state = ReplicationState::default();
    let replication = replicate_dirty_sidecars(
        store,
        data_dir,
        cfg.replication_batch,
        &HashSet::new(),
        &mut state,
    )
    .await?;
    drop(acquired.lock);
    Ok((cleanup, replication))
}

// ---------------------------------------------------------------- hard delete

/// Spec §Hard-delete cleanup, with the in-tx `hard_delete` log errata: the
/// black-box row commits atomically with the row deletions — a crash between
/// tx and fs cleanup must never leave a vanished photo with a silent log.
async fn hard_delete_expired(
    store: &StateStore,
    data_dir: &DataDir,
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
            hard_delete_one(store, data_dir, library.as_ref(), &photo.photo_id, report).await?;
            budget -= 1;
        }
    }
    Ok(())
}

async fn hard_delete_one(
    store: &StateStore,
    data_dir: &DataDir,
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
    // crash-window rationale — the tx is authoritative).
    if let Some(library) = library {
        let paths = BlobPaths::new(&library.blob_root);
        for (hash, ext) in &reap {
            if fs::remove_file(paths.blob_path(hash, ext)).is_ok() {
                report.blob_files_deleted += 1;
            }
        }
        // Local sidecar (rel-path derived from the local hit), then the
        // remote copy best-effort.
        let local_root = data_dir.sidecar_root(&library.library_id);
        if let Some(local) = find_sidecar(&local_root, photo_id)? {
            let rel = local.strip_prefix(&local_root).ok().map(Path::to_path_buf);
            let _ = fs::remove_file(&local);
            if let (Some(rel), Some(remote)) = (rel, &library.sidecar_root_remote) {
                let _ = fs::remove_file(Path::new(remote).join(rel));
            }
        }
    }
    Ok(())
}

// ------------------------------------------------------------------ snapshots

/// Daily snapshots (spec §State snapshots, VACUUM INTO errata): due when a
/// root's newest parseable `state.db.<unix-ts>.sqlite3` is from an earlier
/// UTC day (or absent). One shared VACUUM per tick serves every due root.
async fn snapshot_if_due(
    store: &StateStore,
    data_dir: &DataDir,
    cfg: &CleanupConfig,
    now: DateTime<Utc>,
) -> Result<u64> {
    let libraries = store.libraries().await?;
    let due: Vec<&LibraryConfig> = libraries
        .iter()
        .filter(|l| {
            let dir = BlobPaths::new(&l.blob_root).snapshot_dir();
            match newest_snapshot_ts(&dir) {
                Some(ts) => DateTime::from_timestamp(ts, 0)
                    .map(|t| (t.year(), t.ordinal()) < (now.year(), now.ordinal()))
                    .unwrap_or(true),
                None => true,
            }
        })
        .collect();
    if due.is_empty() {
        return Ok(0);
    }

    // Stage locally: VACUUM INTO refuses an existing target, so clean the
    // temp dir first (a leftover from a crashed run). Runs as a plain
    // statement on the pool — VACUUM cannot run inside a transaction.
    let tmp_dir = data_dir.snapshot_tmp_dir();
    let _ = fs::remove_dir_all(&tmp_dir);
    fs::create_dir_all(&tmp_dir)
        .map_err(|e| IngressError::Invariant(format!("snapshot tmp dir: {e}")))?;
    let name = format!("state.db.{}.sqlite3", now.timestamp());
    let tmp_path = tmp_dir.join(&name);
    // Literal path, not a bound parameter: `VACUUM INTO ?` prepares and
    // "succeeds" under sqlx without writing anything (probed). The path is
    // daemon-derived (data dir + unix timestamp), not user input; quotes
    // escaped anyway.
    let escaped = tmp_path.to_string_lossy().replace('\'', "''");
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!("VACUUM INTO '{escaped}'")))
        .execute(store.pool())
        .await?;
    if !tmp_path.is_file() {
        return Err(IngressError::Invariant(
            "VACUUM INTO produced no snapshot (in-memory store?)".into(),
        ));
    }

    let mut written = 0u64;
    for library in due {
        let dir = BlobPaths::new(&library.blob_root).snapshot_dir();
        // Root unavailable: skip quietly, still due next tick — snapshot
        // staleness is spec-blessed benign.
        if fs::create_dir_all(&dir).is_err() {
            continue;
        }
        let staged = dir.join(format!(".{name}.tmp"));
        if fs::copy(&tmp_path, &staged).is_ok() && fs::rename(&staged, dir.join(&name)).is_ok() {
            written += 1;
            prune_snapshots(&dir, cfg.snapshot_keep);
        }
    }
    let _ = fs::remove_dir_all(&tmp_dir);
    Ok(written)
}

/// Keep the newest `keep` parseable snapshots; unparseable names untouched.
fn prune_snapshots(dir: &Path, keep: usize) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    let mut snapshots: Vec<(i64, PathBuf)> = entries
        .flatten()
        .filter_map(|e| {
            parse_snapshot_ts(&e.file_name().to_string_lossy()).map(|ts| (ts, e.path()))
        })
        .collect();
    snapshots.sort_by_key(|(ts, _)| std::cmp::Reverse(*ts));
    for (_, path) in snapshots.into_iter().skip(keep) {
        let _ = fs::remove_file(path);
    }
}

/// Newest parseable `state.db.<unix-ts>.sqlite3` timestamp in a dir.
/// Shared with Tier-3 recovery's snapshot search.
pub(crate) fn newest_snapshot_ts(dir: &Path) -> Option<i64> {
    let entries = fs::read_dir(dir).ok()?;
    entries
        .flatten()
        .filter_map(|e| parse_snapshot_ts(&e.file_name().to_string_lossy()))
        .max()
}

pub(crate) fn parse_snapshot_ts(name: &str) -> Option<i64> {
    name.strip_prefix("state.db.")?
        .strip_suffix(".sqlite3")?
        .parse()
        .ok()
}

// ---------------------------------------------------------------- replication

/// Drain one batch of the dirty-sidecar set to the remote backup roots.
/// `skip` = photos with a live `photo_task` (their sidecar may be rewritten
/// concurrently; stamping after copying a stale version would record the
/// stale remote as current). First fs failure stops the pass and edge-logs
/// `mount_lost`; the first subsequent success logs `mount_regained`.
pub async fn replicate_dirty_sidecars(
    store: &StateStore,
    data_dir: &DataDir,
    batch: usize,
    skip: &HashSet<PhotoId>,
    state: &mut ReplicationState,
) -> Result<ReplicationReport> {
    let mut report = ReplicationReport::default();
    let candidates = crate::store::photos::dirty_sidecar_photos(store.pool(), batch as i64).await?;
    let libraries: std::collections::HashMap<_, _> = store
        .libraries()
        .await?
        .into_iter()
        .map(|l| (l.library_id.clone(), l))
        .collect();

    let mut copied_any = false;
    // First per-file copy failure of this pass, logged once at the end so a
    // recurring poison photo does not write one row per tick.
    let mut first_failure: Option<(PhotoId, String)> = None;
    for photo in candidates {
        if skip.contains(&photo.photo_id) {
            continue;
        }
        let Some(library) = photo.library_id.as_ref().and_then(|id| libraries.get(id)) else {
            continue; // unmapped rows are filtered by the query; races tolerated
        };
        let Some(remote_root) = &library.sidecar_root_remote else {
            continue;
        };

        let local_root = data_dir.sidecar_root(&library.library_id);
        let Some(local) = find_sidecar(&local_root, &photo.photo_id)? else {
            report.missing += 1;
            continue;
        };
        let rel = local
            .strip_prefix(&local_root)
            .map_err(|_| IngressError::Invariant("sidecar outside its root".into()))?
            .to_path_buf();

        match copy_atomic(&local, &Path::new(remote_root).join(&rel)) {
            Ok(()) => {
                crate::store::photos::stamp_sidecar_replicated(
                    store.pool(),
                    &photo.photo_id,
                    Utc::now(),
                )
                .await?;
                report.replicated += 1;
                copied_any = true;
            }
            Err(e) => {
                // Two distinct failure modes, told apart by probing the root:
                //   - root not a reachable directory  => the mount is gone.
                //     Stop the pass (hammering a dead mount is wasted work)
                //     and edge-log `mount_lost` once.
                //   - root still a live directory      => this one file failed
                //     (destination clash, per-file fs error). Skip it and keep
                //     draining — a single un-copyable photo at the head of the
                //     `photo_id` order must not freeze the whole backlog.
                if !Path::new(remote_root).is_dir() {
                    report.stalled = true;
                    if !state.stalled {
                        state.stalled = true;
                        store
                            .append_log(
                                "mount_lost",
                                None,
                                Some(serde_json::json!({
                                    "op": "sidecar_replication",
                                    "library": library.library_id.to_string(),
                                    "error": e.to_string(),
                                })),
                            )
                            .await?;
                    }
                    return Ok(report);
                }
                report.failed += 1;
                if first_failure.is_none() {
                    first_failure = Some((photo.photo_id.clone(), e.to_string()));
                }
            }
        }
    }

    if let Some((photo_id, error)) = first_failure {
        store
            .append_log(
                "sidecar_copy_failed",
                Some(&photo_id),
                Some(serde_json::json!({
                    "op": "sidecar_replication",
                    "error": error,
                    "failed_in_pass": report.failed,
                })),
            )
            .await?;
    }

    if state.stalled && copied_any {
        state.stalled = false;
        store
            .append_log(
                "mount_regained",
                None,
                Some(serde_json::json!({ "op": "sidecar_replication" })),
            )
            .await?;
    }
    Ok(report)
}

/// Copy sidecar *data* via `.tmp` + rename on the destination filesystem.
///
/// Deliberately NOT `fs::copy`: on macOS that lowers to `copyfile(…,
/// COPYFILE_ALL)`, which also replicates the source's xattrs (Photos-derived
/// sidecars carry `com.apple.provenance`). Network/FUSE backups (macfuse over
/// SMB/NFS) reject that `setxattr` with EPERM *after* creating the
/// destination, aborting the copy and leaving a 0-byte `.tmp`. A plain data
/// copy sidesteps the metadata replication entirely — the sidecar is
/// self-describing JSON; its xattrs are not part of the backup.
///
/// The `.tmp` is removed on any failure so an aborted pass leaves no litter
/// (and no partial file for a later pass to mistake for a real sidecar).
fn copy_atomic(src: &Path, dst: &Path) -> std::io::Result<()> {
    let parent = dst.parent().expect("sidecar dst has YYYY/MM parents");
    fs::create_dir_all(parent)?;
    let tmp = dst.with_extension("json.tmp");
    match copy_data(src, &tmp).and_then(|()| fs::rename(&tmp, dst)) {
        Ok(()) => Ok(()),
        Err(e) => {
            let _ = fs::remove_file(&tmp);
            Err(e)
        }
    }
}

/// Byte-for-byte data copy, flushed to disk before the caller renames it into
/// place. No metadata (mode/xattrs/times) is carried over.
fn copy_data(src: &Path, tmp: &Path) -> std::io::Result<()> {
    let mut reader = fs::File::open(src)?;
    let mut writer = fs::File::create(tmp)?;
    std::io::copy(&mut reader, &mut writer)?;
    writer.sync_all()
}
