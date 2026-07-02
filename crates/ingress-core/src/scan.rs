//! Reconciliation scan (spec §Discovery and the work queue): a full PhotoKit
//! enumeration diffed against `state.db`. Catches everything missed while
//! the daemon was offline — including deletions, detectable only this way.
//!
//! The protocol is probe-first: the platform side sends a light
//! [`ScanProbe`] per asset (identity + scope + modification date — NO
//! resource enumeration, which is the expensive PhotoKit call at library
//! scale) and only builds a full descriptor for assets the probe flags
//! [`ScanVerdict::NeedsFull`]. Those descriptors then flow through the same
//! `classify`/`apply_change` path as observer events.
//!
//! Seen-marking happens at PROBE time, not classify time: full descriptors
//! may lag in the daemon's event queue (or defer behind an inflight photo)
//! past `finish`, and marking late would synthesize bogus deletions for
//! live photos.

use std::collections::HashSet;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, Utc};

use crate::descriptor::LibraryScope;
use crate::error::Result;
use crate::ids::PhotoId;
use crate::paths::DataDir;
use crate::store::StateStore;

/// Light per-asset probe — the fields identity resolution and change
/// detection need, none of the ones that require `PHAssetResource`
/// enumeration.
#[derive(Debug, Clone)]
pub struct ScanProbe {
    pub local_id: String,
    pub cloud_id: Option<String>,
    pub scope: LibraryScope,
    pub asset_modified_at: Option<DateTime<Utc>>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ScanVerdict {
    /// Known, active, right library, nothing newer — no descriptor needed.
    Done,
    /// Unknown / tombstoned / scope flip / modified: build the full
    /// descriptor and push it through the observer path.
    NeedsFull,
}

/// One scan's accumulation state. One scan at a time (the session holds
/// `Mutex<Option<Arc<ScanState>>>`).
pub struct ScanState {
    pub started_at: DateTime<Utc>,
    seen: Mutex<HashSet<PhotoId>>,
    probes: AtomicU64,
    needed_full: AtomicU64,
}

/// Result of [`finish`], for the CLI report and the `scan_completed` event.
#[derive(Debug, PartialEq, Eq)]
pub struct ScanSummary {
    pub probed: u64,
    pub needed_full: u64,
    pub deletions_synthesized: u64,
    pub gave_up_reset: u64,
    /// The health guard fired: zero assets enumerated against a non-empty
    /// store (lost authorization, empty fetch) — synthesis skipped.
    pub synthesis_skipped: bool,
}

/// Start a scan: log `scan_started`, capture the synthesis cutoff.
pub async fn begin(store: &StateStore) -> Result<ScanState> {
    store.append_log("scan_started", None, None).await?;
    Ok(ScanState {
        started_at: Utc::now(),
        seen: Mutex::new(HashSet::new()),
        probes: AtomicU64::new(0),
        needed_full: AtomicU64::new(0),
    })
}

/// Record a photo as alive in this scan. Called by `probe` on resolution and
/// by the daemon loop for events classified while a scan is active
/// (belt-and-braces on top of probe-time marking).
pub fn mark_seen(scan: &ScanState, photo_id: &PhotoId) {
    scan.seen
        .lock()
        .expect("scan seen set")
        .insert(photo_id.clone());
}

/// Probe one enumerated asset. Resolution mirrors identity precedence:
/// `cloud_id` first, then the same-device `local_id` guard for cloud-less
/// assets. Any resolved photo is marked seen regardless of verdict.
pub async fn probe(store: &StateStore, scan: &ScanState, p: &ScanProbe) -> Result<ScanVerdict> {
    scan.probes.fetch_add(1, Ordering::Relaxed);

    let photo = match p.cloud_id.as_deref() {
        Some(cloud_id) => store.photo_by_cloud_id(cloud_id).await?,
        None => crate::store::photos::photo_by_local_id_no_cloud(store.pool(), &p.local_id).await?,
    };
    let Some(photo) = photo else {
        scan.needed_full.fetch_add(1, Ordering::Relaxed);
        return Ok(ScanVerdict::NeedsFull);
    };
    mark_seen(scan, &photo.photo_id);

    // Tombstoned-but-enumerated = restore; needs the descriptor.
    if photo.deleted_at.is_some() {
        scan.needed_full.fetch_add(1, Ordering::Relaxed);
        return Ok(ScanVerdict::NeedsFull);
    }

    // Scope flip (hard move) or unmapped-now-bound (adoption): both need the
    // full path. An unmapped photo whose scope is STILL unbound is Done —
    // there is no work a descriptor could unlock.
    let scope_library = store
        .library_for_scope(p.scope)
        .await?
        .map(|c| c.library_id);
    match (&photo.library_id, &scope_library) {
        (Some(stored), Some(current)) if stored != current => {
            scan.needed_full.fetch_add(1, Ordering::Relaxed);
            return Ok(ScanVerdict::NeedsFull);
        }
        (None, Some(_)) => {
            scan.needed_full.fetch_add(1, Ordering::Relaxed);
            return Ok(ScanVerdict::NeedsFull);
        }
        _ => {}
    }

    // Metadata drift (same rule as resolve: stored NULL = never synced).
    let modified = match (photo.asset_modified_at, p.asset_modified_at) {
        (None, _) => true,
        (Some(stored), Some(incoming)) => incoming > stored,
        (Some(_), None) => false,
    };
    if modified {
        scan.needed_full.fetch_add(1, Ordering::Relaxed);
        return Ok(ScanVerdict::NeedsFull);
    }

    Ok(ScanVerdict::Done)
}

/// Close a scan: synthesize deletions for unseen photos, re-enqueue gave-up
/// resources (spec §Failure Handling), log `scan_completed`.
///
/// `enumerated` is the platform-side fetch count — the health guard (spec:
/// "absence of evidence from PhotoKit is only evidence of deletion when the
/// API is healthy"): zero enumerated against a non-empty store skips
/// synthesis AND the gave-up reset (no evidence either way).
pub async fn finish(
    store: &StateStore,
    data_dir: &DataDir,
    scan: &ScanState,
    enumerated: u64,
    retry_cap: i64,
) -> Result<ScanSummary> {
    let probed = scan.probes.load(Ordering::Relaxed);
    let needed_full = scan.needed_full.load(Ordering::Relaxed);
    let seen = scan.seen.lock().expect("scan seen set").clone();

    let synthesis_skipped = enumerated == 0 && store.count_photos().await? > 0;

    let mut deletions_synthesized = 0u64;
    let mut gave_up_reset = 0u64;
    if !synthesis_skipped {
        let candidates =
            crate::store::photos::active_photo_ids_discovered_before(store.pool(), scan.started_at)
                .await?;
        for photo_id in candidates.into_iter().filter(|id| !seen.contains(id)) {
            let Some(photo) = store.photo(&photo_id).await? else {
                continue;
            };
            if crate::classify::tombstone_photo(store, data_dir, &photo).await? {
                deletions_synthesized += 1;
            }
        }
        gave_up_reset = crate::store::resources::reset_gave_up(store.pool(), retry_cap).await?;
    }

    store
        .append_log(
            "scan_completed",
            None,
            Some(serde_json::json!({
                "enumerated": enumerated,
                "probed": probed,
                "needed_full": needed_full,
                "deletions_synthesized": deletions_synthesized,
                "gave_up_reset": gave_up_reset,
                "synthesis_skipped": synthesis_skipped,
            })),
        )
        .await?;

    Ok(ScanSummary {
        probed,
        needed_full,
        deletions_synthesized,
        gave_up_reset,
        synthesis_skipped,
    })
}
