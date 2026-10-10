//! The existence sweep (RFC-STORAGE-003 S5): disk truth versus the record.
//!
//! One readdir walk of the fragment store, diffed against `fragment_hashes`
//! — the replicated fragment table with this node's `stored_locally` flag —
//! yields three cases, each repaired by the caller:
//! - bytes present but unflagged: re-flag (mark local) and re-attest;
//! - bytes flagged but gone: un-flag (mark remote); the next self-check
//!   un-attests, and the responsible's obligation check refetches;
//! - a file with no `fragment_hashes` row at all: an orphan, deleted once
//!   older than the grace period (racing an in-flight store is the only
//!   way a fresh file lacks its row) — unless this node's upload ledger
//!   names it (`split_orphans`): an own upload is written before the
//!   transaction carrying its row, and that row may be hours away or, for
//!   a straggler that joined meanwhile, never replayed. Held files are
//!   reported, never deleted (consensus-bugs 20).
//!
//! Belief is dishonest for at most one sweep cycle; the cycle therefore
//! sits inside the convergence bound. Pure over its inputs — the host owns
//! the walk, the clock, and the connection.

use std::collections::{HashMap, HashSet};

use hopnet_common::Blake3Hash;
use serde::{Deserialize, Serialize};

/// One on-disk fragment file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiskFragment {
    pub hash: Blake3Hash,
    pub size: u64,
    /// Modification time, unix seconds.
    pub mtime: u64,
}

/// A rowless file past the grace period: `(hash, size)`.
pub type Orphan = (Blake3Hash, u64);

/// The sweep's verdict over one walk.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SweepDiff {
    /// On disk, in the table, flag says absent → flag it, attest it.
    pub present_unflagged: Vec<Blake3Hash>,
    /// Flag says present, bytes gone → un-flag it.
    pub flagged_missing: Vec<Blake3Hash>,
    /// On disk, no table row, older than the grace period → delete,
    /// unless the upload ledger holds it (`split_orphans`).
    pub orphans: Vec<Orphan>,
    /// On disk and in the table (flagged or not) — the attestation's
    /// `present` list, sorted.
    pub present: Vec<Blake3Hash>,
    /// Files younger than the grace period with no row (left alone).
    pub young_orphans: usize,
}

/// Diff the walk against the table. `rows` is every `fragment_hashes` row
/// as `(hash, stored_locally)`; `grace_cutoff` is the unix time before
/// which a rowless file counts as an orphan.
pub fn diff(disk: &[DiskFragment], rows: &[(Blake3Hash, bool)], grace_cutoff: u64) -> SweepDiff {
    let mut flagged: HashMap<Blake3Hash, bool> = HashMap::with_capacity(rows.len());
    for (hash, stored) in rows {
        // A hash can appear on several rows (shared fragments); any
        // flagged row counts as flagged.
        let entry = flagged.entry(*hash).or_insert(false);
        *entry |= *stored;
    }
    let on_disk: HashSet<Blake3Hash> = disk.iter().map(|d| d.hash).collect();

    let mut out = SweepDiff::default();
    for d in disk {
        match flagged.get(&d.hash) {
            Some(true) => out.present.push(d.hash),
            Some(false) => {
                out.present.push(d.hash);
                out.present_unflagged.push(d.hash);
            }
            None => {
                if d.mtime < grace_cutoff {
                    out.orphans.push((d.hash, d.size));
                } else {
                    out.young_orphans += 1;
                }
            }
        }
    }
    for (hash, stored) in &flagged {
        if *stored && !on_disk.contains(hash) {
            out.flagged_missing.push(*hash);
        }
    }
    let by_bytes = |a: &Blake3Hash, b: &Blake3Hash| a.as_bytes().cmp(b.as_bytes());
    out.present.sort_unstable_by(by_bytes);
    out.present.dedup();
    out.present_unflagged.sort_unstable_by(by_bytes);
    out.present_unflagged.dedup();
    out.flagged_missing.sort_unstable_by(by_bytes);
    out.orphans.sort_unstable_by(|a, b| by_bytes(&a.0, &b.0));
    out
}

/// Split the diff's orphans by this node's upload ledger into
/// `(delete, held)`: a rowless file the ledger names is this node's own
/// upload whose `fragment_hashes` row has not arrived, and is never
/// deleted. Order is preserved on both sides.
pub fn split_orphans(
    orphans: &[Orphan],
    ledger: &HashSet<Blake3Hash>,
) -> (Vec<Orphan>, Vec<Orphan>) {
    orphans
        .iter()
        .copied()
        .partition(|(hash, _)| !ledger.contains(hash))
}

/// The sweep's disk-truth attestation, split into `page`-sized
/// `attest_fragments` payloads (RFC-STORAGE-003 S5). One transaction per
/// page keeps every page under the wire frame and the queue's deadline
/// whatever the store's size; `apply_attestation` is idempotent, so a page
/// that commits before a later one fails is simply re-covered by the next
/// sweep. Every hash lands in exactly one page; an empty `present` yields
/// no pages.
pub fn attestation_pages(
    node_id: i32,
    height: u64,
    present: &[Blake3Hash],
    page: usize,
) -> Vec<crate::types::FragmentAttestation> {
    present
        .chunks(page.max(1))
        .map(|chunk| crate::types::FragmentAttestation {
            node_id,
            height,
            present: chunk.to_vec(),
            suspect: Vec::new(),
        })
        .collect()
}

/// What one rotation of the sweep did — the operator's report (the former
/// two-call orphan scan/delete API collapses into this).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SweepReport {
    pub swept_at: i64,
    pub files_on_disk: usize,
    pub present: usize,
    pub reflagged: usize,
    pub unflagged: usize,
    pub orphans_deleted: usize,
    pub orphan_bytes_freed: u64,
    /// Rowless files past the grace that this node's upload ledger names
    /// (its own uploads, rows not yet landed): held on disk, not deleted.
    #[serde(default)]
    pub orphans_held: usize,
    #[serde(default)]
    pub orphan_bytes_held: u64,
    /// Ledger entries given up on this walk: no row landed within the
    /// retention, so their files are ordinary orphans from here on.
    #[serde(default)]
    pub uploads_expired: usize,
    /// Orphan steps (a shard's ledger read, or one unlink batch) skipped
    /// because the database was busy or locked; those files wait for the
    /// next rotation. Never fails the shard.
    #[serde(default)]
    pub orphan_batches_skipped: usize,
    pub young_orphans: usize,
    pub corrupt_deleted: usize,
    /// Scrub-slice files the read failed on for a reason other than
    /// absence (logged, kept — not a verdict on the bytes).
    pub scrub_unreadable: usize,
    /// `attest_fragments` transactions this sweep committed (0 when nothing
    /// was on disk).
    pub attested_pages: usize,
    /// Surplus copies the prompt release deleted on this walk (before the
    /// self-check, so this pass carried their removal to consensus).
    pub surplus_released: usize,
    pub surplus_bytes_freed: u64,
    /// `<hash>.tmp.<nonce>` leftovers of interrupted stores, older than the
    /// orphan grace, deleted on this walk.
    #[serde(default)]
    pub temps_deleted: usize,
    /// Attestation pages whose submit failed; the sweep carries on past
    /// them and the next cycle re-covers their hashes.
    #[serde(default)]
    pub attest_failed_pages: usize,
    /// Shards this report covers (256 for a completed rotation of the
    /// rolling sweep).
    #[serde(default)]
    pub shards: usize,
    /// Wall time of the rotation.
    #[serde(default)]
    pub rotation_secs: u64,
    /// Consensus heights the tip moved during the rotation — what the
    /// attestation recency window is measured in.
    #[serde(default)]
    pub rotation_heights: u64,
    /// Files whose name is neither a fragment nor a temp file.
    #[serde(default)]
    pub unexpected_names: usize,
    /// `self_check_fragments` pages committed and failed.
    #[serde(default)]
    pub belief_pages: usize,
    #[serde(default)]
    pub belief_failed_pages: usize,
    /// Shards whose step failed (logged; retried next rotation).
    #[serde(default)]
    pub failed_shards: usize,
    /// Wall-clock milliseconds per shard step, summed over the rotation
    /// (`ShardTimings`): where a slow disk's rotation time goes.
    #[serde(default)]
    pub ms_height: u64,
    #[serde(default)]
    pub ms_walk: u64,
    #[serde(default)]
    pub ms_db: u64,
    #[serde(default)]
    pub ms_diff: u64,
    #[serde(default)]
    pub ms_mark: u64,
    #[serde(default)]
    pub ms_temps: u64,
    #[serde(default)]
    pub ms_orphans: u64,
    #[serde(default)]
    pub ms_scrub: u64,
    /// Shards scrubbed, and the files and bytes their scrubs read.
    #[serde(default)]
    pub shards_scrubbed: usize,
    #[serde(default)]
    pub scrub_files: usize,
    #[serde(default)]
    pub scrub_bytes: u64,
    #[serde(default)]
    pub ms_release: u64,
    #[serde(default)]
    pub ms_buffer: u64,
    /// Page flushes: total time, pages submitted and the slowest single
    /// submit.
    #[serde(default)]
    pub ms_flush: u64,
    #[serde(default)]
    pub flush_pages: usize,
    #[serde(default)]
    pub flush_slowest_ms: u64,
    /// The rolling walker's per-shard work (step plus flush, including any
    /// wait for `SWEEP_LOCK` behind an operator sweep) and its pacing sleep
    /// between shards.
    #[serde(default)]
    pub ms_work: u64,
    #[serde(default)]
    pub ms_sleep: u64,
    /// Shards whose step timings are in the totals above (a failed shard
    /// counts, up to the step it failed at).
    #[serde(default)]
    pub shards_timed: usize,
}

/// One shard step's wall-clock milliseconds, by step of `sweep_shard`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ShardTimings {
    pub ms_height: u64,
    pub ms_walk: u64,
    pub ms_db: u64,
    pub ms_diff: u64,
    pub ms_mark: u64,
    pub ms_temps: u64,
    pub ms_orphans: u64,
    pub ms_scrub: u64,
    pub scrubbed: bool,
    pub scrub_files: usize,
    pub scrub_bytes: u64,
    pub ms_release: u64,
    pub ms_buffer: u64,
    /// Files the walk listed.
    pub files: usize,
}

/// One `flush_buffers` call: pages submitted, how many committed and
/// failed, its wall time and its slowest single submit.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FlushTimings {
    pub pages: usize,
    pub ok: usize,
    pub failed: usize,
    pub ms: u64,
    pub slowest_ms: u64,
}

/// The rolling sweep logs its rotation's progress at DEBUG after this many
/// shards since the last progress line...
pub const PROGRESS_EVERY_SHARDS: usize = 16;
/// ...and at INFO at most this often (a healthy walker passes 16 shards
/// every few minutes; INFO stays sparse).
pub const PROGRESS_EVERY_SECS: u64 = 900;

/// The level a progress line is due at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressLine {
    Debug,
    Info,
}

/// Which progress line is due, `shards` shards after the last line of
/// either level and `secs_since_info` seconds after the last INFO one (or
/// the rotation's start): INFO once the INFO interval has passed, else
/// DEBUG every `PROGRESS_EVERY_SHARDS` shards. Never without a new shard
/// to report.
pub fn progress_due(shards: usize, secs_since_info: u64) -> Option<ProgressLine> {
    if shards == 0 {
        None
    } else if secs_since_info >= PROGRESS_EVERY_SECS {
        Some(ProgressLine::Info)
    } else if shards >= PROGRESS_EVERY_SHARDS {
        Some(ProgressLine::Debug)
    } else {
        None
    }
}

impl SweepReport {
    /// Fold one shard's step timings into the rotation's totals.
    pub fn add_shard_timings(&mut self, t: &ShardTimings) {
        self.ms_height += t.ms_height;
        self.ms_walk += t.ms_walk;
        self.ms_db += t.ms_db;
        self.ms_diff += t.ms_diff;
        self.ms_mark += t.ms_mark;
        self.ms_temps += t.ms_temps;
        self.ms_orphans += t.ms_orphans;
        self.ms_scrub += t.ms_scrub;
        self.shards_scrubbed += usize::from(t.scrubbed);
        self.scrub_files += t.scrub_files;
        self.scrub_bytes = self.scrub_bytes.saturating_add(t.scrub_bytes);
        self.ms_release += t.ms_release;
        self.ms_buffer += t.ms_buffer;
        self.shards_timed += 1;
    }

    /// Fold one page flush into the rotation's totals.
    pub fn add_flush_timings(&mut self, f: &FlushTimings) {
        self.ms_flush += f.ms;
        self.flush_pages += f.pages;
        self.flush_slowest_ms = self.flush_slowest_ms.max(f.slowest_ms);
    }

    /// Average timed milliseconds per shard, flushes included.
    pub fn avg_ms_per_shard(&self) -> u64 {
        let total = self.ms_height
            + self.ms_walk
            + self.ms_db
            + self.ms_diff
            + self.ms_mark
            + self.ms_temps
            + self.ms_orphans
            + self.ms_scrub
            + self.ms_release
            + self.ms_buffer
            + self.ms_flush;
        total / self.shards_timed.max(1) as u64
    }

    /// The timing totals, for one log line.
    pub fn timing_summary(&self) -> String {
        format!(
            "{} shards timed, avg {} ms/shard; total ms: height={} walk={} db={} diff={} mark={} temps={} orphans={} scrub={} ({} shards, {} files, {} bytes) release={} buffer={} flush={} ({} pages, slowest {}) work={} sleep={}",
            self.shards_timed,
            self.avg_ms_per_shard(),
            self.ms_height,
            self.ms_walk,
            self.ms_db,
            self.ms_diff,
            self.ms_mark,
            self.ms_temps,
            self.ms_orphans,
            self.ms_scrub,
            self.shards_scrubbed,
            self.scrub_files,
            self.scrub_bytes,
            self.ms_release,
            self.ms_buffer,
            self.ms_flush,
            self.flush_pages,
            self.flush_slowest_ms,
            self.ms_work,
            self.ms_sleep,
        )
    }
}

/// The rolling walker's rotation in progress, for the operator: where it
/// is, and its running report with the step timings so far. The report is
/// in memory: it counts from the walker's (re)start, not the rotation's.
#[derive(Debug, Clone, Serialize)]
pub struct SweepProgress {
    pub rotation: u64,
    pub next_shard: u8,
    pub started_unix: u64,
    pub started_height: u64,
    pub avg_ms_per_shard: u64,
    pub report: SweepReport,
}

/// Where a node's rolling sweep resumes (`hopnet_storage_sweep_cursor`,
/// node-local).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SweepCursor {
    pub next_shard: u8,
    pub rotation: u64,
    pub started_unix: u64,
    /// The consensus height when this rotation began.
    pub started_height: u64,
}

impl SweepCursor {
    /// A first rotation, starting at shard 0.
    pub fn fresh(now: u64, height: u64) -> Self {
        SweepCursor {
            next_shard: 0,
            rotation: 0,
            started_unix: now,
            started_height: height,
        }
    }

    /// The cursor after `next_shard` was swept, and whether that step
    /// completed the rotation (a new one starts at shard 0, stamped `now`
    /// and `height`).
    pub fn advance(self, now: u64, height: u64) -> (Self, bool) {
        match self.next_shard.checked_add(1) {
            Some(next) => (
                SweepCursor {
                    next_shard: next,
                    ..self
                },
                false,
            ),
            None => (
                SweepCursor {
                    next_shard: 0,
                    rotation: self.rotation + 1,
                    started_unix: now,
                    started_height: height,
                },
                true,
            ),
        }
    }
}

/// The temp files old enough to reap: an in-flight store is seconds old,
/// so anything past the orphan grace is a leftover.
pub fn stale_temps(
    temps: &[crate::fragstore::TempFile],
    grace_cutoff: u64,
) -> Vec<std::path::PathBuf> {
    temps
        .iter()
        .filter(|t| t.mtime < grace_cutoff)
        .map(|t| t.path.clone())
        .collect()
}

/// The surplus release's input, picked from the sweep's own walk: files
/// that have a table row (`present`) and are older than `grace_cutoff`, as
/// `(hash, size)`. Orphans belong to the orphan step and young files may
/// be in-flight stores; neither is offered.
pub fn release_listing(
    listing: &[DiskFragment],
    present: &[Blake3Hash],
    grace_cutoff: u64,
) -> Vec<(Blake3Hash, u64)> {
    let present: HashSet<&Blake3Hash> = present.iter().collect();
    listing
        .iter()
        .filter(|d| d.mtime < grace_cutoff && present.contains(&d.hash))
        .map(|d| (d.hash, d.size))
        .collect()
}

// ---------------------------------------------------------------------------
// Rolling sweep primitives

/// The rolling sweep's unit: the first byte of a fragment hash, which is
/// also the store's first-level directory (`fragstore::get_fragment_dir`).
/// One rotation visits all 256.
pub const SHARD_COUNT: usize = 256;

/// The shard a fragment belongs to.
pub fn shard_of(hash: &Blake3Hash) -> u8 {
    hash.as_bytes()[0]
}

/// The shard's bounds over a BLOB hash column, `[lo, hi)`; `hi` is `None`
/// for the last shard. SQLite orders BLOBs by memcmp then length, so the
/// one-byte bounds bracket exactly the 32-byte hashes with that prefix.
pub fn shard_bounds(shard: u8) -> (Vec<u8>, Option<Vec<u8>>) {
    (vec![shard], shard.checked_add(1).map(|next| vec![next]))
}

/// The scrub reads one seventh of the store per UTC day, as before the
/// rolling sweep: the shards whose index is the day's slice.
pub const SCRUB_SLICES: u8 = 7;

/// Is `shard` in the scrub slice of `day` (days since the unix epoch)?
pub fn scrub_due(shard: u8, day: i64) -> bool {
    i64::from(shard % SCRUB_SLICES) == day.rem_euclid(i64::from(SCRUB_SLICES))
}

/// Hashes waiting to ride a page-sized transaction, with the lowest
/// height any of them was observed at. A page is stamped with that
/// height, so it never claims a hash was seen later than it was.
#[derive(Debug, Default)]
pub struct PageBuffer {
    hashes: Vec<Blake3Hash>,
    min_height: Option<u64>,
    /// When the oldest hash still waiting arrived (unix seconds).
    opened_at: Option<u64>,
}

impl PageBuffer {
    /// Add hashes observed at `height`.
    pub fn push(&mut self, height: u64, now: u64, hashes: impl IntoIterator<Item = Blake3Hash>) {
        let before = self.hashes.len();
        self.hashes.extend(hashes);
        if self.hashes.len() == before {
            return;
        }
        self.min_height = Some(self.min_height.map_or(height, |h| h.min(height)));
        self.opened_at.get_or_insert(now);
    }

    pub fn len(&self) -> usize {
        self.hashes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.hashes.is_empty()
    }

    /// A full page is waiting, or the oldest hash has waited `max_age`.
    pub fn due(&self, now: u64, page: usize, max_age_secs: u64) -> bool {
        self.hashes.len() >= page
            || self
                .opened_at
                .is_some_and(|t| now.saturating_sub(t) >= max_age_secs)
    }

    /// Take up to `page` hashes with the height to stamp them at. What
    /// stays keeps the same floor height (conservative) and age.
    pub fn take_page(&mut self, page: usize) -> Option<(u64, Vec<Blake3Hash>)> {
        let height = self.min_height?;
        let n = page.max(1).min(self.hashes.len());
        let taken: Vec<_> = self.hashes.drain(..n).collect();
        if self.hashes.is_empty() {
            self.min_height = None;
            self.opened_at = None;
        }
        Some((height, taken))
    }
}

/// The belief side of the rolling sweep: additions and removals buffered
/// separately, paged into `self_check_fragments` reports.
#[derive(Debug, Default)]
pub struct BeliefBuffer {
    pub added: PageBuffer,
    pub removed: PageBuffer,
}

impl BeliefBuffer {
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty()
    }

    pub fn due(&self, now: u64, page: usize, max_age_secs: u64) -> bool {
        self.added.len() + self.removed.len() >= page
            || self.added.due(now, usize::MAX, max_age_secs)
            || self.removed.due(now, usize::MAX, max_age_secs)
    }

    /// One report of at most `page` hashes, removals first (they are the
    /// smaller side and the ones whose delay keeps a stale row). Its height
    /// is the lower of the two floors: for removals the apply's CAS then
    /// errs toward keeping a row, never toward deleting newer evidence.
    pub fn take_report(&mut self, node_id: i32, page: usize) -> Option<crate::SelfCheckFragments> {
        if self.is_empty() {
            return None;
        }
        let (removed_height, removed) = self
            .removed
            .take_page(page)
            .unwrap_or((u64::MAX, Vec::new()));
        let room = page.saturating_sub(removed.len());
        let (added_height, added) = if room > 0 {
            self.added.take_page(room).unwrap_or((u64::MAX, Vec::new()))
        } else {
            (u64::MAX, Vec::new())
        };
        Some(crate::SelfCheckFragments {
            node_id,
            self_verified_height: removed_height.min(added_height),
            previous_count: 0,
            fragments_added: added,
            fragments_removed: removed,
        })
    }
}

// ---------------------------------------------------------------------------
// Attestation refresh: publish only the stamps about to expire

/// Heights reserved beyond the two rotations the refresh threshold budgets
/// (`RefreshPolicy::for_rotation`): a page's wait in the buffer and the
/// consensus queue before its stamp lands.
pub const REFRESH_MARGIN_HEIGHTS: u64 = 512;

/// The refresh's wall-clock cap: a stamp older than this is refreshed
/// whatever its height age, so evidence stays a day fresh in real time on
/// a quiet mesh, where the height window can span days. It also keeps
/// height ages short while the mesh is quiet, which is what lets a sudden
/// drain find every stamp far from the window's end. Node-local policy,
/// not a consensus rule.
pub const REFRESH_MAX_AGE_SECS: u64 = 86_400;

/// How far below the cap a fragment's own cap may sit: a quarter of it, so
/// stamps that landed together (a big upload, the first rotation after an
/// upgrade) come due spread over six hours instead of in one rotation.
pub const REFRESH_CAP_JITTER_SECS: u64 = REFRESH_MAX_AGE_SECS / 4;

/// A fragment's own height threshold sits up to `threshold /
/// REFRESH_JITTER_DIVISOR` below the policy's, for the same spreading on
/// the height side. Wider spreads more but refreshes earlier on average,
/// and every refresh page is itself a height that ages the rest.
pub const REFRESH_JITTER_DIVISOR: u64 = 4;

/// A refresh page goes out full, or once its oldest stamp has waited this
/// long or this many heights (half the margin, which is what absorbs the
/// wait). A refresh is never urgent, and a page that leaves part-full is a
/// height spent on fewer stamps: with minute-old partial pages, refresh
/// pages made most of a quiet mesh's heights and fed their own clock.
pub const REFRESH_BUFFER_MAX_SECS: u64 = 1800;
pub const REFRESH_BUFFER_MAX_HEIGHTS: u64 = REFRESH_MARGIN_HEIGHTS / 2;

/// The shortest rotation the refresh budgets for, in heights: the drain
/// traffic the recency window was sized for (~45 heights a minute over an
/// hourly rotation; `ATTESTATION_RECENCY_HEIGHTS`). Budgeting for a quiet
/// mesh's short rotations lets stamps age toward the window's end, where
/// a sudden drain plus one failed sweep pushes them past it (`sweep_sim`).
pub const REFRESH_ROTATION_FLOOR_HEIGHTS: u64 = 2_700;

/// One completed rotation of the rolling sweep
/// (`hopnet_storage_sweep_rotations`, node-local).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SweepRotation {
    pub rotation: u64,
    pub started_unix: u64,
    pub started_height: u64,
    pub ended_unix: u64,
    pub ended_height: u64,
}

impl SweepRotation {
    /// The heights the rotation spanned.
    pub fn heights(&self) -> u64 {
        self.ended_height.saturating_sub(self.started_height)
    }
}

/// When the tip stood at which height, as this node saw it: `(height,
/// unix)` samples, ascending in height, for dating a stamp's height
/// against the refresh's wall-clock cap. Blocks carry no time, so this is
/// node-local: the sweep records a sample at every step, and completed
/// rotations seed it across restarts.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HeightClock {
    samples: Vec<(u64, u64)>,
}

impl HeightClock {
    /// The tip stood at `height` at `unix`. Kept only when the height has
    /// moved past the newest sample: the first sight of each height.
    pub fn record(&mut self, height: u64, unix: u64) {
        if self.samples.last().is_none_or(|&(h, _)| height > h) {
            self.samples.push((height, unix));
        }
    }

    /// Fold in completed rotations' start and end points (a restart's
    /// history), keeping the earliest sight of each height.
    pub fn seed(&mut self, rotations: &[SweepRotation]) {
        for r in rotations {
            self.samples.push((r.started_height, r.started_unix));
            self.samples.push((r.ended_height, r.ended_unix));
        }
        self.samples.sort_unstable();
        self.samples.dedup_by_key(|(height, _)| *height);
    }

    /// Drop samples older than `before_unix`, keeping the newest of them
    /// so a stamp from just before the cutoff can still be dated.
    pub fn prune(&mut self, before_unix: u64) {
        let fresh = self
            .samples
            .partition_point(|&(_, unix)| unix < before_unix);
        self.samples.drain(..fresh.saturating_sub(1));
    }

    pub fn len(&self) -> usize {
        self.samples.len()
    }

    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }

    /// How long ago a stamp at `verified_height` was made. Dated by the
    /// newest sample strictly below it: the tip had not reached the
    /// stamp's height then, so the stamp is later and the age is never
    /// understated — a refresh comes early, never late. A stamp below
    /// every sample gets the age of the oldest one, a lower bound that
    /// can only show it already past a cap; a stamp at the oldest
    /// sample's height can't be dated at all.
    pub fn wall_age(&self, verified_height: u64, now: u64) -> Option<u64> {
        let below = self.samples.partition_point(|&(h, _)| h < verified_height);
        match below.checked_sub(1) {
            Some(i) => Some(now.saturating_sub(self.samples[i].1)),
            None => self
                .samples
                .first()
                .filter(|&&(h, _)| verified_height < h)
                .map(|&(_, unix)| now.saturating_sub(unix)),
        }
    }
}

/// This node's replicated stamp on one fragment (`fragment_inventory`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InventoryStamp {
    pub verified_height: Option<u64>,
    /// 0 for this node's own disk scan; NULL before the first attestation.
    pub provenance: Option<i64>,
    pub suspect: bool,
}

/// Why a present fragment rides this rotation's attestation, or not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshDue {
    /// No row yet, never verified, verified other than by this node's own
    /// scan, or suspect: only an attestation sets the stamp right.
    Flags,
    /// The stamp's height age reached the fragment's threshold.
    Age,
    /// The stamp is older than the fragment's wall-clock cap.
    Cap,
    /// Fresh enough to leave out.
    No,
}

/// One rotation's refresh policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RefreshPolicy {
    /// Height age at which a stamp is refreshed; 0 refreshes everything.
    pub threshold: u64,
    /// How far below `threshold` a fragment's own threshold may sit.
    pub jitter_heights: u64,
    pub max_age_secs: u64,
    /// How far below `max_age_secs` a fragment's own cap may sit.
    pub cap_jitter_secs: u64,
}

impl RefreshPolicy {
    /// Attest every present fragment every rotation: the behavior before
    /// the refresh, and the fallback while the rotation length is unknown.
    pub const EVERY_ROTATION: Self = RefreshPolicy {
        threshold: 0,
        jitter_heights: 0,
        max_age_secs: 0,
        cap_jitter_secs: 0,
    };

    /// The policy for a sweep whose rotations span up to `rotation_heights`
    /// (`estimate_rotation_heights`) against a `window`-height recency rule.
    /// A stamp is visited once a rotation, so one just under the threshold
    /// waits a rotation for its next visit and, if that sweep fails,
    /// another: refreshing at `window - 2 rotations - margin` keeps it
    /// inside the window through one failed sweep. The rotation counted is
    /// never shorter than `REFRESH_ROTATION_FLOOR_HEIGHTS`, so that holds
    /// when traffic jumps to a drain. A rotation too long for any threshold
    /// refreshes everything, as before.
    pub fn for_rotation(window: u64, rotation_heights: u64) -> Self {
        let rotation_heights = rotation_heights.max(REFRESH_ROTATION_FLOOR_HEIGHTS);
        let budget = rotation_heights
            .saturating_mul(2)
            .saturating_add(REFRESH_MARGIN_HEIGHTS);
        let threshold = window.saturating_sub(budget);
        RefreshPolicy {
            threshold,
            jitter_heights: threshold / REFRESH_JITTER_DIVISOR,
            max_age_secs: REFRESH_MAX_AGE_SECS,
            cap_jitter_secs: REFRESH_CAP_JITTER_SECS,
        }
    }
}

/// The longest rotation the refresh should budget for, in heights: the
/// longest of the recent completed rotations, or the one in progress
/// extrapolated to a full rotation, whichever is longer, so a sudden
/// burst of traffic shortens the threshold within the rotation it starts
/// in. `None` until a rotation has completed or `min_elapsed_secs` of the
/// current one have passed (too short a sample to extrapolate).
pub fn estimate_rotation_heights(
    completed: &[u64],
    in_progress_heights: u64,
    in_progress_secs: u64,
    rotation_secs: u64,
    min_elapsed_secs: u64,
) -> Option<u64> {
    let longest = completed.iter().copied().max();
    let extrapolated = (in_progress_secs >= min_elapsed_secs && in_progress_secs > 0).then(|| {
        let full = u128::from(in_progress_heights)
            * u128::from(rotation_secs.max(in_progress_secs))
            / u128::from(in_progress_secs);
        u64::try_from(full).unwrap_or(u64::MAX)
    });
    match (longest, extrapolated) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (a, b) => a.or(b),
    }
}

/// This node's fixed place in each refresh jitter span for one fragment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RefreshJitter {
    pub height: u64,
    pub cap: u64,
}

/// Uniform over `u64` per (node, fragment): fixed across restarts, and
/// different on each holder so one blob's copies don't refresh in step.
pub fn refresh_jitter(node_id: i32, hash: &Blake3Hash) -> RefreshJitter {
    let mut hasher = blake3::Hasher::new_derive_key("hopnet attestation refresh jitter v1");
    hasher.update(&node_id.to_le_bytes());
    hasher.update(hash.as_bytes());
    let out = hasher.finalize();
    let word = |i: usize| u64::from_le_bytes(out.as_bytes()[i..i + 8].try_into().expect("8 bytes"));
    RefreshJitter {
        height: word(0),
        cap: word(8),
    }
}

/// Whether buffered refresh stamps go out as a page: a full one, or
/// whatever is waiting once its oldest stamp has waited
/// `REFRESH_BUFFER_MAX_SECS` or `REFRESH_BUFFER_MAX_HEIGHTS`.
pub fn refresh_page_due(len: usize, page: usize, waited_secs: u64, waited_heights: u64) -> bool {
    len >= page
        || (len > 0
            && (waited_secs >= REFRESH_BUFFER_MAX_SECS
                || waited_heights >= REFRESH_BUFFER_MAX_HEIGHTS))
}

/// `span * jitter / 2^64`: a point in `[0, span)`, exactly.
fn jitter_offset(span: u64, jitter: u64) -> u64 {
    ((u128::from(span) * u128::from(jitter)) >> 64) as u64
}

/// Whether a present fragment rides this rotation's attestation. `height`
/// is the sweep's current height; `wall_age_secs` how long ago its stamp
/// was made, or `None` when the node can't bound that (no cap applies —
/// the height threshold alone keeps the window). Jitter only ever lowers
/// a fragment's threshold and cap, so it never delays a refresh.
pub fn refresh_due(
    stamp: Option<&InventoryStamp>,
    height: u64,
    wall_age_secs: Option<u64>,
    jitter: RefreshJitter,
    policy: &RefreshPolicy,
) -> RefreshDue {
    let Some(stamp) = stamp else {
        return RefreshDue::Flags;
    };
    let Some(verified) = stamp.verified_height else {
        return RefreshDue::Flags;
    };
    if stamp.suspect || stamp.provenance != Some(0) {
        return RefreshDue::Flags;
    }
    let threshold = policy.threshold
        - jitter_offset(policy.jitter_heights.min(policy.threshold), jitter.height);
    if height.saturating_sub(verified) >= threshold {
        return RefreshDue::Age;
    }
    let cap = policy.max_age_secs
        - jitter_offset(policy.cap_jitter_secs.min(policy.max_age_secs), jitter.cap);
    match wall_age_secs {
        Some(age) if age >= cap => RefreshDue::Cap,
        _ => RefreshDue::No,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Impact: interrupted stores left `<hash>.tmp.<nonce>` files the walk
    // only warned about, forever; they are reaped on the orphan schedule.
    // Should: select temp files older than the grace cutoff.
    // Should not: select a temp file young enough to be an in-flight store.
    #[test]
    fn stale_temps_respect_the_orphan_grace() {
        let temp = |mtime: u64, name: &str| crate::fragstore::TempFile {
            path: std::path::PathBuf::from(name),
            mtime,
        };
        let temps = [temp(10, "old"), temp(99, "fresh"), temp(100, "at-cutoff")];
        assert_eq!(
            stale_temps(&temps, 100),
            vec![
                std::path::PathBuf::from("old"),
                std::path::PathBuf::from("fresh")
            ]
        );
        assert!(stale_temps(&temps, 5).is_empty());
    }

    // Impact: thor's walker never finishes a rotation, so this line is the
    // only log of where its rotation time goes.
    // Should: log progress at INFO once 15 minutes have passed since the
    // last INFO line, however few shards.
    // Should: log progress at DEBUG every 16 shards in between.
    // Should not: log progress at INFO more often than every 15 minutes.
    // Should not: log progress before a single new shard was swept.
    #[test]
    fn progress_line_is_info_every_15_minutes_and_debug_every_16_shards() {
        assert_eq!(progress_due(0, 0), None);
        assert_eq!(progress_due(0, 10_000), None);
        assert_eq!(progress_due(15, 899), None);
        assert_eq!(progress_due(16, 0), Some(ProgressLine::Debug));
        assert_eq!(progress_due(64, 899), Some(ProgressLine::Debug));
        assert_eq!(progress_due(1, 900), Some(ProgressLine::Info));
        assert_eq!(progress_due(40, 3_600), Some(ProgressLine::Info));
    }

    // Should: sum each step's milliseconds and the scrub's reads over every
    // shard folded in, counting the timed and scrubbed shards.
    // Should: keep the slowest single submit across flushes, not their sum.
    #[test]
    fn report_accumulates_step_timings_across_shards() {
        let mut report = SweepReport::default();
        let shard = |ms: u64, scrubbed: bool| ShardTimings {
            ms_height: ms,
            ms_walk: 10 * ms,
            ms_db: 2 * ms,
            ms_diff: ms,
            ms_mark: 3 * ms,
            ms_temps: ms,
            ms_orphans: 4 * ms,
            ms_scrub: if scrubbed { 100 } else { 0 },
            scrubbed,
            scrub_files: if scrubbed { 5 } else { 0 },
            scrub_bytes: if scrubbed { 5_000 } else { 0 },
            ms_release: ms,
            ms_buffer: 2 * ms,
            files: 7,
        };
        report.add_shard_timings(&shard(1, false));
        report.add_shard_timings(&shard(2, true));
        report.add_flush_timings(&FlushTimings {
            pages: 3,
            ok: 2,
            failed: 1,
            ms: 40,
            slowest_ms: 30,
        });
        report.add_flush_timings(&FlushTimings {
            pages: 1,
            ok: 1,
            failed: 0,
            ms: 20,
            slowest_ms: 20,
        });

        assert_eq!(report.shards_timed, 2);
        assert_eq!(
            (
                report.ms_height,
                report.ms_walk,
                report.ms_db,
                report.ms_diff
            ),
            (3, 30, 6, 3)
        );
        assert_eq!(
            (report.ms_mark, report.ms_temps, report.ms_orphans),
            (9, 3, 12)
        );
        assert_eq!((report.ms_release, report.ms_buffer), (3, 6));
        assert_eq!(
            (
                report.ms_scrub,
                report.shards_scrubbed,
                report.scrub_files,
                report.scrub_bytes
            ),
            (100, 1, 5, 5_000)
        );
        assert_eq!(
            (report.ms_flush, report.flush_pages, report.flush_slowest_ms),
            (60, 4, 30)
        );
        // (3+30+6+3+9+3+12+100+3+6+60) / 2
        assert_eq!(report.avg_ms_per_shard(), 117);
    }

    fn h(b: u8) -> Blake3Hash {
        Blake3Hash::from_bytes([b; 32])
    }

    fn file(b: u8, mtime: u64) -> DiskFragment {
        DiskFragment {
            hash: h(b),
            size: 10,
            mtime,
        }
    }

    // Should: offer the surplus release only files that have a table row
    // and are older than the grace cutoff, with their sizes.
    // Should not: offer orphans (no row) or young files (possible in-flight
    // stores), whatever their age or row.
    #[test]
    fn release_listing_is_present_rows_past_the_grace() {
        let listing = [file(1, 100), file(2, 100), file(3, 900), file(4, 100)];
        let present = [h(1), h(3), h(4)];
        assert_eq!(
            release_listing(&listing, &present, 500),
            vec![(h(1), 10), (h(4), 10)]
        );
        assert!(release_listing(&listing, &[], 500).is_empty());
    }

    // Should: classify each file by its row and flag — present-and-
    // flagged, present-but-unflagged (repair the flag), rowless-and-old
    // (orphan), rowless-and-young (left alone) — and list flagged rows
    // whose bytes are gone.
    // Impact: this diff is the whole disk-truth mechanism; a wrong case
    // either lies to belief or deletes a racing store.
    #[test]
    fn classifies_every_case() {
        let disk = [file(1, 100), file(2, 100), file(3, 100), file(4, 900)];
        let rows = [(h(1), true), (h(2), false), (h(5), true), (h(6), false)];
        let d = diff(&disk, &rows, 500);
        assert_eq!(d.present, vec![h(1), h(2)]);
        assert_eq!(d.present_unflagged, vec![h(2)]);
        assert_eq!(d.flagged_missing, vec![h(5)]);
        assert_eq!(d.orphans, vec![(h(3), 10)]);
        assert_eq!(d.young_orphans, 1);
    }

    // Impact: the 2026-10-01 loss — a node's own upload is rowless until
    // its transaction lands, and only the node knows it uploaded; the
    // ledger is that knowledge, and the split is where it spares the file.
    // Should: hand every orphan the ledger names to the held side and the
    // rest to the delete side, sizes and order intact.
    // Should not: hold anything when the ledger is empty.
    #[test]
    fn split_orphans_spares_own_uploads() {
        let orphans = [(h(1), 10), (h(2), 20), (h(3), 30)];
        let ledger = HashSet::from([h(2), h(9)]);
        let (delete, held) = split_orphans(&orphans, &ledger);
        assert_eq!(delete, vec![(h(1), 10), (h(3), 30)]);
        assert_eq!(held, vec![(h(2), 20)]);

        let (delete, held) = split_orphans(&orphans, &HashSet::new());
        assert_eq!(delete, orphans.to_vec());
        assert!(held.is_empty());
    }

    // Should: treat a hash flagged on any of its rows as flagged, and
    // an empty store as nothing present and everything flagged missing.
    #[test]
    fn shared_rows_and_empty_store() {
        let rows = [(h(1), false), (h(1), true), (h(2), true)];
        let d = diff(&[file(1, 0)], &rows, 0);
        assert!(d.present_unflagged.is_empty(), "one flagged row suffices");
        assert_eq!(d.flagged_missing, vec![h(2)]);

        let empty = diff(&[], &rows, 0);
        assert!(empty.present.is_empty());
        assert_eq!(empty.flagged_missing, vec![h(1), h(2)]);
    }

    // Impact: the live mesh's 196k-fragment node attested in one 6.5 MB
    // transaction that never cleared the queue, so no row was ever verified.
    // Should: split the present list into page-sized attestations that
    // together cover every hash exactly once, the last page short.
    // Should not: emit a page for an empty store.
    #[test]
    fn attestation_pages_cover_every_hash_once() {
        let present: Vec<Blake3Hash> = (0u8..10).map(h).collect();
        let pages = attestation_pages(2, 77, &present, 4);
        assert_eq!(pages.len(), 3);
        assert_eq!(pages[2].present.len(), 2, "the last page is short");
        let mut seen: Vec<Blake3Hash> = pages
            .iter()
            .flat_map(|p| p.present.iter().copied())
            .collect();
        seen.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        assert_eq!(seen, present);
        assert!(pages
            .iter()
            .all(|p| p.node_id == 2 && p.height == 77 && p.suspect.is_empty()));
        assert!(attestation_pages(2, 77, &[], 4).is_empty());
    }

    // Should: place a hash in the shard of its first byte, and bound each
    // shard so the first and last shards are covered at their edges.
    #[test]
    fn shard_bounds_bracket_the_first_byte() {
        let mut bytes = [0xabu8; 32];
        assert_eq!(shard_of(&Blake3Hash::from_bytes(bytes)), 0xab);
        assert_eq!(shard_bounds(0x00), (vec![0x00], Some(vec![0x01])));
        assert_eq!(shard_bounds(0xab), (vec![0xab], Some(vec![0xac])));
        assert_eq!(shard_bounds(0xff), (vec![0xff], None));
        // memcmp-then-length ordering, as SQLite compares BLOBs.
        bytes[1..].fill(0xff);
        assert!(vec![0xab] <= bytes.to_vec() && bytes.to_vec() < vec![0xac]);
        bytes[1..].fill(0x00);
        assert!(vec![0xab] <= bytes.to_vec());
    }

    // Impact: the scrub keeps its old budget (one seventh of the store per
    // UTC day) now that shards are visited many times a day.
    // Should: put each shard in exactly one of the seven daily slices.
    #[test]
    fn every_shard_is_scrubbed_on_exactly_one_day_of_seven() {
        for shard in 0..=u8::MAX {
            let days: Vec<i64> = (100..107).filter(|d| scrub_due(shard, *d)).collect();
            assert_eq!(days.len(), 1, "shard {shard}");
        }
    }

    // Impact: decision 2 of the rolling sweep — a page must never claim a
    // fragment was seen later than it was, or freshness is overstated.
    // Should: stamp a page with the lowest height among the hashes in it.
    // Should: be due at a full page or once the oldest hash has waited the
    // maximum age, and reset once drained.
    // Should not: be due when empty.
    #[test]
    fn page_buffer_stamps_the_floor_height_and_flushes_on_size_or_age() {
        let mut buf = PageBuffer::default();
        assert!(!buf.due(1_000, 4, 60));
        buf.push(50, 1_000, [h(1), h(2)]);
        buf.push(40, 1_010, [h(3)]);
        buf.push(60, 1_020, []);
        assert!(!buf.due(1_030, 4, 60));
        assert!(buf.due(1_060, 4, 60), "the oldest hash waited 60 s");
        buf.push(70, 1_030, [h(4)]);
        assert!(buf.due(1_030, 4, 60), "a full page");

        let (height, page) = buf.take_page(3).unwrap();
        assert_eq!(height, 40);
        assert_eq!(page, vec![h(1), h(2), h(3)]);
        let (height, rest) = buf.take_page(3).unwrap();
        assert_eq!(height, 40, "what stays keeps the conservative floor");
        assert_eq!(rest, vec![h(4)]);
        assert!(buf.take_page(3).is_none());
        assert!(!buf.due(9_999, 4, 60));
    }

    // Impact: a node that restarts mid-rotation resumes where it stopped,
    // so even one restarting every few minutes completes rotations.
    // Should: step to the next shard within a rotation, keeping its start.
    // Should: wrap after shard 255 into a new rotation stamped with the
    // wrap's time and height, and report the rotation as complete.
    #[test]
    fn cursor_advances_and_wraps_into_a_new_rotation() {
        let start = SweepCursor::fresh(1_000, 50);
        let (next, done) = start.advance(1_010, 51);
        assert!(!done);
        assert_eq!(
            next,
            SweepCursor {
                next_shard: 1,
                ..start
            }
        );

        let last = SweepCursor {
            next_shard: 255,
            ..start
        };
        let (wrapped, done) = last.advance(4_600, 900);
        assert!(done);
        assert_eq!(
            wrapped,
            SweepCursor {
                next_shard: 0,
                rotation: 1,
                started_unix: 4_600,
                started_height: 900
            }
        );
    }

    // Should: page removals and additions into reports of at most a page,
    // removals first, at the lower of the two floor heights.
    #[test]
    fn belief_buffer_pages_removals_first_at_the_floor_height() {
        let mut buf = BeliefBuffer::default();
        assert!(buf.take_report(7, 4).is_none());
        buf.added.push(30, 0, [h(1), h(2), h(3)]);
        buf.removed.push(20, 0, [h(9), h(8)]);

        let first = buf.take_report(7, 4).unwrap();
        assert_eq!(first.node_id, 7);
        assert_eq!(first.self_verified_height, 20);
        assert_eq!(first.fragments_removed, vec![h(9), h(8)]);
        assert_eq!(first.fragments_added, vec![h(1), h(2)]);
        let second = buf.take_report(7, 4).unwrap();
        assert_eq!(second.self_verified_height, 30);
        assert_eq!(second.fragments_added, vec![h(3)]);
        assert!(second.fragments_removed.is_empty());
        assert!(buf.take_report(7, 4).is_none());
    }

    const WINDOW: u64 = crate::lifecycle::ATTESTATION_RECENCY_HEIGHTS;

    fn own_stamp(verified_height: u64) -> InventoryStamp {
        InventoryStamp {
            verified_height: Some(verified_height),
            provenance: Some(0),
            suspect: false,
        }
    }

    fn jitter(height: u64, cap: u64) -> RefreshJitter {
        RefreshJitter { height, cap }
    }

    // Impact: the recency rule is a consensus rule and the refresh is node-
    // local; this inequality is all that keeps a quiet node's evidence from
    // expiring under it, through one failed sweep.
    // Should: leave two rotations and the margin between every threshold
    // and the window's end, for any rotation length.
    // Should: budget at least a drain-length rotation, however short the
    // measured one.
    // Should: fall back to refreshing everything once no threshold fits.
    #[test]
    fn refresh_threshold_survives_one_failed_sweep_at_any_rotation_length() {
        assert_eq!(
            RefreshPolicy::for_rotation(WINDOW, 0).threshold,
            WINDOW - 2 * REFRESH_ROTATION_FLOOR_HEIGHTS - REFRESH_MARGIN_HEIGHTS
        );
        for rotation in 0..=WINDOW {
            let policy = RefreshPolicy::for_rotation(WINDOW, rotation);
            if policy.threshold > 0 {
                assert!(
                    policy.threshold + 2 * rotation + REFRESH_MARGIN_HEIGHTS <= WINDOW,
                    "rotation {rotation}: threshold {}",
                    policy.threshold
                );
            } else {
                let due = refresh_due(Some(&own_stamp(100)), 100, Some(0), jitter(0, 0), &policy);
                assert_eq!(due, RefreshDue::Age, "rotation {rotation}");
            }
        }
    }

    // Should: refresh a stamp once its height age reaches the threshold.
    // Should: refresh a stamp older than the cap, however young in heights.
    // Should not: refresh a stamp young on both clocks, or apply the cap
    // when the stamp's wall age is unknown.
    #[test]
    fn refresh_is_due_on_either_clock() {
        let policy = RefreshPolicy::for_rotation(WINDOW, 1000);
        let t = policy.threshold;
        let none = jitter(0, 0);
        let at = |age_heights: u64, wall: Option<u64>| {
            refresh_due(
                Some(&own_stamp(10_000)),
                10_000 + age_heights,
                wall,
                none,
                &policy,
            )
        };
        assert_eq!(at(t, Some(0)), RefreshDue::Age);
        assert_eq!(at(t - 1, Some(REFRESH_MAX_AGE_SECS)), RefreshDue::Cap);
        assert_eq!(at(t - 1, Some(REFRESH_MAX_AGE_SECS - 1)), RefreshDue::No);
        assert_eq!(at(t - 1, None), RefreshDue::No);
    }

    // Impact: attestation is the only thing that sets these stamps right;
    // leaving them out to save volume would strand them.
    // Should: refresh a fragment with no row, never verified, verified by a
    // remote challenge, or suspect, however fresh its stamp.
    #[test]
    fn refresh_is_due_for_any_stamp_only_an_attestation_corrects() {
        let policy = RefreshPolicy::for_rotation(WINDOW, 1000);
        let fresh = own_stamp(500);
        let cases = [
            None,
            Some(InventoryStamp {
                verified_height: None,
                ..fresh
            }),
            Some(InventoryStamp {
                provenance: None,
                ..fresh
            }),
            Some(InventoryStamp {
                provenance: Some(1),
                ..fresh
            }),
            Some(InventoryStamp {
                suspect: true,
                ..fresh
            }),
        ];
        for stamp in cases {
            let due = refresh_due(stamp.as_ref(), 500, Some(0), jitter(0, 0), &policy);
            assert_eq!(due, RefreshDue::Flags, "{stamp:?}");
        }
        assert_eq!(
            refresh_due(Some(&fresh), 500, Some(0), jitter(0, 0), &policy),
            RefreshDue::No
        );
    }

    // Should: let jitter only lower a fragment's threshold and cap, by less
    // than its span.
    // Should not: let any jitter delay a refresh past the policy's own.
    #[test]
    fn jitter_only_brings_a_refresh_forward() {
        let policy = RefreshPolicy::for_rotation(WINDOW, 1000);
        for j in [0, 1, u64::MAX / 3, u64::MAX / 2, u64::MAX - 1, u64::MAX] {
            let jit = jitter(j, j);
            let due_at = |age: u64, wall: u64| {
                refresh_due(Some(&own_stamp(0)), age, Some(wall), jit, &policy)
            };
            assert_eq!(due_at(policy.threshold, 0), RefreshDue::Age, "jitter {j}");
            assert_eq!(
                due_at(0, policy.max_age_secs),
                RefreshDue::Cap,
                "jitter {j}"
            );
            let lowest = policy.threshold - policy.jitter_heights + 1;
            assert_eq!(due_at(lowest - 1, 0), RefreshDue::No, "jitter {j}");
            let earliest_cap = policy.max_age_secs - policy.cap_jitter_secs + 1;
            assert_eq!(due_at(0, earliest_cap - 1), RefreshDue::No, "jitter {j}");
        }
    }

    // Should: give every (node, fragment) the same jitter on every call.
    // Should: give one fragment different jitter on different holders.
    // Should: spread fragments evenly over the span on both clocks.
    #[test]
    fn jitter_is_fixed_per_holder_and_uniform() {
        let hash = |i: u32| Blake3Hash::from_bytes(*blake3::hash(&i.to_le_bytes()).as_bytes());
        assert_eq!(refresh_jitter(1, &hash(7)), refresh_jitter(1, &hash(7)));
        assert_ne!(refresh_jitter(1, &hash(7)), refresh_jitter(2, &hash(7)));

        const N: u32 = 64_000;
        const BUCKETS: usize = 16;
        let mut by_height = [0u32; BUCKETS];
        let mut by_cap = [0u32; BUCKETS];
        for i in 0..N {
            let j = refresh_jitter(3, &hash(i));
            by_height[(j.height >> 60) as usize] += 1;
            by_cap[(j.cap >> 60) as usize] += 1;
        }
        let expected = N / BUCKETS as u32;
        for counts in [by_height, by_cap] {
            for (bucket, n) in counts.iter().enumerate() {
                assert!(
                    n.abs_diff(expected) < expected / 10,
                    "bucket {bucket}: {n} of an expected {expected}"
                );
            }
        }
    }

    // Should: budget for the longest recent rotation.
    // Should: extrapolate the rotation in progress once it is long enough
    // to sample, so a traffic surge shortens the threshold within the
    // rotation it starts in.
    // Should not: estimate anything with neither a completed rotation nor
    // a long enough sample.
    #[test]
    fn rotation_estimate_takes_the_longest_of_history_and_the_current_pace() {
        assert_eq!(
            estimate_rotation_heights(&[300, 900, 500], 10, 600, 3600, 300),
            Some(900)
        );
        assert_eq!(
            estimate_rotation_heights(&[300], 450, 600, 3600, 300),
            Some(2700)
        );
        assert_eq!(
            estimate_rotation_heights(&[300], 450, 100, 3600, 300),
            Some(300)
        );
        assert_eq!(
            estimate_rotation_heights(&[], 450, 600, 3600, 300),
            Some(2700)
        );
        assert_eq!(estimate_rotation_heights(&[], 450, 100, 3600, 300), None);
        assert_eq!(
            estimate_rotation_heights(&[], 5000, 5000, 3600, 300),
            Some(5000),
            "an overrunning rotation is its own length"
        );
    }

    // Impact: the cap's only clock; an age understated by it would hold a
    // stamp past the cap, an age overstated only refreshes it early.
    // Should: date a stamp by the newest sample strictly below its height,
    // never under its true age, wherever between samples it was made.
    // Should: give a stamp below every sample the oldest sample's age, and
    // leave a stamp at the oldest sample's height undated.
    #[test]
    fn height_clock_never_understates_a_stamps_age() {
        // The tip as it moved: height 10 + t/60 at second t, sampled every
        // 14 s (a shard step), each sample its first sight of a height.
        let tip = |t: u64| 10 + t / 60;
        let mut clock = HeightClock::default();
        for t in (0..7_200).step_by(14) {
            clock.record(tip(t), t);
        }
        let now = 9_000;
        for made in 60..7_200 {
            let age = clock.wall_age(tip(made), now).expect("dated");
            assert!(age >= now - made, "stamp at {made}: aged {age}");
            // Over by at most the dwell at the height below, the time
            // already spent at the stamp's own, and one sampling step.
            assert!(
                age <= now - made + 2 * 60 + 14,
                "stamp at {made}: aged {age}"
            );
        }
        assert_eq!(clock.wall_age(10, now), None, "at the oldest sample");
        let mut later = HeightClock::default();
        later.record(50, 1_000);
        later.record(60, 2_000);
        assert_eq!(later.wall_age(40, 5_000), Some(4_000), "a lower bound");
    }

    // Should: keep one sample per height, its first sight, whether recorded
    // step by step or seeded from completed rotations.
    // Should: prune samples before a cutoff but the newest of them.
    #[test]
    fn height_clock_keeps_first_sights_and_prunes_to_the_cutoff() {
        let mut clock = HeightClock::default();
        clock.record(5, 100);
        clock.record(5, 200);
        clock.record(4, 300);
        clock.record(6, 400);
        assert_eq!(clock.samples, vec![(5, 100), (6, 400)]);

        clock.seed(&[SweepRotation {
            rotation: 0,
            started_unix: 50,
            started_height: 3,
            ended_unix: 150,
            ended_height: 5,
        }]);
        assert_eq!(clock.samples, vec![(3, 50), (5, 100), (6, 400)]);

        clock.prune(120);
        assert_eq!(clock.samples, vec![(5, 100), (6, 400)]);
        clock.prune(1_000);
        assert_eq!(clock.samples, vec![(6, 400)]);
        assert_eq!(clock.len(), 1);
    }
}
