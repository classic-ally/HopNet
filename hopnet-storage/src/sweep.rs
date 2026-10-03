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
//!   way a fresh file lacks its row).
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

/// The sweep's verdict over one walk.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SweepDiff {
    /// On disk, in the table, flag says absent → flag it, attest it.
    pub present_unflagged: Vec<Blake3Hash>,
    /// Flag says present, bytes gone → un-flag it.
    pub flagged_missing: Vec<Blake3Hash>,
    /// On disk, no table row, older than the grace period → delete.
    pub orphans: Vec<(Blake3Hash, u64)>,
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
}
