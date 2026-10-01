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

/// What one sweep did — the operator's report (the former two-call orphan
/// scan/delete API collapses into this).
#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
