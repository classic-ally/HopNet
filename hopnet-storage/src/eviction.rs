//! Watermark eviction planner (RFC-STORAGE-001 Copy classes / GC;
//! RFC-STORAGE-003 S2 guard).
//!
//! Decentralized GC: a local loop under disk pressure evicts SURPLUS
//! copies, oldest blob first, from the high watermark down to the low.
//! Pure planning over caller-gathered facts — the invariant carrier is
//! the guard, never the watermark values:
//!
//!   evictable ⇔ not protected ∧ another non-departed member attests a
//!   copy in the inventory (or the blob is deleted — which the orphan
//!   flow owns).
//!
//! `protected` is the protection predicate (`crate::protection`): the
//! confirmed epoch's obligation, every in-flight epoch's, the never-
//! confirmed clause, and pins — evaluated by the caller per copy. The
//! attested-other-holder check stays as a second belt: eviction never
//! touches a protected copy, so a stale inventory view can forfeit only
//! surplus margin (checked exhaustively in the model).

use hopnet_common::Blake3Hash;

#[derive(Debug, Clone)]
pub struct EvictionCandidate {
    pub fragment_hash: Blake3Hash,
    /// Blob id string — UUIDv7, so ascending lexicographic = oldest first.
    pub blob_id: String,
    pub size_bytes: u64,
    /// The protection predicate's verdict for this copy on this node
    /// (`protection::Protection::protects`, pins folded in).
    pub protected: bool,
    /// Non-departed members (other than this node) attesting a copy.
    pub other_member_holders: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct DiskPressure {
    pub used_bytes: u64,
    pub total_bytes: u64,
    /// Act above this fill fraction (percent).
    pub high_pct: u8,
    /// Stop once projected fill reaches this (percent).
    pub low_pct: u8,
}

/// The guard alone: every candidate that is neither protected nor the last
/// attested member copy, oldest blob first (UUIDv7 time order; stable
/// within a blob). Both planners share it, so the invariant has one home.
fn evictable_surplus(candidates: Vec<EvictionCandidate>) -> Vec<EvictionCandidate> {
    let mut evictable: Vec<EvictionCandidate> = candidates
        .into_iter()
        .filter(|c| !c.protected && c.other_member_holders >= 1)
        .collect();
    evictable.sort_by(|a, b| a.blob_id.cmp(&b.blob_id));
    evictable
}

/// Plan the prompt release of surplus, regardless of disk pressure: up to
/// `max` evictable copies, oldest blob first. Surplus is evictable any time
/// (durability-policy); the model leaves eviction timing adversarial, so
/// releasing it as soon as placement is confirmed needs only the same
/// guard. Callers feed it candidates whose holder count already demands a
/// recent attestation (stricter than the watermark path).
pub fn plan_surplus_release(candidates: Vec<EvictionCandidate>, max: usize) -> Vec<Blake3Hash> {
    evictable_surplus(candidates)
        .into_iter()
        .take(max)
        .map(|c| c.fragment_hash)
        .collect()
}

/// Plan which fragments to evict. Empty below the high watermark; above
/// it, evictable surplus oldest-first until the projected fill reaches the
/// low watermark (or evictable surplus runs out — the escalation ladder
/// past that point is capacity honesty, never a guard override).
pub fn plan_evictions(
    candidates: Vec<EvictionCandidate>,
    pressure: &DiskPressure,
) -> Vec<Blake3Hash> {
    if pressure.total_bytes == 0 {
        return Vec::new();
    }
    let high_bytes = pressure.total_bytes / 100 * pressure.high_pct as u64;
    if pressure.used_bytes <= high_bytes {
        return Vec::new();
    }
    let low_bytes = pressure.total_bytes / 100 * pressure.low_pct as u64;
    let target_free = pressure.used_bytes.saturating_sub(low_bytes);

    let mut planned = Vec::new();
    let mut freed = 0u64;
    for c in evictable_surplus(candidates) {
        if freed >= target_free {
            break;
        }
        freed += c.size_bytes;
        planned.push(c.fragment_hash);
    }
    planned
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(b: u8) -> Blake3Hash {
        Blake3Hash::new(blake3::hash(&[b]))
    }

    fn candidate(b: u8, blob: &str, size: u64) -> EvictionCandidate {
        EvictionCandidate {
            fragment_hash: hash(b),
            blob_id: blob.to_string(),
            size_bytes: size,
            protected: false,
            other_member_holders: 1,
        }
    }

    const PRESSURE: DiskPressure = DiskPressure {
        used_bytes: 95,
        total_bytes: 100,
        high_pct: 90,
        low_pct: 80,
    };

    // Should: evict nothing below the high watermark.
    // Impact: eviction churning below pressure would throw away read-
    // routing surplus for no reason.
    #[test]
    fn below_high_is_noop() {
        let calm = DiskPressure {
            used_bytes: 50,
            ..PRESSURE
        };
        assert!(plan_evictions(vec![candidate(1, "b", 10)], &calm).is_empty());
    }

    // Should not: evict a protected copy (obligation, in-flight, never-
    // confirmed, or pinned — the predicate's verdict) or a sole-live-
    // holder copy — under ANY pressure.
    // Impact: these two exclusions ARE the eviction-safety invariant;
    // the watermark only decides when pressure acts.
    #[test]
    fn never_evicts_protected_copies() {
        let mut protected = candidate(1, "a", 10);
        protected.protected = true;
        let mut sole = candidate(3, "c", 10);
        sole.other_member_holders = 0;
        let full = DiskPressure {
            used_bytes: 100,
            ..PRESSURE
        };
        assert!(plan_evictions(vec![protected, sole], &full).is_empty());
    }

    // Should: evict oldest blobs first and stop once the low watermark is
    // reached, leaving newer surplus in place.
    // Impact: overshooting the low mark burns surplus that improves read
    // routing for free; undershooting re-triggers next cycle.
    #[test]
    fn oldest_first_stop_at_low() {
        // Need to free 95 - 80 = 15 bytes.
        let planned = plan_evictions(
            vec![
                candidate(3, "0190-newest", 10),
                candidate(1, "0170-oldest", 10),
                candidate(2, "0180-middle", 10),
            ],
            &PRESSURE,
        );
        assert_eq!(planned, vec![hash(1), hash(2)]);
    }

    // Impact: the watermark arithmetic decides how much surplus a
    // pressured node sheds; the conformance harness only ever runs the
    // planner under full pressure, and the 2026-09-27 mutation run
    // rewrote the byte math three ways unnoticed.
    // Should: act only above high_pct of total, and free exactly down to
    // low_pct of total — no more, oldest first — with percentages of a
    // total that is not 100.
    #[test]
    fn watermark_bytes_are_percentages_of_total() {
        let pressure = DiskPressure {
            used_bytes: 950,
            total_bytes: 1000,
            high_pct: 90,
            low_pct: 80,
        };
        // Free 950 − 800 = 150: two of the 100-byte copies, not three.
        let planned = plan_evictions(
            vec![
                candidate(1, "0170-a", 100),
                candidate(2, "0180-b", 100),
                candidate(3, "0190-c", 100),
            ],
            &pressure,
        );
        assert_eq!(planned, vec![hash(1), hash(2)]);

        // 850 used is below 900 = 90% of 1000: nothing.
        let calm = DiskPressure {
            used_bytes: 850,
            ..pressure
        };
        assert!(plan_evictions(vec![candidate(1, "0170-a", 100)], &calm).is_empty());

        // Exactly at the high mark is not above it.
        let edge = DiskPressure {
            used_bytes: 900,
            ..pressure
        };
        assert!(plan_evictions(vec![candidate(1, "0170-a", 100)], &edge).is_empty());
    }

    // Impact: an origin (above all a non-member one, like an ingesting
    // laptop) holds every class of every blob it ingested; waiting for disk
    // pressure to shed that surplus is what let ingest fill the disk.
    // Should: release every evictable surplus copy regardless of pressure,
    // oldest blob first, up to the cap.
    #[test]
    fn surplus_release_ignores_pressure_and_caps() {
        let planned = plan_surplus_release(
            vec![
                candidate(3, "0190-newest", 10),
                candidate(1, "0170-oldest", 10),
                candidate(2, "0180-middle", 10),
            ],
            usize::MAX,
        );
        assert_eq!(planned, vec![hash(1), hash(2), hash(3)]);

        let capped = plan_surplus_release(
            vec![candidate(2, "0180-b", 10), candidate(1, "0170-a", 10)],
            1,
        );
        assert_eq!(capped, vec![hash(1)]);
    }

    // Should not: release a protected copy (never-confirmed, responsible,
    // in-flight, pinned) or the last attested member copy, even when
    // releasing promptly.
    #[test]
    fn surplus_release_keeps_the_guard() {
        let mut protected = candidate(1, "a", 10);
        protected.protected = true;
        let mut sole = candidate(2, "b", 10);
        sole.other_member_holders = 0;
        let free = candidate(3, "c", 10);
        assert_eq!(
            plan_surplus_release(vec![protected, sole, free], usize::MAX),
            vec![hash(3)]
        );
    }
}
