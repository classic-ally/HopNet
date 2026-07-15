//! Watermark eviction planner (RFC-STORAGE-001 Copy classes / GC).
//!
//! Decentralized GC: a local loop under disk pressure evicts SURPLUS
//! copies, oldest blob first, from the high watermark down to the low.
//! Pure planning over caller-gathered facts — the invariant carrier is
//! the guard, never the watermark values:
//!
//!   evictable ⇔ not responsible ∧ not pinned ∧ another non-departed
//!   member attests a copy in the inventory (or the blob is deleted —
//!   which the orphan flow owns).
//!
//! Eviction never touches responsible copies, so a stale inventory view
//! can forfeit only surplus margin (checked exhaustively in the model).

use hopnet_common::Blake3Hash;

#[derive(Debug, Clone)]
pub struct EvictionCandidate {
    pub fragment_hash: Blake3Hash,
    /// Blob id string — UUIDv7, so ascending lexicographic = oldest first.
    pub blob_id: String,
    pub size_bytes: u64,
    /// This node is the placement-responsible holder of this class.
    pub responsible: bool,
    /// Some projection pinned the blob on this node.
    pub pinned: bool,
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

    let mut evictable: Vec<EvictionCandidate> = candidates
        .into_iter()
        .filter(|c| !c.responsible && !c.pinned && c.other_member_holders >= 1)
        .collect();
    // Oldest blob first (UUIDv7 time order); stable within a blob.
    evictable.sort_by(|a, b| a.blob_id.cmp(&b.blob_id));

    let mut planned = Vec::new();
    let mut freed = 0u64;
    for c in evictable {
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
            responsible: false,
            pinned: false,
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

    // Should not: evict responsible, pinned, or sole-live-holder copies —
    // under ANY pressure.
    // Impact: these three exclusions ARE the eviction-safety invariant;
    // the watermark only decides when pressure acts.
    #[test]
    fn never_evicts_protected_copies() {
        let mut responsible = candidate(1, "a", 10);
        responsible.responsible = true;
        let mut pinned = candidate(2, "b", 10);
        pinned.pinned = true;
        let mut sole = candidate(3, "c", 10);
        sole.other_member_holders = 0;
        let full = DiskPressure {
            used_bytes: 100,
            ..PRESSURE
        };
        assert!(plan_evictions(vec![responsible, pinned, sole], &full).is_empty());
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
}
