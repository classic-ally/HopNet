//! Pure distribution-engine policy: every tunable and batching decision,
//! separated from the tokio plumbing in `engine::mod` so semantics are
//! testable without a runtime.

use crate::types::PlacementUpdate;
use std::collections::HashMap;

/// Global distribution workers (RFC-014 engine rule: concurrency tracks the
/// mesh, not the upload count; actual sends are further bounded by
/// SEND_PERMITS).
pub const DISTRIBUTION_WORKERS: usize = 4;

/// Process-wide bound on concurrent fragment sends.
pub const SEND_PERMITS: usize = 16;

/// Per-blob fragment-send workers (clamped to the fragment count).
pub const BLOB_SEND_WORKERS: usize = 4;

/// Fail a blob's distribution if more than this share of fragments could
/// not be placed on any candidate.
pub const FAILURE_THRESHOLD_PERCENT: f64 = 10.0;

/// Domain-level retries per fragment send (server-side transient errors).
pub const SEND_MAX_RETRIES: u32 = 2;
pub const SEND_RETRY_DELAY_MS: u64 = 1000;

/// How long the placement batcher collects updates before flushing them as
/// one `update_placement_heights` consensus tx, and the flush size cap
/// (aligned with the consensus queue's MAX_BATCH_SIZE).
pub const PLACEMENT_FLUSH_MS: u64 = 750;
pub const PLACEMENT_FLUSH_MAX: usize = 100;
/// Flush retry cap for transient submit failures.
pub const PLACEMENT_FLUSH_ATTEMPTS: u8 = 3;

/// The storage-owned consensus transaction the batcher submits.
pub const PLACEMENT_COMMIT_FN: &str = "update_placement_heights";

/// Dedup a flush window by blob id, keeping the LATEST placement height for
/// each blob (a re-distributed blob supersedes its earlier entry). Entries
/// carry their retry attempt count through the dedup.
pub fn dedup_window(pending: Vec<(PlacementUpdate, u8)>) -> Vec<(PlacementUpdate, u8)> {
    let mut seen = HashMap::new();
    for (u, attempts) in pending {
        seen.insert(u.blob_id.clone(), (u, attempts));
    }
    seen.into_values().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    // Should: keep the LAST entry per blob id across a window, preserving
    // attempt counts of the surviving entry.
    // Should not: let a retried (older) entry shadow a newer placement.
    // Impact: duplicate placement commits per window would waste consensus
    // payload; a stale height surviving dedup would pin placement to an
    // outdated validator snapshot.
    #[test]
    fn dedup_keeps_latest_per_blob() {
        let a = crate::CustomUUID::from_str("01890a5d-ac96-774b-b9aa-9f8b24f0c9a1").unwrap();
        let b = crate::CustomUUID::from_str("01890a5d-ac96-774b-b9aa-9f8b24f0c9a2").unwrap();
        let out = dedup_window(vec![
            (
                PlacementUpdate {
                    blob_id: a.clone(),
                    placement_height: 5,
                },
                1,
            ),
            (
                PlacementUpdate {
                    blob_id: b.clone(),
                    placement_height: 6,
                },
                0,
            ),
            (
                PlacementUpdate {
                    blob_id: a.clone(),
                    placement_height: 9,
                },
                0,
            ),
        ]);
        assert_eq!(out.len(), 2);
        let a_entry = out.iter().find(|(u, _)| u.blob_id == a).unwrap();
        assert_eq!((a_entry.0.placement_height, a_entry.1), (9, 0));
    }
}
