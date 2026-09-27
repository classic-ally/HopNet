//! Pure reconciler policy: every tunable, separated from the tokio plumbing
//! in `engine::mod` so semantics are testable without a runtime.
//!
//! RFC-STORAGE-003 S3 retired the push pipeline's knobs (worker pools,
//! send permits, the failure threshold, the placement batcher): transfer
//! pacing now lives on the pull side — one serial worker per node, so
//! concurrency tracks the mesh, not the upload count.

/// In-flight blobs the policy tick re-kicks per run, oldest goal first.
/// Bounded so a ballooning in-flight set (a catch-up drain after a view
/// transition) is paced, never refused — the set records need, the tick
/// works it down.
pub const PULL_KICKS_PER_TICK: usize = 64;

/// The fulfillment pass's base sample (S4): in-flight blobs checked for
/// complete confirmation evidence in the tick's first round. Rounds
/// continue, doubling, while the sample stays dense (`next_fulfillment_sample`).
pub const CONFIRM_CHECKS_PER_TICK: usize = 256;

/// Rounds the fulfillment pass may run per tick: from the base sample,
/// doubling to `CONFIRM_SAMPLE_MAX`, six rounds clear roughly twelve
/// thousand rubber stamps in one tick — a post-transition balloon of a
/// small mesh's whole population — at one consensus round each.
pub const CONFIRM_ROUNDS_PER_TICK: usize = 6;

/// The adaptive sample's step (S4): after a round that sampled `sampled`
/// blobs and found `ready` of them confirm-ready, the next round's sample
/// — doubled (capped at `CONFIRM_SAMPLE_MAX`) while at least half were
/// ready, `None` when the set has gone sparse or empty and the pass
/// should rest until the next tick. Recurrence over a draining set is
/// what makes a random sample comprehensive; density is what makes the
/// doubling safe (a sparse sample means the remaining work is pulls, not
/// stamps).
pub fn next_fulfillment_sample(sample: usize, sampled: usize, ready: usize) -> Option<usize> {
    (sampled > 0 && ready * 2 >= sampled).then(|| (sample * 2).min(CONFIRM_SAMPLE_MAX))
}

/// Consensus function names the reconciler submits through the
/// `TxSubmitter` seam.
pub const SELF_CHECK_FN: &str = "self_check_fragments";
/// Disk-truth attestation (S5): stamps `verified_height` on the rows this
/// node has just verified on its own disk.
pub const ATTEST_FN: &str = "attest_fragments";

/// Confirmation evidence recency (S5) — defined beside the evidence check
/// in `lifecycle` (feature-free), re-exported here with the other knobs.
pub use crate::lifecycle::ATTESTATION_RECENCY_HEIGHTS;

/// Blobs per declare page (S4 staleness pass). Pagination for block-size
/// hygiene, not a cap: the work-list consumes itself, so the next
/// proposal takes the next page.
pub const DECLARE_PAGE_SIZE: usize = 500;

/// The staleness pass's grace rung: a node that has not observed the
/// `desired < T` check (a proposal of its own, or anyone's declare page
/// applying) for this long submits a page directly. Longer than the
/// metrics heartbeat, so it never fires while the propose hook is healthy.
pub const STALENESS_GRACE_SECS: i64 = 900;

/// Upper bound of the adaptive fulfillment sample (S4): the sample doubles
/// while at least half of it is confirm-ready and resets otherwise.
pub const CONFIRM_SAMPLE_MAX: usize = 4096;

#[cfg(test)]
mod tests {
    use super::*;

    // Should: double the sample while at least half of it was ready, hold
    // at the cap, and stop on a sparse or empty round.
    // Should not: keep doubling on a round that found fewer than half ready.
    #[test]
    fn fulfillment_sample_doubles_while_dense_and_stops_when_sparse() {
        assert_eq!(next_fulfillment_sample(256, 256, 256), Some(512));
        assert_eq!(
            next_fulfillment_sample(256, 256, 128),
            Some(512),
            "exactly half is dense"
        );
        assert_eq!(next_fulfillment_sample(256, 256, 127), None);
        assert_eq!(
            next_fulfillment_sample(256, 0, 0),
            None,
            "nothing in flight"
        );
        assert_eq!(
            next_fulfillment_sample(4096, 4096, 4096),
            Some(CONFIRM_SAMPLE_MAX)
        );
        assert_eq!(
            next_fulfillment_sample(3000, 3000, 2000),
            Some(CONFIRM_SAMPLE_MAX)
        );
        // A short final page (fewer in flight than the sample) that is all
        // ready still reports dense — the next round finds it empty.
        assert_eq!(next_fulfillment_sample(512, 40, 40), Some(1024));
    }
}
