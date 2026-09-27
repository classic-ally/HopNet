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

/// In-flight blobs the policy tick checks for complete confirmation
/// evidence per run (the fulfillment pass's floor until S4's sampling).
pub const CONFIRM_CHECKS_PER_TICK: usize = 256;

/// Consensus function names the reconciler submits through the
/// `TxSubmitter` seam.
pub const SELF_CHECK_FN: &str = "self_check_fragments";

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
