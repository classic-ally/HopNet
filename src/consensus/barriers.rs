//! Consensus-phase barriers (test-only). Owns the consensus-specific name
//! registry; mechanics + HTTP routes live in `crate::barriers`. Subsystem
//! registration is via `inventory` so adding a new subsystem doesn't touch
//! shared code.

use crate::barriers::{BarrierRegistration, Barriers};

pub mod names {
    // Malachite-engine barriers (taps at main-crate seams).
    /// Held in the gossip publisher before a full-value proposal leaves.
    pub const BEFORE_PUBLISH_PROPOSAL: &str = "before_publish_proposal";
    /// Held in the gossip publisher before a precommit vote leaves.
    pub const BEFORE_PUBLISH_PRECOMMIT: &str = "before_publish_precommit";
    /// Held in the engine driver before building a value for NeedValue.
    pub const BEFORE_PROPOSE: &str = "before_propose";
    /// Held before answering a DecidedFetch (sync serving).
    pub const BEFORE_SYNC_RESPONSE: &str = "before_sync_response";
    /// Held (via the commit-gate mirror in `malachite::engine`) before a
    /// decide's DB transaction commits.
    pub const BEFORE_DECIDE: &str = "before_decide";
}

pub const ALL_BARRIER_NAMES: &[&str] = &[
    names::BEFORE_PUBLISH_PROPOSAL,
    names::BEFORE_PUBLISH_PRECOMMIT,
    names::BEFORE_PROPOSE,
    names::BEFORE_SYNC_RESPONSE,
    names::BEFORE_DECIDE,
];

pub fn new() -> Barriers {
    Barriers::new(ALL_BARRIER_NAMES)
}

inventory::submit! {
    &BarrierRegistration {
        subsystem: "consensus",
        accessor: |state: &crate::AppState| &state.consensus_barriers,
        names: ALL_BARRIER_NAMES,
    }
}
