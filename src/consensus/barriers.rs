//! Consensus-phase barriers (test-only). Owns the consensus-specific name
//! registry; mechanics + HTTP routes live in `crate::barriers`. Subsystem
//! registration is via `inventory` so adding a new subsystem doesn't touch
//! shared code.

use crate::barriers::{BarrierRegistration, Barriers};

pub mod names {
    // Bespoke-engine barriers (dormant; deleted at Stage 5b).
    pub const BEFORE_BALLOT_DISPATCH: &str = "before_ballot_dispatch";
    pub const AFTER_PROPOSE_QC_BROADCAST: &str = "after_propose_qc_broadcast";
    pub const BEFORE_TC_GST_WAIT: &str = "before_tc_gst_wait";
    pub const BEFORE_TC_APPLICATION: &str = "before_tc_application";
    pub const BEFORE_LOCK_QC_BROADCAST: &str = "before_lock_qc_broadcast";

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
    names::BEFORE_BALLOT_DISPATCH,
    names::AFTER_PROPOSE_QC_BROADCAST,
    names::BEFORE_TC_GST_WAIT,
    names::BEFORE_TC_APPLICATION,
    names::BEFORE_LOCK_QC_BROADCAST,
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
