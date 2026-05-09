//! Consensus-phase barriers (test-only). Owns the consensus-specific name
//! registry; mechanics + HTTP routes live in `crate::barriers`. Subsystem
//! registration is via `inventory` so adding a new subsystem doesn't touch
//! shared code.

use crate::barriers::{BarrierRegistration, Barriers};

pub mod names {
    pub const BEFORE_BALLOT_DISPATCH: &str = "before_ballot_dispatch";
    pub const AFTER_PROPOSE_QC_BROADCAST: &str = "after_propose_qc_broadcast";
    pub const BEFORE_TC_GST_WAIT: &str = "before_tc_gst_wait";
    pub const BEFORE_TC_APPLICATION: &str = "before_tc_application";
    pub const BEFORE_LOCK_QC_BROADCAST: &str = "before_lock_qc_broadcast";
}

pub const ALL_BARRIER_NAMES: &[&str] = &[
    names::BEFORE_BALLOT_DISPATCH,
    names::AFTER_PROPOSE_QC_BROADCAST,
    names::BEFORE_TC_GST_WAIT,
    names::BEFORE_TC_APPLICATION,
    names::BEFORE_LOCK_QC_BROADCAST,
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
