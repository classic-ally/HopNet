//! Takeout/import test barriers. Owns the takeout-specific name registry;
//! mechanics + HTTP routes live in `crate::barriers`. Subsystem registration
//! is via `inventory`.

use crate::barriers::{BarrierRegistration, Barriers};

pub mod names {
    /// Held by the import-resume scenario: pauses the creation walk after
    /// extraction completes (status flipped to Importing, path table seeded)
    /// so the test can stop the owner mid-import and verify resume.
    pub const BEFORE_IMPORT_CREATION_WALK: &str = "before_import_creation_walk";
}

pub const ALL_BARRIER_NAMES: &[&str] = &[names::BEFORE_IMPORT_CREATION_WALK];

pub fn new() -> Barriers {
    Barriers::new(ALL_BARRIER_NAMES)
}

inventory::submit! {
    &BarrierRegistration {
        subsystem: "takeout",
        accessor: |state: &crate::AppState| &state.takeout_runtime.barriers,
        names: ALL_BARRIER_NAMES,
    }
}
