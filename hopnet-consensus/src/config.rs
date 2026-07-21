//! Quorum profiles: the mesh's fault model, made explicit.
//!
//! The `QuorumProfile` enum and its pure arithmetic (`quorum`, `f_eq`,
//! `profile_at`, `as_str`, `parse`, `V_BFT`) live in `hopnet-common` so the
//! storage durability watermark can share the exact same fault-budget math
//! without depending on the malachite engine. This module re-exports them and
//! adds the one malachite-coupled piece — `thresholds_for`, which produces the
//! `ThresholdParams` the engine's Driver and every `Verify*Certificate` effect
//! are built with. Because `QuorumProfile` is now a foreign type, that method
//! is provided by the [`MalachiteThresholds`] extension trait (bring it into
//! scope with `use hopnet_consensus::config::MalachiteThresholds`).
//!
//! - **Bft** (n ≥ 3f+1, quorum > 2/3): tolerates f Byzantine validators.
//! - **Majority** (quorum > 1/2): crash-fault profile for trusted meshes;
//!   safety ASSUMES no equivocation.
//! - **Auto**: majority below `V_BFT`, Byzantine at and above.
//!
//! The profile is recorded at genesis (consensus_meta) and must be identical
//! across the mesh.

use malachitebft_core_types::{ThresholdParam, ThresholdParams};

pub use hopnet_common::quorum::{QuorumProfile, V_BFT};

/// The malachite-coupled arm of [`QuorumProfile`]. Kept out of `hopnet-common`
/// (which is malachite-free); brought in via extension trait because the enum
/// is defined there.
pub trait MalachiteThresholds {
    /// Threshold params for a height whose committed set has `v` members
    /// (the per-height version malachite's Driver is built with). Pinned
    /// profiles are `v`-independent.
    fn thresholds_for(&self, v: u64) -> ThresholdParams;
}

impl MalachiteThresholds for QuorumProfile {
    fn thresholds_for(&self, v: u64) -> ThresholdParams {
        match self.profile_at(v) {
            QuorumProfile::Bft => ThresholdParams::default(),
            QuorumProfile::Majority => ThresholdParams {
                quorum: ThresholdParam::new(1, 2),
                honest: ThresholdParam::new(1, 2),
            },
            QuorumProfile::Auto => unreachable!("profile_at never returns Auto"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Should: the closed-form quorum(v) agree with malachite's
    // ThresholdParam::is_met at every mesh size we care about, both
    // profiles — quorum(v) is the smallest count meeting the threshold.
    // Should not: drift if ThresholdParams defaults ever change upstream.
    // Impact: the membership policy's headroom/parity math (RFC-CONSENSUS-002)
    // is computed from quorum(v); disagreement with the engine's actual vote
    // counting would silently corrupt every guard.
    #[test]
    fn quorum_matches_threshold_param() {
        for profile in [
            QuorumProfile::Bft,
            QuorumProfile::Majority,
            QuorumProfile::Auto,
        ] {
            for v in 1..=12u64 {
                // Per-height thresholds (AUTO picks by v).
                let t = profile.thresholds_for(v).quorum;
                let q = profile.quorum(v);
                assert!(t.is_met(q, v), "{profile:?} q={q} v={v} should meet");
                assert!(
                    !t.is_met(q - 1, v),
                    "{profile:?} q-1={} v={v} should not meet",
                    q - 1
                );
            }
        }
    }
}
