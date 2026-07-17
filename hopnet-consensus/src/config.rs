//! Quorum profiles: the mesh's fault model, made explicit.
//!
//! The bespoke engine auto-switched between simple-majority and BFT thresholds
//! at n = 6. Here the fault model is an explicit, genesis-fixed, per-mesh
//! parameter — two profiles, two theorems:
//!
//! - **Bft** (n ≥ 3f+1, quorum > 2/3): tolerates f Byzantine validators.
//!   Safety holds even if tolerated validators equivocate.
//! - **Majority** (quorum > 1/2): crash-fault profile for trusted home
//!   meshes. Quorum intersection still holds for majorities, but safety now
//!   ASSUMES no equivocation — only sound where every node key is trusted.
//!   Gives a 2-node mesh decidability (quorum 2) and a 3-node mesh crash
//!   tolerance of 1 (quorum 2).
//!
//! The profile is recorded at genesis (consensus_meta) and must be identical
//! across the mesh; it feeds `Params::threshold_params` and every
//! `Verify*Certificate` effect.

use malachitebft_core_types::{ThresholdParam, ThresholdParams};

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum QuorumProfile {
    /// Byzantine fault tolerance: quorum > 2/3, honest > 1/3 (Tendermint
    /// defaults; the safety proof assumes these).
    Bft,
    /// Crash-fault majority for trusted meshes: quorum > 1/2.
    ///
    /// `honest` (the "at least one honest sender" threshold, used e.g. for
    /// skip-round triggers) is also > 1/2: under the no-equivocation
    /// assumption any single message is honest, so a lower value would be
    /// sound — but keeping it at majority is conservative and only affects
    /// liveness (a lagging trigger), never safety. Revisit if round-skip
    /// latency matters in practice.
    Majority,
    /// AUTO composite (RFC-CONSENSUS-002 S6): majority below `V_BFT`,
    /// Byzantine at and above — the fault model becomes opportunistic
    /// hardening the mesh acquires when the count affords it and sheds
    /// gracefully when it doesn't. Selected per height from the committed
    /// validator-set size; locked to each height for certificate
    /// verification forever.
    Auto,
}

/// AUTO switch point (spec Constants): the smallest v where the Byzantine
/// budget survives one crash AND the crossing is crash-neutral.
pub const V_BFT: u64 = 7;

impl QuorumProfile {
    /// Effective profile at set size `v`: AUTO resolves to Bft/Majority by
    /// the seam; pinned profiles ignore `v`.
    pub fn profile_at(&self, v: u64) -> QuorumProfile {
        match self {
            QuorumProfile::Auto => {
                if v >= V_BFT {
                    QuorumProfile::Bft
                } else {
                    QuorumProfile::Majority
                }
            }
            other => *other,
        }
    }

    /// Threshold params for a height whose committed set has `v` members
    /// (the per-height version malachite's Driver is built with). Pinned
    /// profiles are `v`-independent — identical to the old `thresholds()`.
    pub fn thresholds_for(&self, v: u64) -> ThresholdParams {
        match self.profile_at(v) {
            QuorumProfile::Bft => ThresholdParams::default(),
            QuorumProfile::Majority => ThresholdParams {
                quorum: ThresholdParam::new(1, 2),
                honest: ThresholdParam::new(1, 2),
            },
            QuorumProfile::Auto => unreachable!("profile_at never returns Auto"),
        }
    }

    /// Minimum vote count for quorum over `v` uniformly-weighted validators —
    /// the closed form of `thresholds_for(v).quorum.is_met(q, v)` with voting
    /// power 1 per validator (RFC-CONSENSUS-001 `quorum(v)`). The single
    /// source of truth for the membership policy's headroom/parity math.
    pub fn quorum(&self, v: u64) -> u64 {
        match self.profile_at(v) {
            QuorumProfile::Bft => v * 2 / 3 + 1,  // smallest q with 3q > 2v
            QuorumProfile::Majority => v / 2 + 1, // smallest q with 2q > v
            QuorumProfile::Auto => unreachable!(),
        }
    }

    /// Equivocation tolerance at set size `v` (RFC-CONSENSUS-001
    /// Definitions): Tendermint's f = ⌊(v−1)/3⌋ under BFT, 0 under
    /// majority (its safety proof assumes no equivocation). AUTO selects
    /// per v.
    pub fn f_eq(&self, v: u64) -> u64 {
        match self.profile_at(v) {
            QuorumProfile::Bft => v.saturating_sub(1) / 3,
            QuorumProfile::Majority => 0,
            QuorumProfile::Auto => unreachable!(),
        }
    }

    /// Stable string form for persistence (consensus_meta) and config files.
    pub fn as_str(&self) -> &'static str {
        match self {
            QuorumProfile::Bft => "bft",
            QuorumProfile::Majority => "majority",
            QuorumProfile::Auto => "auto",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "bft" => Some(QuorumProfile::Bft),
            "majority" => Some(QuorumProfile::Majority),
            "auto" => Some(QuorumProfile::Auto),
            _ => None,
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

    // Should: the AUTO composite be majority below V_BFT and BFT at/above,
    // with a crash-neutral, Byzantine-meaningful crossing at 7.
    // Impact: the whole S6 threshold selection.
    #[test]
    fn auto_composite() {
        let a = QuorumProfile::Auto;
        // quorum(Auto, 1..=12).
        let q: Vec<u64> = (1..=12).map(|v| a.quorum(v)).collect();
        assert_eq!(q, vec![1, 2, 2, 3, 3, 4, 5, 6, 7, 7, 8, 9]);
        // The seam: quorum(6)=4 (majority), quorum(7)=5 (BFT).
        assert_eq!(a.profile_at(6), QuorumProfile::Majority);
        assert_eq!(a.profile_at(7), QuorumProfile::Bft);
        // f_eq: 0 below the seam, ⌊(v-1)/3⌋ at/above; f_eq(7)=2.
        assert_eq!((1..=10).map(|v| a.f_eq(v)).collect::<Vec<_>>(),
                   vec![0, 0, 0, 0, 0, 0, 2, 2, 2, 3]);
        // Crash tolerance monotone non-decreasing.
        let tol: Vec<u64> = (2..=10).map(|v| v - a.quorum(v)).collect();
        assert_eq!(tol, vec![0, 1, 1, 2, 2, 2, 2, 2, 3]);
    }
}
