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
}

impl QuorumProfile {
    pub fn thresholds(&self) -> ThresholdParams {
        match self {
            QuorumProfile::Bft => ThresholdParams::default(),
            QuorumProfile::Majority => ThresholdParams {
                quorum: ThresholdParam::new(1, 2),
                honest: ThresholdParam::new(1, 2),
            },
        }
    }

    /// Minimum vote count for quorum over `v` uniformly-weighted validators —
    /// the closed form of `thresholds().quorum.is_met(q, v)` with voting
    /// power 1 per validator (RFC-CONSENSUS-001 `quorum(v)`). The single
    /// source of truth for the membership policy's headroom/parity math.
    pub fn quorum(&self, v: u64) -> u64 {
        match self {
            QuorumProfile::Bft => v * 2 / 3 + 1,  // smallest q with 3q > 2v
            QuorumProfile::Majority => v / 2 + 1, // smallest q with 2q > v
        }
    }

    /// Stable string form for persistence (consensus_meta) and config files.
    pub fn as_str(&self) -> &'static str {
        match self {
            QuorumProfile::Bft => "bft",
            QuorumProfile::Majority => "majority",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "bft" => Some(QuorumProfile::Bft),
            "majority" => Some(QuorumProfile::Majority),
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
        for profile in [QuorumProfile::Bft, QuorumProfile::Majority] {
            let t = profile.thresholds().quorum;
            for v in 1..=12u64 {
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
