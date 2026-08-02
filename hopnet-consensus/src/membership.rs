//! Membership policy math (RFC-CONSENSUS-002 S2). Pure functions over
//! `QuorumProfile::quorum`/`f_eq` — no DB, no clock. Guard shapes are
//! normative per `spec/validator_membership.qnt` (`membership_policy`
//! module); the drift-guard tests below pin this code to the model's
//! verified tables.
//!
//! Consumers (S4 vote-out, S5 seating) evaluate these at
//! `ValidationOrigin::Live` only — subjective votes, never replayed
//! deterministically. The profile parameter comes from the committed
//! `consensus_meta` `quorum_profile` until the AUTO composite lands (S6).

use std::time::Duration;

use crate::config::QuorumProfile;
use crate::validators::DepartureKind;

/// Attestation floor: probe attempts fired since last contact before
/// dark(X) may be attested (spec: Evidence & validation — a window you
/// have not probed twice you cannot attest).
pub const ATTESTATION_PROBE_FLOOR: u32 = 2;

/// Batch licence: the model proved gaining batches of ≤ 5 exist
/// everywhere on the composite (batchLemmaTest) — larger batches are
/// never needed and blow up validation.
pub const B_MAX: usize = 5;

/// The activation catch-up gate (spec Constants), unchanged value —
/// now checked evidence-side (last_known_height vs the pending height).
pub const CATCH_UP_TOLERANCE: u64 = 10;

// --- Policy keys (hopnet_consensus_policy rows; setup docs reference) ---
pub const KEY_PROBE_BASE: &str = "probe_base";
pub const KEY_GRACE: &str = "grace";
pub const KEY_S_FULL: &str = "s_full";
pub const KEY_P_PROVE: &str = "p_prove";

/// RFC-CONSENSUS-001 Constants. Stored values are integer seconds;
/// everything derived (T_probe, T_unresponsive, T_out, S_floor) is
/// computed, never stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsensusPolicy {
    /// B: the probe ladder base (cliff B, fast 2B, lazy 4B).
    pub probe_base: Duration,
    /// g: probe response window; drives the status-ping RPC timeout.
    pub grace: Duration,
    /// Full admission span (exposed seatings; comfortable headroom).
    pub s_full: Duration,
    /// In-seat survival before a member is proven (ceiling cushion).
    pub p_prove: Duration,
}

impl Default for ConsensusPolicy {
    fn default() -> Self {
        Self {
            probe_base: Duration::from_secs(30),
            grace: Duration::from_secs(5),
            s_full: Duration::from_secs(1800),
            p_prove: Duration::from_secs(1800),
        }
    }
}

impl ConsensusPolicy {
    /// Resolve from replicated rows: positive integer seconds per key;
    /// malformed or zero values fall back to the key's default; unknown
    /// keys are ignored (forward compatibility).
    pub fn from_rows(rows: &[(String, String)]) -> Self {
        let mut policy = Self::default();
        for (key, value) in rows {
            let parsed = value.parse::<u64>().ok().filter(|v| *v > 0);
            let secs = match parsed {
                Some(v) => Duration::from_secs(v),
                None => continue,
            };
            match key.as_str() {
                KEY_PROBE_BASE => policy.probe_base = secs,
                KEY_GRACE => policy.grace = secs,
                KEY_S_FULL => policy.s_full = secs,
                KEY_P_PROVE => policy.p_prove = secs,
                _ => {}
            }
        }
        policy
    }

    /// T_probe(band): the doubling ladder on B (spec Constants table).
    pub fn t_probe(&self, band: Band) -> Duration {
        match band {
            Band::Cliff => self.probe_base,
            Band::Fast => self.probe_base * 2,
            Band::Lazy => self.probe_base * 4,
        }
    }

    /// Age at which X leaves the observer's live estimate — suspicion
    /// attaches to the unanswered probe, never the deadline.
    pub fn t_unresponsive(&self, band: Band) -> Duration {
        self.t_probe(band) + self.grace
    }

    /// Removal window: two probed misses, the second with its grace
    /// elapsed before the boundary (the pinned ×2 floor).
    pub fn t_out(&self, band: Band) -> Duration {
        self.t_probe(band) * 2 + self.grace
    }

    /// S_floor = one probe cycle of the CURRENT band (only consulted at
    /// H ≤ 1, where the band is Fast or Cliff).
    pub fn s_floor(&self, band: Band) -> Duration {
        self.t_probe(band)
    }

    /// Required bright span for one candidate (model reqSpan; clause
    /// order is normative — exposure overrides the voluntary exemption).
    pub fn req_span(
        &self,
        exposed: bool,
        last_departure: Option<DepartureKind>,
        headroom: i64,
    ) -> Duration {
        if exposed {
            return self.s_full;
        }
        if last_departure == Some(DepartureKind::Voluntary) {
            return Duration::ZERO;
        }
        if headroom <= 1 {
            return self.s_floor(band(headroom));
        }
        self.s_full
    }
}

/// Headroom band (RFC-CONSENSUS-001 Headroom schedule). Headroom is
/// signed: live − quorum(v) goes negative when stalled; stalled maps to
/// Cliff (maximal urgency — though nothing commits there anyway).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Band {
    Lazy,
    Fast,
    Cliff,
}

pub fn band(headroom: i64) -> Band {
    if headroom >= 2 {
        Band::Lazy
    } else if headroom == 1 {
        Band::Fast
    } else {
        Band::Cliff
    }
}

/// ΔH of seating b live members at set size v:
/// b − (quorum(v+b) − quorum(v)). Never underflows — the quorum slope is
/// ≤ 1 per step in every profile (drift-guarded below).
pub fn delta_h(profile: QuorumProfile, v: u64, b: u64) -> u64 {
    let inflation = profile.quorum(v + b) - profile.quorum(v);
    debug_assert!(inflation <= b, "quorum slope exceeded batch size");
    b - inflation
}

/// Borrowed quorum: the portion of a seating not covered by its own
/// headroom gain (e = b − ΔH).
pub fn exposure(profile: QuorumProfile, v: u64, b: u64) -> u64 {
    b - delta_h(profile, v, b)
}

/// Seating-rule posture clause: strict gain, or a lateral that buys
/// equivocation tolerance (the V_bft crossing — vacuous under pinned
/// profiles until the AUTO composite lands at S6).
pub fn posture_ok(profile: QuorumProfile, v: u64, b: u64) -> bool {
    let dh = delta_h(profile, v, b);
    dh >= 1 || (dh == 0 && profile.f_eq(v + b) > profile.f_eq(v))
}

/// Proven-quorum ceiling with the zero-tolerance waiver (model ceilingOk,
/// CEILING_MODE 0): a seating may inflate quorum only within the proven
/// cushion — unproven members are never load-bearing. `proven_live` =
/// |alive ∩ proven| per the approver's own evidence plus committed seat
/// ages (supplied by S3/S4).
pub fn ceiling_ok(profile: QuorumProfile, v: u64, b: u64, proven_live: u64) -> bool {
    let q_v = profile.quorum(v);
    // Zero-tolerance waiver: a set that already stalls on any single
    // death has no tolerance to protect — growth bets on the batch
    // (full S_min still applies via req_span's exposure clause).
    if v - q_v == 0 {
        return true;
    }
    let inflation = profile.quorum(v + b) - q_v;
    inflation <= proven_live.saturating_sub(q_v)
}

#[cfg(test)]
mod tests {
    use super::*;

    const PROFILES: [QuorumProfile; 2] = [QuorumProfile::Bft, QuorumProfile::Majority];

    // Should: from_rows resolve per key with defaults for absent,
    // malformed, zero, and unknown entries.
    // Impact: genesis-seeded policy is the orchestrator test path — a
    // silently-dropped row would run tests at production timescales.
    #[test]
    fn from_rows_edge_cases() {
        assert_eq!(ConsensusPolicy::from_rows(&[]), ConsensusPolicy::default());

        let rows = vec![
            ("probe_base".to_string(), "2".to_string()),
            ("grace".to_string(), "abc".to_string()), // malformed
            ("s_full".to_string(), "0".to_string()),  // zero
            ("mystery".to_string(), "9".to_string()), // unknown
        ];
        let p = ConsensusPolicy::from_rows(&rows);
        assert_eq!(p.probe_base, Duration::from_secs(2));
        assert_eq!(p.grace, Duration::from_secs(5)); // default kept
        assert_eq!(p.s_full, Duration::from_secs(1800)); // default kept
        assert_eq!(p.p_prove, Duration::from_secs(1800));
    }

    // Should: single-seat gain steps match the model's parity patterns —
    // BFT gains iff v+1 ≡ 1 (mod 3), majority iff v+1 odd — and ΔH for a
    // single seat is always 0 or 1 (parityLemmaTest).
    // Impact: the whole seating policy keys off these parities.
    #[test]
    fn parity_drift_guard() {
        for v in 1..=30u64 {
            let bft_gain = u64::from((v + 1) % 3 == 1);
            let maj_gain = u64::from((v + 1) % 2 == 1);
            assert_eq!(delta_h(QuorumProfile::Bft, v, 1), bft_gain, "bft v={v}");
            assert_eq!(
                delta_h(QuorumProfile::Majority, v, 1),
                maj_gain,
                "maj v={v}"
            );
            for p in PROFILES {
                assert!(delta_h(p, v, 1) <= 1);
            }
        }
    }

    // Should: removals gain headroom exactly where admissions are lateral
    // (complementaryParityLemmaTest).
    #[test]
    fn complementary_parity() {
        for v in 2..=30u64 {
            for p in PROFILES {
                let removal_gains = p.quorum(v) - p.quorum(v - 1) == 1;
                let admission_lateral = delta_h(p, v - 1, 1) == 0;
                assert_eq!(removal_gains, admission_lateral, "{p:?} v={v}");
            }
        }
    }

    // Should: any batch of three gain exactly one under BFT, any batch of
    // two under majority; ΔH monotone non-decreasing in batch size
    // (batchLemmaTest).
    #[test]
    fn batch_drift_guard() {
        for v in 1..=27u64 {
            assert_eq!(delta_h(QuorumProfile::Bft, v, 3), 1, "bft v={v}");
        }
        for v in 1..=28u64 {
            assert_eq!(delta_h(QuorumProfile::Majority, v, 2), 1, "maj v={v}");
        }
        for v in 1..=25u64 {
            for b in 1..=4u64 {
                for p in PROFILES {
                    assert!(delta_h(p, v, b + 1) >= delta_h(p, v, b));
                }
            }
        }
    }

    // Should: gain-step seats be exposure-free (quorum flat — the free
    // option), and exposure = b − ΔH always (exposureLemmaTest).
    #[test]
    fn exposure_identity() {
        for v in 1..=29u64 {
            for p in PROFILES {
                if delta_h(p, v, 1) == 1 {
                    assert_eq!(p.quorum(v + 1), p.quorum(v), "{p:?} v={v}");
                }
                for b in 1..=5u64 {
                    assert_eq!(exposure(p, v, b), b - delta_h(p, v, b));
                }
            }
        }
    }

    // Should: the posture lateral clause never fire under pinned profiles
    // (BFT's f_eq-gaining steps coincide with H-gaining steps; majority's
    // f_eq is constant zero) — it exists for the S6 AUTO crossing
    // (postureLemmaTest).
    #[test]
    fn posture_pure_profiles() {
        for v in 1..=29u64 {
            let p = QuorumProfile::Bft;
            if p.f_eq(v + 1) > p.f_eq(v) {
                assert_eq!(delta_h(p, v, 1), 1, "bft f_eq gain must be H gain, v={v}");
            }
            let m = QuorumProfile::Majority;
            assert_eq!(posture_ok(m, v, 1), delta_h(m, v, 1) == 1, "maj v={v}");
        }
    }

    // Should: f_eq match the model's FEQ tables (q_tables, v = 1..15).
    #[test]
    fn f_eq_table_drift() {
        let bft = [0u64, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4];
        for (i, expect) in bft.iter().enumerate() {
            let v = i as u64 + 1;
            assert_eq!(QuorumProfile::Bft.f_eq(v), *expect, "bft v={v}");
            assert_eq!(QuorumProfile::Majority.f_eq(v), 0, "maj v={v}");
        }
    }

    // Should: quorum match the model's literal tables (q_tables, v=1..15).
    #[test]
    fn quorum_table_drift() {
        let maj = [1u64, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8];
        let bft = [1u64, 2, 3, 3, 4, 5, 5, 6, 7, 7, 8, 9, 9, 10, 11];
        for (i, (m, b)) in maj.iter().zip(bft.iter()).enumerate() {
            let v = i as u64 + 1;
            assert_eq!(QuorumProfile::Majority.quorum(v), *m, "maj v={v}");
            assert_eq!(QuorumProfile::Bft.quorum(v), *b, "bft v={v}");
        }
    }

    // Should: the AUTO composite math the guards consume — the four
    // consecutive lateral steps at the seam (v=5..8), and posture admitting
    // ONLY the V_bft crossing (v=6->7, f_eq 0->2) among them.
    // Impact: near-seam seating (plan_seating_batch) and every guard under
    // the AUTO default.
    #[test]
    fn auto_composite_drift() {
        let a = QuorumProfile::Auto;
        // Four laterals in a row, then a gain.
        for v in 5..=8 {
            assert_eq!(delta_h(a, v, 1), 0, "v={v} should be lateral");
        }
        assert_eq!(delta_h(a, 9, 1), 1, "v=9 gains");
        // Posture admits only the crossing among the laterals.
        assert!(posture_ok(a, 6, 1), "V_bft crossing seats (f_eq 0->2)");
        assert!(!posture_ok(a, 5, 1), "pre-seam lateral refused");
        assert!(!posture_ok(a, 7, 1), "post-seam lateral refused");
        assert!(!posture_ok(a, 8, 1), "post-seam lateral refused");
        // A batch of 3 gains at the seam (forms/crosses).
        assert!(delta_h(a, 4, 3) >= 1);
    }

    // Should: the proven-quorum ceiling refuse a stacked exposed batch
    // while the first is unproven and unlock once it proves — the model's
    // compositionRefusedThenProvenTest (cfg_maj7), which is exactly the
    // counterexample that killed the per-batch e ≤ H rule.
    // Impact: the composition stall (two legal batches jointly dying)
    // is unreachable under this rule.
    #[test]
    fn ceiling_refused_then_proven() {
        let m = QuorumProfile::Majority;
        // v=3, all proven: cushion = 3 − q(3)=2 → 1; batch{4,5} inflation
        // q(5)−q(3) = 3−2 = 1 ≤ 1: allowed.
        assert!(ceiling_ok(m, 3, 2, 3));
        // v=5 with the first batch unproven (proven_live still 3):
        // inflation q(7)−q(5) = 4−3 = 1 > cushion 3−3 = 0: refused.
        assert!(!ceiling_ok(m, 5, 2, 3));
        // First batch proves (proven_live 5): cushion 2 ≥ 1: unlocked.
        assert!(ceiling_ok(m, 5, 2, 5));
    }

    // Should: refuse quorum inflation at the cliff under a pinned profile
    // (the exposure-ceiling half of negCeilingCliffCrossingTest).
    #[test]
    fn ceiling_cliff_refusal() {
        // BFT v=4 (tol 1, no waiver), one member dark: proven_live 3 =
        // q(4) → cushion 0; single seat inflation q(5)−q(4) = 1: refused.
        assert!(!ceiling_ok(QuorumProfile::Bft, 4, 1, 3));
    }

    // Should: waive the ceiling exactly where tol(v) = 0 — arithmetic
    // form, not a size special-case (a zero-tolerance set has nothing to
    // protect; growth from there strictly dominates).
    #[test]
    fn zero_tolerance_waiver() {
        for p in PROFILES {
            for v in 1..=6u64 {
                if v - p.quorum(v) == 0 {
                    for b in 1..=5u64 {
                        assert!(ceiling_ok(p, v, b, 0), "{p:?} v={v} b={b}");
                    }
                }
            }
        }
        // Sanity: the waiver set is {1,2} (majority) and {1,2,3} (BFT).
        let waived =
            |p: QuorumProfile| -> Vec<u64> { (1..=6).filter(|v| v - p.quorum(*v) == 0).collect() };
        assert_eq!(waived(QuorumProfile::Majority), vec![1, 2]);
        assert_eq!(waived(QuorumProfile::Bft), vec![1, 2, 3]);
    }

    // Should: bands split at H ≥ 2 / 1 / ≤ 0 and the default ladder give
    // the spec's wall-clock values — T_out 65 s / 2 m 5 s / 4 m 5 s.
    #[test]
    fn band_window_math() {
        assert_eq!(band(5), Band::Lazy);
        assert_eq!(band(2), Band::Lazy);
        assert_eq!(band(1), Band::Fast);
        assert_eq!(band(0), Band::Cliff);
        assert_eq!(band(-3), Band::Cliff);

        let p = ConsensusPolicy::default();
        assert_eq!(p.t_probe(Band::Cliff), Duration::from_secs(30));
        assert_eq!(p.t_probe(Band::Fast), Duration::from_secs(60));
        assert_eq!(p.t_probe(Band::Lazy), Duration::from_secs(120));
        assert_eq!(p.t_unresponsive(Band::Cliff), Duration::from_secs(35));
        assert_eq!(p.t_out(Band::Cliff), Duration::from_secs(65));
        assert_eq!(p.t_out(Band::Fast), Duration::from_secs(125));
        assert_eq!(p.t_out(Band::Lazy), Duration::from_secs(245));
    }

    // Should: req_span honor the clause order — exposure overrides the
    // voluntary exemption; voluntary leavers pay nothing exposure-free;
    // fresh/voted-out candidates pay the floor at H ≤ 1 and the full span
    // when comfortable.
    #[test]
    fn req_span_clauses() {
        let p = ConsensusPolicy::default();
        let vol = Some(DepartureKind::Voluntary);
        let out = Some(DepartureKind::VotedOut);

        assert_eq!(p.req_span(true, vol, 5), p.s_full);
        assert_eq!(p.req_span(false, vol, 5), Duration::ZERO);
        assert_eq!(p.req_span(false, vol, 0), Duration::ZERO);
        assert_eq!(p.req_span(false, None, 1), p.s_floor(Band::Fast));
        assert_eq!(p.req_span(false, out, 0), p.s_floor(Band::Cliff));
        assert_eq!(p.req_span(false, None, 3), p.s_full);
        assert_eq!(p.req_span(false, out, 3), p.s_full);
    }
}
