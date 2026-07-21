//! Storage membership: decay tiers and the derived watermark
//! (RFC-STORAGE-001 Membership / Watermark; normative model
//! `spec/storage_policy.qnt`).
//!
//! Pure functions only — no DB, no clock. All time arithmetic operates on
//! caller-supplied bucketed availability samples anchored to the newest
//! replicated sample, never the wall clock: every node must derive the same
//! view from the same replicated rows.

use std::collections::HashMap;

use hopnet_common::quorum::QuorumProfile;

/// Default decay tiers in seconds (RFC-STORAGE-001 Decay tiers table):
/// 6 h always-on, 24 h daily driver, 72 h weekday-only, 7 d ceiling.
/// Deliberately no 48 h tier — it would fire Sunday evening on every
/// weekend-idle machine. Values are mesh policy config
/// (`hopnet_storage_policy`); this is the code default.
pub const DEFAULT_DECAY_TIERS: [i64; 4] = [
    6 * 3600,
    24 * 3600,
    72 * 3600,
    7 * 24 * 3600,
];

/// Spans required before a node's own history outweighs the cold-start
/// tier.
pub const TIER_MIN_HISTORY: usize = 5;

/// Mesh policy config (RFC-STORAGE-002 Configuration): the
/// determinism-bearing tunables, replicated via the genesis-seeded
/// `hopnet_storage_policy` key/value table. Absent or unparsable keys fall
/// back to these code defaults — every node must resolve the same policy
/// from the same rows.
#[derive(Debug, Clone, PartialEq)]
pub struct StoragePolicy {
    pub decay_tiers: Vec<i64>,
    pub b_max: usize,
    pub sigma: usize,
    pub epsilon: usize,
    /// Availability grid bucket width (seconds). Mesh-identical because
    /// membership derivation depends on it; tests seed a tiny step so
    /// decay is exercisable in seconds. Default matches the ~10-min
    /// metrics collection cadence.
    pub availability_step_secs: i64,
}

impl Default for StoragePolicy {
    fn default() -> Self {
        Self {
            decay_tiers: DEFAULT_DECAY_TIERS.to_vec(),
            b_max: 5,
            sigma: 1,
            epsilon: 0,
            availability_step_secs: 600,
        }
    }
}

impl StoragePolicy {
    /// Resolve policy from replicated key/value rows. Keys:
    /// `decay_tiers` (comma-separated seconds, ascending), `burst_cap`,
    /// `reserve_slack`, `climb_back`. Unknown keys are ignored (forward
    /// compatibility); malformed values fall back per key.
    pub fn from_rows(rows: &[(String, String)]) -> Self {
        let mut policy = Self::default();
        for (key, value) in rows {
            match key.as_str() {
                "decay_tiers" => {
                    let tiers: Option<Vec<i64>> =
                        value.split(',').map(|t| t.trim().parse().ok()).collect();
                    if let Some(tiers) = tiers.filter(|t| {
                        !t.is_empty() && t.windows(2).all(|w| w[0] < w[1]) && t[0] > 0
                    }) {
                        policy.decay_tiers = tiers;
                    }
                }
                "burst_cap" => {
                    if let Ok(v) = value.parse() {
                        policy.b_max = v;
                    }
                }
                "reserve_slack" => {
                    if let Ok(v) = value.parse() {
                        policy.sigma = v;
                    }
                }
                "climb_back" => {
                    if let Ok(v) = value.parse() {
                        policy.epsilon = v;
                    }
                }
                "availability_step_secs" => {
                    if let Ok(v) = value.parse::<i64>() {
                        if v > 0 {
                            policy.availability_step_secs = v;
                        }
                    }
                }
                _ => {}
            }
        }
        policy
    }

    pub fn watermark_params(&self) -> WatermarkParams {
        WatermarkParams {
            b_max: self.b_max,
            sigma: self.sigma,
            epsilon: self.epsilon,
            ..WatermarkParams::default()
        }
    }
}

/// Closed offline spans (seconds) from a node's dense availability grid.
///
/// `samples` are `(bucket_start, available)` in ascending bucket order at
/// `step_secs` spacing. Only CLOSED runs count — a trailing offline run is
/// the node's current absence (see [`current_absence`]), not history: it
/// has no duration yet.
pub fn offline_spans(samples: &[(i64, bool)], step_secs: i64) -> Vec<i64> {
    let mut spans = Vec::new();
    let mut run: i64 = 0;
    for &(_, available) in samples {
        if available {
            if run > 0 {
                spans.push(run * step_secs);
                run = 0;
            }
        } else {
            run += 1;
        }
    }
    // Trailing run deliberately dropped (open, not closed).
    spans
}

/// The node's current continuous absence in seconds: the trailing offline
/// run of its grid. Zero if the newest sample saw it available.
///
/// The grid must extend to the global anchor bucket (newest sample across
/// ALL nodes) — a node nobody could measure still gets `available = false`
/// rows from online reporters, so a well-formed grid is dense.
pub fn current_absence(samples: &[(i64, bool)], step_secs: i64) -> i64 {
    let trailing = samples.iter().rev().take_while(|(_, a)| !a).count();
    trailing as i64 * step_secs
}

/// Decay tier for one node: the smallest configured tier strictly above
/// the ~P95 of its closed offline spans; the largest tier is the ceiling.
///
/// Anti-flap is structural, not stateful: P95 over the caller's lookback
/// window is sticky — one long span holds the tier up until it ages out of
/// the window (fast upgrade on new evidence, slow downgrade), which is the
/// policy's deliberate long bias. No previous-tier input, so the
/// derivation stays a pure function of replicated history.
///
/// Cold start (fewer than `min_history` closed spans): at least the
/// second-largest tier (72 h in the default set), blending toward measured
/// as history accumulates — a fresh node's silence is not evidence of
/// reliability.
pub fn derive_tier(closed_spans: &[i64], tiers: &[i64], min_history: usize) -> i64 {
    debug_assert!(!tiers.is_empty() && tiers.windows(2).all(|w| w[0] < w[1]));
    let ceiling = *tiers.last().expect("tiers non-empty");
    let cold = if tiers.len() >= 2 {
        tiers[tiers.len() - 2]
    } else {
        ceiling
    };

    let measured = if closed_spans.is_empty() {
        tiers[0]
    } else {
        let mut sorted = closed_spans.to_vec();
        sorted.sort_unstable();
        // Nearest-rank P95.
        let rank = (sorted.len() * 95).div_ceil(100);
        let p95 = sorted[rank - 1];
        tiers
            .iter()
            .copied()
            .find(|&t| t > p95)
            .unwrap_or(ceiling)
    };

    if closed_spans.len() < min_history {
        measured.max(cold)
    } else {
        measured
    }
}

/// Storage members: registered nodes minus those whose current continuous
/// absence has outlived their decay tier (RFC-STORAGE-001 Membership).
/// Nodes missing from `absence_by_node` are treated as present.
pub fn storage_members(
    nodes: &[i32],
    absence_by_node: &HashMap<i32, i64>,
    tier_by_node: &HashMap<i32, i64>,
    default_tier: i64,
) -> Vec<i32> {
    nodes
        .iter()
        .copied()
        .filter(|n| {
            let absence = absence_by_node.get(n).copied().unwrap_or(0);
            let tier = tier_by_node.get(n).copied().unwrap_or(default_tier);
            absence < tier
        })
        .collect()
}

/// Watermark tunables (RFC-STORAGE-001 Constants). K/N ride the RS
/// geometry; the rest are mesh policy config with these code defaults.
#[derive(Debug, Clone)]
pub struct WatermarkParams {
    pub k: usize,
    pub n: usize,
    /// Largest single non-site burst event (strip/breaker/switch).
    pub b_max: usize,
    /// Reserve variance slack in classes; raise toward the adversarial
    /// ceiling for co-located high-weight cores (load-diverse power-domain
    /// assumption, RFC-STORAGE-001 Watermark).
    pub sigma: usize,
    /// Climb-back cover; ~0 on healthy links (stub this RFC).
    pub epsilon: usize,
}

impl Default for WatermarkParams {
    fn default() -> Self {
        Self {
            k: crate::rs::ORIGINAL_FRAGMENTS_PER_CHUNK,
            n: crate::rs::TOTAL_FRAGMENTS_PER_CHUNK,
            b_max: 5,
            sigma: 1,
            epsilon: 0,
        }
    }
}

/// Derived watermark W(v) for a view of `v` storage members
/// (RFC-STORAGE-001 Watermark):
///
/// ```text
/// W(v)       = K + reserve(v) + ε
/// reserve(v) = min( ⌈B·N/v⌉ + σ ,  advMax(v) )
/// B(v)       = min( v − quorum(profile, v), B_max )
/// advMax(v)  = min(B,f)·c + max(0, B−f)·(c−1),  c = ⌈N/v⌉, f = N − v(c−1)
/// ```
///
/// Meaning: at maximum laziness, storage survives the worst burst the
/// control plane survives, capped at B_max. The fault budget `B(v)` is keyed
/// off the ACTIVE quorum profile (RFC-CONSENSUS-002 AUTO seam) — a mesh
/// running the majority profile (below `V_BFT`) tolerates more loss than BFT,
/// so it must buffer the larger burst. Sizing `B` off BFT unconditionally
/// under-provisions a majority mesh at small `v` (v∈{3,5,6}), where a burst
/// consensus survives could drop live fragments below K = permanent loss.
pub fn watermark(v: usize, profile: QuorumProfile) -> usize {
    watermark_with(v, profile, &WatermarkParams::default())
}

pub fn watermark_with(v: usize, profile: QuorumProfile, p: &WatermarkParams) -> usize {
    if v == 0 {
        return p.k + p.epsilon;
    }
    let quorum = profile.quorum(v as u64) as usize;
    let b = v.saturating_sub(quorum).min(p.b_max);
    let c = p.n.div_ceil(v);
    let f = p.n - v * (c - 1);
    let adv_max = b.min(f) * c + b.saturating_sub(f) * (c - 1);
    let reserve = ((b * p.n).div_ceil(v) + p.sigma).min(adv_max);
    p.k + reserve + p.epsilon
}

/// The durability cliff K + ⌈N/v⌉ — one member from unreconstructable.
/// Logging aid only: at every mesh size the operating watermark fires
/// first, while consensus is still live (RFC-STORAGE-001 Watermark floor).
pub fn watermark_floor(v: usize) -> usize {
    let p = WatermarkParams::default();
    if v == 0 {
        return p.k;
    }
    p.k + p.n.div_ceil(v)
}

/// A node's already-read placement inputs, host-agnostic. The host reads the
/// rows from its DB and maps them to this; [`derive_view`] is then pure.
#[derive(Debug, Clone)]
pub struct ViewNode {
    pub node_id: i32,
    pub pubkey: [u8; 32],
    pub metrics: crate::placement::MetricsRow,
}

/// Derive the decay-tiered storage view from already-read rows — the pure
/// kernel of the host's `storage_view()`. Sans-io: same inputs → same view on
/// every node.
///
/// Crucially, the view is a function of the STORAGE inputs only — the node
/// universe (`nodes`), their availability (`grid_per_node`), the mesh policy,
/// and the active quorum `profile`. The consensus VALIDATOR set is not an
/// input: membership is derived from availability, not from who validates
/// (RFC-STORAGE-001 three-timescale design / RFC-CONSENSUS-002 §"Hysteresis":
/// "validator churn moves zero bytes"). `profile` is the sole consensus-
/// derived input and it touches only the watermark's fault budget, never the
/// member set or placement.
pub fn derive_view(
    height: i32,
    nodes: Vec<ViewNode>,
    grid_per_node: &HashMap<i32, Vec<(i64, bool)>>,
    grid_step_secs: i64,
    policy: &StoragePolicy,
    profile: QuorumProfile,
) -> crate::traits::StorageView {
    let cold_tier = policy.decay_tiers[policy.decay_tiers.len().saturating_sub(2)];
    let mut tiers = HashMap::new();
    let mut absence = HashMap::new();
    for (node_id, samples) in grid_per_node {
        let spans = offline_spans(samples, grid_step_secs);
        tiers.insert(
            *node_id,
            derive_tier(&spans, &policy.decay_tiers, TIER_MIN_HISTORY),
        );
        absence.insert(*node_id, current_absence(samples, grid_step_secs));
    }

    // Node universe = every registered node (unmeasured nodes appear with
    // defaults: absence 0, cold tier).
    let node_ids: Vec<i32> = nodes.iter().map(|n| n.node_id).collect();
    let member_ids = storage_members(&node_ids, &absence, &tiers, cold_tier);
    let online: Vec<i32> = node_ids
        .iter()
        .copied()
        .filter(|n| absence.get(n).copied().unwrap_or(0) == 0)
        .collect();
    let member_set: std::collections::HashSet<i32> = member_ids.iter().copied().collect();
    let watermark = watermark_with(member_ids.len(), profile, &policy.watermark_params());

    let mut members = Vec::with_capacity(member_ids.len());
    let mut weights = HashMap::new();
    let mut rows = Vec::with_capacity(nodes.len());
    for n in nodes {
        weights.insert(n.node_id, crate::placement::quantized_weight(&n.metrics));
        if member_set.contains(&n.node_id) {
            members.push(hopnet_comms::PeerRef {
                node_id: n.node_id,
                pubkey: n.pubkey,
            });
        }
        rows.push(n.metrics);
    }

    crate::traits::StorageView {
        height,
        members,
        tiers,
        weights,
        watermark,
        online,
        metrics: rows,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const H: i64 = 3600;
    const STEP: i64 = 600; // 10-min metrics grid

    fn grid(pattern: &[(bool, i64)]) -> Vec<(i64, bool)> {
        // Expand (available, duration_secs) runs into a dense 10-min grid.
        let mut out = Vec::new();
        let mut t = 0i64;
        for &(available, dur) in pattern {
            let buckets = dur / STEP;
            for _ in 0..buckets {
                out.push((t, available));
                t += STEP;
            }
        }
        out
    }

    // Should: report each closed offline run's duration and drop the
    // trailing open run.
    // Should not: count the current absence as history.
    // Impact: a trailing run counted as a closed span would let a node's
    // ongoing outage retroactively raise its own tier while it is dark.
    #[test]
    fn offline_spans_closed_runs_only() {
        let g = grid(&[
            (true, 4 * H),
            (false, 12 * H),
            (true, 8 * H),
            (false, 2 * H),
            (true, H),
            (false, 5 * H), // trailing, open
        ]);
        let spans = offline_spans(&g, STEP);
        assert_eq!(spans, vec![12 * H, 2 * H]);
        assert_eq!(current_absence(&g, STEP), 5 * H);
    }

    // Should: report zero current absence when the newest bucket saw the
    // node available.
    // Impact: a stale non-zero absence would eject a live node from the
    // member view and trigger spurious repair.
    #[test]
    fn current_absence_zero_when_online() {
        let g = grid(&[(false, 6 * H), (true, 2 * H)]);
        assert_eq!(current_absence(&g, STEP), 0);
    }

    // Should: give an always-on server (short reboot spans) the 6 h tier.
    // Impact: servers going dark are anomalous; a longer tier would delay
    // repair for the most reliable class of node.
    #[test]
    fn tier_server_archetype() {
        let spans: Vec<i64> = vec![STEP * 2; 10]; // ~20-min reboots
        assert_eq!(
            derive_tier(&spans, &DEFAULT_DECAY_TIERS, TIER_MIN_HISTORY),
            6 * H
        );
    }

    // Should: give a daily driver (8–16 h overnight sleeps) the 24 h tier.
    // Should not: decay it mid-sleep.
    // Impact: nightly sleep is the routine absence the 24 h tier exists to
    // clear; a 6 h tier would move responsibility every night.
    #[test]
    fn tier_daily_driver_archetype() {
        let spans: Vec<i64> = (0..10).map(|i| (8 + i % 9) * H).collect(); // 8..16 h
        assert_eq!(
            derive_tier(&spans, &DEFAULT_DECAY_TIERS, TIER_MIN_HISTORY),
            24 * H
        );
    }

    // Should: give a weekday-only machine (63 h weekends in history) the
    // 72 h tier, not 24 h.
    // Impact: the no-48 h property — a weekend-idle machine must clear
    // Fri 18:00 → Mon 09:00 without a false departure every Sunday night.
    #[test]
    fn tier_weekend_machine_archetype() {
        let mut spans: Vec<i64> = vec![12 * H; 20]; // nightly
        spans.extend([63 * H; 4]); // weekends
        assert_eq!(
            derive_tier(&spans, &DEFAULT_DECAY_TIERS, TIER_MIN_HISTORY),
            72 * H
        );
    }

    // Should: cap at the 7 d ceiling even when history exceeds it.
    // Impact: the ceiling bounds the dark-margin window and the
    // sleep-then-vanish pattern; without it one erratic node could make
    // itself immortal in the placement view.
    #[test]
    fn tier_ceiling() {
        let spans: Vec<i64> = vec![10 * 24 * H; 8];
        assert_eq!(
            derive_tier(&spans, &DEFAULT_DECAY_TIERS, TIER_MIN_HISTORY),
            7 * 24 * H
        );
    }

    // Should: hold a thin-history node at the 72 h cold-start tier even
    // when its few spans look server-like, and release to measured once
    // history accumulates.
    // Impact: a fresh node's silence is not evidence of reliability;
    // decaying it at 6 h off two data points would churn placement.
    #[test]
    fn tier_cold_start_blend() {
        let thin: Vec<i64> = vec![STEP; 2];
        assert_eq!(
            derive_tier(&thin, &DEFAULT_DECAY_TIERS, TIER_MIN_HISTORY),
            72 * H
        );
        let grown: Vec<i64> = vec![STEP; TIER_MIN_HISTORY];
        assert_eq!(
            derive_tier(&grown, &DEFAULT_DECAY_TIERS, TIER_MIN_HISTORY),
            6 * H
        );
    }

    // Should: keep the tier raised while a PATTERN of long spans remains
    // in the lookback window and drop it only once they age out — and
    // ignore one isolated fluke (a single outlier is not a pattern; the
    // rare false departure it risks is what the tier SLO budgets for).
    // Impact: this window stickiness IS the anti-flap discipline — no
    // stored tier state to disagree about between nodes.
    #[test]
    fn tier_window_stickiness_anti_flap() {
        let mut pattern: Vec<i64> = vec![10 * H; 18];
        pattern.extend([60 * H; 2]);
        assert_eq!(
            derive_tier(&pattern, &DEFAULT_DECAY_TIERS, TIER_MIN_HISTORY),
            72 * H
        );
        let mut fluke: Vec<i64> = vec![10 * H; 19];
        fluke.push(60 * H);
        assert_eq!(
            derive_tier(&fluke, &DEFAULT_DECAY_TIERS, TIER_MIN_HISTORY),
            24 * H
        );
        let aged_out: Vec<i64> = vec![10 * H; 18];
        assert_eq!(
            derive_tier(&aged_out, &DEFAULT_DECAY_TIERS, TIER_MIN_HISTORY),
            24 * H
        );
    }

    // Should: resolve replicated key/value rows to the policy, ignore
    // unknown keys, and fall back per-key on malformed values.
    // Should not: accept a non-ascending or non-positive tier list.
    // Impact: nodes resolving different policies from the same rows would
    // derive divergent member views and watermarks — silent placement
    // divergence.
    #[test]
    fn policy_from_rows_resolution() {
        let rows = vec![
            ("decay_tiers".to_string(), "60,120,180,240".to_string()),
            ("burst_cap".to_string(), "3".to_string()),
            ("future_knob".to_string(), "whatever".to_string()),
            ("reserve_slack".to_string(), "not a number".to_string()),
        ];
        let p = StoragePolicy::from_rows(&rows);
        assert_eq!(p.decay_tiers, vec![60, 120, 180, 240]);
        assert_eq!(p.b_max, 3);
        assert_eq!(p.sigma, 1); // malformed → default
        assert_eq!(p.epsilon, 0);

        let bad = vec![("decay_tiers".to_string(), "240,120".to_string())];
        assert_eq!(
            StoragePolicy::from_rows(&bad).decay_tiers,
            DEFAULT_DECAY_TIERS.to_vec()
        );
        assert_eq!(StoragePolicy::from_rows(&[]), StoragePolicy::default());
    }

    // Should: drop a node whose current absence outlived its tier, keep
    // one still within tier, and treat unknown absence as present.
    // Impact: dropping too early is repair churn; dropping too late only
    // spends margin-days — the asymmetry the tiers encode.
    #[test]
    fn storage_members_tier_gate() {
        let nodes = [1, 2, 3];
        let absence = HashMap::from([(1, 30 * H), (2, 30 * H)]);
        let tiers = HashMap::from([(1, 24 * H), (2, 72 * H), (3, 24 * H)]);
        let members = storage_members(&nodes, &absence, &tiers, 72 * H);
        assert_eq!(members, vec![2, 3]);
    }

    // Should: reproduce the hand-computed W(v) values at the real
    // K=10/N=30 geometry under the default AUTO profile — majority below
    // V_BFT (so v∈{3,5,6} carry a real reserve), BFT at/above the seam
    // (v≥7 unchanged from the old BFT-only formula).
    // Should not: let a pinned-BFT mesh silently under-buffer at small v —
    // the last assertion pins the gap the active-profile fix closes.
    // Impact: W is the safety/laziness split; a drifted formula silently
    // converts margin into risk at every mesh size at once.
    #[test]
    fn watermark_spot_values() {
        use QuorumProfile::{Auto, Bft};
        // AUTO / majority arm below the seam — B(v) = v − (v/2+1).
        assert_eq!(watermark(3, Auto), 20); // majority B=1, reserve caps at advMax=10
        assert_eq!(watermark(5, Auto), 22); // majority B=2
        assert_eq!(watermark(6, Auto), 20); // majority B=2
        // AUTO / BFT arm at and above the seam — identical to the old formula.
        assert_eq!(watermark(10, Auto), 19); // spec session table value
        assert_eq!(watermark(15, Auto), 18);
        assert_eq!(watermark(30, Auto), 15);
        // Pinned BFT under-buffers below the seam — the durability bug the
        // active-profile fix closes (16 < AUTO's 22 at v=5).
        assert_eq!(watermark(5, Bft), 16);
        assert_eq!(watermark(3, Bft), 10); // BFT B=0: no reserve at all
    }

    // Should: keep the operating watermark at or above the durability
    // cliff wherever the ACTIVE profile tolerates at least one fault
    // (B(v) ≥ 1), so urgency always fires while the control plane is still
    // live. Under AUTO/majority that boundary is v ≥ 3 (majority tolerates
    // one loss at v=3), not v ≥ 4 — the old BFT-only claim understated it.
    // Impact: a W below the floor at fault-tolerant sizes would mark
    // chunks lazy while one member from unreconstructable.
    #[test]
    fn watermark_at_or_above_floor_when_fault_tolerant() {
        for profile in [QuorumProfile::Auto, QuorumProfile::Bft, QuorumProfile::Majority] {
            for v in 1..=30usize {
                let tolerant = v.saturating_sub(profile.quorum(v as u64) as usize) >= 1;
                if !tolerant {
                    continue;
                }
                let w = watermark(v, profile);
                let floor = watermark_floor(v);
                assert!(w >= floor, "{profile:?} v={v}: W={w} < floor={floor}");
            }
        }
    }
}
