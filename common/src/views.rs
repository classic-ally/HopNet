//! View models for the Network Resilience pane.
//!
//! These are deliberately shaped for one frontend consumer rather than as a
//! general API — the pane's two panels, field for field. What keeps that honest
//! is that the route assembling them owns no arithmetic: every number here is
//! produced by an existing domain primitive (`hopnet_common::quorum`,
//! `live_estimate`, `derive_view`, `db::resilience`), so a view model cannot
//! become a second source of truth the way the pane's old TypeScript quorum
//! copy did.

use serde::{Deserialize, Serialize};
use typeshare::typeshare;

use crate::db::FaultToleranceCurvePoint;

/// Whole-pane payload. One request, one consistent snapshot — the panels sit
/// side by side, so fetching them separately would let them disagree about the
/// same node across two round trips.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[typeshare]
pub struct ResiliencePaneView {
    pub consensus: ConsensusPanelView,
    pub storage: StoragePanelView,
}

/// State Machine Replication panel.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[typeshare]
pub struct ConsensusPanelView {
    /// Seated validators.
    pub v: u32,
    /// Subjectively live, from `live_estimate`'s band fixpoint.
    pub live: u32,
    /// `QuorumProfile::quorum(v)` — never recomputed downstream.
    pub quorum: u32,
    /// `live - quorum`. Signed: negative when stalled.
    pub headroom: i32,
    /// `B(v) = v - quorum(v)`, the fault budget at full strength.
    pub fault_budget: u32,

    /// Configured profile from `consensus_meta`: "auto" | "bft" | "majority".
    pub profile_mode: String,
    /// Resolved via `profile_at(v)`. Equals `profile_mode` unless it is "auto".
    pub profile: String,
    /// The AUTO seam constant, so the frontend never hardcodes 7.
    pub v_bft: u32,

    /// "Lazy" | "Fast" | "Cliff" — a pure function of headroom, but sent rather
    /// than derived so its thresholds stay in `membership::band` alone.
    pub band: String,
    #[typeshare(serialized_as = "number")]
    pub t_probe_ms: u64,
    #[typeshare(serialized_as = "number")]
    pub t_out_ms: u64,

    /// Every registered node, split three ways for the Validator Pool bar.
    /// The reachability test is the literal `live_estimate` predicate, so these
    /// cannot drift from `live` above.
    pub total_nodes: u32,
    pub reachable_unseated: u32,
    pub unreachable_unseated: u32,
}

/// Data Replication panel.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[typeshare]
pub struct StoragePanelView {
    /// Ideal fault tolerance as data volume grows, under perfectly even spread.
    pub curve: Vec<FaultToleranceCurvePoint>,

    /// The observed resilience frontier: one entry per distinct tolerance level.
    /// The frontend sorts descending and cumulative-sums to rebuild the curve,
    /// so ordering here is irrelevant.
    pub observed_levels: Vec<ResilienceLevelBytes>,

    /// Fewer than K classes survive on member disks — already lost, and not a
    /// low point on the frontier, so it is reported separately.
    pub unrecoverable_gb: f64,
    /// No attestation at all: an observability gap, not a durability state.
    pub unknown_gb: f64,

    /// Storage members currently out of contact. Their fragments still count
    /// toward the frontier (intact, merely offline), which is what makes the
    /// frontier optimistic about this instant.
    pub unreachable_members: u32,

    /// Unattested volume by age, youngest first.
    pub unattested_buckets: Vec<UnattestedBucket>,
}

/// Raw user bytes sitting at one worst-case tolerance level.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[typeshare]
pub struct ResilienceLevelBytes {
    /// Node failures this data survives, computed adversarially (largest
    /// holders removed first). Uncapped — the frontend plots it against a
    /// curve that runs well past 3.
    pub tolerance: i32,
    /// Raw user plaintext bytes, from `data_blocks.file_size`. NOT
    /// post-erasure-coding, so it shares the curve's x-axis.
    pub raw_gb: f64,
}

/// One age decade of unattested data.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[typeshare]
pub struct UnattestedBucket {
    pub label: String,
    pub gb: f64,
    /// Absent means "explainable as in flight".
    pub severity: Option<UnattestedSeverity>,
}

/// How far past explainable a bucket is. Transient unattested data is normal —
/// only a tail that fails to decay indicates attestation is stuck.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[typeshare]
#[serde(rename_all = "lowercase")]
pub enum UnattestedSeverity {
    Warn,
    Stale,
}
