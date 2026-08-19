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

    /// Unplaced volume by age, youngest first.
    pub unplaced_buckets: Vec<UnplacedBucket>,
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

/// One age decade of unplaced data.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[typeshare]
pub struct UnplacedBucket {
    pub label: String,
    pub gb: f64,
    /// Absent means "explainable as in flight".
    pub severity: Option<UnplacedSeverity>,
}

/// How far past explainable a bucket is. Transient unplaced data is normal —
/// only a tail that fails to decay indicates distribution is stuck.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[typeshare]
#[serde(rename_all = "lowercase")]
pub enum UnplacedSeverity {
    Warn,
    Stale,
}

/// RFC-019 S3 upgrade-readiness advisory. FACTS ONLY — no rollup: the
/// readiness precondition (every seated validator has the target staged)
/// is the S5 regenesis_start handler's arithmetic, once a target exists.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[typeshare]
pub struct UpgradeReadinessView {
    /// This node's running version (Cargo.toml, CalVer).
    pub running: String,
    /// Committed attestations for every registered node.
    pub mesh: Vec<NodeVersionsView>,
    /// Upstream releases per the provider's last poll, newest first.
    pub available: Vec<AvailableReleaseView>,
    pub provider: ProviderStatusView,
    /// What THIS deployment can do at an upgrade boundary (RFC-021).
    pub activation: ActivationView,
}

/// This node's deployment-declared upgrade capabilities. Currently only
/// nix deployments can stage and activate; every other class parks at an
/// upgrade boundary for its operator — advertised here so the operator
/// learns it before the boundary, not from a parked mesh.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[typeshare]
pub struct ActivationView {
    /// Provider kind ("nix", "git-release").
    pub provider: String,
    /// Whether this deployment can stage bytes (make an upgrade epoch
    /// decidable without running the target first).
    pub can_stage: bool,
    /// Whether it will cross an upgrade boundary unattended.
    pub auto_activate: bool,
}

/// One node's committed version claims.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[typeshare]
pub struct NodeVersionsView {
    pub node_id: i32,
    pub name: String,
    /// None = the node has never attested.
    pub running: Option<String>,
    /// Staged-but-not-running; None until a staging-capable provider
    /// exists.
    pub staged: Option<String>,
}

/// One upstream release the provider reported.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[typeshare]
pub struct AvailableReleaseView {
    pub version: String,
    pub prerelease: bool,
    /// Both this and the running version parse as CalVer and this is
    /// strictly newer — integer compare of the codes, computed by the
    /// version primitive so the view owns no arithmetic.
    pub newer_than_running: bool,
}

/// Outcome of the provider's most recent poll.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[typeshare]
pub struct ProviderStatusView {
    /// None = no poll has run (disabled, pre-setup, or test mode).
    pub name: Option<String>,
    /// ISO-8601.
    pub fetched_at: Option<String>,
    pub error: Option<String>,
}

/// 503 body when the regenesis moratorium refuses a new submission
/// (RFC-019 S5) — the structured "regenesis in progress" contract, so
/// callers know to retry rather than treat the refusal as an error.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[typeshare]
pub struct RegenesisRefusalView {
    /// "moratorium" | "sealed".
    pub phase: String,
    /// The version the next epoch requires, formatted CalVer.
    pub target_version: Option<String>,
    pub message: String,
}

/// GET /views/regenesis-status (RFC-019 S5): the committed boundary
/// phase plus this node's drain observation. Facts only.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[typeshare]
pub struct RegenesisStatusView {
    /// "normal" | "moratorium" | "sealed".
    pub phase: String,
    pub target_version: Option<String>,
    /// Terminal height H once sealed (stringified u64).
    pub seal_height: Option<String>,
    /// This node's pool observation: nothing staged, nothing inflight.
    pub drained: bool,
    /// This database's epoch (stringified u64; "1" for pre-regenesis meshes).
    pub epoch: String,
    /// This epoch's chain id, hex. Operator-facing on purpose: it is the
    /// fingerprint `POST /consensus/regenesis/retrust` requires, read off a
    /// node already known good and passed to the node being recovered.
    /// Public knowledge — every peer in the mesh signs against it.
    pub chain_id: String,
    /// The version this binary effectively runs.
    pub running_version: String,
    /// Sealed for a version this binary does not run: the node is parked
    /// until its operator swaps the binary (RFC-019 S6 boot gate 1).
    pub awaiting_upgrade: bool,
    /// The retained previous-epoch database still exists (rollback
    /// window: until this epoch's first decide).
    pub rollback_retained: bool,
    /// Last boot-gate refusal, if the boundary could not be crossed.
    pub boundary_error: Option<String>,
    /// Epoch-join progress or last failure (RFC-019 S7), while this node
    /// is rebuilding from peers: fetching lineage, downloading the
    /// snapshot, staged and awaiting restart.
    pub epoch_join: Option<String>,
    /// Per-module schema chain positions stamped in this database
    /// (RFC-020 §Version Surfaces), sorted by module. Empty before the
    /// database is stamped.
    pub schema_ordinals: Vec<SchemaOrdinalView>,
}

/// One module's recorded schema chain position (RFC-020): the ordinal of
/// the last migration step this database has applied.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[typeshare]
pub struct SchemaOrdinalView {
    pub module: String,
    pub ordinal: u32,
}
