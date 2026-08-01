//! Assembly for the Network Resilience pane.
//!
//! Split into a consensus half and a storage half so the storage side carries
//! no `AppState` dependency and stays portable on its own; only the consensus
//! side needs the in-memory evidence map and the decided-height watch.

use std::collections::HashSet;

use r2d2::PooledConnection;
use r2d2_sqlite::SqliteConnectionManager;

use hopnet_common::views::{
    ConsensusPanelView, ResilienceLevelBytes, StoragePanelView, UnplacedBucket, UnplacedSeverity,
};

const BYTES_PER_GB: f64 = 1024.0 * 1024.0 * 1024.0;

/// Which age decades count as past explainable. Kept here rather than in the
/// component so the line stays a backend decision; it should eventually be
/// derived from the storage engine's own repair cadence rather than fixed.
fn severity_for(label: &str) -> Option<UnplacedSeverity> {
    match label {
        "1h-1d" => Some(UnplacedSeverity::Warn),
        ">1d" => Some(UnplacedSeverity::Stale),
        _ => None,
    }
}

/// State Machine Replication panel.
///
/// Reads its inputs through `evidence::evidence_inputs` and derives liveness
/// through `evidence::live_estimate`, so this view and `GET /consensus/evidence`
/// cannot disagree about the same mesh.
pub fn consensus_view(
    app_state: &crate::AppState,
    conn: &PooledConnection<SqliteConnectionManager>,
) -> Option<ConsensusPanelView> {
    use crate::consensus::evidence::{contact_age, evidence_inputs, live_estimate};

    let my_id = app_state.get_node_id().ok()?;
    let decided = app_state
        .malachite
        .get()
        .map(|e| *e.decided.borrow())
        .unwrap_or(0);

    let inputs = evidence_inputs(conn, decided);
    let now = std::time::Instant::now();
    let origin = app_state.evidence.origin();
    let snap = app_state.evidence.snapshot();
    let est = live_estimate(
        &snap,
        origin,
        &inputs.policy,
        inputs.profile,
        &inputs.seated,
        my_id,
        now,
    );

    let v = inputs.seated.len() as u64;

    // The literal live_estimate predicate, against the same snapshot and the
    // same resolved band — so the pool split cannot drift from `live` above it.
    let deadline = inputs.policy.t_unresponsive(est.band);
    let seated_set: HashSet<i32> = inputs.seated.iter().copied().collect();
    let in_contact = |id: i32| -> bool {
        if id == my_id {
            return true;
        }
        let view = snap
            .binary_search_by_key(&id, |(k, _)| *k)
            .ok()
            .map(|i| snap[i].1);
        contact_age(view.as_ref(), origin, now) <= deadline
    };

    let (reachable_unseated, unreachable_unseated) = inputs
        .registered
        .iter()
        .filter(|id| !seated_set.contains(id))
        .fold((0u32, 0u32), |(ok, bad), id| {
            if in_contact(*id) {
                (ok + 1, bad)
            } else {
                (ok, bad + 1)
            }
        });

    Some(ConsensusPanelView {
        v: v as u32,
        live: est.live as u32,
        quorum: est.quorum as u32,
        headroom: est.headroom as i32,
        // B(v) = v - quorum(v). Both terms come from QuorumProfile; nothing
        // here reimplements the formula.
        fault_budget: v.saturating_sub(est.quorum) as u32,

        profile_mode: inputs.profile.as_str().to_string(),
        profile: inputs.profile.profile_at(v).as_str().to_string(),
        v_bft: hopnet_common::quorum::V_BFT as u32,

        band: format!("{:?}", est.band),
        t_probe_ms: inputs.policy.t_probe(est.band).as_millis() as u64,
        t_out_ms: inputs.policy.t_out(est.band).as_millis() as u64,

        total_nodes: inputs.registered.len() as u32,
        reachable_unseated,
        unreachable_unseated,
    })
}

/// Data Replication panel.
///
/// `AppState`-free: everything comes off the connection, which is what lets the
/// storage half move independently of the host.
pub fn storage_view(
    app_state: &crate::AppState,
    conn: &PooledConnection<SqliteConnectionManager>,
) -> StoragePanelView {
    use crate::db::resilience;

    // Member view — the `durable` predicate. There is no membership table, so
    // ids are derived in Rust and bound into the query as parameters.
    let view = crate::storage_host::substrate_host::storage_view_with_conn(conn).ok();
    let member_ids: Vec<i32> = view
        .as_ref()
        .map(|v| v.members.iter().map(|p| p.node_id).collect())
        .unwrap_or_default();

    let rows = resilience::resilience_level_rows(conn, &member_ids).unwrap_or_default();

    let mut observed_levels = Vec::new();
    let mut unrecoverable_gb = 0.0;
    let mut unknown_gb = 0.0;
    for (level, bytes) in rows {
        let gb = bytes / BYTES_PER_GB;
        match level {
            -2 => unknown_gb += gb,
            -1 => unrecoverable_gb += gb,
            t => observed_levels.push(ResilienceLevelBytes {
                tolerance: t,
                raw_gb: gb,
            }),
        }
    }

    let unplaced_buckets = resilience::unplaced_age_buckets(conn)
        .unwrap_or_default()
        .into_iter()
        .map(|(label, bytes)| UnplacedBucket {
            label: label.to_string(),
            gb: bytes / BYTES_PER_GB,
            severity: severity_for(label),
        })
        .collect();

    // Storage members out of contact right now. Uses the consensus evidence
    // predicate rather than StorageView.online, which is ~10-minute-grid
    // granular and would disagree with the Validator Pool beside it. The
    // three-timescale rule constrains the control plane — derive_view excludes
    // the validator set because it drives placement — and this is a read-only
    // overlay that moves no bytes.
    let unreachable_members = unreachable_member_count(app_state, conn, &member_ids);

    // Second checkout: get_node_storage_baselines takes an owned connection.
    // Threshold 0.9 matches admin::routes, which is where this curve came from.
    let curve = resilience::get_node_storage_baselines(app_state.db_pool.get())
        .map(|baselines| resilience::generate_fault_tolerance_curve(baselines, 0.9))
        .unwrap_or_default();

    StoragePanelView {
        curve,
        observed_levels,
        unrecoverable_gb,
        unknown_gb,
        unreachable_members,
        unplaced_buckets,
    }
}

/// The mount's statfs numbers (RFC-018 S8): total = user-data capacity
/// while the placement curve still tolerates this many node failures,
/// used = raw bytes observed at tolerance >= 0. Composes the same pieces
/// as `storage_view` (same member predicate, same 0.9 threshold, same
/// level rows) so `df` on a mounted drive and the resilience pane cannot
/// disagree; unrecoverable/unknown bytes are excluded from `used` exactly
/// as the pane excludes them from its consumed figure.
const STATFS_MIN_TOLERANCE: i32 = 2;

pub fn mount_statfs_bytes(
    pool: &r2d2::Pool<SqliteConnectionManager>,
) -> Result<(u64, u64), crate::db::DatabaseError> {
    use crate::db::resilience;

    let conn = pool
        .get()
        .map_err(|_| crate::db::DatabaseError::LockError)?;

    let view = crate::storage_host::substrate_host::storage_view_with_conn(&conn).ok();
    let member_ids: Vec<i32> = view
        .as_ref()
        .map(|v| v.members.iter().map(|p| p.node_id).collect())
        .unwrap_or_default();

    let used: f64 = resilience::resilience_level_rows(&conn, &member_ids)?
        .into_iter()
        .filter(|(level, _)| *level >= 0)
        .map(|(_, bytes)| bytes)
        .sum();

    let curve = resilience::get_node_storage_baselines(pool.get())
        .map(|baselines| resilience::generate_fault_tolerance_curve(baselines, 0.9))
        .unwrap_or_default();
    let total_gb = resilience::capacity_at_tolerance(&curve, STATFS_MIN_TOLERANCE);

    Ok(((total_gb * BYTES_PER_GB) as u64, used as u64))
}

/// Count members the evidence layer cannot currently reach.
fn unreachable_member_count(
    app_state: &crate::AppState,
    conn: &PooledConnection<SqliteConnectionManager>,
    member_ids: &[i32],
) -> u32 {
    use crate::consensus::evidence::{contact_age, evidence_inputs, live_estimate};

    let Ok(my_id) = app_state.get_node_id() else {
        return 0;
    };
    let decided = app_state
        .malachite
        .get()
        .map(|e| *e.decided.borrow())
        .unwrap_or(0);
    let inputs = evidence_inputs(conn, decided);
    let now = std::time::Instant::now();
    let origin = app_state.evidence.origin();
    let snap = app_state.evidence.snapshot();
    let est = live_estimate(
        &snap,
        origin,
        &inputs.policy,
        inputs.profile,
        &inputs.seated,
        my_id,
        now,
    );
    let deadline = inputs.policy.t_unresponsive(est.band);

    member_ids
        .iter()
        .copied()
        .filter(|&id| id != my_id)
        .filter(|&id| {
            let view = snap
                .binary_search_by_key(&id, |(k, _)| *k)
                .ok()
                .map(|i| snap[i].1);
            contact_age(view.as_ref(), origin, now) > deadline
        })
        .count() as u32
}
