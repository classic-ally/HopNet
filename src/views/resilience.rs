//! Assembly for the Network Resilience pane.
//!
//! Split into a consensus half and a storage half so the storage side carries
//! no `AppState` dependency and stays portable on its own; only the consensus
//! side needs the in-memory evidence map and the decided-height watch.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use r2d2::PooledConnection;
use r2d2_sqlite::SqliteConnectionManager;

use hopnet_common::db::{FaultToleranceCurvePoint, NodeStorageBaseline};
use hopnet_common::views::{
    ConsensusPanelView, ResilienceLevelBytes, StoragePanelView, UnplacedBucket, UnplacedSeverity,
};

use crate::db::DatabaseError;

const BYTES_PER_GB: f64 = 1024.0 * 1024.0 * 1024.0;

/// How long the DB-derived storage numbers are reused before a rescan.
///
/// Deliberately far longer than the pane's 5s poll. The two halves of this
/// view want different freshness: the consensus panel's headroom and band
/// move on failures and are cheap to read, while these numbers come from a
/// full-table pass over `fragment_hashes` and describe fragment placement,
/// which moves on the placement/repair timescale. The mount's client-side
/// cache (`hopnet-mount/src/vfs.rs`) uses 15s against this same computation
/// for the same reason; this is the node-side equivalent, and it is the
/// cache that actually bounds the work (issue #68).
///
/// `HOPNET_RESILIENCE_TTL_SECS` overrides it; `0` disables caching entirely,
/// which is how the tests exercise the scan itself. A node with a very large
/// `fragment_hashes` may reasonably want this longer than the default.
const STORAGE_TTL_DEFAULT_SECS: u64 = 60;

fn storage_ttl() -> Duration {
    static TTL: std::sync::OnceLock<Duration> = std::sync::OnceLock::new();
    *TTL.get_or_init(|| {
        let secs = std::env::var("HOPNET_RESILIENCE_TTL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(STORAGE_TTL_DEFAULT_SECS);
        Duration::from_secs(secs)
    })
}

/// The expensive, member-filtered half of the storage panel.
///
/// Everything here is derived purely from the database. The live evidence
/// overlay (`unreachable_members`) is deliberately NOT part of it: that is a
/// "can this node be reached right now" answer, and serving a minute-old one
/// would make the pane quietly disagree with the Validator Pool beside it.
#[derive(Clone)]
pub struct StorageParts {
    /// The member view these numbers were computed against. Recorded so a
    /// refresh can be reasoned about after the fact; membership moves on the
    /// ~10-minute metrics grid, so a change lands within one TTL.
    pub member_ids: Vec<i32>,
    pub levels: Vec<(i32, f64)>,
    pub baselines: Vec<NodeStorageBaseline>,
    pub curve: Vec<FaultToleranceCurvePoint>,
    pub unplaced: Vec<(&'static str, f64)>,
}

struct Cached {
    built_at: Instant,
    parts: StorageParts,
}

/// TTL cache in front of the storage scan, shared by every consumer.
///
/// The lock is held ACROSS the refresh on purpose — that is the single-flight.
/// Without it each expiry is a thundering herd and the pool exhaustion returns
/// at one-per-TTL instead of one-per-poll. With it, concurrency is 1 by
/// construction: N waiters produce one scan and then all read its result.
#[derive(Default)]
pub struct ResilienceCache {
    inner: tokio::sync::Mutex<Option<Cached>>,
}

/// Storage numbers for any consumer, rescanning only past the TTL.
///
/// A cache hit touches the pool ZERO times, which is what makes an open pane
/// free rather than merely cheaper.
pub async fn cached_storage_parts(
    app_state: &crate::AppState,
) -> Result<StorageParts, DatabaseError> {
    let mut guard = app_state.resilience.inner.lock().await;

    if let Some(cached) = guard
        .as_ref()
        .filter(|c| c.built_at.elapsed() < storage_ttl())
    {
        return Ok(cached.parts.clone());
    }

    let pool = app_state.db_pool.clone();
    let scanned = tokio::task::spawn_blocking(move || {
        let conn = pool.get().map_err(|_| DatabaseError::LockError)?;
        storage_parts(&conn)
    })
    .await
    .map_err(|_| DatabaseError::ProcessingError)?;

    absorb(scanned, &mut guard, Instant::now())
}

/// What a refresh attempt yields: a good scan replaces the entry, a failed one
/// leaves it untouched and serves whatever was already there.
///
/// Never cache a failure, and prefer stale numbers to invented ones. A failed
/// scan used to degrade into an empty curve, which reads as "capacity zero" —
/// `df` showing a healthy mesh as a full disk. Behind a TTL that lie would
/// outlive the request that produced it. Mirrors what the mount already does
/// on its side of the wire (`hopnet-mount/src/vfs.rs` serves last-known on a
/// transport blip rather than turning `df` into an error).
fn absorb(
    scanned: Result<StorageParts, DatabaseError>,
    entry: &mut Option<Cached>,
    now: Instant,
) -> Result<StorageParts, DatabaseError> {
    match scanned {
        Ok(parts) => {
            *entry = Some(Cached {
                built_at: now,
                parts: parts.clone(),
            });
            Ok(parts)
        }
        Err(e) => match entry.as_ref() {
            Some(cached) => Ok(cached.parts.clone()),
            None => Err(e),
        },
    }
}

/// One scan, one connection: the whole DB-derived storage half.
pub fn storage_parts(
    conn: &PooledConnection<SqliteConnectionManager>,
) -> Result<StorageParts, DatabaseError> {
    use crate::db::resilience;

    // Member view — the `durable` predicate. There is no membership table, so
    // ids are derived in Rust and bound into the query as parameters.
    let member_ids: Vec<i32> = crate::storage_host::substrate_host::storage_view_with_conn(conn)
        .ok()
        .map(|v| v.members.iter().map(|p| p.node_id).collect())
        .unwrap_or_default();

    let levels = resilience::resilience_level_rows(conn, &member_ids)?;
    let unplaced = resilience::unplaced_age_buckets(conn)?;
    let baselines = resilience::get_node_storage_baselines(conn)?;
    // Threshold 0.9 matches admin::routes, which is where this curve came from.
    let curve = resilience::generate_fault_tolerance_curve(baselines.clone(), 0.9);

    Ok(StorageParts {
        member_ids,
        levels,
        baselines,
        curve,
        unplaced,
    })
}

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
    use crate::consensus::evidence::{
        evidence_inputs, live_estimate, seen_age, version_banner_rows,
    };

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

    // The pool split rides the VISIBILITY clock (RFC-025): "reachable"
    // means any authenticated sighting on any class within the deadline —
    // deliberately broader than `live`, which rides the liveness clock.
    // A straggler staging over the compat class reads reachable here
    // while staying dark on `live`; the divergence is the design.
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
        seen_age(view.as_ref(), origin, now) <= deadline
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

    let local_version_code = crate::version::effective_running_code();
    let (version_skew, stranded_peers) = version_banner_rows(
        &snap,
        local_version_code,
        hopnet_comms::alpn::compat_floor(hopnet_comms::alpn::COMPAT_HEAD),
        hopnet_comms::alpn::COMPAT_HEAD,
        now,
    );

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

        version_skew,
        stranded_peers,
        local_version: crate::version::format_code(local_version_code),
    })
}

/// Data Replication panel.
///
/// Takes the cached scan rather than doing one: `parts` is up to `STORAGE_TTL`
/// old. The one number that must NOT be stale — which members are out of
/// contact right now — is computed here, per request, off the connection.
pub fn storage_view(
    app_state: &crate::AppState,
    conn: &PooledConnection<SqliteConnectionManager>,
    parts: &StorageParts,
) -> StoragePanelView {
    let mut observed_levels = Vec::new();
    let mut unrecoverable_gb = 0.0;
    let mut unknown_gb = 0.0;
    for &(level, bytes) in &parts.levels {
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

    let unplaced_buckets = parts
        .unplaced
        .iter()
        .map(|&(label, bytes)| UnplacedBucket {
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
    //
    // Live, never cached: reachability is the fastest-moving number on the
    // panel, and a stale one would show a departed node as in contact.
    let unreachable_members = unreachable_member_count(app_state, conn, &parts.member_ids);

    StoragePanelView {
        curve: parts.curve.clone(),
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

/// Shares the pane's cached scan, so `df` and the pane cannot disagree and a
/// file manager polling statfs costs nothing between rescans.
pub async fn mount_statfs_bytes(app_state: &crate::AppState) -> Result<(u64, u64), DatabaseError> {
    Ok(statfs_from_parts(&cached_storage_parts(app_state).await?))
}

/// Pure projection of a scan onto the two numbers `df` wants.
pub fn statfs_from_parts(parts: &StorageParts) -> (u64, u64) {
    let used: f64 = parts
        .levels
        .iter()
        .filter(|(level, _)| *level >= 0)
        .map(|(_, bytes)| bytes)
        .sum();
    let total_gb = crate::db::resilience::capacity_at_tolerance(&parts.curve, STATFS_MIN_TOLERANCE);

    ((total_gb * BYTES_PER_GB) as u64, used as u64)
}

/// Count members the evidence layer cannot currently SEE (RFC-025: the
/// visibility clock — a member serving compat traffic counts as
/// reachable here even while dark on the liveness clock).
fn unreachable_member_count(
    app_state: &crate::AppState,
    conn: &PooledConnection<SqliteConnectionManager>,
    member_ids: &[i32],
) -> u32 {
    use crate::consensus::evidence::{evidence_inputs, live_estimate, seen_age};

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
            seen_age(view.as_ref(), origin, now) > deadline
        })
        .count() as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Three same-sized nodes with a storage metric each — the minimum for a
    /// non-empty capacity curve. `max_size(1)` is what makes this a test of the
    /// connection budget rather than of the SQL, and `memory()` requires it
    /// anyway: each connection would otherwise get its own database.
    fn one_connection_pool() -> r2d2::Pool<SqliteConnectionManager> {
        let pool = r2d2::Pool::builder()
            .max_size(1)
            .connection_customizer(Box::new(crate::db::shared::SqliteInitializer))
            .build(SqliteConnectionManager::memory())
            .expect("pool");
        let conn = pool.get().expect("conn");
        crate::db::chains::install(&conn).expect("schema");
        conn.execute_batch(
            "
            INSERT INTO users (user_id, username, pubkey, x25519_pubkey, encrypted_privkey, key_salt)
                VALUES (1, 'alice', x'00', x'00', x'00', x'00');
            INSERT INTO nodes (node_id, name, owner, pubkey) VALUES
                (1, 'node-1', 1, x'01'), (2, 'node-2', 1, x'02'), (3, 'node-3', 1, x'03');
            INSERT INTO metrics (from_node, to_node, start_time, height, available,
                                 storage_total_gb, storage_used_gb) VALUES
                (1, 1, '2026-01-01T00:00:00Z', 1, 1, 1000, 100),
                (1, 2, '2026-01-01T00:00:00Z', 1, 1, 1000, 100),
                (1, 3, '2026-01-01T00:00:00Z', 1, 1, 1000, 100);
            ",
        )
        .expect("fixture");
        pool
    }

    // Impact: the capacity curve used to come from a SECOND pool checkout taken
    // while the first was still held. Under pool pressure that checkout timed
    // out, and the error was swallowed into an empty curve — so statfs reported
    // zero total bytes and `df` rendered a healthy mesh as a full filesystem,
    // silently. One connection per scan is what prevents it (issue #68).
    // Should: report a non-zero capacity total from a single-connection pool.
    #[test]
    fn one_scan_needs_only_one_pool_connection() {
        let pool = one_connection_pool();
        let conn = pool.get().expect("conn");

        let parts = storage_parts(&conn).expect("scan");
        let (total_bytes, _used_bytes) = statfs_from_parts(&parts);

        assert!(
            total_bytes > 0,
            "capacity collapsed to zero — a second connection was taken"
        );
    }

    fn parts_reporting(user_data_gb: f64) -> StorageParts {
        StorageParts {
            member_ids: vec![1, 2, 3],
            levels: vec![(2, 4096.0)],
            baselines: vec![],
            curve: vec![FaultToleranceCurvePoint {
                user_data_gb,
                active_nodes: 3,
                nodes_can_fail: 0,
                participating_nodes: vec![],
            }],
            unplaced: vec![],
        }
    }

    // Impact: a failed scan degrades into an empty curve, which reads as
    // "capacity zero" — `df` showing a healthy mesh as full. Behind a TTL the
    // lie would outlive the request that produced it, so a failure must never
    // land in the cache.
    // Should: serve the last good numbers when a rescan fails.
    // Should not: overwrite the cached entry with the failure.
    #[test]
    fn a_failed_rescan_serves_the_last_good_numbers() {
        let good = parts_reporting(800.0);
        let mut entry = Some(Cached {
            built_at: Instant::now(),
            parts: good.clone(),
        });

        let served = absorb(Err(DatabaseError::LockError), &mut entry, Instant::now())
            .expect("stale beats nothing");

        assert_eq!(statfs_from_parts(&served), statfs_from_parts(&good));
        assert_eq!(
            entry.as_ref().map(|c| statfs_from_parts(&c.parts)),
            Some(statfs_from_parts(&good)),
            "the failure must not have replaced the entry"
        );
    }

    // Should: surface the error when a scan fails and nothing was ever cached.
    #[test]
    fn a_failed_first_scan_has_nothing_to_fall_back_to() {
        let mut entry = None;
        assert!(absorb(Err(DatabaseError::LockError), &mut entry, Instant::now()).is_err());
        assert!(entry.is_none());
    }

    // Should: replace the cached entry with the result of a successful rescan.
    #[test]
    fn a_good_rescan_replaces_the_entry() {
        let mut entry = Some(Cached {
            built_at: Instant::now(),
            parts: parts_reporting(800.0),
        });

        let fresh = parts_reporting(1600.0);
        let served = absorb(Ok(fresh.clone()), &mut entry, Instant::now()).expect("fresh");

        assert_eq!(statfs_from_parts(&served), statfs_from_parts(&fresh));
        assert_eq!(
            entry.as_ref().map(|c| statfs_from_parts(&c.parts)),
            Some(statfs_from_parts(&fresh)),
        );
    }
}
