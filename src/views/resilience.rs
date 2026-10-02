//! Assembly for the Network Resilience pane.
//!
//! Split into a consensus half and a storage half so the storage side carries
//! no `AppState` dependency and stays portable on its own; only the consensus
//! side needs the in-memory evidence map and the decided-height watch.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::watch;

use r2d2::PooledConnection;
use r2d2_sqlite::SqliteConnectionManager;

use hopnet_common::db::{FaultToleranceCurvePoint, NodeStorageBaseline};
use hopnet_common::views::{
    AgeBucketView, AgeSeverity, ConsensusPanelView, EtaTierView, EtaView, LifecycleView,
    NodeVerificationView, PercentilesView, ResilienceLevelBytes, StoragePanelView, TransferView,
    UnplacedBucket, UnplacedSeverity, VerificationView,
};
use hopnet_storage::observe;

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
    /// Fetches this node owes across the in-flight set (S7's pull tier)
    /// and whether the bounded scan was cut short. Behind the TTL with the
    /// other scans because it walks the in-flight set.
    pub owed_pull: (u64, bool),
}

struct Cached {
    built_at: Instant,
    parts: StorageParts,
}

/// What an in-flight refresh publishes to its waiters. `DatabaseError` is not
/// `Clone`, and every consumer maps failure to a 500 anyway.
type Outcome = Result<StorageParts, ()>;

#[derive(Default)]
struct CacheState {
    entry: Option<Cached>,
    inflight: Option<watch::Receiver<Option<Outcome>>>,
}

/// TTL cache in front of the storage scan, shared by every consumer.
///
/// Single-flight without holding a lock across the scan: the refresh runs in
/// its own task, which owns the cache write, and callers wait on it through a
/// watch channel. The refresh used to run inside the request future with the
/// lock held; when a slow scan outlived the mount's 30s statfs timeout, the
/// dropped request released the lock and discarded the finished scan, so the
/// cache never filled and every poll started another concurrent scan.
#[derive(Default)]
pub struct ResilienceCache {
    state: std::sync::Mutex<CacheState>,
}

impl ResilienceCache {
    /// Fresh entry → served. Otherwise a refresh is started (or joined), and
    /// a stale entry is served immediately while it runs. With nothing cached,
    /// or with `ttl` zero, the caller waits for the refresh.
    async fn get_or_refresh<F>(
        self: &Arc<Self>,
        ttl: Duration,
        scan: F,
    ) -> Result<StorageParts, DatabaseError>
    where
        F: FnOnce() -> Result<StorageParts, DatabaseError> + Send + 'static,
    {
        let rx = {
            let mut state = self.state.lock().expect("resilience cache poisoned");
            if let Some(cached) = state.entry.as_ref().filter(|c| c.built_at.elapsed() < ttl) {
                return Ok(cached.parts.clone());
            }
            let rx = match state.inflight.clone() {
                Some(rx) => rx,
                None => self.start_refresh(&mut state, scan),
            };
            if let Some(cached) = state.entry.as_ref().filter(|_| !ttl.is_zero()) {
                return Ok(cached.parts.clone());
            }
            rx
        };
        wait_for_refresh(rx).await
    }

    fn start_refresh<F>(
        self: &Arc<Self>,
        state: &mut CacheState,
        scan: F,
    ) -> watch::Receiver<Option<Outcome>>
    where
        F: FnOnce() -> Result<StorageParts, DatabaseError> + Send + 'static,
    {
        let (tx, rx) = watch::channel(None);
        state.inflight = Some(rx.clone());
        let cache = Arc::clone(self);
        tokio::spawn(async move {
            let scanned = tokio::task::spawn_blocking(scan)
                .await
                .unwrap_or(Err(DatabaseError::ProcessingError));
            let outcome = {
                let mut state = cache.state.lock().expect("resilience cache poisoned");
                state.inflight = None;
                absorb(scanned, &mut state.entry, Instant::now()).map_err(|_| ())
            };
            // No waiters left is fine: the entry above is what the next caller reads.
            let _ = tx.send(Some(outcome));
        });
        rx
    }
}

async fn wait_for_refresh(
    mut rx: watch::Receiver<Option<Outcome>>,
) -> Result<StorageParts, DatabaseError> {
    let outcome = rx
        .wait_for(Option::is_some)
        .await
        .map_err(|_| DatabaseError::ProcessingError)?;
    match outcome.as_ref() {
        Some(Ok(parts)) => Ok(parts.clone()),
        _ => Err(DatabaseError::ProcessingError),
    }
}

/// Storage numbers for any consumer, rescanning only past the TTL.
///
/// A cache hit touches the pool ZERO times, which is what makes an open pane
/// free rather than merely cheaper.
pub async fn cached_storage_parts(
    app_state: &crate::AppState,
) -> Result<StorageParts, DatabaseError> {
    let pool = app_state.db_pool.clone();
    let me = app_state.node_id.get().copied();
    app_state
        .resilience
        .get_or_refresh(storage_ttl(), move || {
            let conn = pool.get().map_err(|_| DatabaseError::LockError)?;
            storage_parts(&conn, me)
        })
        .await
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
    me: Option<i32>,
) -> Result<StorageParts, DatabaseError> {
    use crate::db::resilience;

    // Member view — the `durable` predicate. There is no membership table, so
    // ids are derived in Rust and bound into the query as parameters.
    let member_ids: Vec<i32> = crate::storage_host::substrate_host::storage_view_with_conn(conn)
        .ok()
        .map(|v| v.members.iter().map(|p| p.node_id).collect())
        .unwrap_or_default();

    let counts = resilience::BlockNodeCounts::build(conn)?;
    let levels = resilience::resilience_level_rows(&counts, &member_ids)?;
    let unplaced = resilience::unplaced_age_buckets(conn)?;
    let baselines = resilience::get_node_storage_baselines(&counts)?;
    // Threshold 0.9 matches admin::routes, which is where this curve came from.
    let curve = resilience::generate_fault_tolerance_curve(baselines.clone(), 0.9);
    // S7: the pull tier's owed fetches — the engine's own owed rule over the
    // in-flight set, bounded. Nothing owed before this node has an id.
    let owed_pull = match me {
        Some(me) => observe::owed_pull_fetches(conn, me, observe::ETA_SCAN_LIMIT)
            .map_err(|_| DatabaseError::ProcessingError)?,
        None => (0, false),
    };

    Ok(StorageParts {
        member_ids,
        levels,
        baselines,
        curve,
        unplaced,
        owed_pull,
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
    let (local_floor, local_head) = crate::consensus::evidence::local_window();
    let (version_skew, stranded_peers) =
        version_banner_rows(&snap, local_version_code, local_floor, local_head, now);

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

    // S7: the lifecycle's own predicates, read live on the same connection.
    // These are indexed counts, not the full-table scan behind the TTL, so
    // they can follow the 5s poll; the tick report and the fetch histograms
    // are process state and cost nothing to read.
    let tip = crate::db::consensus::get_current_consensus_height(conn).unwrap_or(0);
    let lifecycle = lifecycle_view(conn, tip);
    let verification = verification_view(conn, tip);
    let transfers = observe::transfers();
    let eta = eta_view(app_state, parts, &transfers);

    StoragePanelView {
        curve: parts.curve.clone(),
        observed_levels,
        unrecoverable_gb,
        unknown_gb,
        unreachable_members,
        unplaced_buckets,
        lifecycle,
        verification,
        transfers: TransferView {
            fetches: transfers.fetches,
            failures: transfers.failures,
            latency_us: percentiles(&transfers.latency_us),
            throughput_bps: percentiles(&transfers.throughput_bps),
        },
        eta,
    }
}

fn percentiles(h: &observe::HistogramSnapshot) -> PercentilesView {
    PercentilesView {
        count: h.count,
        p50: h.p50,
        p90: h.p90,
        p99: h.p99,
        p999: h.p999,
        max: h.max,
    }
}

/// Block Lifecycle card. A read failure yields an empty card rather than
/// failing the whole pane — the durability half is the one that matters.
fn lifecycle_view(conn: &rusqlite::Connection, tip: u64) -> LifecycleView {
    let transition_height = hopnet_storage::lifecycle::latest_transition_height(conn)
        .ok()
        .flatten();
    let counts = observe::lifecycle_counts(conn, transition_height).unwrap_or_default();
    let in_flight_buckets = observe::in_flight_age_buckets(conn, tip)
        .unwrap_or_default()
        .into_iter()
        .map(|b| AgeBucketView {
            label: b.label.to_string(),
            blobs: b.blobs,
            gb: b.bytes as f64 / BYTES_PER_GB,
            severity: b.severity.map(|s| match s {
                observe::AgeSeverity::Warn => AgeSeverity::Warn,
                observe::AgeSeverity::Stale => AgeSeverity::Stale,
            }),
        })
        .collect();
    LifecycleView {
        tip,
        transition_height,
        owed: counts.owed,
        in_flight: counts.in_flight,
        confirmed: counts.confirmed,
        converged: counts.converged(),
        in_flight_buckets,
    }
}

/// Disk Truth card: the confirm evidence rule over the whole inventory,
/// per holder, named where the node is known.
fn verification_view(conn: &rusqlite::Connection, tip: u64) -> VerificationView {
    let window = observe::INFLIGHT_STALE_HEIGHTS;
    let nodes = observe::verification_by_node(conn, tip, window).unwrap_or_default();
    let names: std::collections::HashMap<i32, String> = conn
        .prepare_cached("SELECT node_id, name FROM nodes")
        .and_then(|mut s| {
            s.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
                .collect::<Result<_, _>>()
        })
        .unwrap_or_default();
    let row = |node_id: i32, name: Option<String>, c: &observe::VerificationCounts| {
        NodeVerificationView {
            node_id,
            name,
            fresh: c.fresh,
            stale: c.stale,
            never: c.never,
            suspect: c.suspect,
        }
    };
    VerificationView {
        window,
        mesh: row(-1, None, &observe::VerificationCounts::total(&nodes)),
        nodes: nodes
            .iter()
            .map(|n| row(n.node_id, names.get(&n.node_id).cloned(), &n.counts))
            .collect(),
    }
}

/// Reconciler card, ETA half: the worker's three queues as owed fetches
/// (a rebuild fetches K) at this node's measured median fetch.
fn eta_view(
    app_state: &crate::AppState,
    parts: &StorageParts,
    transfers: &observe::TransferSnapshot,
) -> EtaView {
    let k = hopnet_storage::rs::ORIGINAL_FRAGMENTS_PER_CHUNK as u64;
    let tick = app_state.last_tick.lock().unwrap().clone();
    let owed = observe::OwedFetches {
        urgent: tick.as_ref().map_or(0, |t| t.urgent_chunks_owed as u64 * k),
        pull: parts.owed_pull.0,
        lazy: tick.as_ref().map_or(0, |t| t.lazy_chunks_owed as u64 * k),
    };
    let p50_fetch_us = (transfers.fetches > 0).then_some(transfers.latency_us.p50);
    let etas = owed.etas(p50_fetch_us, transfers.wall_us_per_fetch);
    EtaView {
        p50_fetch_us,
        wall_us_per_fetch: transfers.wall_us_per_fetch,
        tiers: ["urgent", "pull", "lazy"]
            .into_iter()
            .map(|tier| {
                let (owed_fetches, eta_secs) = etas[tier];
                EtaTierView {
                    tier: tier.to_string(),
                    owed_fetches,
                    eta_secs,
                }
            })
            .collect(),
        partial: parts.owed_pull.1,
        tick_at: tick.map(|t| t.at),
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

        let parts = storage_parts(&conn, Some(1)).expect("scan");
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
            owed_pull: (0, false),
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

    // Should: leave no scan tables on a pooled connection after a scan.
    // Should: scan again on the same connection.
    #[test]
    fn a_scan_cleans_up_after_itself_on_a_reused_connection() {
        let pool = one_connection_pool();
        let conn = pool.get().expect("conn");

        storage_parts(&conn, None).expect("first scan");
        storage_parts(&conn, None).expect("second scan on the reused connection");

        let leftover: i64 = conn
            .query_row("SELECT COUNT(*) FROM sqlite_temp_master", [], |r| r.get(0))
            .expect("temp schema");
        assert_eq!(leftover, 0);
    }

    fn counted_scan(
        scans: &Arc<std::sync::atomic::AtomicUsize>,
        user_data_gb: f64,
        takes: Duration,
    ) -> impl FnOnce() -> Result<StorageParts, DatabaseError> + Send + 'static {
        let scans = Arc::clone(scans);
        move || {
            scans.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            std::thread::sleep(takes);
            Ok(parts_reporting(user_data_gb))
        }
    }

    // Impact: the mount's statfs gave up at 30s while the scan took longer;
    // the dropped request discarded the finished scan, the cache never
    // filled, and every poll started another — four concurrent full-table
    // scans pinning a node at ~400% CPU.
    // Should: keep a refresh running after its caller gives up, and serve
    // its result to the next caller.
    // Should not: start a second scan while the first is still in flight.
    #[tokio::test]
    async fn a_cancelled_caller_does_not_lose_the_refresh() {
        let cache = Arc::new(ResilienceCache::default());
        let scans = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let ttl = Duration::from_secs(60);
        let slow = Duration::from_millis(200);

        let gave_up = tokio::time::timeout(
            Duration::from_millis(20),
            cache.get_or_refresh(ttl, counted_scan(&scans, 800.0, slow)),
        )
        .await;
        assert!(gave_up.is_err(), "caller should have timed out first");

        let served = cache
            .get_or_refresh(ttl, counted_scan(&scans, 1600.0, slow))
            .await
            .expect("joined refresh");

        assert_eq!(
            statfs_from_parts(&served),
            statfs_from_parts(&parts_reporting(800.0))
        );
        assert_eq!(scans.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    // Should: answer a stale read immediately with the cached numbers while
    // a refresh runs, then serve the refreshed numbers once it lands.
    #[tokio::test]
    async fn a_stale_entry_is_served_while_the_refresh_runs() {
        let cache = Arc::new(ResilienceCache::default());
        let scans = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        cache
            .get_or_refresh(
                Duration::from_secs(60),
                counted_scan(&scans, 800.0, Duration::ZERO),
            )
            .await
            .expect("first scan");

        let asked = Instant::now();
        let stale = cache
            .get_or_refresh(
                Duration::from_nanos(1),
                counted_scan(&scans, 1600.0, Duration::from_millis(200)),
            )
            .await
            .expect("stale");
        assert!(
            asked.elapsed() < Duration::from_millis(100),
            "stale read waited on the scan"
        );
        assert_eq!(
            statfs_from_parts(&stale),
            statfs_from_parts(&parts_reporting(800.0))
        );

        // A zero TTL waits, and joins the refresh already in flight.
        let fresh = cache
            .get_or_refresh(Duration::ZERO, counted_scan(&scans, 3200.0, Duration::ZERO))
            .await
            .expect("fresh");
        assert_eq!(
            statfs_from_parts(&fresh),
            statfs_from_parts(&parts_reporting(1600.0))
        );
        assert_eq!(scans.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    // Should: wait for a fresh scan when caching is disabled, even with an
    // entry cached.
    #[tokio::test]
    async fn a_zero_ttl_always_waits_for_a_fresh_scan() {
        let cache = Arc::new(ResilienceCache::default());
        let scans = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        cache
            .get_or_refresh(Duration::ZERO, counted_scan(&scans, 800.0, Duration::ZERO))
            .await
            .expect("first scan");

        let fresh = cache
            .get_or_refresh(Duration::ZERO, counted_scan(&scans, 1600.0, Duration::ZERO))
            .await
            .expect("second scan");

        assert_eq!(
            statfs_from_parts(&fresh),
            statfs_from_parts(&parts_reporting(1600.0))
        );
    }

    // Impact: the view owns no arithmetic — every number here must be the
    // storage crate's own, so a fixture with known shapes is the drift net.
    // Should: report the lifecycle stages, the converged predicate, the
    // in-flight ages and per-holder verification exactly as observe does,
    // with node names joined where known and a -1 mesh total.
    #[test]
    fn lifecycle_and_verification_views_carry_the_crate_numbers() {
        let pool = one_connection_pool();
        let conn = pool.get().expect("conn");
        conn.execute_batch(
            "INSERT INTO data_blocks (id, file_hash, fragment_count, added_bytes, placement_height, file_size, desired_placement_height) VALUES
                ('01890a5d-ac96-774b-b9aa-9f8b24f0c9a1', X'01', 3, 0, 90, 1073741824, 90),
                ('01890a5d-ac96-774b-b9aa-9f8b24f0c9a2', X'02', 3, 0, NULL, 1073741824, 40),
                ('01890a5d-ac96-774b-b9aa-9f8b24f0c9a3', X'03', 3, 0, 10, 1073741824, 10);
             INSERT INTO fragment_inventory (fragment_hash, node_id, verified_height, suspect) VALUES
                (X'A1', 1, 1999, 0), (X'A2', 1, NULL, 0), (X'A3', 2, 5, 0), (X'A4', 2, 1999, 1);",
        )
        .expect("fixture");
        // T = 50: blobs 2 (goal 40, unplaced) and 3 (goal 10) are owed a
        // declaration; blob 1 is quiescent. The age histogram still counts
        // blob 2 at age 60: its goal is unconfirmed whatever the backlog says.
        conn.execute(
            "INSERT INTO storage_view_transitions (height, snapshot) VALUES (50, X'00')",
            [],
        )
        .expect("transition");

        let lifecycle = lifecycle_view(&conn, 100);
        assert_eq!(
            (lifecycle.owed, lifecycle.in_flight, lifecycle.confirmed),
            (2, 0, 1)
        );
        assert!(!lifecycle.converged);
        assert_eq!(lifecycle.transition_height, Some(50));
        let ages: Vec<u64> = lifecycle
            .in_flight_buckets
            .iter()
            .map(|b| b.blobs)
            .collect();
        assert_eq!(ages, vec![0, 1, 0, 0, 0], "age 60 lands in <64");
        assert!((lifecycle.in_flight_buckets[1].gb - 1.0).abs() < 1e-9);

        // Tip 2000 puts the window floor at 976: 1999 is fresh, 5 is stale.
        let v = verification_view(&conn, 2000);
        assert_eq!(v.window, observe::INFLIGHT_STALE_HEIGHTS);
        assert_eq!(
            (
                v.mesh.node_id,
                v.mesh.fresh,
                v.mesh.stale,
                v.mesh.never,
                v.mesh.suspect
            ),
            (-1, 1, 1, 1, 1)
        );
        assert_eq!(v.nodes.len(), 2);
        assert_eq!(v.nodes[0].name.as_deref(), Some("node-1"));
        assert_eq!((v.nodes[0].fresh, v.nodes[0].never), (1, 1));
        assert_eq!((v.nodes[1].stale, v.nodes[1].suspect), (1, 1));
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
