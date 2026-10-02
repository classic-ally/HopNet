//! Observability reads (RFC-STORAGE-003 S7): the pane shows the worker's
//! own queries.
//!
//! Every observable here is a work-list predicate the reconciler already
//! runs — the same indexed columns, the same rules — so the pane and the
//! machinery cannot drift apart. Ages are HEIGHTS, never wall clock:
//! replicated, identical on every node. The one node-local surface is
//! the transfer histograms (this process, since start), in the same shape
//! as the host's commit-latency instrumentation.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

use hdrhistogram::Histogram;
use hopnet_common::height::{height_from_db, height_to_db};
use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};

use crate::lifecycle::{self, ViewSnapshot, ATTESTATION_RECENCY_HEIGHTS};
use crate::store::db_err;
use crate::StorageError;

// ---------------------------------------------------------------------------
// Lifecycle counts and the converged predicate

/// The staleness backlog, the in-flight set and the quiescent rest — the
/// three stages every blob is in exactly one of (disjoint, so they stack
/// to the total). A blob still owed a declaration is counted there
/// whatever its pair says: its goal is about to move.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleCounts {
    /// `desired < T`: declarations still owed by the staleness pass.
    pub owed: u64,
    /// Not owed, and `placement_height IS NULL OR placement_height !=
    /// desired`: a current goal not yet confirmed.
    pub in_flight: u64,
    /// Not owed, and `placement_height = desired`: quiescent.
    pub confirmed: u64,
}

impl LifecycleCounts {
    /// The displayed, checked predicate: no blob below T, nothing in
    /// flight. A quiet mesh is quiet-because-done, not because nobody
    /// looked.
    pub fn converged(&self) -> bool {
        self.owed == 0 && self.in_flight == 0
    }
}

/// One pass over `data_blocks` (one row per blob). `t` is the latest
/// transition height; `None` before the first transition means nothing
/// is owed yet.
pub fn lifecycle_counts(
    conn: &rusqlite::Connection,
    t: Option<u64>,
) -> Result<LifecycleCounts, StorageError> {
    conn.query_row(
        "SELECT
            COALESCE(SUM(CASE WHEN desired_placement_height < ?1 THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN desired_placement_height < ?1 THEN 0
                               WHEN placement_height IS NULL
                                 OR placement_height != desired_placement_height
                               THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN desired_placement_height < ?1 THEN 0
                               WHEN placement_height = desired_placement_height
                               THEN 1 ELSE 0 END), 0)
         FROM data_blocks",
        params![t.map(height_to_db)],
        |row| {
            Ok(LifecycleCounts {
                owed: row.get::<_, i64>(0)? as u64,
                in_flight: row.get::<_, i64>(1)? as u64,
                confirmed: row.get::<_, i64>(2)? as u64,
            })
        },
    )
    .map_err(db_err("count lifecycle stages"))
}

// ---------------------------------------------------------------------------
// In-flight ages

/// Bucket edges for in-flight age (heights since the goal was set):
/// `[0,8) [8,64) [64,256) [256,1024) [1024,∞)`.
pub const INFLIGHT_AGE_EDGES: [u64; 4] = [8, 64, 256, 1024];
/// Labels for the buckets above, youngest first.
pub const INFLIGHT_AGE_LABELS: [&str; 5] = ["<8", "<64", "<256", "<1k", "≥1k"];
/// A goal this old and still unconfirmed is past explainable-as-in-flight:
/// a pull page is 64 blobs per 5-minute tick, so 256 heights of traffic
/// is many ticks.
pub const INFLIGHT_WARN_HEIGHTS: u64 = 256;
/// Older than the attestation window: even a completed pull's evidence
/// would have aged out — the handoff is stalled.
pub const INFLIGHT_STALE_HEIGHTS: u64 = ATTESTATION_RECENCY_HEIGHTS;

/// How far past explainable an age bucket is. Set here, never in a
/// component, so the lines fall where the engine's cadence puts them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AgeSeverity {
    Warn,
    Stale,
}

/// One age bucket of the in-flight set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgeBucket {
    pub label: &'static str,
    pub blobs: u64,
    /// Raw user bytes (`file_size`), the same axis as the durability chart.
    pub bytes: u64,
    pub severity: Option<AgeSeverity>,
}

/// Severity of bucket `i` from its lower edge.
fn bucket_severity(i: usize) -> Option<AgeSeverity> {
    let lower = if i == 0 { 0 } else { INFLIGHT_AGE_EDGES[i - 1] };
    if lower >= INFLIGHT_STALE_HEIGHTS {
        Some(AgeSeverity::Stale)
    } else if lower >= INFLIGHT_WARN_HEIGHTS {
        Some(AgeSeverity::Warn)
    } else {
        None
    }
}

/// The in-flight set bucketed by `tip − desired` — one row, ten sums.
/// A plateau at the right is a stalled handoff.
pub fn in_flight_age_buckets(
    conn: &rusqlite::Connection,
    tip: u64,
) -> Result<Vec<AgeBucket>, StorageError> {
    let [e0, e1, e2, e3] = INFLIGHT_AGE_EDGES.map(|e| e as i64);
    let row: [(i64, i64); 5] = conn
        .query_row(
            "SELECT
                COALESCE(SUM(CASE WHEN age < ?2 THEN 1 ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN age < ?2 THEN bytes ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN age >= ?2 AND age < ?3 THEN 1 ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN age >= ?2 AND age < ?3 THEN bytes ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN age >= ?3 AND age < ?4 THEN 1 ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN age >= ?3 AND age < ?4 THEN bytes ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN age >= ?4 AND age < ?5 THEN 1 ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN age >= ?4 AND age < ?5 THEN bytes ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN age >= ?5 THEN 1 ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN age >= ?5 THEN bytes ELSE 0 END), 0)
             FROM (SELECT MAX(0, ?1 - desired_placement_height) AS age,
                          COALESCE(file_size, 0) AS bytes
                   FROM data_blocks
                   WHERE placement_height IS NULL
                      OR placement_height != desired_placement_height)",
            params![height_to_db(tip), e0, e1, e2, e3],
            |r| {
                Ok([
                    (r.get(0)?, r.get(1)?),
                    (r.get(2)?, r.get(3)?),
                    (r.get(4)?, r.get(5)?),
                    (r.get(6)?, r.get(7)?),
                    (r.get(8)?, r.get(9)?),
                ])
            },
        )
        .map_err(db_err("bucket in-flight ages"))?;
    Ok(row
        .iter()
        .enumerate()
        .map(|(i, &(blobs, bytes))| AgeBucket {
            label: INFLIGHT_AGE_LABELS[i],
            blobs: blobs as u64,
            bytes: bytes as u64,
            severity: bucket_severity(i),
        })
        .collect())
}

// ---------------------------------------------------------------------------
// Verification freshness

/// Inventory rows by how recently the bytes were seen on disk.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerificationCounts {
    /// Disk-verified within the recency window — confirmation evidence.
    pub fresh: u64,
    /// Disk-verified, but longer ago than the window.
    pub stale: u64,
    /// Belief only: never disk-verified since the S5 crossing.
    pub never: u64,
    /// Flagged suspect: treated as missing until re-verified.
    pub suspect: u64,
}

impl VerificationCounts {
    pub fn rows(&self) -> u64 {
        self.fresh + self.stale + self.never + self.suspect
    }

    /// The mesh total: one honest row per node summed.
    pub fn total(nodes: &[NodeVerification]) -> VerificationCounts {
        nodes
            .iter()
            .fold(VerificationCounts::default(), |a, n| VerificationCounts {
                fresh: a.fresh + n.counts.fresh,
                stale: a.stale + n.counts.stale,
                never: a.never + n.counts.never,
                suspect: a.suspect + n.counts.suspect,
            })
    }
}

/// One node's inventory rows by freshness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeVerification {
    pub node_id: i32,
    pub counts: VerificationCounts,
}

/// Rows per node by freshness against `window` heights below `tip` — the
/// confirm evidence rule (`lifecycle::evidence_complete`) applied to the
/// whole table, grouped by holder.
pub fn verification_by_node(
    conn: &rusqlite::Connection,
    tip: u64,
    window: u64,
) -> Result<Vec<NodeVerification>, StorageError> {
    let floor = height_to_db(tip.saturating_sub(window));
    let mut stmt = conn
        .prepare_cached(
            "SELECT node_id,
                COALESCE(SUM(CASE WHEN suspect = 0 AND verified_height >= ?1 THEN 1 ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN suspect = 0 AND verified_height < ?1 THEN 1 ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN suspect = 0 AND verified_height IS NULL THEN 1 ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN suspect != 0 THEN 1 ELSE 0 END), 0)
             FROM fragment_inventory GROUP BY node_id ORDER BY node_id",
        )
        .map_err(db_err("prepare verification by node"))?;
    let rows = stmt
        .query_map(params![floor], |r| {
            Ok(NodeVerification {
                node_id: r.get(0)?,
                counts: VerificationCounts {
                    fresh: r.get::<_, i64>(1)? as u64,
                    stale: r.get::<_, i64>(2)? as u64,
                    never: r.get::<_, i64>(3)? as u64,
                    suspect: r.get::<_, i64>(4)? as u64,
                },
            })
        })
        .map_err(db_err("read verification by node"))?
        .collect::<Result<_, _>>()
        .map_err(db_err("collect verification by node"))?;
    Ok(rows)
}

// ---------------------------------------------------------------------------
// Transfer timing (this node, since process start)

fn histogram(max: u64) -> Mutex<Histogram<u64>> {
    Mutex::new(Histogram::<u64>::new_with_bounds(1, max, 3).expect("hdrhistogram bounds are valid"))
}

/// Fragment fetch duration in microseconds, 1µs..60s.
pub static FETCH_LATENCY_US: LazyLock<Mutex<Histogram<u64>>> =
    LazyLock::new(|| histogram(60_000_000));
/// Fragment fetch throughput in bytes per second, up to 100 GB/s.
pub static FETCH_THROUGHPUT_BPS: LazyLock<Mutex<Histogram<u64>>> =
    LazyLock::new(|| histogram(100_000_000_000));
/// Fetches that returned no bytes (transport error, peer error, not found).
pub static FETCH_FAILURES: AtomicU64 = AtomicU64::new(0);

fn lock(h: &Mutex<Histogram<u64>>) -> std::sync::MutexGuard<'_, Histogram<u64>> {
    h.lock().unwrap_or_else(|e| e.into_inner())
}

/// When this process recorded its first fetch — the start of the wall
/// clock the drain rate is measured against.
static FETCH_FIRST_SEEN: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();

/// Record one successful fetch: how long it took and how many bytes came.
pub fn record_fetch(elapsed: Duration, bytes: usize) {
    FETCH_FIRST_SEEN.get_or_init(std::time::Instant::now);
    let us = (elapsed.as_micros() as u64).max(1);
    let bps = (bytes as f64 / elapsed.as_secs_f64().max(1e-6)) as u64;
    let _ = lock(&FETCH_LATENCY_US).record(us);
    let _ = lock(&FETCH_THROUGHPUT_BPS).record(bps.max(1));
}

/// Wall time per fetch this process has actually sustained: elapsed since
/// the first fetch over fetches recorded. The transfer itself is a small
/// part of what the serial worker spends per class (the belief and
/// attestation rounds, the rehash, the write-gate ack), so this is the
/// honest drain rate; `None` until two fetches have been seen.
pub fn mean_wall_us_per_fetch() -> Option<u64> {
    let since = FETCH_FIRST_SEEN.get()?.elapsed();
    let fetches = lock(&FETCH_LATENCY_US).len();
    if fetches < 2 {
        return None;
    }
    Some((since.as_micros() / fetches as u128).max(1) as u64)
}

/// Record one fetch that yielded nothing.
pub fn record_fetch_failure() {
    FETCH_FAILURES.fetch_add(1, Ordering::Relaxed);
}

/// The commit-latency shape: count and five quantiles.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistogramSnapshot {
    pub count: u64,
    pub p50: u64,
    pub p90: u64,
    pub p99: u64,
    pub p999: u64,
    pub max: u64,
}

impl HistogramSnapshot {
    pub fn of(h: &Histogram<u64>) -> HistogramSnapshot {
        if h.is_empty() {
            return HistogramSnapshot::default();
        }
        HistogramSnapshot {
            count: h.len(),
            p50: h.value_at_quantile(0.50),
            p90: h.value_at_quantile(0.90),
            p99: h.value_at_quantile(0.99),
            p999: h.value_at_quantile(0.999),
            max: h.max(),
        }
    }
}

/// This node's fetch record since process start.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferSnapshot {
    pub fetches: u64,
    pub failures: u64,
    pub latency_us: HistogramSnapshot,
    pub throughput_bps: HistogramSnapshot,
    /// Sustained wall time per fetch since the first one (see
    /// `mean_wall_us_per_fetch`); `None` until two fetches.
    pub wall_us_per_fetch: Option<u64>,
}

pub fn transfers() -> TransferSnapshot {
    let latency_us = HistogramSnapshot::of(&lock(&FETCH_LATENCY_US));
    let throughput_bps = HistogramSnapshot::of(&lock(&FETCH_THROUGHPUT_BPS));
    TransferSnapshot {
        fetches: latency_us.count,
        failures: FETCH_FAILURES.load(Ordering::Relaxed),
        latency_us,
        throughput_bps,
        wall_us_per_fetch: mean_wall_us_per_fetch(),
    }
}

// ---------------------------------------------------------------------------
// Time to conformance

/// In-flight blobs the ETA pass inspects for owed pulls. Bounded like the
/// pane's other scans: past this, the figure is reported as partial.
pub const ETA_SCAN_LIMIT: usize = 2000;

/// The reconciler is one serial worker, so a tier's time to conformance is
/// its owed fetches at the slower of two per-fetch figures: the measured
/// median transfer and the sustained wall time per fetch. The median alone
/// read 31 seconds for 2,894 owed fetches on a node draining three fetches
/// every five minutes (2026-10-02). `None` until there is a sample (or
/// when nothing is owed, `Some(0)`).
pub fn eta_secs(fetches: u64, p50_us: Option<u64>, wall_us: Option<u64>) -> Option<u64> {
    let per_fetch = p50_us
        .filter(|p| *p > 0)
        .into_iter()
        .chain(wall_us.filter(|w| *w > 0))
        .max()?;
    Some((fetches as u128 * per_fetch as u128).div_ceil(1_000_000) as u64)
}

/// Fetches this node owes under its goals across the in-flight set (the
/// pull tier): classes assigned to `me` under each blob's goal view that
/// are not on this disk — `engine::pull_owed`'s rule, counted instead of
/// acted on. Blobs whose goal the record does not reach owe nothing yet.
/// Returns `(fetches, partial)`; `partial` when more than `limit` blobs
/// were in flight.
pub fn owed_pull_fetches(
    conn: &rusqlite::Connection,
    me: i32,
    limit: usize,
) -> Result<(u64, bool), StorageError> {
    let mut blobs = lifecycle::in_flight_blobs(conn, limit + 1)?;
    let partial = blobs.len() > limit;
    blobs.truncate(limit);

    let mut pair = conn
        .prepare_cached("SELECT desired_placement_height FROM data_blocks WHERE id = ?")
        .map_err(db_err("prepare goal read"))?;
    let mut missing = conn
        .prepare_cached(
            "SELECT local_index FROM fragment_hashes
             WHERE data_block_id = ? AND stored_locally = 0",
        )
        .map_err(db_err("prepare missing classes"))?;
    // The view is constant between transitions: one decode per goal height.
    let mut views: HashMap<u64, Option<ViewSnapshot>> = HashMap::new();
    let mut fetches = 0u64;
    for blob in &blobs {
        let Some(desired) = pair
            .query_row(params![blob], |r| r.get::<_, i64>(0))
            .optional()
            .map_err(db_err("read goal"))?
        else {
            continue;
        };
        let desired = height_from_db(desired);
        let snapshot = match views.get(&desired) {
            Some(s) => s,
            None => {
                let s = lifecycle::snapshot_at(conn, desired)?;
                views.entry(desired).or_insert(s)
            }
        };
        let Some(snapshot) = snapshot else {
            continue;
        };
        let assignment = snapshot.assignment(blob);
        let classes: Vec<u32> = missing
            .query_map(params![blob], |r| r.get(0))
            .map_err(db_err("read missing classes"))?
            .collect::<Result<_, _>>()
            .map_err(db_err("collect missing classes"))?;
        fetches += classes
            .iter()
            .filter(|c| assignment.get(**c as usize) == Some(&me))
            .count() as u64;
    }
    Ok((fetches, partial))
}

/// The worker's three queues, as owed fetches. Re-encode tiers count
/// chunks; a rebuild fetches K, so the caller scales by K.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwedFetches {
    pub urgent: u64,
    pub pull: u64,
    pub lazy: u64,
}

impl OwedFetches {
    /// Per-tier ETAs at the slower of the median fetch and the sustained
    /// wall time per fetch, in the fixed order urgent / pull / lazy.
    pub fn etas(
        &self,
        p50_us: Option<u64>,
        wall_us: Option<u64>,
    ) -> BTreeMap<&'static str, (u64, Option<u64>)> {
        [
            ("urgent", self.urgent),
            ("pull", self.pull),
            ("lazy", self.lazy),
        ]
        .into_iter()
        .map(|(tier, fetches)| (tier, (fetches, eta_secs(fetches, p50_us, wall_us))))
        .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::BlobId;
    use std::str::FromStr;

    fn test_conn() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE data_blocks (
                id TEXT PRIMARY KEY, modified_at TEXT, file_hash BLOB,
                fragment_count INTEGER, added_bytes INTEGER,
                placement_height INTEGER, file_size INTEGER,
                desired_placement_height INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE fragment_hashes (
                data_block_id TEXT, chunk_number INTEGER, local_index INTEGER,
                fragment_id TEXT, fragment_hash BLOB, chunk_type INTEGER,
                stored_locally INTEGER
            );
            CREATE TABLE fragment_inventory (
                fragment_hash BLOB NOT NULL, node_id INTEGER NOT NULL,
                self_verified_height INTEGER, verified_height INTEGER,
                provenance INTEGER, suspect INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (fragment_hash, node_id)
            );
            CREATE TABLE storage_view_transitions (
                height INTEGER PRIMARY KEY, snapshot BLOB NOT NULL
            );",
        )
        .unwrap();
        conn
    }

    fn blob(n: u8) -> BlobId {
        BlobId::from_str(&format!("01890a5d-ac96-774b-b9aa-9f8b24f0c9{n:02x}")).unwrap()
    }

    fn insert_blob(
        conn: &rusqlite::Connection,
        id: &BlobId,
        desired: u64,
        placed: Option<u64>,
        size: u64,
    ) {
        conn.execute(
            "INSERT INTO data_blocks (id, file_hash, fragment_count, added_bytes, placement_height, file_size, desired_placement_height)
             VALUES (?, X'00', 3, 0, ?, ?, ?)",
            params![id, placed.map(height_to_db), size as i64, height_to_db(desired)],
        )
        .unwrap();
        for i in 0..3u32 {
            conn.execute(
                "INSERT INTO fragment_hashes VALUES (?, 0, ?, 'f', ?, 0, 0)",
                params![id, i, vec![i as u8; 32]],
            )
            .unwrap();
        }
    }

    // Should: count owed (below T), in flight (goal unconfirmed) and
    // confirmed blobs, owe nothing before the first transition, and
    // report converged exactly when nothing is owed or in flight.
    #[test]
    fn lifecycle_counts_partition_the_blobs() {
        let conn = test_conn();
        insert_blob(&conn, &blob(1), 4, Some(4), 10); // quiescent, but below T=8: owed
        insert_blob(&conn, &blob(2), 9, Some(9), 10); // confirmed
        insert_blob(&conn, &blob(3), 9, None, 10); // in flight
        insert_blob(&conn, &blob(4), 6, Some(2), 10); // unconfirmed AND below T: owed
        let c = lifecycle_counts(&conn, Some(8)).unwrap();
        assert_eq!(
            c,
            LifecycleCounts {
                owed: 2,
                in_flight: 1,
                confirmed: 1
            }
        );
        assert!(!c.converged());
        // No transition yet: nothing owed, the pair alone decides.
        assert_eq!(
            lifecycle_counts(&conn, None).unwrap(),
            LifecycleCounts {
                owed: 0,
                in_flight: 2,
                confirmed: 2
            }
        );

        let quiet = test_conn();
        insert_blob(&quiet, &blob(1), 9, Some(9), 10);
        assert!(lifecycle_counts(&quiet, Some(9)).unwrap().converged());
        assert!(lifecycle_counts(&test_conn(), Some(9)).unwrap().converged());
    }

    // Should: bucket in-flight blobs by heights since their goal, edges
    // inclusive on the lower side, bytes beside counts, severity from the
    // policy constants only on the two oldest buckets.
    // Should not: count quiescent blobs, or let a goal above the tip go
    // negative.
    #[test]
    fn in_flight_ages_bucket_by_heights_since_goal() {
        let conn = test_conn();
        let tip = 2000;
        for (n, age) in [
            (1u8, 0u64),
            (2, 7),
            (3, 8),
            (4, 63),
            (5, 64),
            (6, 1023),
            (7, 1024),
        ] {
            insert_blob(&conn, &blob(n), tip - age, None, 100);
        }
        insert_blob(&conn, &blob(8), tip + 5, None, 100); // goal above tip: age 0
        insert_blob(&conn, &blob(9), tip - 500, Some(tip - 500), 100); // quiescent
        let b = in_flight_age_buckets(&conn, tip).unwrap();
        let counts: Vec<u64> = b.iter().map(|x| x.blobs).collect();
        assert_eq!(counts, vec![3, 2, 1, 1, 1]);
        assert_eq!(b[0].bytes, 300);
        assert_eq!(
            b.iter().map(|x| x.label).collect::<Vec<_>>(),
            INFLIGHT_AGE_LABELS.to_vec()
        );
        assert_eq!(
            b.iter().map(|x| x.severity).collect::<Vec<_>>(),
            vec![
                None,
                None,
                None,
                Some(AgeSeverity::Warn),
                Some(AgeSeverity::Stale)
            ]
        );
    }

    // Should: group inventory rows per node into fresh (at or above the
    // window floor), stale, never verified and suspect, and total them.
    #[test]
    fn verification_groups_rows_by_node_and_freshness() {
        let conn = test_conn();
        let row = |hash: u8, node: i32, verified: Option<i64>, suspect: i32| {
            conn.execute(
                "INSERT INTO fragment_inventory (fragment_hash, node_id, verified_height, suspect) VALUES (?, ?, ?, ?)",
                params![vec![hash; 32], node, verified, suspect],
            )
            .unwrap();
        };
        row(1, 1, Some(976), 0); // exactly at the floor: fresh
        row(2, 1, Some(975), 0); // one below: stale
        row(3, 1, None, 0); // never
        row(4, 1, Some(2000), 1); // suspect wins
        row(1, 2, Some(1999), 0);
        let v = verification_by_node(&conn, 2000, 1024).unwrap();
        assert_eq!(
            v,
            vec![
                NodeVerification {
                    node_id: 1,
                    counts: VerificationCounts {
                        fresh: 1,
                        stale: 1,
                        never: 1,
                        suspect: 1
                    }
                },
                NodeVerification {
                    node_id: 2,
                    counts: VerificationCounts {
                        fresh: 1,
                        stale: 0,
                        never: 0,
                        suspect: 0
                    }
                },
            ]
        );
        let total = VerificationCounts::total(&v);
        assert_eq!((total.fresh, total.rows()), (2, 5));
    }

    // Should: count the classes this node owes under each in-flight
    // blob's goal that are not on disk, skip blobs whose goal is off the
    // record, and flag a truncated scan.
    #[test]
    fn owed_pull_fetches_counts_my_missing_classes() {
        let mut conn = test_conn();
        let tx = conn.transaction().unwrap();
        let a = blob(1);
        let b = blob(2);
        insert_blob(&tx, &a, 6, None, 10);
        insert_blob(&tx, &b, 3, None, 10); // goal below the record: owes nothing yet
        lifecycle::record_transition(
            &tx,
            5,
            &ViewSnapshot {
                members: vec![1, 2, 3],
                weights: [(1, 1), (2, 1), (3, 1)].into_iter().collect(),
            },
        )
        .unwrap();
        let assignment = lifecycle::snapshot_at(&tx, 6)
            .unwrap()
            .unwrap()
            .assignment(&a);
        let me = assignment[0];
        let mine = assignment.iter().take(3).filter(|n| **n == me).count() as u64;
        assert_eq!(owed_pull_fetches(&tx, me, 10).unwrap(), (mine, false));

        // One of mine lands on disk: one fewer owed.
        tx.execute(
            "UPDATE fragment_hashes SET stored_locally = 1 WHERE data_block_id = ? AND local_index = 0",
            params![a],
        )
        .unwrap();
        assert_eq!(owed_pull_fetches(&tx, me, 10).unwrap().0, mine - 1);

        // A stranger owes nothing; a limit below the in-flight count is partial.
        assert_eq!(owed_pull_fetches(&tx, 99, 10).unwrap(), (0, false));
        assert!(owed_pull_fetches(&tx, me, 1).unwrap().1);
    }

    // Should: give no ETA without a sample, zero for nothing owed, and
    // round the serial-worker product up to whole seconds.
    #[test]
    fn eta_is_owed_fetches_at_the_median() {
        assert_eq!(eta_secs(10, None, None), None);
        assert_eq!(eta_secs(10, Some(0), None), None);
        assert_eq!(eta_secs(0, Some(42_000), None), Some(0));
        assert_eq!(eta_secs(100, Some(42_000), None), Some(5)); // 4.2s → 5
        let owed = OwedFetches {
            urgent: 0,
            pull: 100,
            lazy: 30,
        };
        let etas = owed.etas(Some(1_000_000), None);
        assert_eq!(etas["urgent"], (0, Some(0)));
        assert_eq!(etas["pull"], (100, Some(100)));
        assert_eq!(etas["lazy"], (30, Some(30)));
    }

    // Impact: the median transfer is a sliver of what the serial worker
    // spends per class; a node draining three fetches in five minutes
    // showed a 31-second ETA for 2,894 owed fetches.
    // Should: take the slower of the median fetch and the sustained wall
    // time per fetch, and fall back to whichever one exists.
    #[test]
    fn eta_prefers_the_slower_observed_rate() {
        // 100 fetches at 11 ms median but 24 s of wall each → 2400 s.
        assert_eq!(eta_secs(100, Some(11_000), Some(24_000_000)), Some(2400));
        // A fast wall clock never shortens the median estimate.
        assert_eq!(eta_secs(100, Some(42_000), Some(1_000)), Some(5));
        // Wall alone, median alone.
        assert_eq!(eta_secs(10, None, Some(500_000)), Some(5));
        assert_eq!(eta_secs(10, Some(500_000), Some(0)), Some(5));
        assert_eq!(eta_secs(10, None, Some(0)), None);
    }

    // Should: snapshot an empty histogram as zeros and a recorded one with
    // its count and quantiles; count failures separately.
    #[test]
    fn histogram_snapshots_and_failure_counter() {
        let mut h = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
        assert_eq!(HistogramSnapshot::of(&h), HistogramSnapshot::default());
        for v in [1_000u64, 2_000, 3_000, 4_000] {
            h.record(v).unwrap();
        }
        let s = HistogramSnapshot::of(&h);
        assert_eq!(s.count, 4);
        assert!(s.p50 >= 2_000 && s.p50 <= 3_000, "{s:?}");
        assert!(s.max >= 4_000);

        let before = transfers();
        record_fetch(Duration::from_millis(10), 1_000_000);
        record_fetch_failure();
        let after = transfers();
        assert_eq!(after.fetches, before.fetches + 1);
        assert_eq!(after.failures, before.failures + 1);
        assert!(after.throughput_bps.max >= 90_000_000, "{after:?}");
    }
}
