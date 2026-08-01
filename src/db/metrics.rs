use super::*;
use crate::metrics::types::Metric;
use chrono::DateTime;

pub fn get_metric(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
) -> Result<Vec<Metric>, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            // Prepare the query
            let mut stmt = db_lock
                .prepare("SELECT * FROM metrics")
                .map_err(|_| DatabaseError::RecallError)?;
            // Execute the query and map each row to a Metric
            let results = stmt.query_map([], |row| {
                let from_node: i32 = row.get(0)?;
                let to_node: i32 = row.get(1)?;

                // Read timestamp as RFC3339 string (SQLite stores as text)
                let start_time_str: String = row.get(2)?;
                let start_time = DateTime::parse_from_rfc3339(&start_time_str)
                    .map(|dt| dt.with_timezone(&chrono::Utc))
                    .map_err(|_| {
                        tracing::error!("Invalid timestamp: {}", start_time_str);
                        rusqlite::Error::InvalidColumnName("start_time".to_string())
                    })?;

                Ok(Metric {
                    from_node,
                    to_node,
                    start_time,
                    rtt_latency: row.get(3)?,
                    rtt_variance: row.get(4)?,
                    rtt_jitter: row.get(5)?,
                    throughput: row.get(6)?,
                    height: row.get(7)?,           // New: consensus height
                    available: row.get(8)?,        // New: node availability
                    storage_total_gb: row.get(9)?, // New: storage capacity
                    storage_used_gb: row.get(10)?, // New: storage utilization
                })
            });

            match results {
                Ok(metrics) => metrics.collect::<Result<Vec<_>, _>>().map_err(|e| {
                    tracing::error!("Error parsing metric row: {:?}", e);
                    DatabaseError::ProcessingError
                }),
                Err(e) => {
                    tracing::error!("Error querying metrics: {:?}", e);
                    Err(DatabaseError::RecordError)
                }
            }
        }
        Err(e) => {
            tracing::error!("Database connection error in get_metric: {:?}", e);
            Err(DatabaseError::LockError)
        }
    }
}

pub fn insert_metric(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    metric: Metric,
) -> Result<(), DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            // Use DateTime directly - no conversion needed!
            let start_time_str = metric.start_time.to_rfc3339();
            tracing::debug!("Inserting metric into database");
            let result = db_lock.execute(
                "INSERT INTO metrics (from_node, to_node, start_time, rtt_latency, rtt_variance, rtt_jitter, throughput, height, available, storage_total_gb, storage_used_gb) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    metric.from_node,
                    metric.to_node,
                    start_time_str,
                    metric.rtt_latency,
                    metric.rtt_variance,
                    metric.rtt_jitter,
                    metric.throughput,
                    metric.height,
                    metric.available,
                    metric.storage_total_gb,
                    metric.storage_used_gb,
                ]
            );
            match result {
                Ok(_) => {
                    tracing::debug!("Successfully inserted metric into database");
                    Ok(())
                }
                Err(e) => {
                    tracing::error!("Error inserting metric: {:?}", e);
                    Err(DatabaseError::InsertError)
                }
            }
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}

/// Insert multiple metrics using a shared transaction (for consensus transaction processing)
pub fn insert_metrics_batch(
    db_tx: &rusqlite::Transaction,
    metrics: Vec<Metric>,
) -> Result<(), DatabaseError> {
    let metrics_len = metrics.len();
    for metric in metrics {
        let start_time_str = metric.start_time.to_rfc3339();
        db_tx.execute(
            "INSERT INTO metrics (from_node, to_node, start_time, rtt_latency, rtt_variance, rtt_jitter, throughput, height, available, storage_total_gb, storage_used_gb) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                metric.from_node,
                metric.to_node,
                start_time_str,
                metric.rtt_latency,
                metric.rtt_variance,
                metric.rtt_jitter,
                metric.throughput,
                metric.height,
                metric.available,
                metric.storage_total_gb,
                metric.storage_used_gb,
            ]
        ).map_err(|e| {
            tracing::error!("Error inserting metric from {} to {}: {:?}", metric.from_node, metric.to_node, e);
            DatabaseError::InsertError
        })?;
    }

    tracing::debug!("Inserted {} metrics using shared transaction", metrics_len);
    Ok(())
}

/// Get all network nodes excluding specified node (for metrics collection)
pub fn get_nodes_to_measure(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    exclude_node_id: i32,
) -> Result<Vec<crate::types::Node>, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            let mut stmt = db_lock
                .prepare(
                    "SELECT node_id, name, owner, pubkey
                FROM nodes
                WHERE node_id != ?
                ORDER BY node_id",
                )
                .map_err(|_| DatabaseError::RecallError)?;

            let nodes = stmt
                .query_map([exclude_node_id], |row| {
                    Ok(crate::types::Node {
                        node_id: row.get(0)?,
                        name: row.get(1)?,
                        owner: row.get(2)?,
                        pubkey: row.get(3)?,
                    })
                })
                .map_err(|_| DatabaseError::RecallError)?
                .collect::<Result<Vec<crate::types::Node>, _>>()
                .map_err(|_| DatabaseError::RecallError)?;

            tracing::debug!(
                "Found {} network nodes to measure (excluding node {})",
                nodes.len(),
                exclude_node_id
            );
            Ok(nodes)
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}

/// Computed node scores for placement algorithm
#[derive(Debug, Clone, serde::Serialize)]
pub struct NodeMetrics {
    pub node_id: i32,
    pub pubkey: crate::types::PubKey,
    pub sample_count_7d: u32,
    pub trust_factor: f64,
    pub availability_score: f64,  // Time-weighted: 24h * 0.7 + 7d * 0.3
    pub throughput_score: f64,    // Log-normalized with consistency factor
    pub latency_score: f64,       // Inverse normalized latency
    pub stability_score: f64,     // Inverse of 7d latency variance
    pub storage_utilization: f64, // used_gb / total_gb ratio
    pub storage_multiplier: f64,  // e^(-5 * utilization)
}

/// Get computed placement scores for all nodes at specific consensus height
/// Uses SQLite analytical functions to calculate RFC-compliant score components
/// Includes all registered nodes (validators + storage-only nodes) for fragment placement
pub fn get_all_node_metrics(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    consensus_height: i32,
) -> Result<Vec<NodeMetrics>, DatabaseError> {
    match db_connection {
        Ok(db_lock) => get_all_node_metrics_with_conn(&db_lock, consensus_height),
        Err(_) => Err(DatabaseError::LockError),
    }
}

/// Connection-taking variant so callers holding a scoped checkout (or a
/// transaction, via deref) can read metrics without a second pool hit.
pub fn get_all_node_metrics_with_conn(
    db_lock: &rusqlite::Connection,
    consensus_height: i32,
) -> Result<Vec<NodeMetrics>, DatabaseError> {
    {
        {
            // SQLite replacements for DuckDB-specific functions:
            // - STDDEV(x) → sqrt(AVG(x*x) - AVG(x)*AVG(x))  (population stddev)
            // - PERCENTILE_CONT(0.5) → AVG() as approximation for median
            // - LEAST(a,b) → MIN(a,b)  (scalar)
            // - LOG(x)/LOG(10) → log10(x)
            // - POWER(x,y) → pow(x,y)
            //
            // Time windows are anchored to the NEWEST replicated metrics row
            // at or below the requested height — never the wall clock — so
            // every node derives identical scores from identical rows
            // (RFC-STORAGE-002: scores feed quantized placement weights,
            // which must be mesh-deterministic).
            let query =
                "WITH
                anchor AS (
                    SELECT COALESCE(MAX(start_time), '1970-01-01 00:00:00') AS t
                    FROM metrics WHERE height <= ?1
                ),
                -- Aggregate metrics for nodes that have data
                node_metrics AS (
                    SELECT
                        m.to_node as node_id,
                        -- 24-hour metrics
                        COUNT(CASE WHEN m.start_time >= datetime((SELECT t FROM anchor), '-24 hours') THEN 1 END) as sample_count_24h,
                        AVG(CASE WHEN m.available AND m.start_time >= datetime((SELECT t FROM anchor), '-24 hours') THEN 1.0 ELSE 0.0 END) as availability_24h,
                        AVG(CASE WHEN m.start_time >= datetime((SELECT t FROM anchor), '-24 hours') THEN m.rtt_latency END) as avg_latency_24h,
                        -- 7-day metrics
                        COUNT(*) as sample_count_7d,
                        AVG(CASE WHEN m.available THEN 1.0 ELSE 0.0 END) as availability_7d,
                        -- Latency stddev via population variance: sqrt(E[X^2] - E[X]^2)
                        CASE
                            WHEN COUNT(m.rtt_latency) > 1 THEN
                                sqrt(MAX(0.0, AVG(m.rtt_latency * m.rtt_latency) - AVG(m.rtt_latency) * AVG(m.rtt_latency)))
                            ELSE NULL
                        END as latency_variance_7d,
                        -- Throughput: AVG approximates median for placement scoring
                        AVG(m.throughput) as median_throughput,
                        CASE
                            WHEN COUNT(m.throughput) > 1 THEN
                                sqrt(MAX(0.0, AVG(m.throughput * m.throughput) - AVG(m.throughput) * AVG(m.throughput)))
                            ELSE NULL
                        END as throughput_stddev,
                        MAX(m.storage_total_gb) as storage_total_gb,
                        MAX(m.storage_used_gb) as storage_used_gb
                    FROM metrics m
                    WHERE m.height <= ?1
                      AND m.start_time >= datetime((SELECT t FROM anchor), '-7 days')
                    GROUP BY m.to_node
                ),
                -- Calculate network-wide fallback statistics per RFC requirements
                -- AVG approximates median/percentile for fallback defaults
                network_stats AS (
                    SELECT
                        COALESCE(AVG(availability_24h), 0.8) as median_availability_24h,
                        COALESCE(AVG(availability_7d), 0.8) as median_availability_7d,
                        COALESCE(
                            (SELECT nm2.avg_latency_24h FROM node_metrics nm2
                             WHERE nm2.avg_latency_24h IS NOT NULL
                             ORDER BY nm2.avg_latency_24h
                             LIMIT 1
                             OFFSET MAX(0, (SELECT COUNT(*) * 3 / 4 FROM node_metrics nm3 WHERE nm3.avg_latency_24h IS NOT NULL) - 1)),
                            50.0
                        ) as p75_latency,
                        COALESCE(AVG(median_throughput), 1000000) as median_throughput,
                        COALESCE(AVG(CAST(storage_used_gb AS REAL) / CAST(storage_total_gb AS REAL)), 0.3) as avg_storage_utilization
                    FROM node_metrics
                    WHERE storage_total_gb > 0
                )
                SELECT
                    n.node_id,
                    n.pubkey,
                    COALESCE(nm.sample_count_7d, 0) as sample_count_7d,
                    -- Trust factor: gradual confidence building for new nodes
                    MIN(CAST(COALESCE(nm.sample_count_7d, 0) AS REAL) / 100.0, 1.0) as trust_factor,
                    -- Time-weighted availability score (RFC formula)
                    COALESCE(nm.availability_24h, ns.median_availability_24h * 0.5) * 0.7 +
                    COALESCE(nm.availability_7d, ns.median_availability_7d * 0.5) * 0.3 as availability_score,
                    -- Throughput score: log-normalized percentile rank with consistency
                    CASE
                        WHEN nm.median_throughput IS NOT NULL THEN
                            log10(1.0 + (PERCENT_RANK() OVER (ORDER BY nm.median_throughput)) * 9.0) *
                            -- Consistency factor: 1 / (1 + coefficient_of_variation)
                            (1.0 / (1.0 + COALESCE(nm.throughput_stddev / NULLIF(nm.median_throughput, 0), 0.5)))
                        ELSE 0.5  -- Conservative default for new nodes
                    END as throughput_score,
                    -- Latency score: inverse normalized (lower latency = higher score)
                    1.0 / (1.0 + (COALESCE(nm.avg_latency_24h, ns.p75_latency * 1.5) / ns.p75_latency)) as latency_score,
                    -- Stability score: inverse of 7-day latency variance
                    CASE
                        WHEN nm.latency_variance_7d IS NOT NULL AND nm.latency_variance_7d > 0 THEN
                            1.0 / (1.0 + nm.latency_variance_7d)
                        ELSE 0.5  -- Default for nodes without variance data
                    END as stability_score,
                    -- Storage utilization ratio
                    CASE
                        WHEN nm.storage_total_gb IS NOT NULL AND nm.storage_total_gb > 0 THEN
                            CAST(nm.storage_used_gb AS REAL) / CAST(nm.storage_total_gb AS REAL)
                        ELSE 0.5  -- Conservative default assuming 50% full
                    END as storage_utilization,
                    -- Storage multiplier: 90% threshold with quartic decay
                    CASE
                        WHEN nm.storage_total_gb IS NOT NULL AND nm.storage_total_gb > 0 THEN
                            CASE
                                WHEN (CAST(nm.storage_used_gb AS REAL) / CAST(nm.storage_total_gb AS REAL)) <= 0.9 THEN
                                    1.0  -- No penalty until 90% full
                                ELSE
                                    -- Quartic decay in final 10%: (1 - excess)^4
                                    pow(
                                        1.0 - ((CAST(nm.storage_used_gb AS REAL) / CAST(nm.storage_total_gb AS REAL)) - 0.9) / 0.1,
                                        4.0
                                    )
                            END
                        ELSE
                            0.5  -- Conservative default for nodes without storage data
                    END as storage_multiplier
                FROM nodes n
                LEFT JOIN node_metrics nm ON n.node_id = nm.node_id
                CROSS JOIN network_stats ns
                ORDER BY n.node_id";

            // Only consensus height parameter needed now
            let params = [&consensus_height as &dyn rusqlite::ToSql];

            let mut stmt = db_lock.prepare(query).map_err(|e| {
                tracing::error!("Failed to prepare metrics query: {:?}", e);
                DatabaseError::RecallError
            })?;

            let metrics = stmt
                .query_map(params, |row| {
                    Ok(NodeMetrics {
                        node_id: row.get("node_id")?,
                        pubkey: row.get("pubkey")?,
                        sample_count_7d: row.get::<_, i64>("sample_count_7d")? as u32,
                        trust_factor: row.get("trust_factor")?,
                        availability_score: row.get("availability_score")?,
                        throughput_score: row.get("throughput_score")?,
                        latency_score: row.get("latency_score")?,
                        stability_score: row.get("stability_score")?,
                        storage_utilization: row.get("storage_utilization")?,
                        storage_multiplier: row.get("storage_multiplier")?,
                    })
                })
                .map_err(|e| {
                    tracing::error!("Failed to execute metrics query: {:?}", e);
                    DatabaseError::RecallError
                })?
                .collect::<Result<Vec<NodeMetrics>, _>>()
                .map_err(|e| {
                    tracing::error!("Failed to parse metrics results: {:?}", e);
                    DatabaseError::ProcessingError
                })?;

            tracing::debug!(
                "Retrieved metrics for {} nodes at height {}",
                metrics.len(),
                consensus_height
            );

            Ok(metrics)
        }
    }
}

/// Per-node availability grid for decay-tier derivation
/// (RFC-STORAGE-002 S2): dense 10-minute buckets over the lookback
/// window, anchored to the newest replicated metrics row.
#[derive(Debug)]
pub struct AvailabilityGrid {
    /// Unix seconds of the newest (anchor) bucket. None = no metrics yet.
    pub anchor: Option<i64>,
    pub step_secs: i64,
    /// node_id → ascending (bucket_start, available).
    pub per_node: std::collections::HashMap<i32, Vec<(i64, bool)>>,
}

/// Bucketed availability history per node, from replicated metrics rows at
/// or below `height`. A node is available in a bucket if ANY reporter saw
/// it (MAX over reporters — biases toward presence, which biases tiers
/// long, matching the policy's asymmetric-cost bias; RFC-STORAGE-002).
///
/// The grid is densified by carry-forward: a bucket nobody reported (cron
/// jitter, mesh-wide gap) inherits the previous bucket's state, so a
/// missed round can neither split a real absence run into two short ones
/// nor invent one. State before a node's first sample is `available`.
///
/// Window and buckets are anchored to the newest row's start_time, never
/// the wall clock. NOTE: the metrics table has no pruning today; this read
/// is bounded by the window but the table grows unbounded (deferred).
pub fn get_availability_history_with_conn(
    conn: &rusqlite::Connection,
    height: i32,
    lookback_buckets: i64,
    step_secs: i64,
) -> Result<AvailabilityGrid, DatabaseError> {
    let anchor: Option<i64> = conn
        .query_row(
            "SELECT CAST(strftime('%s', MAX(start_time)) AS INTEGER)
             FROM metrics WHERE height <= ?1",
            rusqlite::params![height],
            |row| row.get(0),
        )
        .map_err(|e| {
            tracing::error!("availability anchor query failed: {e:?}");
            DatabaseError::RecallError
        })?;
    let Some(anchor_ts) = anchor else {
        return Ok(AvailabilityGrid {
            anchor: None,
            step_secs,
            per_node: Default::default(),
        });
    };
    let anchor_bucket = (anchor_ts / step_secs) * step_secs;
    // Window is bounded in BUCKETS, not wall time, so a test-shrunk step
    // keeps the grid (and this loop) the same size: 4320 buckets = 30 days
    // at the default 10-minute step.
    let window_start = anchor_bucket - lookback_buckets * step_secs;

    let mut stmt = conn
        .prepare(
            "SELECT m.to_node,
                    (CAST(strftime('%s', m.start_time) AS INTEGER) / ?2) * ?2 AS bucket,
                    MAX(m.available) AS available
             FROM metrics m
             WHERE m.height <= ?1
               AND CAST(strftime('%s', m.start_time) AS INTEGER) >= ?3
             GROUP BY m.to_node, bucket
             ORDER BY m.to_node, bucket",
        )
        .map_err(|e| {
            tracing::error!("availability history prepare failed: {e:?}");
            DatabaseError::RecallError
        })?;
    let sparse: Vec<(i32, i64, bool)> = stmt
        .query_map(rusqlite::params![height, step_secs, window_start], |row| {
            Ok((
                row.get::<_, i32>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)? != 0,
            ))
        })
        .map_err(|e| {
            tracing::error!("availability history query failed: {e:?}");
            DatabaseError::RecallError
        })?
        .collect::<Result<_, _>>()
        .map_err(|e| {
            tracing::error!("availability history rows failed: {e:?}");
            DatabaseError::ProcessingError
        })?;

    let mut per_node: std::collections::HashMap<i32, Vec<(i64, bool)>> = Default::default();
    let mut sparse_by_node: std::collections::HashMap<i32, std::collections::HashMap<i64, bool>> =
        Default::default();
    for (node, bucket, available) in sparse {
        sparse_by_node
            .entry(node)
            .or_default()
            .insert(bucket, available);
    }
    for (node, buckets) in sparse_by_node {
        let mut grid = Vec::new();
        let mut state = true; // before first sample: presence bias
        let mut t = window_start;
        while t <= anchor_bucket {
            if let Some(&observed) = buckets.get(&t) {
                state = observed;
            }
            grid.push((t, state));
            t += step_secs;
        }
        per_node.insert(node, grid);
    }

    Ok(AvailabilityGrid {
        anchor: Some(anchor_bucket),
        step_secs,
        per_node,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::SqliteConnectionManager;
    use rusqlite::params;

    fn setup_test_db() -> r2d2::Pool<SqliteConnectionManager> {
        let manager = SqliteConnectionManager::memory();
        let pool = r2d2::Pool::builder()
            .max_size(1)
            .connection_customizer(Box::new(crate::db::shared::SqliteInitializer))
            .build(manager)
            .unwrap();
        crate::db::shared::initialize(pool.get().unwrap()).unwrap();
        pool
    }

    fn insert_fixture_nodes(conn: &rusqlite::Connection, count: i32) {
        let key = ed25519_dalek::SigningKey::from_bytes(&[7u8; 32]);
        let pubkey = crate::db::PubKey(key.verifying_key());
        conn.execute(
            "INSERT INTO users (user_id, username, pubkey, x25519_pubkey, encrypted_privkey, key_salt)
             VALUES (?, ?, ?, ?, ?, ?)",
            params![1, "test", &pubkey, &vec![0u8; 32], &vec![0u8; 44], &vec![0u8; 16]],
        )
        .unwrap();
        for n in 1..=count {
            // nodes.pubkey is UNIQUE — each fixture node needs its own key.
            let node_key = ed25519_dalek::SigningKey::from_bytes(&[100 + n as u8; 32]);
            let node_pubkey = crate::db::PubKey(node_key.verifying_key());
            conn.execute(
                "INSERT INTO nodes (node_id, name, owner, pubkey) VALUES (?, ?, ?, ?)",
                params![n, format!("node{n}"), 1, &node_pubkey],
            )
            .unwrap();
        }
    }

    /// Insert one availability observation at `minutes_before` the fixed
    /// anchor "2026-07-01 12:00:00" (10-minute aligned).
    fn insert_availability(
        conn: &rusqlite::Connection,
        to_node: i32,
        minutes_before: i64,
        available: bool,
    ) {
        conn.execute(
            "INSERT INTO metrics (from_node, to_node, start_time, height, available)
             VALUES (1, ?1, datetime('2026-07-01 12:00:00', '-' || ?2 || ' minutes'), 5, ?3)",
            params![to_node, minutes_before, available],
        )
        .unwrap();
    }

    // Should: bucket replicated availability rows onto a dense 10-minute
    // grid where a reporting gap carries the previous state forward, so a
    // missed collection round can neither split a real absence run nor
    // invent one; closed runs and the trailing (current) absence come out
    // through the membership helpers.
    // Should not: count the trailing run as a closed span.
    // Impact: run-splitting would shift P95 down and assign too-short
    // decay tiers — false departures every weekend.
    #[test]
    fn availability_grid_carry_forward_runs() {
        let pool = setup_test_db();
        let conn = pool.get().unwrap();
        insert_fixture_nodes(&conn, 2);

        // Node 2 timeline (minutes before anchor):
        // 240..=180 available; 170..=110 absent WITH a gap at 140 (no row);
        // 100..=60 available; 50..=0 absent (trailing).
        for m in [240, 230, 220, 210, 200, 190, 180] {
            insert_availability(&conn, 2, m, true);
        }
        for m in [170, 160, 150, 130, 120, 110] {
            insert_availability(&conn, 2, m, false); // 140 deliberately missing
        }
        for m in [100, 90, 80, 70, 60] {
            insert_availability(&conn, 2, m, true);
        }
        for m in [50, 40, 30, 20, 10, 0] {
            insert_availability(&conn, 2, m, false);
        }

        let grid = get_availability_history_with_conn(&conn, 10, 144, 600).unwrap();
        let samples = &grid.per_node[&2];
        let spans = hopnet_storage::membership::offline_spans(samples, grid.step_secs);
        // One closed run: 170..110 inclusive = 7 buckets (gap carried) = 4200s.
        assert_eq!(spans, vec![4200]);
        // Trailing run: 50..0 inclusive = 6 buckets = 3600s.
        assert_eq!(
            hopnet_storage::membership::current_absence(samples, grid.step_secs),
            3600
        );
    }

    // Should: anchor scoring windows to the newest replicated row, not the
    // wall clock — rows from any calendar time score identically on every
    // node and at every real-world moment.
    // Impact: datetime('now') windows made scores node-divergent; balanced
    // rendezvous makes weights placement-relevant, so divergent scores
    // would silently diverge placement mesh-wide.
    #[test]
    fn node_metrics_anchored_to_newest_row() {
        let pool = setup_test_db();
        let conn = pool.get().unwrap();
        insert_fixture_nodes(&conn, 2);

        // Rows far in the wall-clock past: within the 24h/7d windows
        // relative to their own newest row, outside them relative to now().
        conn.execute(
            "INSERT INTO metrics (from_node, to_node, start_time, height, available, rtt_latency, throughput)
             VALUES (1, 2, '2020-01-01 00:00:00', 5, 1, 20.0, 1000000)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO metrics (from_node, to_node, start_time, height, available, rtt_latency, throughput)
             VALUES (1, 2, '2020-01-01 01:00:00', 5, 1, 20.0, 1000000)",
            [],
        )
        .unwrap();

        let metrics = get_all_node_metrics_with_conn(&conn, 10).unwrap();
        let node2 = metrics.iter().find(|m| m.node_id == 2).unwrap();
        assert_eq!(
            node2.sample_count_7d, 2,
            "anchored window must see the rows"
        );
        assert!(
            node2.availability_score > 0.9,
            "available rows inside the anchored 24h window must score: {}",
            node2.availability_score
        );
    }
}
