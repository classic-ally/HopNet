use r2d2::PooledConnection;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::params;

use crate::db::DatabaseError;
use hopnet_common::db::{CustomUUID, FaultToleranceCurvePoint, NodeStorageBaseline};

/// Raw user bytes sitting at each distinct worst-case tolerance level.
///
/// `-2` is unknown (no attestation anywhere) and `-1` unrecoverable (fewer than
/// K classes survive on member disks). Levels are UNCAPPED — the old
/// `>= 3 THEN 3` clamp made everything above 3 unrepresentable, which destroyed
/// exactly the range the ideal curve is plotted over.
///
/// `member_ids` is the current storage member view. There is no membership
/// table — `derive_view` is a pure Rust function — so ids are bound as
/// parameters, following the placeholder pattern in `db::inventory`. Filtering
/// on membership rather than on `metrics.available` is what makes this the
/// `durable` predicate the storage spec names, and it drops the dependency on
/// the ~10-minute metrics cron.
pub fn resilience_level_rows(
    conn: &PooledConnection<SqliteConnectionManager>,
    member_ids: &[i32],
) -> Result<Vec<(i32, f64)>, DatabaseError> {
    let start_time = std::time::Instant::now();

    // An empty member set is legitimate (fresh mesh): `IN (NULL)` matches
    // nothing, so every attested block correctly falls to -1.
    let placeholders = if member_ids.is_empty() {
        "NULL".to_string()
    } else {
        member_ids.iter().map(|_| "?").collect::<Vec<_>>().join(", ")
    };

    // Anchored on data_blocks, not fragment_hashes, so a block with no fragment
    // rows at all is still counted rather than silently dropped.
    //
    // The three-way split below fixes a real bug in the previous query: it put
    // the liveness predicate in a LEFT JOIN ... ON followed by
    // `WHERE fi.node_id IS NOT NULL`, so a block whose inventory rows were ALL
    // on excluded nodes vanished from the counts entirely — and the
    // no-attestation CTE could not catch it either, since it joined unfiltered
    // and required NULL. Harmless while the filter was availability-based;
    // serious once it is membership-based, because departed nodes' inventory
    // rows persist indefinitely (pruning is deferred), so blocks stranded on
    // departed nodes would disappear instead of reporting as lost.
    let query = format!(
        r#"
        WITH
        block_k AS (
            SELECT data_block_id, COUNT(*) AS k
            FROM fragment_hashes
            WHERE chunk_type = 0
            GROUP BY data_block_id
        ),

        -- Attested anywhere, member or not: separates "never placed" (-2)
        -- from "placed, but nothing survives on a member" (-1).
        block_attested AS (
            SELECT DISTINCT fh.data_block_id
            FROM fragment_hashes fh
            JOIN fragment_inventory fi ON fi.fragment_hash = fh.fragment_hash
        ),

        member_counts AS (
            SELECT fh.data_block_id, fi.node_id, COUNT(*) AS on_node, bk.k
            FROM fragment_hashes fh
            JOIN block_k bk ON bk.data_block_id = fh.data_block_id
            JOIN fragment_inventory fi ON fi.fragment_hash = fh.fragment_hash
            WHERE fi.node_id IN ({placeholders})
            GROUP BY fh.data_block_id, fi.node_id, bk.k
        ),

        -- Adversarial ordering: largest holders lost first, so the level is a
        -- worst case rather than an average.
        ranked AS (
            SELECT
                data_block_id, k, on_node,
                ROW_NUMBER() OVER (
                    PARTITION BY data_block_id ORDER BY on_node DESC
                ) AS node_rank,
                SUM(on_node) OVER (PARTITION BY data_block_id) AS total,
                SUM(on_node) OVER (
                    PARTITION BY data_block_id ORDER BY on_node DESC
                    ROWS UNBOUNDED PRECEDING
                ) AS cumulative
            FROM member_counts
        ),

        tolerance AS (
            SELECT
                data_block_id, k, total,
                COALESCE(
                    MAX(CASE
                        WHEN (total - cumulative + on_node) >= k THEN node_rank - 1
                    END),
                    -1
                ) AS level
            FROM ranked
            GROUP BY data_block_id, k, total
        )

        SELECT
            CASE
                WHEN ba.data_block_id IS NULL THEN -2
                WHEN t.data_block_id IS NULL THEN -1
                WHEN t.total < t.k THEN -1
                ELSE t.level
            END AS fault_tolerance_level,
            SUM(db.file_size) AS raw_bytes
        FROM data_blocks db
        LEFT JOIN block_attested ba ON ba.data_block_id = db.id
        LEFT JOIN tolerance t ON t.data_block_id = db.id
        GROUP BY fault_tolerance_level
        ORDER BY fault_tolerance_level DESC
        "#
    );

    let mut stmt = conn
        .prepare(&query)
        .map_err(|_| DatabaseError::ProcessingError)?;

    let bound: Vec<&dyn rusqlite::ToSql> = member_ids
        .iter()
        .map(|id| id as &dyn rusqlite::ToSql)
        .collect();

    let rows = stmt
        .query_map(bound.as_slice(), |row| {
            let level: i32 = row.get(0)?;
            let bytes: i64 = row.get(1).unwrap_or(0);
            Ok((level, bytes as f64))
        })
        .map_err(|_| DatabaseError::RecallError)?;

    let levels = rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| DatabaseError::RecallError)?;

    tracing::debug!(
        "resilience levels computed: {} distinct in {}ms",
        levels.len(),
        start_time.elapsed().as_millis()
    );

    Ok(levels)
}

/// Age decades for committed blocks whose fragments have not yet been placed.
///
/// Returns `(label, raw_bytes)` youngest first. This measures distribution
/// pipeline latency: a block is committed (`data_blocks` row exists) but no
/// `placement_height` has been set, meaning the engine has not yet finished
/// distributing its fragments to storage members. Transient unplaced data is
/// normal (upload just completed); the diagnostic is the SHAPE — a healthy
/// mesh decays toward nothing as the engine works through the queue, while a
/// tail that refuses to decay indicates stalled or failed distribution.
///
/// Age comes from `data_blocks.id` being a UUIDv7: the creation timestamp is
/// the leading 48 bits and the ids are stored as lowercase hyphenated text, so
/// lexicographic comparison against cutoff UUIDs orders chronologically on the
/// primary-key index — no timestamp parsing, no extra column.
pub fn unplaced_age_buckets(
    conn: &PooledConnection<SqliteConnectionManager>,
) -> Result<Vec<(&'static str, f64)>, DatabaseError> {
    use chrono::Duration;

    let c1m = CustomUUID::cutoff_before(Duration::minutes(1)).to_string();
    let c10m = CustomUUID::cutoff_before(Duration::minutes(10)).to_string();
    let c1h = CustomUUID::cutoff_before(Duration::hours(1)).to_string();
    let c1d = CustomUUID::cutoff_before(Duration::days(1)).to_string();

    let query = r#"
        SELECT
            SUM(CASE WHEN db.id >= ?1 THEN db.file_size ELSE 0 END),
            SUM(CASE WHEN db.id >= ?2 AND db.id < ?1 THEN db.file_size ELSE 0 END),
            SUM(CASE WHEN db.id >= ?3 AND db.id < ?2 THEN db.file_size ELSE 0 END),
            SUM(CASE WHEN db.id >= ?4 AND db.id < ?3 THEN db.file_size ELSE 0 END),
            SUM(CASE WHEN db.id < ?4 THEN db.file_size ELSE 0 END)
        FROM data_blocks db
        WHERE db.placement_height IS NULL
    "#;

    let mut stmt = conn
        .prepare(query)
        .map_err(|_| DatabaseError::ProcessingError)?;

    let sums: [f64; 5] = stmt
        .query_row(params![c1m, c10m, c1h, c1d], |row| {
            Ok([
                row.get::<_, Option<i64>>(0)?.unwrap_or(0) as f64,
                row.get::<_, Option<i64>>(1)?.unwrap_or(0) as f64,
                row.get::<_, Option<i64>>(2)?.unwrap_or(0) as f64,
                row.get::<_, Option<i64>>(3)?.unwrap_or(0) as f64,
                row.get::<_, Option<i64>>(4)?.unwrap_or(0) as f64,
            ])
        })
        .map_err(|_| DatabaseError::RecallError)?;

    Ok(vec![
        ("<1m", sums[0]),
        ("1-10m", sums[1]),
        ("10m-1h", sums[2]),
        ("1h-1d", sums[3]),
        (">1d", sums[4]),
    ])
}


/// Get node storage baselines for fault tolerance curve generation
/// Returns each node's total capacity and baseline usage for simulation
pub fn get_node_storage_baselines(
    db_connection: Result<PooledConnection<SqliteConnectionManager>, r2d2::Error>,
) -> Result<Vec<NodeStorageBaseline>, DatabaseError> {
    let start_time = std::time::Instant::now();

    match db_connection {
        Ok(conn) => {
            let query = r#"
                WITH
                -- Calculate original fragment counts per data block first
                data_block_original_counts AS (
                    SELECT
                        fh.data_block_id,
                        COUNT(*) as original_count
                    FROM fragment_hashes fh
                    WHERE fh.chunk_type = 0
                    GROUP BY fh.data_block_id
                ),

                -- Calculate current HopNet storage per node
                node_hopnet_storage AS (
                    SELECT
                        fi.node_id,
                        SUM((CAST(db.file_size AS REAL) / MAX(dboc.original_count, 1)) * 1.1 / (1024.0 * 1024.0 * 1024.0)) as hopnet_storage_gb
                    FROM fragment_inventory fi
                    JOIN fragment_hashes fh ON fi.fragment_hash = fh.fragment_hash
                    JOIN data_blocks db ON fh.data_block_id = db.id
                    JOIN data_block_original_counts dboc ON db.id = dboc.data_block_id
                    GROUP BY fi.node_id
                ),

                -- Get latest storage metrics for each node
                latest_node_metrics AS (
                    SELECT node_id, storage_total_gb, storage_used_gb FROM (
                        SELECT
                            to_node as node_id,
                            storage_total_gb,
                            storage_used_gb,
                            ROW_NUMBER() OVER (PARTITION BY to_node ORDER BY height DESC, start_time DESC) as rn
                        FROM metrics
                        WHERE storage_total_gb > 0
                    ) WHERE rn = 1
                )

                SELECT
                    n.node_id,
                    n.name,
                    COALESCE(n.name, 'Node ' || n.node_id) as display_name,
                    lnm.storage_total_gb,
                    -- Baseline: current usage minus HopNet = x=0 point on curve
                    MAX(0.0, lnm.storage_used_gb - COALESCE(nhs.hopnet_storage_gb, 0.0)) as baseline_storage_gb
                FROM nodes n
                INNER JOIN latest_node_metrics lnm ON n.node_id = lnm.node_id
                LEFT JOIN node_hopnet_storage nhs ON n.node_id = nhs.node_id
                ORDER BY lnm.storage_total_gb DESC
            "#;

            let mut stmt = conn.prepare(query).map_err(|e| {
                tracing::error!(
                    "Failed to prepare query for node storage baselines: {:?}",
                    e
                );
                DatabaseError::ProcessingError
            })?;

            let rows = stmt
                .query_map(params![], |row| {
                    Ok(NodeStorageBaseline {
                        node_id: row.get(0)?,
                        name: row.get(1)?,
                        display_name: row.get(2)?,
                        storage_total_gb: row.get(3)?,
                        baseline_storage_gb: row.get(4)?,
                        source: hopnet_common::db::NodeSource::System,
                        original_values: None,
                    })
                })
                .map_err(|e| {
                    tracing::error!(
                        "Failed to execute query for node storage baselines: {:?}",
                        e
                    );
                    DatabaseError::RecallError
                })?;

            let baselines: Vec<NodeStorageBaseline> = rows
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| DatabaseError::RecallError)?;

            let computation_time_ms = start_time.elapsed().as_millis() as u64;

            tracing::debug!(
                "Retrieved storage baselines for {} nodes in {}ms",
                baselines.len(),
                computation_time_ms
            );

            Ok(baselines)
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}

/// Generate fault tolerance curve from node storage baselines
/// Models how fault tolerance degrades as network fills up with user data
pub fn generate_fault_tolerance_curve(
    nodes: Vec<NodeStorageBaseline>,
    threshold_ratio: f64,
) -> Vec<FaultToleranceCurvePoint> {
    // Step 1: Filter to nodes that can accept new fragments (not already over threshold)
    let viable_nodes: Vec<NodeStorageBaseline> = nodes
        .into_iter()
        .filter(|node| {
            let available_capacity =
                node.storage_total_gb * threshold_ratio - node.baseline_storage_gb;
            available_capacity > 0.0
        })
        .collect();

    // Step 2: Handle edge case - no viable nodes
    if viable_nodes.is_empty() {
        return vec![FaultToleranceCurvePoint {
            user_data_gb: 0.0,
            active_nodes: 0,
            nodes_can_fail: 0,
            participating_nodes: vec![],
        }];
    }

    // Step 3: Calculate total network capacity from viable nodes only
    let total_network_capacity: f64 = viable_nodes
        .iter()
        .map(|node| node.storage_total_gb * threshold_ratio - node.baseline_storage_gb)
        .sum();

    // Step 4: Generate curve iteratively
    let mut curve = Vec::new();
    let mut current_nodes = viable_nodes;
    let mut total_user_data_gb = 0.0;

    // Helper function to calculate fault tolerance for given nodes
    let calculate_fault_tolerance = |num_nodes: usize| -> i32 {
        if num_nodes == 0 {
            0
        } else {
            let fragments_per_node = 30.0 / (num_nodes as f64);
            let min_nodes_needed = (10.0 / fragments_per_node).ceil() as i32;
            ((num_nodes as i32) - min_nodes_needed).clamp(0, 20)
        }
    };

    // Add starting point at x=0
    curve.push(FaultToleranceCurvePoint {
        user_data_gb: 0.0,
        active_nodes: current_nodes.len(),
        nodes_can_fail: calculate_fault_tolerance(current_nodes.len()),
        participating_nodes: current_nodes.clone(),
    });

    // Iteratively find failure points until no nodes remain
    while !current_nodes.is_empty() {
        // Find the node(s) that will hit threshold next
        let next_to_fail = current_nodes
            .iter()
            .min_by(|a, b| {
                let a_available = a.storage_total_gb * threshold_ratio - a.baseline_storage_gb;
                let b_available = b.storage_total_gb * threshold_ratio - b.baseline_storage_gb;
                a_available.partial_cmp(&b_available).unwrap()
            })
            .unwrap();

        // Calculate how much additional user data fills this node
        let node_available_capacity =
            next_to_fail.storage_total_gb * threshold_ratio - next_to_fail.baseline_storage_gb;
        let additional_user_data = node_available_capacity / 3.0 * current_nodes.len() as f64;
        total_user_data_gb += additional_user_data;

        // Remove all nodes that hit threshold at this failure point
        let failure_threshold = node_available_capacity;
        current_nodes.retain(|node| {
            let available = node.storage_total_gb * threshold_ratio - node.baseline_storage_gb;
            available > failure_threshold
        });

        // Add curve point at this failure
        curve.push(FaultToleranceCurvePoint {
            user_data_gb: total_user_data_gb,
            active_nodes: current_nodes.len(),
            nodes_can_fail: calculate_fault_tolerance(current_nodes.len()),
            participating_nodes: current_nodes.clone(),
        });
    }

    curve
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_fault_tolerance_curve_generation() {
        // Test basic 3-node case
        let test_nodes = vec![
            NodeStorageBaseline {
                node_id: 1,
                name: Some("test-node-1".to_string()),
                display_name: "test-node-1".to_string(),
                storage_total_gb: 100.0,
                baseline_storage_gb: 20.0,
                source: hopnet_common::db::NodeSource::System,
                original_values: None,
            },
            NodeStorageBaseline {
                node_id: 2,
                name: Some("test-node-2".to_string()),
                display_name: "test-node-2".to_string(),
                storage_total_gb: 1000.0,
                baseline_storage_gb: 200.0,
                source: hopnet_common::db::NodeSource::System,
                original_values: None,
            },
            NodeStorageBaseline {
                node_id: 3,
                name: None,
                display_name: "Node 3".to_string(),
                storage_total_gb: 10000.0,
                baseline_storage_gb: 1000.0,
                source: hopnet_common::db::NodeSource::System,
                original_values: None,
            },
        ];

        let curve = generate_fault_tolerance_curve(test_nodes, 0.9);
        assert_eq!(curve.len(), 4); // Complete curve: 0→3→2→1→0 nodes

        // Point 1: x=0, all 3 nodes active
        assert_eq!(curve[0].user_data_gb, 0.0);
        assert_eq!(curve[0].active_nodes, 3);
        assert_eq!(curve[0].nodes_can_fail, 2);

        // Point 2: Node 1 fails (100*0.9 - 20 = 70GB available)
        assert_eq!(curve[1].user_data_gb, 70.0);
        assert_eq!(curve[1].active_nodes, 2);
        assert_eq!(curve[1].nodes_can_fail, 1);

        // Point 3: Node 2 fails (1000*0.9 - 200 = 700GB available)
        // Additional data needed: 700GB / 3.0 * 2 nodes = 466.67GB
        // Total: 70 + 466.67 = 536.67GB
        assert!((curve[2].user_data_gb - 536.6666666666666).abs() < 0.001);
        assert_eq!(curve[2].active_nodes, 1);
        assert_eq!(curve[2].nodes_can_fail, 0);

        // Point 4: Node 3 fails (10000*0.9 - 1000 = 8000GB available)
        // Additional data needed: 8000GB / 3.0 * 1 node = 2666.67GB
        // Total: 536.67 + 2666.67 = 3203.33GB
        assert!((curve[3].user_data_gb - 3203.3333333333335).abs() < 0.001);
        assert_eq!(curve[3].active_nodes, 0);
        assert_eq!(curve[3].nodes_can_fail, 0);
    }

    #[test]
    fn test_fault_tolerance_edge_cases() {
        // Test single node: 30 fragments on 1 node, need 1 node min, can fail 0
        let single_node = vec![NodeStorageBaseline {
            node_id: 1,
            name: Some("single-node".to_string()),
            display_name: "single-node".to_string(),
            storage_total_gb: 1000.0,
            baseline_storage_gb: 100.0,
            source: hopnet_common::db::NodeSource::System,
            original_values: None,
        }];
        let curve = generate_fault_tolerance_curve(single_node, 0.9);
        assert_eq!(curve[0].active_nodes, 1);
        assert_eq!(curve[0].nodes_can_fail, 0);

        // Test two nodes: 15 fragments each, need 1 node min (ceil(10/15)=1), can fail 1
        let two_nodes = vec![
            NodeStorageBaseline {
                node_id: 1,
                name: None,
                display_name: "Node 1".to_string(),
                storage_total_gb: 1000.0,
                baseline_storage_gb: 100.0,
                source: hopnet_common::db::NodeSource::System,
                original_values: None,
            },
            NodeStorageBaseline {
                node_id: 2,
                name: None,
                display_name: "Node 2".to_string(),
                storage_total_gb: 1000.0,
                baseline_storage_gb: 100.0,
                source: hopnet_common::db::NodeSource::System,
                original_values: None,
            },
        ];
        let curve = generate_fault_tolerance_curve(two_nodes, 0.9);
        assert_eq!(curve[0].active_nodes, 2);
        assert_eq!(curve[0].nodes_can_fail, 1);

        // Test ten nodes with identical storage: simultaneous failure scenario
        // All nodes: 1000GB total, 100GB baseline
        // 90% threshold = 900GB, minus 100GB baseline = 800GB available capacity each
        // 3 fragments each, need 4 nodes min (ceil(10/3)=4), can fail 6
        let ten_nodes: Vec<NodeStorageBaseline> = (1..=10)
            .map(|i| NodeStorageBaseline {
                node_id: i,
                name: None,
                display_name: format!("Node {}", i),
                storage_total_gb: 1000.0,
                baseline_storage_gb: 100.0,
                source: hopnet_common::db::NodeSource::System,
                original_values: None,
            })
            .collect();
        let curve = generate_fault_tolerance_curve(ten_nodes, 0.9);

        // Initial state: all 10 nodes active
        assert_eq!(curve[0].active_nodes, 10);
        assert_eq!(curve[0].nodes_can_fail, 6);
        assert_eq!(curve[0].user_data_gb, 0.0);

        // Should have exactly 2 curve points: initial state and simultaneous failure cliff
        assert_eq!(curve.len(), 2);

        // All nodes fail simultaneously: 800GB available capacity per node * 10 nodes / 3.0 Reed-Solomon overhead
        let expected = 800.0 / 3.0 * 10.0; // 2666.67GB user data
        assert!((curve[1].user_data_gb - expected).abs() < 0.001);
        assert_eq!(curve[1].active_nodes, 0);
        assert_eq!(curve[1].nodes_can_fail, 0);

        // Test thirty nodes: 1 fragment each, need 10 nodes min, can fail 20 (capped)
        let thirty_nodes: Vec<NodeStorageBaseline> = (1..=30)
            .map(|i| NodeStorageBaseline {
                node_id: i,
                name: None,
                display_name: format!("Node {}", i),
                storage_total_gb: 1000.0,
                baseline_storage_gb: 100.0,
                source: hopnet_common::db::NodeSource::System,
                original_values: None,
            })
            .collect();
        let curve = generate_fault_tolerance_curve(thirty_nodes, 0.9);
        assert_eq!(curve[0].active_nodes, 30);
        assert_eq!(curve[0].nodes_can_fail, 20); // 30-10=20, exactly at cap

        // Test fifty nodes: 0.6 fragments each, need 17 nodes min (ceil(10/0.6)=17), can fail 33 but capped at 20
        let fifty_nodes: Vec<NodeStorageBaseline> = (1..=50)
            .map(|i| NodeStorageBaseline {
                node_id: i,
                name: None,
                display_name: format!("Node {}", i),
                storage_total_gb: 1000.0,
                baseline_storage_gb: 100.0,
                source: hopnet_common::db::NodeSource::System,
                original_values: None,
            })
            .collect();
        let curve = generate_fault_tolerance_curve(fifty_nodes, 0.9);
        assert_eq!(curve[0].active_nodes, 50);
        assert_eq!(curve[0].nodes_can_fail, 20); // 50-17=33, but capped at 20
    }
}
