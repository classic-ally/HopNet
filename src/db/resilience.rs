use r2d2::PooledConnection;
use duckdb::DuckdbConnectionManager;
use duckdb::params;

use crate::db::DatabaseError;
use hopnet_common::db::{NetworkResilienceStats, ResilienceLevel, NodeStorageBaseline, FaultToleranceCurvePoint};

/// Compute network-wide file resilience statistics using OLAP-optimized query
/// Returns distribution of files across fault tolerance levels
pub fn compute_network_resilience_stats(
    db_connection: Result<PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
) -> Result<NetworkResilienceStats, DatabaseError> {
    let start_time = std::time::Instant::now();

    match db_connection {
        Ok(conn) => {
            let query = r#"
                WITH
                -- Step 1: Get original_chunks count for each data block from fragment_hashes
                data_block_original_chunks AS (
                    SELECT
                        data_block_id,
                        COUNT(*) as original_chunks
                    FROM fragment_hashes
                    WHERE chunk_type = 'original'
                    GROUP BY data_block_id
                ),

                -- Step 2: Vectorized fragment distribution (columnar friendly)
                fragment_node_counts AS (
                    SELECT
                        fh.data_block_id,
                        fi.node_id,
                        COUNT(*) as fragments_on_node,
                        dbo.original_chunks
                    FROM fragment_hashes fh
                    JOIN data_block_original_chunks dbo ON fh.data_block_id = dbo.data_block_id
                    LEFT JOIN fragment_inventory fi ON fh.fragment_hash = fi.fragment_hash
                        AND (
                            -- Include nodes that are either available in metrics OR have no metrics data
                            fi.node_id IN (
                                SELECT DISTINCT from_node FROM metrics
                                WHERE available = true
                                AND height IN (
                                    SELECT MAX(height)
                                    FROM metrics m2
                                    WHERE m2.from_node = metrics.from_node
                                )
                            )
                            OR NOT EXISTS (SELECT 1 FROM metrics WHERE from_node = fi.node_id)
                        )
                    WHERE fi.node_id IS NOT NULL  -- Only include rows with valid inventory data
                    GROUP BY fh.data_block_id, fi.node_id, dbo.original_chunks
                ),

                -- Step 3: Window function ranking (vectorized, no arrays needed)
                ranked_nodes AS (
                    SELECT
                        data_block_id,
                        original_chunks,
                        fragments_on_node,
                        ROW_NUMBER() OVER (PARTITION BY data_block_id ORDER BY fragments_on_node DESC) as node_rank,
                        SUM(fragments_on_node) OVER (PARTITION BY data_block_id) as total_fragments,
                        -- Running sum of largest nodes (for fault tolerance calc)
                        SUM(fragments_on_node) OVER (
                            PARTITION BY data_block_id
                            ORDER BY fragments_on_node DESC
                            ROWS UNBOUNDED PRECEDING
                        ) as cumulative_largest_fragments
                    FROM fragment_node_counts
                ),

                -- Step 4: Vectorized fault tolerance calculation
                file_fault_tolerance AS (
                    SELECT
                        data_block_id,
                        original_chunks,
                        total_fragments,
                        -- Calculate how many nodes can fail before unrecoverable
                        COALESCE(
                            MAX(
                                CASE
                                    WHEN (total_fragments - cumulative_largest_fragments + fragments_on_node) >= original_chunks
                                    THEN node_rank - 1
                                    ELSE NULL
                                END
                            ),
                            -1  -- Unrecoverable if no case matches
                        ) as fault_tolerance_level
                    FROM ranked_nodes
                    GROUP BY data_block_id, original_chunks, total_fragments
                ),

                -- Step 5: Files without attestation data (unknown status)
                files_without_attestation AS (
                    SELECT DISTINCT
                        fh.data_block_id,
                        -2 as classified_level  -- Special code for "unknown"
                    FROM fragment_hashes fh
                    LEFT JOIN fragment_inventory fi ON fh.fragment_hash = fi.fragment_hash
                    WHERE fi.fragment_hash IS NULL  -- No attestation data at all
                ),

                -- Step 6: Classify fault tolerance levels
                fault_tolerance_classified AS (
                    SELECT
                        data_block_id,
                        CASE
                            WHEN total_fragments < original_chunks THEN -1  -- Unrecoverable
                            WHEN fault_tolerance_level = -1 THEN -1         -- Also unrecoverable
                            WHEN fault_tolerance_level >= 3 THEN 3          -- Cap at 3+ for display
                            ELSE fault_tolerance_level
                        END as classified_level
                    FROM file_fault_tolerance

                    UNION ALL

                    SELECT data_block_id, classified_level FROM files_without_attestation
                )

                -- Step 7: Final aggregation (DuckDB's sweet spot)
                SELECT
                    classified_level as fault_tolerance_level,
                    COUNT(*) as file_count,
                    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
                FROM fault_tolerance_classified
                GROUP BY classified_level
                ORDER BY classified_level DESC
            "#;

            let mut stmt = conn.prepare(query).map_err(|_| DatabaseError::ProcessingError)?;

            let rows = stmt.query_map(params![], |row| {
                let level: i32 = row.get(0)?;
                let count: i64 = row.get(1)?;
                let percentage: f64 = row.get(2)?;
                Ok((level, count as u32, percentage))
            }).map_err(|_| DatabaseError::RecallError)?;

            // Initialize all levels to zero
            let mut unknown = ResilienceLevel { file_count: 0, percentage: 0.0 };
            let mut unrecoverable = ResilienceLevel { file_count: 0, percentage: 0.0 };
            let mut critical = ResilienceLevel { file_count: 0, percentage: 0.0 };
            let mut good = ResilienceLevel { file_count: 0, percentage: 0.0 };
            let mut excellent = ResilienceLevel { file_count: 0, percentage: 0.0 };
            let mut exceptional = ResilienceLevel { file_count: 0, percentage: 0.0 };
            let mut total_files = 0u32;

            // Process results
            for row_result in rows {
                let (level, count, percentage) = row_result.map_err(|_| DatabaseError::RecallError)?;
                total_files += count;

                match level {
                    -2 => unknown = ResilienceLevel { file_count: count, percentage },
                    -1 => unrecoverable = ResilienceLevel { file_count: count, percentage },
                    0 => critical = ResilienceLevel { file_count: count, percentage },
                    1 => good = ResilienceLevel { file_count: count, percentage },
                    2 => excellent = ResilienceLevel { file_count: count, percentage },
                    3 => exceptional = ResilienceLevel { file_count: count, percentage },
                    _ => {
                        tracing::warn!("Unexpected fault tolerance level: {}", level);
                    }
                }
            }

            let computation_time_ms = start_time.elapsed().as_millis() as u64;

            tracing::debug!(
                "Network resilience computed: {} files total in {}ms",
                total_files, computation_time_ms
            );

            Ok(NetworkResilienceStats {
                unknown,
                unrecoverable,
                critical,
                good,
                excellent,
                exceptional,
                total_files,
                computation_time_ms,
            })
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}

/// Get node storage baselines for fault tolerance curve generation
/// Returns each node's total capacity and baseline usage for simulation
pub fn get_node_storage_baselines(
    db_connection: Result<PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
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
                    WHERE fh.chunk_type = 'original'
                    GROUP BY fh.data_block_id
                ),

                -- Calculate current HopNet storage per node
                node_hopnet_storage AS (
                    SELECT
                        fi.node_id,
                        SUM((db.file_size::DOUBLE / GREATEST(dboc.original_count, 1)) * 1.1 / (1024.0 * 1024.0 * 1024.0)) as hopnet_storage_gb
                    FROM fragment_inventory fi
                    JOIN fragment_hashes fh ON fi.fragment_hash = fh.fragment_hash
                    JOIN data_blocks db ON fh.data_block_id = db.id
                    JOIN data_block_original_counts dboc ON db.id = dboc.data_block_id
                    GROUP BY fi.node_id
                ),

                -- Get latest storage metrics for each node
                latest_node_metrics AS (
                    SELECT DISTINCT ON (to_node)
                        to_node as node_id,
                        storage_total_gb,
                        storage_used_gb
                    FROM metrics
                    WHERE storage_total_gb > 0
                    ORDER BY to_node, height DESC, start_time DESC
                )

                SELECT
                    n.node_id,
                    n.name,
                    COALESCE(n.name, 'Node ' || n.node_id) as display_name,
                    lnm.storage_total_gb,
                    -- Baseline: current usage minus HopNet = x=0 point on curve
                    GREATEST(0.0, lnm.storage_used_gb - COALESCE(nhs.hopnet_storage_gb, 0.0)) as baseline_storage_gb
                FROM nodes n
                INNER JOIN latest_node_metrics lnm ON n.node_id = lnm.node_id
                LEFT JOIN node_hopnet_storage nhs ON n.node_id = nhs.node_id
                ORDER BY lnm.storage_total_gb DESC
            "#;

            let mut stmt = conn.prepare(query).map_err(|e| {
                tracing::error!("Failed to prepare query for node storage baselines: {:?}", e);
                DatabaseError::ProcessingError
            })?;

            let rows = stmt.query_map(params![], |row| {
                Ok(NodeStorageBaseline {
                    node_id: row.get(0)?,
                    name: row.get(1)?,
                    display_name: row.get(2)?,
                    storage_total_gb: row.get(3)?,
                    baseline_storage_gb: row.get(4)?,
                    source: hopnet_common::db::NodeSource::System,
                    original_values: None,
                })
            }).map_err(|e| {
                tracing::error!("Failed to execute query for node storage baselines: {:?}", e);
                DatabaseError::RecallError
            })?;

            let baselines: Vec<NodeStorageBaseline> = rows
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| DatabaseError::RecallError)?;

            let computation_time_ms = start_time.elapsed().as_millis() as u64;

            tracing::debug!(
                "Retrieved storage baselines for {} nodes in {}ms",
                baselines.len(), computation_time_ms
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
            let available_capacity = node.storage_total_gb * threshold_ratio - node.baseline_storage_gb;
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
            ((num_nodes as i32) - min_nodes_needed).max(0).min(20)
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
        let node_available_capacity = next_to_fail.storage_total_gb * threshold_ratio - next_to_fail.baseline_storage_gb;
        let additional_user_data = node_available_capacity / 3.0 * current_nodes.len() as f64;
        total_user_data_gb += additional_user_data;

        // Remove all nodes that hit threshold at this failure point
        let failure_threshold = node_available_capacity;
        current_nodes = current_nodes
            .into_iter()
            .filter(|node| {
                let available = node.storage_total_gb * threshold_ratio - node.baseline_storage_gb;
                available > failure_threshold
            })
            .collect();

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
    fn test_resilience_level_serialization() {
        let level = ResilienceLevel {
            file_count: 42,
            percentage: 15.7,
        };

        let json = serde_json::to_string(&level).unwrap();
        let deserialized: ResilienceLevel = serde_json::from_str(&json).unwrap();

        assert_eq!(level.file_count, deserialized.file_count);
        assert_eq!(level.percentage, deserialized.percentage);
    }

    #[test]
    fn test_network_stats_serialization() {
        let stats = NetworkResilienceStats {
            unknown: ResilienceLevel { file_count: 0, percentage: 0.0 },
            unrecoverable: ResilienceLevel { file_count: 5, percentage: 2.1 },
            critical: ResilienceLevel { file_count: 15, percentage: 6.3 },
            good: ResilienceLevel { file_count: 80, percentage: 33.6 },
            excellent: ResilienceLevel { file_count: 100, percentage: 42.0 },
            exceptional: ResilienceLevel { file_count: 38, percentage: 16.0 },
            total_files: 238,
            computation_time_ms: 156,
        };

        let json = serde_json::to_string(&stats).unwrap();
        let deserialized: NetworkResilienceStats = serde_json::from_str(&json).unwrap();

        assert_eq!(stats.total_files, deserialized.total_files);
        assert_eq!(stats.computation_time_ms, deserialized.computation_time_ms);
    }

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
        }];
        let curve = generate_fault_tolerance_curve(single_node, 0.9);
        assert_eq!(curve[0].active_nodes, 1);
        assert_eq!(curve[0].nodes_can_fail, 0);

        // Test two nodes: 15 fragments each, need 1 node min (ceil(10/15)=1), can fail 1
        let two_nodes = vec![
            NodeStorageBaseline { node_id: 1, name: None, display_name: "Node 1".to_string(), storage_total_gb: 1000.0, baseline_storage_gb: 100.0, source: hopnet_common::db::NodeSource::System, original_values: None },
            NodeStorageBaseline { node_id: 2, name: None, display_name: "Node 2".to_string(), storage_total_gb: 1000.0, baseline_storage_gb: 100.0, source: hopnet_common::db::NodeSource::System, original_values: None },
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
            })
            .collect();
        let curve = generate_fault_tolerance_curve(fifty_nodes, 0.9);
        assert_eq!(curve[0].active_nodes, 50);
        assert_eq!(curve[0].nodes_can_fail, 20); // 50-17=33, but capped at 20
    }
}