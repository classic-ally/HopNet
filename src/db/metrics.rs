use super::*;
use crate::metrics::types::Metric;
use chrono::DateTime;

pub fn get_metric(
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
) -> Result<Vec<Metric>, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            // Prepare the query
            let mut stmt = db_lock.prepare("SELECT * FROM metrics").map_err(|_| DatabaseError::RecallError)?;
            // Execute the query and map each row to a Metric
            let results = stmt.query_map([], |row| {
                let from_node: i32 = row.get(0)?;
                let to_node: i32 = row.get(1)?;
                
                // Read timestamp as i64 (DuckDB stores as microseconds since Unix epoch)
                let timestamp_microseconds: i64 = row.get(2)?;
                let timestamp_seconds = timestamp_microseconds / 1_000_000;
                let nanoseconds = ((timestamp_microseconds % 1_000_000) * 1000) as u32;
                let start_time = DateTime::from_timestamp(timestamp_seconds, nanoseconds)
                    .ok_or_else(|| {
                        tracing::error!("Invalid timestamp: {} microseconds", timestamp_microseconds);
                        duckdb::Error::InvalidColumnName("start_time".to_string())
                    })?;
                
                Ok(Metric {
                    from_node,
                    to_node,
                    start_time,
                    rtt_latency: row.get(3)?,
                    rtt_variance: row.get(4)?,
                    rtt_jitter: row.get(5)?,
                    throughput: row.get(6)?,
                    height: row.get(7)?,        // New: consensus height
                    available: row.get(8)?,     // New: node availability
                    storage_total_gb: row.get(9)?,  // New: storage capacity
                    storage_used_gb: row.get(10)?,  // New: storage utilization
                })
            });

            match results {
                Ok(metrics) => {
                    metrics.collect::<Result<Vec<_>, _>>()
                        .map_err(|e| {
                            tracing::error!("Error parsing metric row: {:?}", e);
                            DatabaseError::ProcessingError
                        })
                },
                Err(e) => {
                    tracing::error!("Error querying metrics: {:?}", e);
                    Err(DatabaseError::RecordError)
                }
            }
        },
        Err(e) => {
            tracing::error!("Database connection error in get_metric: {:?}", e);
            Err(DatabaseError::LockError)
        }
    }
}

pub fn insert_metric(
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
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
        Err(_) => Err(DatabaseError::LockError)
    }
}

/// Insert multiple metrics in a batch transaction for efficiency
pub fn insert_metrics_batch(
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
    metrics: Vec<Metric>,
    execute: bool,
) -> Result<(), DatabaseError> {
    match db_connection {
        Ok(mut db_lock) => {
            let tx = db_lock.transaction().map_err(|_| DatabaseError::LockError)?;
            
            let metrics_len = metrics.len();
            for metric in metrics {
                let start_time_str = metric.start_time.to_rfc3339();
                tx.execute(
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
            
            if execute {
                tx.commit().map_err(|_| DatabaseError::InsertError)?;
                tracing::debug!("Successfully inserted {} metrics in batch", metrics_len);
            } else {
                // Validation only - rollback the transaction
                tx.rollback().map_err(|_| DatabaseError::InsertError)?;
                tracing::debug!("Validated {} metrics batch (dry run)", metrics_len);
            }
            Ok(())
        }
        Err(_) => Err(DatabaseError::LockError)
    }
}

/// Get all network nodes excluding specified node (for metrics collection)
pub fn get_nodes_to_measure(
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
    exclude_node_id: i32
) -> Result<Vec<crate::types::Node>, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            let mut stmt = db_lock.prepare(
                "SELECT node_id, name, ip_address, port, owner, pubkey
                FROM nodes
                WHERE node_id != ?
                ORDER BY node_id"
            ).map_err(|_| DatabaseError::RecallError)?;
            
            let nodes = stmt.query_map([exclude_node_id], |row| {
                Ok(crate::types::Node {
                    node_id: row.get(0)?,
                    name: row.get(1)?,
                    ip_address: row.get(2)?,
                    port: row.get(3)?,
                    owner: row.get(4)?,
                    pubkey: row.get(5)?,
                })
            }).map_err(|_| DatabaseError::RecallError)?
            .collect::<Result<Vec<crate::types::Node>, _>>()
            .map_err(|_| DatabaseError::RecallError)?;
            
            tracing::debug!("Found {} network nodes to measure (excluding node {})", 
                nodes.len(), exclude_node_id);
            Ok(nodes)
        }
        Err(_) => Err(DatabaseError::LockError)
    }
}

/// Computed node scores for placement algorithm
#[derive(Debug, Clone, serde::Serialize)]
pub struct NodeMetrics {
    pub node_id: i32,
    pub sample_count_7d: u32,
    pub trust_factor: f64,
    pub availability_score: f64,      // Time-weighted: 24h * 0.7 + 7d * 0.3
    pub throughput_score: f64,        // Log-normalized with consistency factor
    pub latency_score: f64,           // Inverse normalized latency
    pub stability_score: f64,         // Inverse of 7d latency variance
    pub storage_utilization: f64,     // used_gb / total_gb ratio
    pub storage_multiplier: f64,      // e^(-5 * utilization)
}

/// Get computed placement scores for all nodes at specific consensus height
/// Uses DuckDB analytical functions to calculate RFC-compliant score components
/// Includes all registered nodes (validators + storage-only nodes) for fragment placement
pub fn get_all_node_metrics(
    db_connection: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
    consensus_height: i32,
) -> Result<Vec<NodeMetrics>, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            let query = 
                "WITH 
                -- Aggregate metrics for nodes that have data
                node_metrics AS (
                    SELECT 
                        m.to_node as node_id,
                        -- 24-hour metrics
                        COUNT(CASE WHEN m.start_time >= (CURRENT_TIMESTAMP AT TIME ZONE 'UTC' - INTERVAL 24 HOUR) THEN 1 END) as sample_count_24h,
                        AVG(CASE WHEN m.available AND m.start_time >= (CURRENT_TIMESTAMP AT TIME ZONE 'UTC' - INTERVAL 24 HOUR) THEN 1.0 ELSE 0.0 END) as availability_24h,
                        AVG(CASE WHEN m.start_time >= (CURRENT_TIMESTAMP AT TIME ZONE 'UTC' - INTERVAL 24 HOUR) THEN m.rtt_latency END) as avg_latency_24h,
                        -- 7-day metrics
                        COUNT(*) as sample_count_7d,
                        AVG(CASE WHEN m.available THEN 1.0 ELSE 0.0 END) as availability_7d,
                        STDDEV(m.rtt_latency) as latency_variance_7d,
                        -- Throughput and storage (use 7-day window for more stable estimates)
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY m.throughput) as median_throughput,
                        STDDEV(m.throughput) as throughput_stddev,
                        MAX(m.storage_total_gb) as storage_total_gb,
                        MAX(m.storage_used_gb) as storage_used_gb
                    FROM metrics m
                    WHERE m.height <= ?
                      AND m.start_time >= (CURRENT_TIMESTAMP AT TIME ZONE 'UTC' - INTERVAL 7 DAY)
                    GROUP BY m.to_node
                ),
                -- Calculate network-wide fallback statistics per RFC requirements
                network_stats AS (
                    SELECT 
                        COALESCE(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY availability_24h), 0.8) as median_availability_24h,
                        COALESCE(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY availability_7d), 0.8) as median_availability_7d,
                        COALESCE(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY avg_latency_24h), 50.0) as p75_latency,
                        COALESCE(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY median_throughput), 1000000) as median_throughput,
                        COALESCE(AVG(CAST(storage_used_gb AS REAL) / CAST(storage_total_gb AS REAL)), 0.3) as avg_storage_utilization
                    FROM node_metrics
                    WHERE storage_total_gb > 0
                )
                SELECT 
                    n.node_id,
                    COALESCE(nm.sample_count_7d, 0) as sample_count_7d,
                    -- Trust factor: gradual confidence building for new nodes
                    LEAST(CAST(COALESCE(nm.sample_count_7d, 0) AS REAL) / 100.0, 1.0) as trust_factor,
                    -- Time-weighted availability score (RFC formula)
                    COALESCE(nm.availability_24h, ns.median_availability_24h * 0.5) * 0.7 + 
                    COALESCE(nm.availability_7d, ns.median_availability_7d * 0.5) * 0.3 as availability_score,
                    -- Throughput score: log-normalized percentile rank with consistency
                    CASE 
                        WHEN nm.median_throughput IS NOT NULL THEN
                            (LOG(1 + (PERCENT_RANK() OVER (ORDER BY nm.median_throughput)) * 9) / LOG(10)) *
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
                    -- Storage multiplier: exponential decay (RFC formula)
                    EXP(-5.0 * CASE 
                        WHEN nm.storage_total_gb IS NOT NULL AND nm.storage_total_gb > 0 THEN
                            CAST(nm.storage_used_gb AS REAL) / CAST(nm.storage_total_gb AS REAL)
                        ELSE 0.5
                    END) as storage_multiplier
                FROM nodes n
                LEFT JOIN node_metrics nm ON n.node_id = nm.node_id
                CROSS JOIN network_stats ns
                ORDER BY n.node_id";
            
            // Only consensus height parameter needed now
            let params = [&consensus_height as &dyn duckdb::ToSql];
            
            let mut stmt = db_lock.prepare(&query).map_err(|e| {
                tracing::error!("Failed to prepare metrics query: {:?}", e);
                DatabaseError::RecallError
            })?;
            
            let metrics = stmt.query_map(&params, |row| {
                Ok(NodeMetrics {
                    node_id: row.get("node_id")?,
                    sample_count_7d: row.get::<_, i64>("sample_count_7d")? as u32,
                    trust_factor: row.get("trust_factor")?,
                    availability_score: row.get("availability_score")?,
                    throughput_score: row.get("throughput_score")?,
                    latency_score: row.get("latency_score")?,
                    stability_score: row.get("stability_score")?,
                    storage_utilization: row.get("storage_utilization")?,
                    storage_multiplier: row.get("storage_multiplier")?,
                })
            }).map_err(|e| {
                tracing::error!("Failed to execute metrics query: {:?}", e);
                DatabaseError::RecallError
            })?
            .collect::<Result<Vec<NodeMetrics>, _>>()
            .map_err(|e| {
                tracing::error!("Failed to parse metrics results: {:?}", e);
                DatabaseError::ProcessingError
            })?;
            
            tracing::debug!("Retrieved metrics for {} nodes at height {}", 
                metrics.len(), consensus_height);
            
            Ok(metrics)
        }
        Err(_) => Err(DatabaseError::LockError)
    }
}