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
                "INSERT INTO metrics (from_node, to_node, start_time, rtt_latency, rtt_variance, rtt_jitter, throughput, height, available) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
                    "INSERT INTO metrics (from_node, to_node, start_time, rtt_latency, rtt_variance, rtt_jitter, throughput, height, available) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
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