use super::*;
use crate::metrics::types::Metric;
use std::time::{SystemTime, Duration};
use chrono::{DateTime,Utc};

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
                
                // Convert start_time from nanoseconds to SystemTime
                let timestamp_nanos: i64 = row.get(2)?;
                
                // Convert duration from milliseconds to Duration
                let duration_ms: i32 = row.get(3)?;
                
                Ok(Metric {
                    from_node,
                    to_node,
                    start_time: SystemTime::UNIX_EPOCH + Duration::from_nanos(timestamp_nanos as u64),
                    duration: Duration::from_millis(duration_ms as u64),
                    rtt_latency: row.get(4)?,
                    rtt_variance: row.get(5)?,
                    rtt_jitter: row.get(6)?,
                    throughput: row.get(7)?,
                    version: row.get(8)?,
                })
            });

            match results {
                Ok(metrics) => Ok(metrics.collect::<Result<_, _>>().map_err(|_| DatabaseError::ProcessingError)?), // collect into Vec
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
            // Convert SystemTime to DateTime<Utc>
            let start_time_utc: DateTime<Utc> = match metric.start_time {
                SystemTime::UNIX_EPOCH => Utc::now(), // fallback
                _ => match metric.start_time.duration_since(SystemTime::UNIX_EPOCH) {
                    Ok(dur) => DateTime::<Utc>::from(
                        SystemTime::UNIX_EPOCH + dur
                    ),
                    Err(_) => return Err(DatabaseError::RecordError),
                }
            };

            // Convert to ISO string or Unix timestamp in seconds
            let start_time_str = start_time_utc.to_rfc3339(); // "2025-04-16T12:00:00Z"

            let duration_ms = metric.duration.as_millis() as i32;

            tracing::debug!("Inserting metric into database");
            let result = db_lock.execute(
                "INSERT INTO metrics (from_node, to_node, start_time, duration, rtt_latency, rtt_variance, rtt_jitter, throughput, version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    metric.from_node,
                    metric.to_node,
                    start_time_str,
                    duration_ms,
                    metric.rtt_latency,
                    metric.rtt_variance,
                    metric.rtt_jitter,
                    metric.throughput,
                    metric.version,
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