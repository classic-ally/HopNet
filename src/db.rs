use duckdb::{params, Connection, Error};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime,Duration};
use serde::{Serialize,Deserialize};
use chrono::{DateTime, Utc};

pub enum DatabaseError {
    LockError,
    InsertError,
    RecordError,
    RecallError,
    ProcessingError
}
#[derive(Serialize, Deserialize)]
#[derive(Debug)]
pub struct Metric {
    pub from_node: i32,
    pub to_node: i32,
    pub start_time: SystemTime,
    pub duration: Duration,
    pub rtt_latency: Option<f64>,
    pub rtt_variance: Option<f64>,
    pub rtt_jitter: Option<f64>,
    pub throughput: Option<i64>,
    pub version: u8
}

pub fn initialize() -> Result<Arc<Mutex<Connection>>, Error> {
    let db = Connection::open(":memory:")?;
    db.execute_batch(
        "
            CREATE TABLE metrics (
                from_node       INTEGER NOT NULL,
                to_node         INTEGER NOT NULL,
                start_time      TIMESTAMP NOT NULL,
                duration        SMALLINT NOT NULL,
                rtt_latency     REAL,
                rtt_variance    REAL,
                rtt_jitter      REAL,
                throughput      BIGINT,
                version         TINYINT NOT NULL,
                PRIMARY KEY     (from_node, to_node, start_time)
            );

            -- Create indexes for common query patterns
            CREATE INDEX idx_start_time ON metrics(start_time);
            CREATE INDEX idx_version ON metrics(version);

            -- Add comments for documentation
            COMMENT ON TABLE metrics IS 'Network performance metrics between distributed system nodes';
            COMMENT ON COLUMN metrics.duration IS 'Measurement duration in milliseconds (max ~32 seconds)';
            COMMENT ON COLUMN metrics.rtt_latency IS 'Round-trip time latency in milliseconds';
            COMMENT ON COLUMN metrics.rtt_variance IS 'RTT variance in milliseconds';
            COMMENT ON COLUMN metrics.rtt_jitter IS 'RTT jitter in milliseconds';
            COMMENT ON COLUMN metrics.throughput IS 'Network throughput in bytes per second';
            COMMENT ON COLUMN metrics.version IS 'Schema version for backwards compatibility';
        "
    )?;
    Ok(Arc::new(Mutex::new(db)))
}

pub fn insert_metric(
    db: &Arc<Mutex<Connection>>,
    metric: Metric,
) -> Result<(), DatabaseError> {
    match db.lock() {
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

            dbg!("Attempting to place metric into db");
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
                    dbg!("Successfully placed metric into db");
                    Ok(())
                }
                Err(e) => {
                    dbg!(e);
                    Err(DatabaseError::InsertError)
                }
            }
        }
        Err(_) => Err(DatabaseError::LockError)
    }
}

pub fn get_metric(
    db: &Arc<Mutex<Connection>>,
) -> Result<Vec<Metric>, DatabaseError> {
    match db.lock() {
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
                    dbg!(e);
                    Err(DatabaseError::RecordError)
                }
            }
        },
        Err(e) => {
            dbg!(e);
            Err(DatabaseError::LockError)
        }
    }
}