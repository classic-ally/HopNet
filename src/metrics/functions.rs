use std::collections::HashMap;
use tokio::sync::Mutex;
use chrono::{DateTime, Utc};
use crate::db::CustomUUID;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThroughputResult {
    pub throughput_bps: i64,
    pub total_bytes: usize,
    pub duration_ms: u64,
    pub client_addr: String,
}

pub struct ThroughputResultCollector {
    // Map: session_id -> ThroughputResult
    pending_results: Mutex<HashMap<CustomUUID, ThroughputResult>>,
}

impl ThroughputResultCollector {
    pub fn new() -> Self {
        Self {
            pending_results: Mutex::new(HashMap::new()),
        }
    }
    
    pub async fn store_result(&self, session_id: CustomUUID, result: ThroughputResult) {
        // Clean up old results first (following TimeoutVoteCollector pattern)
        self.cleanup_old_results().await;
        
        let mut pending = self.pending_results.lock().await;
        tracing::debug!("Stored throughput result for session {:?}: {} bytes/sec", 
            session_id, result.throughput_bps);
        
        pending.insert(session_id, result);
    }
    
    pub async fn get_result(&self, session_id: CustomUUID) -> Option<ThroughputResult> {
        // Clean up old results first (following TimeoutVoteCollector pattern)
        self.cleanup_old_results().await;
        
        let mut pending = self.pending_results.lock().await;
        let result = pending.remove(&session_id); // Immediate cleanup on retrieval
        
        if result.is_some() {
            tracing::debug!("Retrieved and removed throughput result for session {:?}", session_id);
        } else {
            tracing::debug!("No throughput result found for session {:?}", session_id);
        }
        
        result
    }
    
    async fn cleanup_old_results(&self) {
        let cutoff_time = Utc::now() - chrono::Duration::hours(1);
        let mut pending = self.pending_results.lock().await;
        let initial_count = pending.len();
        
        pending.retain(|uuid, _| {
            match uuid.get_timestamp() {
                Some(timestamp) => {
                    let (seconds, nanos) = timestamp.to_unix();
                    let timestamp_millis = seconds as i64 * 1000 + nanos as i64 / 1_000_000;
                    timestamp_millis > cutoff_time.timestamp_millis()
                }
                None => false, // Remove invalid UUIDs
            }
        });
        
        let removed_count = initial_count - pending.len();
        if removed_count > 0 {
            tracing::debug!("Cleaned up {} old throughput results", removed_count);
        }
    }
    
    pub async fn get_pending_count(&self) -> usize {
        let pending = self.pending_results.lock().await;
        pending.len()
    }
}