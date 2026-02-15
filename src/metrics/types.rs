use serde::{Serialize, Deserialize};
use chrono::{DateTime, Utc};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Metric {
    pub from_node: i32,
    pub to_node: i32,
    pub start_time: DateTime<Utc>,
    pub rtt_latency: Option<f64>,
    pub rtt_variance: Option<f64>,
    pub rtt_jitter: Option<f64>,
    pub throughput: Option<i64>,
    pub height: i32,           // Consensus height when measurement was taken
    pub available: bool,       // Whether target node was reachable
    pub storage_total_gb: Option<u32>,  // Total storage capacity in GB
    pub storage_used_gb: Option<u32>,   // Used storage capacity in GB
}

// response for storage metrics
#[derive(Serialize, Deserialize)]
pub struct StorageResponse {
    pub total_gb: u32,
    pub used_gb: u32,
}
