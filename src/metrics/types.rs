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
    pub available: bool        // Whether target node was reachable
}

#[derive(Deserialize)]
pub struct RemoteLatencyQuery {
    pub ip: String,
}

// response for remote latency
#[derive(Serialize)]
pub struct LatencyResponse {
    pub address: String,
    pub average_rtt: f64,
    pub variance: f64,
    pub jitter: f64,
}

// error response
#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

// unified response wrapper
#[derive(Serialize)]
#[serde(untagged)]
pub enum LatencyResponseWrapper {
    Success(LatencyResponse),
    Error(ErrorResponse),
}