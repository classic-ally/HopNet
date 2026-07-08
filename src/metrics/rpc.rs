//! Metrics measurement RPC over the "metrics" comms scope. This module owns
//! the scope's wire vocabulary ([`MetricsRequest`]/[`MetricsResponse`]); the
//! server side lives in `net::scopes::MetricsScope`.

use crate::AppState;
use hopnet_comms::{CommsError, IrohComms, PeerRef, ProtocolError, Rpc};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Stream I/O timeout for latency pings (lightweight, should be fast)
const LATENCY_TIMEOUT: Duration = Duration::from_secs(5);

/// Stream I/O timeout for throughput uploads (large data, needs more time)
const THROUGHPUT_TIMEOUT: Duration = Duration::from_secs(15);

/// Stream I/O timeout for storage queries (lightweight)
const STORAGE_TIMEOUT: Duration = Duration::from_secs(5);

/// Duration to run latency measurement
const LATENCY_TEST_DURATION: Duration = Duration::from_secs(5);

/// Duration to run throughput upload
const THROUGHPUT_TEST_DURATION: Duration = Duration::from_secs(10);

/// Size of throughput upload chunks (~4MB, matches fragment transfer size, within 8MB MAX_MESSAGE_SIZE)
const THROUGHPUT_CHUNK_SIZE: usize = 4 * 1024 * 1024;

/// Wire request for the "metrics" scope.
#[derive(Serialize, Deserialize, Debug)]
pub enum MetricsRequest {
    /// Latency ping (echo timestamp back for RTT measurement)
    Latency(LatencyPingRequest),
    /// Upload throughput data chunk (server acks, client measures speed)
    Throughput(ThroughputUploadRequest),
    /// Query remote node's storage usage
    Storage,
}

/// Wire response for the "metrics" scope.
#[derive(Serialize, Deserialize, Debug)]
pub enum MetricsResponse {
    /// Latency pong (echoed timestamp)
    Latency(LatencyPongResponse),
    /// Ack for a throughput upload chunk
    ThroughputAck,
    /// Storage query result
    Storage { total_gb: u32, used_gb: u32 },
    Error { message: String },
}

// ============================================================================
// Latency Measurement
// ============================================================================

#[derive(Serialize, Deserialize, Debug)]
pub struct LatencyPingRequest {
    pub timestamp_nanos: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct LatencyPongResponse {
    pub timestamp_nanos: u64,
}

/// Server-side: echo back the timestamp for RTT measurement.
pub fn handle_latency_ping(req: LatencyPingRequest) -> LatencyPongResponse {
    LatencyPongResponse {
        timestamp_nanos: req.timestamp_nanos,
    }
}

/// Client-side: measure latency to a remote node over the mesh.
/// Sends pings for ~5 seconds, discards the first sample, returns (avg_rtt, variance, jitter) in ms.
pub async fn measure_latency(
    comms: &IrohComms,
    peer: &PeerRef,
) -> Result<(f64, f64, f64), CommsError> {
    let start = std::time::Instant::now();
    let mut rtts: Vec<Duration> = Vec::new();

    while start.elapsed() < LATENCY_TEST_DURATION {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        let payload = crate::net::encode_payload(&MetricsRequest::Latency(LatencyPingRequest {
            timestamp_nanos: ts,
        }));
        let send_time = std::time::Instant::now();
        let reply = comms.rpc(peer, "metrics", payload, LATENCY_TIMEOUT).await?;

        match crate::net::decode_payload::<MetricsResponse>(&reply)? {
            MetricsResponse::Latency(resp) if resp.timestamp_nanos == ts => {
                rtts.push(send_time.elapsed());
            }
            MetricsResponse::Latency(_) => {
                // Timestamp mismatch — skip this sample
            }
            MetricsResponse::Error { message } => {
                return Err(CommsError::Protocol(ProtocolError::PeerError(message)));
            }
            other => {
                return Err(CommsError::Protocol(ProtocolError::MalformedResponse(
                    format!("unexpected response to LatencyPing: {:?}", other),
                )));
            }
        }
    }

    // Discard first sample (connection warmup)
    if !rtts.is_empty() {
        rtts.remove(0);
    }

    Ok(calculate_rtt_metrics(rtts))
}

/// Calculate RTT average, variance, and jitter from a series of round-trip durations.
fn calculate_rtt_metrics(rtts: Vec<Duration>) -> (f64, f64, f64) {
    if rtts.is_empty() {
        return (0.0, 0.0, 0.0);
    }

    let ms: Vec<f64> = rtts
        .iter()
        .map(|d| d.as_nanos() as f64 / 1_000_000.0)
        .collect();
    let count = ms.len();

    // RTT Average
    let total: f64 = ms.iter().sum();
    let avg_rtt = total / count as f64;

    // RTT Variance (sample variance)
    let variance = if count == 1 {
        0.0
    } else {
        let sum_squares: f64 = ms.iter().map(|&x| (x - avg_rtt).powi(2)).sum();
        sum_squares / (count as f64 - 1.0)
    };

    // Jitter (standard deviation of inter-packet delays)
    let mut diffs = Vec::new();
    for i in 1..count {
        diffs.push(ms[i] - ms[i - 1]);
    }

    let jitter = if diffs.len() < 2 {
        0.0
    } else {
        let avg_diff = diffs.iter().sum::<f64>() / diffs.len() as f64;
        let sum_diff_squares: f64 = diffs.iter().map(|&x| (x - avg_diff).powi(2)).sum();
        (sum_diff_squares / (diffs.len() as f64 - 1.0)).sqrt()
    };

    (avg_rtt, variance, jitter)
}

// ============================================================================
// Throughput Measurement
// ============================================================================

#[derive(Serialize, Deserialize, Debug)]
pub struct ThroughputUploadRequest {
    pub data: Vec<u8>,
}

/// Client-side: measure upload throughput to a remote node over the mesh.
/// Sends ~4MB chunks for ~10 seconds, measures round-trip time client-side.
pub async fn measure_throughput(comms: &IrohComms, peer: &PeerRef) -> Result<i64, CommsError> {
    let start = std::time::Instant::now();
    let data = vec![0u8; THROUGHPUT_CHUNK_SIZE]; // zeros are fine — we're measuring transport speed
    let mut total_bytes: usize = 0;

    while start.elapsed() < THROUGHPUT_TEST_DURATION {
        let payload = crate::net::encode_payload(&MetricsRequest::Throughput(
            ThroughputUploadRequest { data: data.clone() },
        ));
        let reply = comms
            .rpc(peer, "metrics", payload, THROUGHPUT_TIMEOUT)
            .await?;

        match crate::net::decode_payload::<MetricsResponse>(&reply)? {
            MetricsResponse::ThroughputAck => {
                total_bytes += THROUGHPUT_CHUNK_SIZE;
            }
            MetricsResponse::Error { message } => {
                return Err(CommsError::Protocol(ProtocolError::PeerError(message)));
            }
            other => {
                return Err(CommsError::Protocol(ProtocolError::MalformedResponse(
                    format!("unexpected response to ThroughputUpload: {:?}", other),
                )));
            }
        }
    }

    let duration = start.elapsed();
    let bytes_per_second = if duration.as_secs_f64() > 0.0 {
        (total_bytes as f64 / duration.as_secs_f64()) as i64
    } else {
        0
    };

    Ok(bytes_per_second)
}

// ============================================================================
// Storage Query
// ============================================================================

/// Server-side: handle a storage query from a peer node.
pub async fn handle_storage_query(app_state: &AppState) -> MetricsResponse {
    match crate::metrics::routes::calculate_storage_usage(&app_state.fragments_dir).await {
        Ok(storage) => MetricsResponse::Storage {
            total_gb: storage.total_gb,
            used_gb: storage.used_gb,
        },
        Err(e) => MetricsResponse::Error {
            message: format!("storage query failed: {}", e),
        },
    }
}

/// Client-side: query a remote node's storage usage over the mesh.
pub async fn query_storage(comms: &IrohComms, peer: &PeerRef) -> Result<(u32, u32), CommsError> {
    let payload = crate::net::encode_payload(&MetricsRequest::Storage);
    let reply = comms.rpc(peer, "metrics", payload, STORAGE_TIMEOUT).await?;

    match crate::net::decode_payload::<MetricsResponse>(&reply)? {
        MetricsResponse::Storage { total_gb, used_gb } => Ok((total_gb, used_gb)),
        MetricsResponse::Error { message } => {
            Err(CommsError::Protocol(ProtocolError::PeerError(message)))
        }
        other => Err(CommsError::Protocol(ProtocolError::MalformedResponse(
            format!("unexpected response to StorageQuery: {:?}", other),
        ))),
    }
}
