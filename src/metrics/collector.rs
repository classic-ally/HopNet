use crate::db::{DatabaseError, metrics::get_nodes_to_measure};
use crate::AppState;
use crate::metrics::types::Metric;
use crate::metrics::routes::get_remote_latency;
use crate::metrics::throughput::send_throughput;
use crate::db::consensus::get_consensus;
use chrono::Utc;
use tokio::time::{timeout, Duration as TokioDuration};
use std::net::IpAddr;
use ed25519_dalek::{Signature, Signer};

/// Error types for metrics collection
#[derive(Debug)]
pub enum CollectionError {
    DatabaseError(DatabaseError),
    NetworkError(String),
    ConfigurationError,
}

impl From<DatabaseError> for CollectionError {
    fn from(err: DatabaseError) -> Self {
        CollectionError::DatabaseError(err)
    }
}

/// Measure throughput to a remote node using Option D: Session-based results retrieval
async fn get_remote_throughput(str_ip: &str, app_state: &AppState) -> Result<i64, String> {
    use serde::{Deserialize, Serialize};
    
    #[derive(Deserialize)]
    struct ThroughputServerResponse {
        port: u16,
        session_id: crate::db::CustomUUID,
    }
    
    #[derive(Deserialize)]
    struct ThroughputResult {
        throughput_bps: i64,
        total_bytes: usize,
        duration_ms: u64,
        client_addr: String,
    }
    
    // Get authentication credentials
    let my_node_id = app_state.get_node_id()
        .map_err(|_| "Node not properly configured".to_string())?;
    let user_keys = app_state.get_user_keys()
        .map_err(|_| "User keys not available".to_string())?;
    let user_id = app_state.get_user_id()
        .map_err(|_| "User ID not available".to_string())?;
    
    // Step 1: Get throughput server port and session ID from remote node
    let url = format!("http://{}:34632/rpc/throughput-server", str_ip);
    
    // Prepare authentication for GET request (empty body)
    let body = b"";
    let node_signature = app_state.private_key.sign(body);
    let user_signature = user_keys.private_key.sign(body);
    
    let client = reqwest::Client::new();
    let response = client
        .get(&url)
        .header("X-Node-ID", my_node_id.to_string())
        .header("X-User-ID", user_id.to_string())
        .header("X-Node-Signature", hex::encode(node_signature.to_bytes()))
        .header("X-User-Signature", hex::encode(user_signature.to_bytes()))
        .send()
        .await
        .map_err(|e| format!("Failed to connect to throughput server: {}", e))?;
    
    if !response.status().is_success() {
        return Err(format!("Throughput server returned status: {}", response.status()));
    }
    
    let server_response: ThroughputServerResponse = response.json().await
        .map_err(|e| format!("Failed to parse server response: {}", e))?;
    
    let ip = str_ip.parse::<IpAddr>()
        .map_err(|_| "Invalid IP address".to_string())?;
    
    // Step 2: Send throughput data to remote receiver port
    send_throughput(ip, server_response.port).await
        .map_err(|e| format!("Failed to send throughput data: {}", e))?;
    
    // Step 3: Poll for measurement results using session ID
    let results_url = format!("http://{}:34632/rpc/throughput-result/{}", 
        str_ip, *server_response.session_id);
    
    // Poll with exponential backoff for up to 15 seconds (10s test + 5s buffer)
    let mut poll_delay = tokio::time::Duration::from_millis(500);
    let max_polls = 8; // ~15 seconds total with exponential backoff
    
    for attempt in 1..=max_polls {
        tokio::time::sleep(poll_delay).await;
        
        // Use authenticated client for polling results
        match client
            .get(&results_url)
            .header("X-Node-ID", my_node_id.to_string())
            .header("X-User-ID", user_id.to_string())
            .header("X-Node-Signature", hex::encode(node_signature.to_bytes()))
            .header("X-User-Signature", hex::encode(user_signature.to_bytes()))
            .send()
            .await {
            Ok(response) if response.status().is_success() => {
                match response.json::<ThroughputResult>().await {
                    Ok(result) => {
                        tracing::debug!("Retrieved throughput result from {} after {} attempts: {} bytes/sec",
                            str_ip, attempt, result.throughput_bps);
                        return Ok(result.throughput_bps);
                    }
                    Err(e) => {
                        tracing::debug!("Failed to parse throughput result: {}", e);
                        return Err("Failed to parse throughput result".to_string());
                    }
                }
            }
            Ok(response) if response.status() == reqwest::StatusCode::NOT_FOUND => {
                // Result not ready yet, continue polling
                tracing::debug!("Throughput result not ready yet, attempt {}/{}", attempt, max_polls);
            }
            Ok(response) => {
                return Err(format!("Unexpected response status: {}", response.status()));
            }
            Err(e) => {
                return Err(format!("Failed to poll for results: {}", e));
            }
        }
        
        // Exponential backoff: 500ms, 1s, 2s, 4s, etc.
        poll_delay = std::cmp::min(poll_delay * 2, tokio::time::Duration::from_secs(4));
    }
    
    Err("Timed out waiting for throughput measurement results".to_string())
}

/// Collect metrics for all active validator nodes, returning Vec<Metric> ready for database
pub async fn collect_all_node_metrics(
    app_state: &AppState,
    measurement_timeout_per_node: TokioDuration
) -> Result<Vec<Metric>, CollectionError> {
    let collection_start = std::time::Instant::now();
    let measurement_time = Utc::now();
    
    tracing::info!("Starting metrics collection for all validator nodes");
    
    // Get our node ID and current consensus state
    let source_node_id = app_state.get_node_id()
        .map_err(|_| CollectionError::ConfigurationError)?;
    
    let consensus_state = get_consensus(app_state.db_pool.get())?;
    let current_height = consensus_state.committed_block.data.height;
    
    // Get nodes to measure using database function
    let target_nodes = get_nodes_to_measure(
        app_state.db_pool.get(), 
        source_node_id
    )?;
    
    if target_nodes.is_empty() {
        tracing::warn!("No nodes found to measure");
        return Ok(vec![]);
    }
    
    tracing::info!("Measuring metrics for {} nodes at height {}", target_nodes.len(), current_height);
    
    let mut metrics = Vec::new();
    
    // Measure each node using existing infrastructure
    for node in &target_nodes {
        tracing::debug!("Measuring node {} ({}:{})", node.node_id, node.ip_address, node.port);
        
        // Measure latency using existing infrastructure
        let latency_result = timeout(
            measurement_timeout_per_node,
            get_remote_latency(app_state.db_pool.get(), &node.ip_address)
        ).await;
        
        // Measure throughput using newly integrated infrastructure
        let throughput_result = timeout(
            measurement_timeout_per_node,
            get_remote_throughput(&node.ip_address, app_state)
        ).await;
        
        // Extract throughput measurement result
        let throughput_value = match throughput_result {
            Ok(Ok(throughput)) => {
                tracing::debug!("Successful throughput measurement for node {}: {} bytes/sec", 
                    node.node_id, throughput);
                Some(throughput)
            }
            Ok(Err(e)) => {
                tracing::debug!("Throughput measurement failed for node {}: {}", node.node_id, e);
                None
            }
            Err(_) => {
                tracing::debug!("Throughput measurement timeout for node {}", node.node_id);
                None
            }
        };
        
        // Extract latency data and availability status from measurement results
        let (rtt_latency, rtt_variance, rtt_jitter, available) = match latency_result {
            Ok((status, Some(response))) if status.is_success() => {
                tracing::debug!("Successful latency measurement for node {}: avg={:.2}ms, var={:.2}ms, jitter={:.2}ms", 
                    node.node_id, response.average_rtt, response.variance, response.jitter);
                (Some(response.average_rtt), Some(response.variance), Some(response.jitter), true)
            }
            Ok((status, _)) => {
                tracing::warn!("Failed latency measurement for node {} - HTTP {}", node.node_id, status);
                (None, None, None, false)
            }
            Err(_) => {
                tracing::warn!("Timeout measuring latency for node {}", node.node_id);
                (None, None, None, false)
            }
        };

        // Create single Metric struct with extracted data
        let metric = Metric {
            from_node: source_node_id,
            to_node: node.node_id,
            start_time: measurement_time,
            rtt_latency,
            rtt_variance,
            rtt_jitter,
            throughput: throughput_value, // May be Some even if latency failed
            height: current_height,
            available,
        };
        
        metrics.push(metric);
        
        // Small delay between measurements to be network-friendly
        tokio::time::sleep(TokioDuration::from_millis(500)).await;
    }
    
    let total_duration = collection_start.elapsed();
    let available_count = metrics.iter().filter(|m| m.available).count();
    
    tracing::info!("Metrics collection completed in {:.2}s: {} nodes measured, {} available", 
        total_duration.as_secs_f64(), metrics.len(), available_count);
    
    Ok(metrics)
}