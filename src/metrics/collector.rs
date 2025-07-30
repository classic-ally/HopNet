use crate::db::{DatabaseError, metrics::get_nodes_to_measure};
use crate::AppState;
use crate::metrics::types::Metric;
use crate::metrics::routes::get_remote_latency;
use crate::db::consensus::get_consensus;
use chrono::Utc;
use tokio::time::{timeout, Duration as TokioDuration};

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
        let node_start = std::time::Instant::now();
        
        tracing::debug!("Measuring node {} ({}:{})", node.node_id, node.ip_address, node.port);
        
        // Use existing get_remote_latency function with timeout
        let latency_result = timeout(
            measurement_timeout_per_node,
            get_remote_latency(app_state.db_pool.get(), &node.ip_address)
        ).await;
        
        let node_duration = node_start.elapsed();
        
        // TODO: Add throughput measurement using existing throughput infrastructure
        //       - Integrate metrics/throughput.rs send_throughput() function
        //       - Add throughput server endpoint similar to latency server
        //       - Combine latency + throughput into single measurement cycle
        //       - This is HIGH PRIORITY for complete node reliability scoring
        
        // Create Metric struct based on measurement result
        let metric = match latency_result {
            Ok((status, Some(response))) if status.is_success() => {
                tracing::debug!("Successful measurement for node {}: avg={:.2}ms, var={:.2}ms, jitter={:.2}ms", 
                    node.node_id, response.average_rtt, response.variance, response.jitter);
                
                Metric {
                    from_node: source_node_id,
                    to_node: node.node_id,
                    start_time: measurement_time,
                    duration: node_duration,
                    rtt_latency: Some(response.average_rtt),
                    rtt_variance: Some(response.variance),
                    rtt_jitter: Some(response.jitter),
                    throughput: None, // TODO: Add throughput measurement
                    height: current_height,
                    available: true,
                }
            }
            Ok((status, _)) => {
                tracing::warn!("Failed measurement for node {} - HTTP {}", node.node_id, status);
                
                Metric {
                    from_node: source_node_id,
                    to_node: node.node_id,
                    start_time: measurement_time,
                    duration: node_duration,
                    rtt_latency: None,
                    rtt_variance: None,
                    rtt_jitter: None,
                    throughput: None,
                    height: current_height,
                    available: false,
                }
            }
            Err(_) => {
                tracing::warn!("Timeout measuring node {}", node.node_id);
                
                Metric {
                    from_node: source_node_id,
                    to_node: node.node_id,
                    start_time: measurement_time,
                    duration: node_duration,
                    rtt_latency: None,
                    rtt_variance: None,
                    rtt_jitter: None,
                    throughput: None,
                    height: current_height,
                    available: false,
                }
            }
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