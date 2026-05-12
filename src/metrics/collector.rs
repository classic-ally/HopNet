use crate::AppState;
use crate::db::consensus::get_consensus;
use crate::db::{DatabaseError, metrics::get_nodes_to_measure};
use crate::metrics::rpc;
use crate::metrics::types::Metric;
use chrono::Utc;
use tokio::time::{Duration as TokioDuration, timeout};

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
    measurement_timeout_per_node: TokioDuration,
) -> Result<Vec<Metric>, CollectionError> {
    let collection_start = std::time::Instant::now();
    let measurement_time = Utc::now();

    tracing::info!("Starting metrics collection for all validator nodes");

    // Get our node ID and current consensus state
    let source_node_id = app_state
        .get_node_id()
        .map_err(|_| CollectionError::ConfigurationError)?;

    let consensus_state = get_consensus(app_state.db_pool.get())?;
    let current_height = consensus_state.committed_block.data.height;

    // Get nodes to measure using database function
    let target_nodes = get_nodes_to_measure(app_state.db_pool.get(), source_node_id)?;

    if target_nodes.is_empty() {
        tracing::warn!(
            "No other nodes found to measure (single-node setup) - proceeding with self-test only"
        );
    }

    tracing::info!(
        "Measuring metrics for {} nodes at height {}",
        target_nodes.len(),
        current_height
    );

    let transport = &app_state.iroh_transport;
    let mut metrics = Vec::new();

    // Measure each node using iroh RPC
    for node in &target_nodes {
        let iroh_node_id = node.pubkey.to_iroh_node_id();
        tracing::debug!("Measuring node {} via iroh", node.node_id);

        // Measure latency using iroh RPC
        let latency_result = timeout(
            measurement_timeout_per_node,
            rpc::measure_latency(transport, node.node_id, iroh_node_id),
        )
        .await;

        // Measure throughput using iroh RPC
        let throughput_result = timeout(
            measurement_timeout_per_node,
            rpc::measure_throughput(transport, node.node_id, iroh_node_id),
        )
        .await;

        // Measure storage using iroh RPC
        let storage_result = timeout(
            measurement_timeout_per_node,
            rpc::query_storage(transport, node.node_id, iroh_node_id),
        )
        .await;

        // Extract throughput measurement result
        let throughput_value = match throughput_result {
            Ok(Ok(throughput)) => {
                tracing::debug!(
                    "Successful throughput measurement for node {}: {} bytes/sec",
                    node.node_id,
                    throughput
                );
                Some(throughput)
            }
            Ok(Err(e)) => {
                tracing::debug!(
                    "Throughput measurement failed for node {}: {}",
                    node.node_id,
                    e
                );
                None
            }
            Err(_) => {
                tracing::debug!("Throughput measurement timeout for node {}", node.node_id);
                None
            }
        };

        // Extract storage measurement result
        let (storage_total_gb, storage_used_gb) = match storage_result {
            Ok(Ok((total, used))) => {
                tracing::debug!(
                    "Successful storage measurement for node {}: {}/{} GB",
                    node.node_id,
                    used,
                    total
                );
                (Some(total), Some(used))
            }
            Ok(Err(e)) => {
                tracing::debug!(
                    "Storage measurement failed for node {}: {}",
                    node.node_id,
                    e
                );
                (None, None)
            }
            Err(_) => {
                tracing::debug!("Storage measurement timeout for node {}", node.node_id);
                (None, None)
            }
        };

        // Extract latency data and availability status from measurement results
        let (rtt_latency, rtt_variance, rtt_jitter, available) = match latency_result {
            Ok(Ok((avg_rtt, variance, jitter))) => {
                tracing::debug!(
                    "Successful latency measurement for node {}: avg={:.2}ms, var={:.2}ms, jitter={:.2}ms",
                    node.node_id,
                    avg_rtt,
                    variance,
                    jitter
                );
                (Some(avg_rtt), Some(variance), Some(jitter), true)
            }
            Ok(Err(e)) => {
                tracing::debug!(
                    "Latency measurement failed for node {}: {}",
                    node.node_id,
                    e
                );
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
            throughput: throughput_value,
            height: current_height,
            available,
            storage_total_gb,
            storage_used_gb,
        };

        metrics.push(metric);

        // Small delay between measurements to be network-friendly
        tokio::time::sleep(TokioDuration::from_millis(500)).await;
    }

    // Add self-test: measure our own storage and submit it
    tracing::debug!("Performing self-storage test for node {}", source_node_id);
    let self_storage_result =
        crate::metrics::routes::calculate_storage_usage(&app_state.fragments_dir).await;

    match self_storage_result {
        Ok(storage) => {
            tracing::debug!(
                "Self-storage measurement successful: {}/{} GB",
                storage.used_gb,
                storage.total_gb
            );

            let self_metric = Metric {
                from_node: source_node_id,
                to_node: source_node_id,
                start_time: measurement_time,
                rtt_latency: None,
                rtt_variance: None,
                rtt_jitter: None,
                throughput: None,
                height: current_height,
                available: true,
                storage_total_gb: Some(storage.total_gb),
                storage_used_gb: Some(storage.used_gb),
            };

            metrics.push(self_metric);
        }
        Err(e) => {
            tracing::warn!("Self-storage measurement failed: {}", e);
        }
    }

    let total_duration = collection_start.elapsed();
    let available_count = metrics.iter().filter(|m| m.available).count();

    tracing::info!(
        "Metrics collection completed in {:.2}s: {} nodes measured, {} available",
        total_duration.as_secs_f64(),
        metrics.len(),
        available_count
    );

    Ok(metrics)
}
