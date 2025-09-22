use std::net::IpAddr;
use axum::{extract::{Path, Query, State}, http::StatusCode, response::{IntoResponse, Response}, Extension, Json};
use crate::{consensus::routes::AuthenticatedUser, AppState};
use crate::db::metrics::{get_metric, get_all_node_metrics};
use crate::metrics::collector::{collect_all_node_metrics, CollectionError};
use crate::consensus::functions::consensus_middleware;
use crate::consensus::types::Transaction;
use crate::metrics::{
    latency::{
        listener,
        send_latency
    },
    throughput::{downloader, send_throughput},
    functions::{ThroughputResult, ThroughputResultCollector},
    types::{
        LatencyResponseWrapper,
        LatencyResponse,
        RemoteLatencyQuery,
        Metric,
        ErrorResponse,
        StorageResponse,
    },
};
use crate::files::placement::{FragmentType, calculate_final_placement_scores};
use duckdb::DuckdbConnectionManager;
use serde::{Deserialize, Serialize};
use crate::db::CustomUUID;

#[derive(Serialize)]
pub struct ThroughputServerResponse {
    pub port: u16,
    pub session_id: CustomUUID,
}

#[derive(Deserialize)]
pub struct PlacementScoresQuery {
    pub height: i32,
    pub fragment_type: Option<String>,  // "original" or "recovery"
}

pub async fn get_metrics(
    State(app_state): State<AppState>,
) -> impl IntoResponse {
    match get_metric(app_state.db_pool.get()) {
        Ok(metrics) => {
            tracing::debug!("Retrieved {} metrics from database", metrics.len());
            (StatusCode::OK, Json(metrics))
        }
        Err(e) => {
            tracing::error!("Failed to retrieve metrics: {:?}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(Vec::<Metric>::new()))
        }
    }
}

pub async fn get_remote_latency_handler(
    Query(params): Query<RemoteLatencyQuery>,
    State(app_state): State<AppState>,
) -> impl IntoResponse {
    let (status, response) = get_remote_latency(app_state.db_pool.get(), &params.ip).await;
    match response {
        Some(latency_response) => (status, Json(LatencyResponseWrapper::Success(latency_response))),
        None => (status, Json(LatencyResponseWrapper::Error(ErrorResponse { error: "Failed to get remote latency".to_string() })))
    }
}

pub async fn get_remote_latency(
    db_conn: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
    str_ip: &str
) -> (StatusCode, Option<LatencyResponse>) {
    // let's hit the remote
    let url = format!("http://{}:34632/rpc/latency-server", str_ip);
    match reqwest::get(&url).await {
        Ok(response) => {
            if response.status().is_success() {
                match response.text().await {
                    Ok(str) => {
                        match str.parse::<u16>() {
                            // yes it's a u16
                            Ok(port) => {
                                match str_ip.parse::<IpAddr>() {
                                    Ok(ip) => {
                                        match send_latency(ip, port).await {
                                            Ok((average_rtt, variance, jitter)) => {
                                                let response = LatencyResponse {
                                                    address: str_ip.to_string() + ":" + &str,
                                                    average_rtt: average_rtt,
                                                    variance: variance,
                                                    jitter: jitter,
                                                };
                                                return (StatusCode::OK, Some(response));
                                            }
                                            Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, None)
                                        }
                                    }
                                    Err(_) => (StatusCode::UNPROCESSABLE_ENTITY, None)
                                }
                            }
                            // port isn't a u16-> gateway is naughty
                            Err(_) => (StatusCode::BAD_GATEWAY, None)
                        }
                    }
                    Err(_) => (StatusCode::BAD_GATEWAY, None)
                }
            } else {
                return (response.status(), None)
            }
        }
        Err(e) => {
            // handle reqwest errors
            match e.status() {
                Some(status) => (status, None),
                None => (StatusCode::GATEWAY_TIMEOUT, None)
            }
        }
    }
}

pub async fn get_latency_server() -> impl IntoResponse {
    match listener().await {
        Ok((_, latency_port)) => {
            (StatusCode::CREATED, Json(latency_port))
        }
        Err(_error) => {
            (StatusCode::INTERNAL_SERVER_ERROR, Json(0))
        }
    }
}

#[axum::debug_handler]
pub async fn get_throughput_server(
    State(app_state): State<AppState>,
    Extension(_auth): Extension<AuthenticatedUser>,
) -> impl IntoResponse {
    match downloader().await {
        Ok((task_handle, throughput_port)) => {
            // Generate session ID for this measurement
            let session_id = CustomUUID::new(None);
            let session_id_clone = session_id.clone();
            
            // Spawn task to handle measurement and store results
            let collector = app_state.throughput_result_collector.clone();
            tokio::spawn(async move {
                if let Ok(result) = task_handle.await {
                    match result {
                        Ok((timestamp, client_addr, total_bytes, duration)) => {
                            let throughput_result = ThroughputResult {
                                throughput_bps: if duration.as_secs_f64() > 0.0 {
                                    (total_bytes as f64 / duration.as_secs_f64()) as i64
                                } else {
                                    0
                                },
                                total_bytes,
                                duration_ms: duration.as_millis() as u64,
                                client_addr: client_addr.to_string(),
                            };
                            
                            collector.store_result(session_id_clone.clone(), throughput_result).await;
                        }
                        Err(e) => {
                            tracing::debug!("Throughput measurement failed for session {:?}: {:?}", session_id_clone, e);
                        }
                    }
                }
            });
            
            let response = ThroughputServerResponse {
                port: throughput_port,
                session_id,
            };
            
            (StatusCode::CREATED, Json(response))
        }
        Err(_error) => {
            let error_response = ThroughputServerResponse {
                port: 0,
                session_id: CustomUUID::new(None),
            };
            (StatusCode::INTERNAL_SERVER_ERROR, Json(error_response))
        }
    }
}

#[axum::debug_handler]
pub async fn get_throughput_result(
    State(app_state): State<AppState>,
    Path(session_id): Path<CustomUUID>,
    Extension(_auth): Extension<AuthenticatedUser>,
) -> impl IntoResponse {
    let session_id_for_log = session_id.clone();
    match app_state.throughput_result_collector.get_result(session_id).await {
        Some(result) => {
            tracing::debug!("Retrieved throughput result for session {:?}: {} bytes/sec", 
                session_id_for_log, result.throughput_bps);
            (StatusCode::OK, Json(serde_json::json!(result)))
        }
        None => {
            tracing::debug!("No throughput result found for session {:?}", session_id_for_log);
            (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "Session not found or expired"})))
        }
    }
}

pub async fn get_storage_server(
    State(app_state): State<AppState>,
) -> impl IntoResponse {
    // Calculate local storage metrics using configured fragments directory
    match calculate_storage_usage(&app_state.fragments_dir).await {
        Ok(storage) => (StatusCode::OK, Json(storage)),
        Err(e) => {
            tracing::error!("Failed to calculate storage usage: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(StorageResponse {
                total_gb: 0,
                used_gb: 0,
            }))
        }
    }
}

pub async fn get_placement_scores(
    Query(params): Query<PlacementScoresQuery>,
    State(app_state): State<AppState>,
) -> impl IntoResponse {
    // Get all node metrics at the specified consensus height
    let node_metrics = match get_all_node_metrics(app_state.db_pool.get(), params.height) {
        Ok(metrics) => metrics,
        Err(e) => {
            tracing::error!("Failed to retrieve node metrics for height {}: {:?}", params.height, e);
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({
                "error": "Failed to retrieve node metrics"
            })));
        }
    };

    // If fragment_type is specified, apply weighted scoring
    if let Some(fragment_type_str) = params.fragment_type {
        let fragment_type = match fragment_type_str.as_str() {
            "original" => FragmentType::Original,
            "recovery" => FragmentType::Recovery,
            _ => {
                return (StatusCode::BAD_REQUEST, Json(serde_json::json!({
                    "error": "fragment_type must be 'original' or 'recovery'"
                })));
            }
        };

        // Calculate final placement scores using the placement algorithm
        let scored_candidates = calculate_final_placement_scores(node_metrics, fragment_type);
        
        (StatusCode::OK, Json(serde_json::json!({
            "height": params.height,
            "fragment_type": fragment_type_str,
            "weighted_scores": scored_candidates
        })))
    } else {
        // Return raw metrics without fragment-specific weighting
        (StatusCode::OK, Json(serde_json::json!({
            "height": params.height,
            "raw_metrics": node_metrics
        })))
    }
}

pub async fn calculate_storage_usage(fragments_dir: &str) -> Result<StorageResponse, Box<dyn std::error::Error>> {
    use tokio::task;
    
    let fragments_dir_owned = fragments_dir.to_string();
    let (total_bytes, used_bytes) = task::spawn_blocking(move || -> Result<(u64, u64), std::io::Error> {
        use fs4::statvfs;
        
        let stats = statvfs(&fragments_dir_owned)?;
        let total = stats.total_space();
        let available = stats.available_space();
        let used = total - available;
        
        Ok((total, used))
    }).await.map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?
      .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
    
    // Convert to GB (using 1024^3 for consistency)
    let used_gb = (used_bytes / (1024_u64.pow(3))) as u32;
    let total_gb = (total_bytes / (1024_u64.pow(3))) as u32;
    
    Ok(StorageResponse {
        total_gb,
        used_gb,
    })
}

/// GET endpoint to manually trigger metrics collection and return results
pub async fn get_metrics_trigger(
    State(app_state): State<AppState>,
) -> impl IntoResponse {
    tracing::info!("Manual metrics collection triggered via GET");
    
    // Get node_id for consensus middleware (metrics collection needs authenticated node)
    let source_node_id = match app_state.get_node_id() {
        Ok(id) => id,
        Err(_) => {
            tracing::error!("Node not properly configured for metrics collection");
            return (StatusCode::PRECONDITION_REQUIRED, Json(serde_json::json!({
                "error": "Node not properly configured"
            })));
        }
    };
    
    // Use 30 second timeout per node for manual testing (more generous than background)
    let timeout_per_node = tokio::time::Duration::from_secs(30);
    
    // Collect metrics using the reusable collector
    let metrics = match collect_all_node_metrics(&app_state, timeout_per_node).await {
        Ok(metrics) => metrics,
        Err(CollectionError::DatabaseError(e)) => {
            tracing::error!("Database error during metrics collection: {:?}", e);
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({
                "error": "Database error during metrics collection"
            })));
        }
        Err(CollectionError::ConfigurationError) => {
            tracing::error!("Configuration error: Node not properly configured");
            return (StatusCode::PRECONDITION_REQUIRED, Json(serde_json::json!({
                "error": "Node not properly configured"
            })));
        }
        Err(CollectionError::NetworkError(msg)) => {
            tracing::error!("Network error during metrics collection: {}", msg);
            return (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({
                "error": format!("Network error: {}", msg)
            })));
        }
    };
    
    if metrics.is_empty() {
        tracing::warn!("No metrics collected - no network nodes to measure");
        return (StatusCode::OK, Json(serde_json::json!({
            "message": "No network nodes to measure",
            "metrics": []
        })));
    }
    
    // Submit metrics to consensus for distributed storage
    match bincode::serde::encode_to_vec(&metrics, bincode::config::standard()) {
        Ok(encoded_metrics) => {
            let transaction = Transaction {
                function: "submit_metrics".to_string(),
                payload: encoded_metrics,
            };
            let transactions = vec![transaction];

            // Use consensus middleware to ensure distributed agreement
            match consensus_middleware(&app_state, transactions, source_node_id).await {
                Ok(()) => {
                    let available_count = metrics.iter().filter(|m| m.available).count();
                    tracing::info!("Successfully submitted {} metrics to consensus ({} nodes available)", 
                        metrics.len(), available_count);
                    
                    (StatusCode::OK, Json(serde_json::json!({
                        "message": "Metrics collection and consensus submission completed successfully",
                        "collected": metrics.len(),
                        "available_nodes": available_count,
                        "metrics": metrics
                    })))
                }
                Err(e) => {
                    tracing::error!("Consensus middleware error for metrics submission: {:?}", e);
                    (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({
                        "error": "Failed to submit metrics to consensus",
                        "metrics": metrics
                    })))
                }
            }
        }
        Err(e) => {
            tracing::error!("Bincode encoding error for metrics: {:?}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({
                "error": "Failed to encode metrics for consensus",
                "metrics": metrics
            })))
        }
    }
}