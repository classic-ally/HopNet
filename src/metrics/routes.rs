use std::net::IpAddr;
use axum::{extract::{State, Query}, response::IntoResponse, http::StatusCode, Json};
use crate::AppState;
use crate::db::metrics::get_metric;
use crate::metrics::collector::{collect_all_node_metrics, CollectionError};
use crate::consensus::functions::consensus_middleware;
use crate::consensus::types::Transaction;
use crate::metrics::{
    latency::{
        listener,
        send_latency
    },
    types::{
        LatencyResponseWrapper,
        LatencyResponse,
        RemoteLatencyQuery,
        Metric,
        ErrorResponse,
    },
};
use duckdb::DuckdbConnectionManager;

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