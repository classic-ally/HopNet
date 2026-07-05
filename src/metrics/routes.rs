use crate::AppState;
use crate::db::metrics::{get_all_node_metrics, get_metric};
use crate::files::placement::{FragmentType, calculate_final_placement_scores};
use crate::metrics::collector::{CollectionError, collect_all_node_metrics};
use crate::metrics::types::{Metric, StorageResponse};
use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::Deserialize;

#[derive(Deserialize)]
pub struct PlacementScoresQuery {
    pub height: i32,
    pub fragment_type: Option<String>, // "original" or "recovery"
}

pub async fn get_metrics(State(app_state): State<AppState>) -> impl IntoResponse {
    match get_metric(app_state.db_pool.get()) {
        Ok(metrics) => {
            tracing::debug!("Retrieved {} metrics from database", metrics.len());
            (StatusCode::OK, Json(metrics))
        }
        Err(e) => {
            tracing::error!("Failed to retrieve metrics: {:?}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(Vec::<Metric>::new()),
            )
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
            tracing::error!(
                "Failed to retrieve node metrics for height {}: {:?}",
                params.height,
                e
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "Failed to retrieve node metrics"
                })),
            );
        }
    };

    // If fragment_type is specified, apply weighted scoring
    if let Some(fragment_type_str) = params.fragment_type {
        let fragment_type = match fragment_type_str.as_str() {
            "original" => FragmentType::Original,
            "recovery" => FragmentType::Recovery,
            _ => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "error": "fragment_type must be 'original' or 'recovery'"
                    })),
                );
            }
        };

        // Calculate final placement scores using the placement algorithm
        let scored_candidates = calculate_final_placement_scores(node_metrics, fragment_type);

        (
            StatusCode::OK,
            Json(serde_json::json!({
                "height": params.height,
                "fragment_type": fragment_type_str,
                "weighted_scores": scored_candidates
            })),
        )
    } else {
        // Return raw metrics without fragment-specific weighting
        (
            StatusCode::OK,
            Json(serde_json::json!({
                "height": params.height,
                "raw_metrics": node_metrics
            })),
        )
    }
}

pub async fn calculate_storage_usage(
    fragments_dir: &str,
) -> Result<StorageResponse, Box<dyn std::error::Error>> {
    use tokio::task;

    let fragments_dir_owned = fragments_dir.to_string();
    let (total_bytes, used_bytes) =
        task::spawn_blocking(move || -> Result<(u64, u64), std::io::Error> {
            use fs4::statvfs;

            let stats = statvfs(&fragments_dir_owned)?;
            let total = stats.total_space();
            let available = stats.available_space();
            let used = total - available;

            Ok((total, used))
        })
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

    // Convert to GB (using 1024^3 for consistency)
    let used_gb = (used_bytes / (1024_u64.pow(3))) as u32;
    let total_gb = (total_bytes / (1024_u64.pow(3))) as u32;

    Ok(StorageResponse { total_gb, used_gb })
}

/// GET endpoint to manually trigger metrics collection and return results
pub async fn get_metrics_trigger(State(app_state): State<AppState>) -> impl IntoResponse {
    tracing::info!("Manual metrics collection triggered via GET");

    // Get node_id for consensus middleware (metrics collection needs authenticated node)
    let source_node_id = match app_state.get_node_id() {
        Ok(id) => id,
        Err(_) => {
            tracing::error!("Node not properly configured for metrics collection");
            return (
                StatusCode::PRECONDITION_REQUIRED,
                Json(serde_json::json!({
                    "error": "Node not properly configured"
                })),
            );
        }
    };

    // Use 30 second timeout per node for manual testing (more generous than background)
    let timeout_per_node = tokio::time::Duration::from_secs(30);

    // Collect metrics using the reusable collector
    let metrics = match collect_all_node_metrics(&app_state, timeout_per_node).await {
        Ok(metrics) => metrics,
        Err(CollectionError::DatabaseError(e)) => {
            tracing::error!("Database error during metrics collection: {:?}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "Database error during metrics collection"
                })),
            );
        }
        Err(CollectionError::ConfigurationError) => {
            tracing::error!("Configuration error: Node not properly configured");
            return (
                StatusCode::PRECONDITION_REQUIRED,
                Json(serde_json::json!({
                    "error": "Node not properly configured"
                })),
            );
        }
        Err(CollectionError::NetworkError(msg)) => {
            tracing::error!("Network error during metrics collection: {}", msg);
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({
                    "error": format!("Network error: {}", msg)
                })),
            );
        }
    };

    if metrics.is_empty() {
        tracing::warn!("No metrics collected - no network nodes to measure");
        return (
            StatusCode::OK,
            Json(serde_json::json!({
                "message": "No network nodes to measure",
                "metrics": []
            })),
        );
    }

    // Submit metrics to consensus for distributed storage
    match bincode::serde::encode_to_vec(&metrics, bincode::config::standard()) {
        Ok(encoded_metrics) => {
            let transaction = match crate::consensus::dispatch::create_signed_transaction(
                &app_state,
                "submit_metrics".to_string(),
                encoded_metrics,
            ) {
                Ok(tx) => tx,
                Err(_) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({"error": "Failed to sign transaction"})),
                    );
                }
            };
            // Submit to consensus queue
            match app_state.consensus_queue.submit(transaction).await {
                Ok(()) => {
                    let available_count = metrics.iter().filter(|m| m.available).count();
                    tracing::info!(
                        "Successfully submitted {} metrics to consensus ({} nodes available)",
                        metrics.len(),
                        available_count
                    );

                    (
                        StatusCode::OK,
                        Json(serde_json::json!({
                            "message": "Metrics collection and consensus submission completed successfully",
                            "collected": metrics.len(),
                            "available_nodes": available_count,
                            "metrics": metrics
                        })),
                    )
                }
                Err(e) => {
                    tracing::error!("Consensus middleware error for metrics submission: {:?}", e);
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({
                            "error": "Failed to submit metrics to consensus",
                            "metrics": metrics
                        })),
                    )
                }
            }
        }
        Err(e) => {
            tracing::error!("Bincode encoding error for metrics: {:?}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "Failed to encode metrics for consensus",
                    "metrics": metrics
                })),
            )
        }
    }
}
