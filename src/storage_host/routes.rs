//! Maintenance, diagnostics, and storage routes for the /files subsystem.
//!
//! Drive-owned (RFC-015, Stage D4): the drive surface — browse, upload,
//! download, mutate (`get_files`, `post_files`, …) plus its router — lives
//! in `hopnet_drive::http::files`; the host mounts it in main.rs. Only the
//! node-local maintenance/storage handlers remain here.

use axum::http::StatusCode;
use axum::{
    Json,
    extract::{Extension, Query, State},
    response::IntoResponse,
};

use crate::db::{self, Blake3Hash, DatabaseError};
use crate::storage_host::functions::encrypt_path;
use serde::{Deserialize, Serialize};

use super::*;

/// Drive-owned (RFC-015, Stage D4): shared ingest wrapper re-exported at
/// the old path so call sites (storage_host::tests) don't churn.
pub use hopnet_drive::http::files::process_uploaded_file;

#[derive(Deserialize)]
pub struct GetQueryParams {
    path: String,
}

#[derive(Deserialize)]
pub struct CleanupQueryParams {
    batch_size: i32,
    retention_days: i64,
}

#[derive(Deserialize)]
pub struct RebalanceQueryParams {
    max_data_blocks: i32,
    min_age_heights: i32,
}

#[derive(Serialize)]
pub struct FileFragmentsResponse {
    pub file_hash: Blake3Hash,
    pub fragments: Vec<(Blake3Hash, crate::db::ChunkType)>,
}

/// GET /fragments
/// Get count of fragments stored locally on this node
/// GET /storage/view — the decay-tiered storage membership view
/// (RFC-STORAGE-002 S2 observability): members, per-node tiers/weights,
/// derived watermark. Every node must report the same view at the same
/// height; orchestrator tests assert that cross-node.
pub async fn get_storage_view(State(app_state): State<AppState>) -> impl IntoResponse {
    use hopnet_storage::traits::StateReader;
    let host = super::substrate_host::SubstrateHost::new(app_state);
    // DB reads on a blocking thread — same discipline as the engine seams.
    let view = tokio::task::spawn_blocking(move || host.storage_view()).await;
    match view {
        Ok(Ok(view)) => {
            #[derive(Serialize)]
            struct StorageViewResponse {
                height: i32,
                watermark: usize,
                members: Vec<i32>,
                tiers: std::collections::HashMap<i32, i64>,
                weights: std::collections::HashMap<i32, u64>,
            }
            (
                StatusCode::OK,
                Json(StorageViewResponse {
                    height: view.height,
                    watermark: view.watermark,
                    members: view.members.iter().map(|p| p.node_id).collect(),
                    tiers: view.tiers,
                    weights: view.weights,
                }),
            )
                .into_response()
        }
        Ok(Err(e)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("storage view: {e}"),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("storage view join: {e}"),
        )
            .into_response(),
    }
}

pub async fn get_fragments_count(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>, // Extract user_id from JWT via auth middleware
) -> impl IntoResponse {
    match crate::db::files::get_local_fragment_count(app_state.db_pool.get()) {
        Ok(count) => {
            #[derive(Serialize)]
            struct FragmentCountResponse {
                locally_stored_fragments: i64,
            }

            (
                StatusCode::OK,
                Json(FragmentCountResponse {
                    locally_stored_fragments: count,
                }),
            )
                .into_response()
        }
        Err(e) => {
            tracing::error!("Failed to get local fragment count: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// Manual trigger for orphaned data block cleanup
pub async fn post_cleanup_orphaned_data_blocks(
    State(app_state): State<AppState>,
    Query(params): Query<CleanupQueryParams>,
    Extension(uid): Extension<i32>,
) -> impl IntoResponse {
    tracing::info!(
        "Manual cleanup trigger requested by user {} (batch_size: {}, retention_days: {})",
        uid,
        params.batch_size,
        params.retention_days
    );

    // Run the cleanup job directly with parameters
    match super::jobs::run_orphaned_data_block_cleanup(
        &app_state,
        params.batch_size,
        params.retention_days,
    )
    .await
    {
        Ok(data_blocks_cleaned) => {
            #[derive(Serialize)]
            struct CleanupResponse {
                status: String,
                data_blocks_cleaned: usize,
            }

            let response = CleanupResponse {
                status: "success".to_string(),
                data_blocks_cleaned,
            };

            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            tracing::error!("Manual cleanup failed: {:?}", e);

            #[derive(Serialize)]
            struct ErrorResponse {
                status: String,
                error: String,
            }

            let response = ErrorResponse {
                status: "error".to_string(),
                error: format!("Cleanup failed: {:?}", e),
            };

            (StatusCode::INTERNAL_SERVER_ERROR, Json(response)).into_response()
        }
    }
}

/// POST /maintenance/rebalance
/// Manually trigger network rebalancing to redistribute fragments to optimal nodes
pub async fn post_rebalance_network(
    State(app_state): State<AppState>,
    Query(params): Query<RebalanceQueryParams>,
    Extension(uid): Extension<i32>,
) -> impl IntoResponse {
    tracing::info!(
        "Manual rebalancing trigger requested by user {} (max_data_blocks: {}, min_age_heights: {})",
        uid,
        params.max_data_blocks,
        params.min_age_heights
    );

    // Validate parameters
    if params.max_data_blocks <= 0 {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "status": "error",
                "error": "max_data_blocks must be positive"
            })),
        )
            .into_response();
    }

    if params.min_age_heights < 0 {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "status": "error",
                "error": "min_age_heights cannot be negative"
            })),
        )
            .into_response();
    }

    // Run the rebalancing job directly with parameters
    match super::jobs::run_network_rebalancing(
        &app_state,
        params.max_data_blocks,
        params.min_age_heights,
    )
    .await
    {
        Ok(result) => {
            tracing::info!("Manual rebalancing completed: {:?}", result);
            (StatusCode::OK, Json(result)).into_response()
        }
        Err(e) => {
            tracing::error!("Manual rebalancing failed: {:?}", e);

            #[derive(Serialize)]
            struct ErrorResponse {
                status: String,
                error: String,
            }

            let response = ErrorResponse {
                status: "error".to_string(),
                error: format!("Rebalancing failed: {:?}", e),
            };

            (StatusCode::INTERNAL_SERVER_ERROR, Json(response)).into_response()
        }
    }
}

/// GET /diagnostics/fragment-inventory-differential
/// Returns the differential between consensus inventory and local fragments
/// Used for testing and monitoring the self-attestation system
pub async fn get_fragment_inventory_differential(
    State(app_state): State<AppState>,
    Extension(uid): Extension<i32>,
) -> impl IntoResponse {
    let node_id = match app_state.get_node_id() {
        Ok(id) => id,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    match crate::db::inventory::compute_inventory_differential(app_state.db_pool.get(), node_id) {
        Ok(differential) => {
            tracing::debug!(
                "Fragment inventory differential computed for node {}: {} added, {} removed",
                node_id,
                differential.fragments_added.len(),
                differential.fragments_removed.len()
            );
            (StatusCode::OK, Json(differential)).into_response()
        }
        Err(e) => {
            tracing::error!("Failed to compute fragment inventory differential: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// POST /maintenance/fragment-inventory-self-check
/// Manually trigger fragment inventory self-check and consensus submission
pub async fn post_fragment_inventory_self_check(
    State(app_state): State<AppState>,
    Extension(uid): Extension<i32>,
) -> impl IntoResponse {
    tracing::info!(
        "Manual fragment inventory self-check triggered by user {}",
        uid
    );

    match super::jobs::run_fragment_inventory_self_check(&app_state).await {
        Ok(_) => StatusCode::OK.into_response(),
        Err(e) => {
            tracing::error!("Manual fragment inventory self-check failed: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

#[derive(Deserialize)]
pub struct OrphanedFragmentsScanParams {
    #[serde(default = "default_grace_period_hours")]
    grace_period_hours: i64,
}

fn default_grace_period_hours() -> i64 {
    1
}

/// GET /maintenance/orphaned-fragments
/// Scan filesystem for fragments not in database (older than grace_period_hours)
/// Returns scan results and stores them for subsequent DELETE operation
pub async fn get_orphaned_fragments_scan(
    State(app_state): State<AppState>,
    Extension(uid): Extension<i32>,
    Query(params): Query<OrphanedFragmentsScanParams>,
) -> impl IntoResponse {
    tracing::info!(
        "Orphaned fragments scan triggered by user {} (grace_period_hours: {})",
        uid,
        params.grace_period_hours
    );

    if params.grace_period_hours < 0 {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "status": "error",
                "error": "grace_period_hours cannot be negative"
            })),
        )
            .into_response();
    }

    match super::jobs::run_orphaned_fragments_scan(&app_state, params.grace_period_hours).await {
        Ok(scan_result) => {
            tracing::info!(
                "Scan complete: {} orphaned fragments found ({} bytes)",
                scan_result.orphaned_fragments.len(),
                scan_result.total_bytes
            );
            (StatusCode::OK, Json(scan_result)).into_response()
        }
        Err(e) => {
            tracing::error!("Orphaned fragments scan failed: {:?}", e);

            #[derive(Serialize)]
            struct ErrorResponse {
                status: String,
                error: String,
            }

            let response = ErrorResponse {
                status: "error".to_string(),
                error: format!("Scan failed: {:?}", e),
            };

            (StatusCode::INTERNAL_SERVER_ERROR, Json(response)).into_response()
        }
    }
}

/// DELETE /maintenance/orphaned-fragments
/// Delete orphaned fragments based on previous scan results
/// Validates scan exists and isn't stale (> 1 hour old)
pub async fn delete_orphaned_fragments(
    State(app_state): State<AppState>,
    Extension(uid): Extension<i32>,
) -> impl IntoResponse {
    tracing::info!("Orphaned fragments cleanup triggered by user {}", uid);

    match super::jobs::run_orphaned_fragments_cleanup(&app_state).await {
        Ok(result) => {
            tracing::info!(
                "Cleanup complete: {} deleted, {} failed, {} bytes freed",
                result.deleted_count,
                result.failed_count,
                result.bytes_freed
            );
            (StatusCode::OK, Json(result)).into_response()
        }
        Err(e) => {
            tracing::error!("Orphaned fragments cleanup failed: {:?}", e);

            #[derive(Serialize)]
            struct ErrorResponse {
                status: String,
                error: String,
            }

            let response = ErrorResponse {
                status: "error".to_string(),
                error: format!("{:?}", e),
            };

            (StatusCode::INTERNAL_SERVER_ERROR, Json(response)).into_response()
        }
    }
}

/// GET /diagnostics/file-fragments
/// Returns complete fragment distribution data for a specific file
/// Shows which nodes have which fragments according to fragment_inventory
pub async fn get_file_fragment_distribution(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
    Query(params): Query<GetQueryParams>,
) -> impl IntoResponse {
    // Encrypt path server-side (following existing pattern)
    let session = match app_state.get_session(user_id).await {
        Ok(s) => s,
        Err(e) => return e.into_response(),
    };

    let encrypted_path = match encrypt_path(params.path, &session.siv_key, &session.siv_nonce).await
    {
        Ok(path) => path,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    // Query database for fragment distribution
    match crate::db::debug::get_file_fragment_distribution(
        app_state.db_pool.get(),
        encrypted_path,
        user_id,
    ) {
        Ok(distribution) => {
            tracing::debug!(
                "Fragment distribution query for file {}: {} fragments ({} original, {} recovery)",
                distribution.inode_id,
                distribution.fragment_count,
                distribution.original_count,
                distribution.recovery_count
            );
            (StatusCode::OK, Json(distribution)).into_response()
        }
        Err(DatabaseError::NotFound) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => {
            tracing::error!("Failed to get file fragment distribution: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// GET /diagnostics/network-resilience
/// Returns network-wide file resilience statistics for system overview dashboard
/// Shows distribution of files across fault tolerance levels (cliff chart data)
pub async fn get_network_resilience_stats(
    State(app_state): State<AppState>,
    Extension(uid): Extension<i32>,
) -> Result<Json<hopnet_common::db::NetworkResilienceStats>, StatusCode> {
    tracing::info!("Network resilience statistics requested by user {}", uid);

    match crate::db::resilience::compute_network_resilience_stats(app_state.db_pool.get()) {
        Ok(stats) => {
            tracing::info!(
                "Network resilience computed: {} total files ({} unknown, {} unrecoverable, {} critical, {} good, {} excellent, {} exceptional) in {}ms",
                stats.total_files,
                stats.unknown.file_count,
                stats.unrecoverable.file_count,
                stats.critical.file_count,
                stats.good.file_count,
                stats.excellent.file_count,
                stats.exceptional.file_count,
                stats.computation_time_ms
            );
            Ok(Json(stats))
        }
        Err(e) => {
            tracing::error!("Failed to compute network resilience statistics: {:?}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}
