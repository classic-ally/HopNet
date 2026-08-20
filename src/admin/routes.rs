use crate::AppState;
use axum::{
    Router,
    extract::{Extension, State},
    http::StatusCode,
    response::Json,
    routing::{get, post},
};

pub fn admin_routes() -> Router<AppState> {
    Router::new()
        .route("/system-nodes-baseline", get(get_system_nodes_baseline))
        .route(
            "/hypothetical-fault-tolerance",
            post(analyze_hypothetical_fault_tolerance),
        )
}

/// POST /admin/hypothetical-fault-tolerance
/// Analyzes fault tolerance curves for hypothetical network configurations
/// Useful for network planning and capacity analysis
async fn analyze_hypothetical_fault_tolerance(
    State(_state): State<AppState>,
    Extension(uid): Extension<i32>,
    Json(nodes): Json<Vec<hopnet_common::db::NodeStorageBaseline>>,
) -> Result<Json<Vec<hopnet_common::db::FaultToleranceCurvePoint>>, StatusCode> {
    tracing::info!(
        "Hypothetical fault tolerance analysis requested by user {} for {} nodes",
        uid,
        nodes.len()
    );

    // Validate input
    if nodes.is_empty() {
        tracing::warn!("Empty node list provided for hypothetical analysis");
        return Err(StatusCode::BAD_REQUEST);
    }

    if nodes.len() > 1000 {
        tracing::warn!(
            "Too many nodes ({}) provided for hypothetical analysis",
            nodes.len()
        );
        return Err(StatusCode::BAD_REQUEST);
    }

    // Use hardcoded 90% threshold (network setting)
    let threshold_ratio = 0.9;

    // Generate fault tolerance curve
    let curve = crate::db::resilience::generate_fault_tolerance_curve(nodes, threshold_ratio);

    tracing::info!(
        "Generated hypothetical fault tolerance curve with {} points for user {}",
        curve.len(),
        uid
    );

    Ok(Json(curve))
}

/// GET /admin/system-nodes-baseline
/// Returns current network nodes with their storage capacity and baseline usage
/// Used as the starting point for hypothetical network analysis
async fn get_system_nodes_baseline(
    State(state): State<AppState>,
    Extension(uid): Extension<i32>,
) -> Result<Json<Vec<hopnet_common::db::NodeStorageBaseline>>, StatusCode> {
    // Reads the resilience cache rather than rescanning: the baseline query
    // joins fragment_inventory against every fragment row, so on a large node
    // it costs seconds. Sharing the cache also keeps this route's numbers and
    // the resilience pane's from drifting apart (issue #68).
    let nodes = crate::views::resilience::cached_storage_parts(&state)
        .await
        .map_err(|e| {
            tracing::error!("Failed to get system nodes baseline: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .baselines;

    Ok(Json(nodes))
}
