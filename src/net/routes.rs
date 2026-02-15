use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::Serialize;

use crate::AppState;
use crate::types::Node;

#[derive(Serialize)]
pub struct IrohPingAllResponse {
    total_nodes: usize,
    successful: usize,
    failed: usize,
    results: Vec<NodePingResult>,
}

#[derive(Serialize)]
pub struct NodePingResult {
    node_id: i32,
    success: bool,
    latency_ms: Option<f64>,
    error: Option<String>,
}

/// Debug endpoint for testing iroh connectivity to all nodes
/// GET /debug/iroh-ping
pub async fn debug_iroh_ping(
    State(app_state): State<AppState>,
) -> impl IntoResponse {
    let my_node_id = app_state.get_node_id().unwrap_or(-1);

    // Get all nodes from database
    let nodes: Vec<Node> = {
        let conn = match app_state.db_pool.get() {
            Ok(c) => c,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(IrohPingAllResponse {
                        total_nodes: 0,
                        successful: 0,
                        failed: 0,
                        results: vec![NodePingResult {
                            node_id: -1,
                            success: false,
                            latency_ms: None,
                            error: Some(format!("database error: {}", e)),
                        }],
                    }),
                );
            }
        };

        let mut stmt = match conn.prepare("SELECT node_id, name, owner, pubkey FROM nodes") {
            Ok(s) => s,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(IrohPingAllResponse {
                        total_nodes: 0,
                        successful: 0,
                        failed: 0,
                        results: vec![NodePingResult {
                            node_id: -1,
                            success: false,
                            latency_ms: None,
                            error: Some(format!("query error: {}", e)),
                        }],
                    }),
                );
            }
        };

        match stmt.query_map([], |row| {
            Ok(Node {
                node_id: row.get(0)?,
                name: row.get(1)?,
                owner: row.get(2)?,
                pubkey: row.get(3)?,
            })
        }) {
            Ok(rows) => rows.filter_map(|r| r.ok()).collect(),
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(IrohPingAllResponse {
                        total_nodes: 0,
                        successful: 0,
                        failed: 0,
                        results: vec![NodePingResult {
                            node_id: -1,
                            success: false,
                            latency_ms: None,
                            error: Some(format!("query error: {}", e)),
                        }],
                    }),
                );
            }
        }
    };

    // Filter out self and ping all others in parallel
    let other_nodes: Vec<&Node> = nodes.iter().filter(|n| n.node_id != my_node_id).collect();
    let total_nodes = other_nodes.len();

    let mut tasks = Vec::new();
    for node in other_nodes {
        let transport = app_state.iroh_transport.clone();
        let node_id = node.node_id;
        let iroh_node_id = node.pubkey.to_iroh_node_id();

        tasks.push(tokio::spawn(async move {
            match transport.ping(node_id, iroh_node_id).await {
                Ok(latency_ns) => NodePingResult {
                    node_id,
                    success: true,
                    latency_ms: Some(latency_ns as f64 / 1_000_000.0),
                    error: None,
                },
                Err(e) => NodePingResult {
                    node_id,
                    success: false,
                    latency_ms: None,
                    error: Some(e.to_string()),
                },
            }
        }));
    }

    // Collect results
    let mut results = Vec::with_capacity(tasks.len());
    for task in tasks {
        if let Ok(result) = task.await {
            results.push(result);
        }
    }

    let successful = results.iter().filter(|r| r.success).count();
    let failed = results.iter().filter(|r| !r.success).count();

    (
        StatusCode::OK,
        Json(IrohPingAllResponse {
            total_nodes,
            successful,
            failed,
            results,
        }),
    )
}
