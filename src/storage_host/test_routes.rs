use axum::http::StatusCode;
use axum::{
    Json,
    extract::{Path, State},
    response::IntoResponse,
};
use serde::Serialize;

use crate::AppState;
use crate::types::{Blake3Hash, Node};

#[derive(Serialize)]
pub struct FragmentHealthCheckResponse {
    pub fragment_hash: String,
    pub total_nodes: usize,
    pub healthy: usize,
    pub unhealthy: usize,
    pub errors: usize,
    pub results: Vec<NodeFragmentHealthResult>,
}

#[derive(Serialize)]
pub struct NodeFragmentHealthResult {
    pub node_id: i32,
    pub healthy: Option<bool>,
    pub error: Option<String>,
    pub latency_ms: f64,
}

/// GET /test/fragment-health-check/{fragment_hash}
/// Ask all peer nodes via iroh whether they have a specific fragment.
pub async fn get_fragment_health_check(
    State(app_state): State<AppState>,
    Path(fragment_hash): Path<Blake3Hash>,
) -> impl IntoResponse {
    let my_node_id = app_state.get_node_id().unwrap_or(-1);

    let nodes: Vec<Node> = match crate::db::nodes::get_nodes(app_state.db_pool.get()) {
        Ok(n) => n.into_iter().filter(|n| n.node_id != my_node_id).collect(),
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(FragmentHealthCheckResponse {
                    fragment_hash: fragment_hash.to_hex(),
                    total_nodes: 0,
                    healthy: 0,
                    unhealthy: 0,
                    errors: 1,
                    results: vec![NodeFragmentHealthResult {
                        node_id: -1,
                        healthy: None,
                        error: Some(format!("database error: {:?}", e)),
                        latency_ms: 0.0,
                    }],
                }),
            );
        }
    };

    let total_nodes = nodes.len();
    let mut tasks = Vec::new();

    for node in &nodes {
        let comms = app_state.comms.clone();
        let node_id = node.node_id;
        let peer = hopnet_comms::PeerRef {
            node_id,
            pubkey: node.pubkey.0.to_bytes(),
        };
        let hash = fragment_hash;

        tasks.push(tokio::spawn(async move {
            use hopnet_storage::traits::Transport;
            let start = std::time::Instant::now();
            let transport = hopnet_storage::rpc::RpcTransport { rpc: comms };
            match transport.fragment_health(&peer, &hash).await {
                Ok(healthy) => NodeFragmentHealthResult {
                    node_id,
                    healthy: Some(healthy),
                    error: None,
                    latency_ms: start.elapsed().as_secs_f64() * 1000.0,
                },
                Err(e) => NodeFragmentHealthResult {
                    node_id,
                    healthy: None,
                    error: Some(e.to_string()),
                    latency_ms: start.elapsed().as_secs_f64() * 1000.0,
                },
            }
        }));
    }

    let mut results = Vec::with_capacity(tasks.len());
    for task in tasks {
        if let Ok(r) = task.await {
            results.push(r);
        }
    }

    let healthy = results.iter().filter(|r| r.healthy == Some(true)).count();
    let unhealthy = results.iter().filter(|r| r.healthy == Some(false)).count();
    let errors = results.iter().filter(|r| r.healthy.is_none()).count();

    (
        StatusCode::OK,
        Json(FragmentHealthCheckResponse {
            fragment_hash: fragment_hash.to_hex(),
            total_nodes,
            healthy,
            unhealthy,
            errors,
            results,
        }),
    )
}
