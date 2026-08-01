use axum::{Router, extract::State, http::StatusCode, response::Json, routing::get};

use hopnet_common::views::{ResiliencePaneView, UpgradeReadinessView};

use crate::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/network-resilience", get(get_network_resilience))
        .route("/upgrade-readiness", get(get_upgrade_readiness))
}

/// GET /views/upgrade-readiness (RFC-019 S3)
///
/// Committed per-node version attestations + the provider's last poll.
/// Facts only — the readiness rollup is the S5 precondition's job.
async fn get_upgrade_readiness(
    State(app_state): State<AppState>,
) -> Result<Json<UpgradeReadinessView>, StatusCode> {
    let provider_status = app_state.upgrade.last.read().await.clone();

    let mesh = {
        let app_state = app_state.clone();
        tokio::task::spawn_blocking(move || {
            let conn = app_state
                .db_pool
                .get()
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            crate::db::versions::read_mesh_versions(&conn)
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
        })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)??
    };

    Ok(Json(crate::views::upgrade::upgrade_view(
        mesh,
        provider_status.as_ref(),
    )))
}

/// GET /views/network-resilience
///
/// Both panels in one response: they sit side by side, so fetching separately
/// would let them disagree about the same node across two round trips.
///
/// All DB work goes through `spawn_blocking` — the resilience query is a full
/// scan over `fragment_hashes x fragment_inventory` with per-block window
/// functions, and the route this supersedes ran it straight on the async
/// runtime.
async fn get_network_resilience(
    State(app_state): State<AppState>,
) -> Result<Json<ResiliencePaneView>, StatusCode> {
    let started = std::time::Instant::now();

    let view = tokio::task::spawn_blocking(move || {
        let conn = app_state
            .db_pool
            .get()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let consensus = crate::views::resilience::consensus_view(&app_state, &conn)
            .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
        let storage = crate::views::resilience::storage_view(&app_state, &conn);

        Ok::<_, StatusCode>(ResiliencePaneView { consensus, storage })
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)??;

    tracing::debug!(
        "network-resilience view assembled in {}ms",
        started.elapsed().as_millis()
    );

    Ok(Json(view))
}
