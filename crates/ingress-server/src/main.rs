//! Read-only web viewer for the Apple Photos ingress blob store.
//!
//! Slice 1: load config, build the sidecar-tree index, run an incremental
//! refresh loop, and serve a minimal router (`/health`). The full REST surface,
//! Renderer, and OIDC auth arrive in later slices — the `State<Arc<Index>>`
//! wiring here is what they bolt onto.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::routing::get;

use ingress_server::config::Config;
use ingress_server::index::Index;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let config = Config::load(&config_path())?;
    let index = Index::open(&config).await?;
    let stats = index.build().await?;
    tracing::info!(
        photos = stats.photos,
        resources = stats.resources,
        parsed = stats.parsed,
        "index built"
    );

    spawn_refresh_loop(index.clone(), config.refresh_interval_secs);

    let app = axum::Router::new()
        .route("/health", get(health))
        .with_state(index.clone());
    let listener = tokio::net::TcpListener::bind(config.bind).await?;
    tracing::info!(bind = %config.bind, "serving");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health(State(index): State<Arc<Index>>) -> String {
    match index.libraries().await {
        Ok(libs) => format!("ok ({} libraries)", libs.len()),
        Err(e) => format!("degraded: {e}"),
    }
}

/// Periodic incremental refresh. Only non-trivial passes are logged.
fn spawn_refresh_loop(index: Arc<Index>, interval_secs: u64) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(interval_secs.max(1)));
        tick.tick().await; // consume the immediate first fire (build already ran)
        loop {
            tick.tick().await;
            match index.refresh().await {
                Ok(s) if s.parsed > 0 || s.removed > 0 => {
                    tracing::info!(
                        parsed = s.parsed,
                        removed = s.removed,
                        photos = s.photos,
                        "index refreshed"
                    )
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(?e, "index refresh failed"),
            }
        }
    });
}

/// Config path resolution: `$INGRESS_SERVER_CONFIG`, else `argv[1]`, else
/// `./config.toml`.
fn config_path() -> PathBuf {
    if let Ok(p) = std::env::var("INGRESS_SERVER_CONFIG") {
        return PathBuf::from(p);
    }
    std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("config.toml"))
}
