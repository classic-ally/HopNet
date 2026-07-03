//! Read-only web viewer for the Apple Photos ingress blob store.
//!
//! Wires the sidecar-tree index, the OIDC auth context, per-library access
//! rules, and the HTTP router into a running server.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tower_http::trace::TraceLayer;
use tower_sessions::cookie::SameSite;
use tower_sessions::{Expiry, MemoryStore, SessionManagerLayer};

use ingress_server::auth::{self, AccessRules};
use ingress_server::config::Config;
use ingress_server::index::Index;
use ingress_server::routes::{self, AppState};

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

    // OIDC is required for this build — discovery + env secret at startup.
    let oidc = config
        .oidc
        .clone()
        .ok_or_else(|| anyhow::anyhow!("[oidc] config section is required"))?;
    let auth = Arc::new(auth::build_auth(&oidc).await?);

    let rules: AccessRules = config
        .libraries
        .iter()
        .map(|l| (l.library_id.clone(), l.access.groups.clone()))
        .collect();

    let state = AppState {
        index: index.clone(),
        auth,
        rules: Arc::new(rules),
    };

    // Interim in-memory session store: LOSES SESSIONS ON RESTART (users
    // re-login). Swap for a persistent tower-sessions store later — no handler
    // changes (the interface is `Session`).
    tracing::warn!("session store is in-memory; sessions do not survive a restart");
    let session_layer = SessionManagerLayer::new(MemoryStore::default())
        .with_secure(oidc.secure_cookies())
        .with_http_only(true)
        .with_same_site(SameSite::Lax) // survives the provider→callback top-level redirect
        .with_name("ingress_sid")
        .with_expiry(Expiry::OnInactivity(
            tower_sessions::cookie::time::Duration::days(7),
        ));

    let app = routes::router(state)
        .layer(session_layer)
        .layer(TraceLayer::new_for_http());

    let listener = tokio::net::TcpListener::bind(config.bind).await?;
    tracing::info!(bind = %config.bind, "serving");
    axum::serve(listener, app).await?;
    Ok(())
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
