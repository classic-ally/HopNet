//! Shared test-only barrier primitive. Each subsystem (consensus, takeout/import,
//! …) owns its own `Barriers` instance with module-specific name registry, but
//! the hold/release/wait/status mechanics + status-payload type live here so we
//! don't re-implement the AtomicBool + Notify wiring per module.
//!
//! Subsystems wire their own HTTP test routes (URL prefix + `AppState` accessor
//! to find their `Barriers` instance) — only the wire format and the runtime
//! primitive are shared.

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};

use crate::AppState;

/// The runtime primitive moved down to hopnet-projection (RFC-015 Stage
/// D5b) so service crates (hopnet-takeout) can own their own instance;
/// re-exported here so every host call site is unchanged. The HTTP test
/// routes + the `BarrierRegistration` inventory stay host-side.
pub use hopnet_projection::barriers::{BarrierStatus, Barriers};

// -- Subsystem registration via `inventory` --
//
// Each module that owns a `Barriers` instance submits a `BarrierRegistration`.
// The HTTP routes resolve by iterating the inventory, so adding a new
// subsystem is purely additive — no central match to edit.

pub struct BarrierRegistration {
    pub subsystem: &'static str,
    pub accessor: fn(&AppState) -> &Arc<Barriers>,
    pub names: &'static [&'static str],
}

inventory::collect!(&'static BarrierRegistration);

fn lookup(subsystem: &str) -> Option<&'static BarrierRegistration> {
    for reg in inventory::iter::<&'static BarrierRegistration> {
        if reg.subsystem == subsystem {
            return Some(*reg);
        }
    }
    None
}

// -- HTTP test routes --
//
// URL layout: /test/barriers/{subsystem}/{name}/hold|release|status,
//             /test/barriers/{subsystem}        (list)
// `subsystem` is matched against the inventory at request time.

async fn post_barrier_hold(
    State(state): State<AppState>,
    Path((subsystem, name)): Path<(String, String)>,
) -> impl IntoResponse {
    let Some(reg) = lookup(&subsystem) else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "unknown subsystem"})),
        );
    };
    let barriers = (reg.accessor)(&state);
    if barriers.hold(&name) {
        (StatusCode::OK, Json(serde_json::json!({"ok": true})))
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "unknown barrier"})),
        )
    }
}

async fn post_barrier_release(
    State(state): State<AppState>,
    Path((subsystem, name)): Path<(String, String)>,
) -> impl IntoResponse {
    let Some(reg) = lookup(&subsystem) else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "unknown subsystem"})),
        );
    };
    let barriers = (reg.accessor)(&state);
    if barriers.release(&name) {
        (StatusCode::OK, Json(serde_json::json!({"ok": true})))
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "unknown barrier"})),
        )
    }
}

async fn get_barrier_status(
    State(state): State<AppState>,
    Path((subsystem, name)): Path<(String, String)>,
) -> impl IntoResponse {
    let Some(reg) = lookup(&subsystem) else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "unknown subsystem"})),
        )
            .into_response();
    };
    let barriers = (reg.accessor)(&state);
    match barriers.status(&name) {
        Some(s) => (StatusCode::OK, Json(serde_json::json!(s))).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "unknown barrier"})),
        )
            .into_response(),
    }
}

async fn list_barriers(
    State(state): State<AppState>,
    Path(subsystem): Path<String>,
) -> impl IntoResponse {
    let Some(reg) = lookup(&subsystem) else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "unknown subsystem"})),
        )
            .into_response();
    };
    let barriers = (reg.accessor)(&state);
    let map: HashMap<String, BarrierStatus> = barriers
        .list_with_names(reg.names)
        .into_iter()
        .map(|(n, s)| (n.to_string(), s))
        .collect();
    (StatusCode::OK, Json(map)).into_response()
}

pub fn test_routes() -> Router<AppState> {
    Router::new()
        .route("/{subsystem}/{name}/hold", post(post_barrier_hold))
        .route("/{subsystem}/{name}/release", post(post_barrier_release))
        .route("/{subsystem}/{name}/status", get(get_barrier_status))
        .route("/{subsystem}", get(list_barriers))
}
