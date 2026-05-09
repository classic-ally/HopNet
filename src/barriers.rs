//! Shared test-only barrier primitive. Each subsystem (consensus, takeout/import,
//! …) owns its own `Barriers` instance with module-specific name registry, but
//! the hold/release/wait/status mechanics + status-payload type live here so we
//! don't re-implement the AtomicBool + Notify wiring per module.
//!
//! Subsystems wire their own HTTP test routes (URL prefix + `AppState` accessor
//! to find their `Barriers` instance) — only the wire format and the runtime
//! primitive are shared.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::Serialize;

use crate::AppState;

/// Status payload exposed via test routes — held + waiting flags.
#[derive(Serialize, Clone, Debug)]
pub struct BarrierStatus {
    pub held: bool,
    pub waiting: bool,
}

struct BarrierState {
    held: AtomicBool,
    waiting: AtomicBool,
    released: Notify,
}

pub struct Barriers {
    barriers: HashMap<&'static str, BarrierState>,
}

impl Barriers {
    /// Build a registry pre-populated with `names`. Unknown names passed to
    /// `wait`/`hold`/`release` later are no-ops and log a warning.
    pub fn new(names: &[&'static str]) -> Self {
        let mut barriers = HashMap::new();
        for &name in names {
            barriers.insert(
                name,
                BarrierState {
                    held: AtomicBool::new(false),
                    waiting: AtomicBool::new(false),
                    released: Notify::new(),
                },
            );
        }
        Self { barriers }
    }

    /// Block while `name` is held. No-op when unheld. Latches `waiting` to
    /// true on first hit so the test can verify the call site reached the
    /// wait point.
    pub async fn wait(&self, name: &str) {
        let state = match self.barriers.get(name) {
            Some(s) => s,
            None => {
                tracing::warn!("Barrier '{}' not found", name);
                return;
            }
        };
        if !state.held.load(Ordering::SeqCst) {
            return;
        }
        tracing::info!("Barrier '{}' held, blocking", name);
        state.waiting.store(true, Ordering::SeqCst);
        while state.held.load(Ordering::SeqCst) {
            state.released.notified().await;
        }
        tracing::info!("Barrier '{}' released, continuing", name);
    }

    /// Pause future calls to `wait`. Returns true if the name is registered.
    pub fn hold(&self, name: &str) -> bool {
        match self.barriers.get(name) {
            Some(state) => {
                state.held.store(true, Ordering::SeqCst);
                true
            }
            None => false,
        }
    }

    /// Resume `wait` calls and clear the latched `waiting` flag. Returns true
    /// if the name is registered.
    pub fn release(&self, name: &str) -> bool {
        match self.barriers.get(name) {
            Some(state) => {
                state.waiting.store(false, Ordering::SeqCst);
                state.held.store(false, Ordering::SeqCst);
                state.released.notify_waiters();
                true
            }
            None => false,
        }
    }

    pub fn status(&self, name: &str) -> Option<BarrierStatus> {
        self.barriers.get(name).map(|state| BarrierStatus {
            held: state.held.load(Ordering::SeqCst),
            waiting: state.waiting.load(Ordering::SeqCst),
        })
    }

    /// Snapshot every registered barrier. Caller-supplied iteration order
    /// keeps the routing layer's response shape predictable.
    pub fn list_with_names(&self, names: &[&'static str]) -> Vec<(&'static str, BarrierStatus)> {
        names
            .iter()
            .filter_map(|&name| self.status(name).map(|s| (name, s)))
            .collect()
    }
}

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
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "unknown subsystem"})));
    };
    let barriers = (reg.accessor)(&state);
    if barriers.hold(&name) {
        (StatusCode::OK, Json(serde_json::json!({"ok": true})))
    } else {
        (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "unknown barrier"})))
    }
}

async fn post_barrier_release(
    State(state): State<AppState>,
    Path((subsystem, name)): Path<(String, String)>,
) -> impl IntoResponse {
    let Some(reg) = lookup(&subsystem) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "unknown subsystem"})));
    };
    let barriers = (reg.accessor)(&state);
    if barriers.release(&name) {
        (StatusCode::OK, Json(serde_json::json!({"ok": true})))
    } else {
        (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "unknown barrier"})))
    }
}

async fn get_barrier_status(
    State(state): State<AppState>,
    Path((subsystem, name)): Path<(String, String)>,
) -> impl IntoResponse {
    let Some(reg) = lookup(&subsystem) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "unknown subsystem"}))).into_response();
    };
    let barriers = (reg.accessor)(&state);
    match barriers.status(&name) {
        Some(s) => (StatusCode::OK, Json(serde_json::json!(s))).into_response(),
        None => (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "unknown barrier"}))).into_response(),
    }
}

async fn list_barriers(
    State(state): State<AppState>,
    Path(subsystem): Path<String>,
) -> impl IntoResponse {
    let Some(reg) = lookup(&subsystem) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "unknown subsystem"}))).into_response();
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
