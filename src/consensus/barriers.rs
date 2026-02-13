use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Notify;
use serde::Serialize;
use axum::{
    extract::{Path, State},
    response::IntoResponse,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use crate::AppState;

pub mod names {
    pub const BEFORE_BALLOT_DISPATCH: &str = "before_ballot_dispatch";
    pub const AFTER_PROPOSE_QC_BROADCAST: &str = "after_propose_qc_broadcast";
    pub const BEFORE_TC_GST_WAIT: &str = "before_tc_gst_wait";
    pub const BEFORE_TC_APPLICATION: &str = "before_tc_application";
    pub const BEFORE_LOCK_QC_BROADCAST: &str = "before_lock_qc_broadcast";
}

const ALL_BARRIER_NAMES: &[&str] = &[
    names::BEFORE_BALLOT_DISPATCH,
    names::AFTER_PROPOSE_QC_BROADCAST,
    names::BEFORE_TC_GST_WAIT,
    names::BEFORE_TC_APPLICATION,
    names::BEFORE_LOCK_QC_BROADCAST,
];

struct BarrierState {
    held: AtomicBool,
    waiting: AtomicBool,
    released: Notify,
}

pub struct ConsensusBarriers {
    barriers: HashMap<&'static str, BarrierState>,
}

#[derive(Serialize, Clone, Debug)]
pub struct BarrierStatus {
    pub held: bool,
    pub waiting: bool,
}

impl ConsensusBarriers {
    pub fn new() -> Self {
        let mut barriers = HashMap::new();
        for &name in ALL_BARRIER_NAMES {
            barriers.insert(name, BarrierState {
                held: AtomicBool::new(false),
                waiting: AtomicBool::new(false),
                released: Notify::new(),
            });
        }
        Self { barriers }
    }

    /// Block while barrier is held. Returns immediately if released.
    /// Sets `waiting` to true when hitting a held barrier (latches until release).
    pub async fn wait(&self, name: &str) {
        let state = match self.barriers.get(name) {
            Some(s) => s,
            None => {
                tracing::warn!("Barrier '{}' not found", name);
                return;
            }
        };

        // Fast path: not held
        if !state.held.load(Ordering::SeqCst) {
            return;
        }

        tracing::info!("Barrier '{}' is held, blocking", name);
        state.waiting.store(true, Ordering::SeqCst);

        // Wait until released (re-check held after each notification to handle spurious wakes)
        while state.held.load(Ordering::SeqCst) {
            state.released.notified().await;
        }
        tracing::info!("Barrier '{}' released, continuing", name);
        // Note: waiting is NOT cleared here — it latches until release() is called.
    }

    /// Hold (pause) a barrier. Returns true if barrier exists.
    pub fn hold(&self, name: &str) -> bool {
        match self.barriers.get(name) {
            Some(state) => {
                state.held.store(true, Ordering::SeqCst);
                true
            }
            None => false,
        }
    }

    /// Release a barrier and clear the waiting latch. Returns true if barrier exists.
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

    /// Get status of a single barrier.
    pub fn status(&self, name: &str) -> Option<BarrierStatus> {
        self.barriers.get(name).map(|state| BarrierStatus {
            held: state.held.load(Ordering::SeqCst),
            waiting: state.waiting.load(Ordering::SeqCst),
        })
    }

    /// List all barriers with their status.
    pub fn list(&self) -> Vec<(&str, BarrierStatus)> {
        ALL_BARRIER_NAMES
            .iter()
            .filter_map(|&name| {
                self.status(name).map(|s| (name, s))
            })
            .collect()
    }
}

// -- HTTP route handlers --

async fn post_barrier_hold(
    State(app_state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    if app_state.consensus_barriers.hold(&name) {
        (StatusCode::OK, Json(serde_json::json!({"ok": true})))
    } else {
        (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "unknown barrier"})))
    }
}

async fn post_barrier_release(
    State(app_state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    if app_state.consensus_barriers.release(&name) {
        (StatusCode::OK, Json(serde_json::json!({"ok": true})))
    } else {
        (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "unknown barrier"})))
    }
}

async fn get_barrier_status(
    State(app_state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    match app_state.consensus_barriers.status(&name) {
        Some(status) => (StatusCode::OK, Json(serde_json::json!(status))).into_response(),
        None => (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "unknown barrier"}))).into_response(),
    }
}

async fn get_barriers(
    State(app_state): State<AppState>,
) -> impl IntoResponse {
    let barriers: HashMap<String, BarrierStatus> = app_state.consensus_barriers.list()
        .into_iter()
        .map(|(name, status)| (name.to_string(), status))
        .collect();
    (StatusCode::OK, Json(barriers))
}

pub fn test_routes() -> Router<AppState> {
    Router::new()
        .route("/barrier/{name}/hold", post(post_barrier_hold))
        .route("/barrier/{name}/release", post(post_barrier_release))
        .route("/barrier/{name}/status", get(get_barrier_status))
        .route("/barriers", get(get_barriers))
}
