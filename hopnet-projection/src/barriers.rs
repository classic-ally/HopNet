//! Shared test-only barrier primitive (moved down from the host at RFC-015
//! Stage D5b so service crates like hopnet-takeout can hold their own
//! `Barriers` instance). Each subsystem (consensus, takeout/import, …) owns
//! its own instance with a module-specific name registry; the host keeps the
//! HTTP test routes and the `BarrierRegistration` inventory shim — only the
//! hold/release/wait/status mechanics + status-payload type live here.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Notify;

use serde::Serialize;

/// Status payload exposed via the host's test routes — held + waiting flags.
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
