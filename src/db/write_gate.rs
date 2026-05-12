use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Notify;

use crate::types::Blake3Hash;

/// Async-aware gate that gives consensus writers priority over background tasks.
///
/// Consensus acquires a guard (closing the gate) before each Immediate transaction.
/// Background tasks call `wait_for_open()` and block until consensus finishes.
/// Maximum background delay is bounded to one in-flight batch UPDATE.
pub struct WriteGate {
    consensus_pending: AtomicBool,
    gate_open: Notify,
}

impl WriteGate {
    pub fn new() -> Self {
        Self {
            consensus_pending: AtomicBool::new(false),
            gate_open: Notify::new(),
        }
    }

    /// Acquire the gate — background writers will wait until the guard is dropped.
    pub fn guard(self: &Arc<Self>) -> WriteGateGuard {
        self.consensus_pending.store(true, Ordering::Release);
        WriteGateGuard {
            gate: Arc::clone(self),
        }
    }

    /// Wait until no consensus write is pending. Called by background tasks.
    pub async fn wait_for_open(&self) {
        while self.consensus_pending.load(Ordering::Acquire) {
            self.gate_open.notified().await;
        }
    }
}

/// RAII guard — clears the consensus-pending flag and notifies waiters on drop.
pub struct WriteGateGuard {
    gate: Arc<WriteGate>,
}

impl Drop for WriteGateGuard {
    fn drop(&mut self) {
        self.gate.consensus_pending.store(false, Ordering::Release);
        self.gate.gate_open.notify_waiters();
    }
}

/// Updates queued for the drain task.
pub enum LocalStateUpdate {
    /// Fragment is now on local disk.
    MarkLocal { fragment_hash: Blake3Hash },
    /// Fragment sent to a remote node (no longer local).
    MarkRemote { fragment_hash: Blake3Hash },
    /// Batch variant for distribution — multiple fragments sent to remotes.
    MarkRemoteBatch { fragment_hashes: Vec<Blake3Hash> },
}

/// Long-lived task that batches `LocalStateUpdate` messages and flushes them
/// through the write gate. Spawned once at startup.
pub async fn drain_local_state_queue(
    mut rx: tokio::sync::mpsc::Receiver<LocalStateUpdate>,
    db_pool: r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
    write_gate: Arc<WriteGate>,
) {
    loop {
        // Block until at least one item arrives
        let Some(first) = rx.recv().await else {
            tracing::debug!("Local state queue closed, drain task shutting down");
            return;
        };

        // Drain any additional queued items (non-blocking)
        let mut mark_local: Vec<Blake3Hash> = Vec::new();
        let mut mark_remote: Vec<Blake3Hash> = Vec::new();

        let mut classify = |item: LocalStateUpdate| match item {
            LocalStateUpdate::MarkLocal { fragment_hash } => mark_local.push(fragment_hash),
            LocalStateUpdate::MarkRemote { fragment_hash } => mark_remote.push(fragment_hash),
            LocalStateUpdate::MarkRemoteBatch { fragment_hashes } => {
                mark_remote.extend(fragment_hashes)
            }
        };

        classify(first);
        while let Ok(item) = rx.try_recv() {
            classify(item);
        }

        // Wait for consensus to finish before touching the DB
        write_gate.wait_for_open().await;

        if !mark_local.is_empty() {
            if let Err(e) =
                super::files::mark_fragments_local_state_batch(db_pool.get(), &mark_local, true)
            {
                tracing::warn!(
                    "Failed to batch-mark {} fragments as local: {:?}",
                    mark_local.len(),
                    e
                );
            }
        }

        if !mark_remote.is_empty() {
            if let Err(e) =
                super::files::mark_fragments_local_state_batch(db_pool.get(), &mark_remote, false)
            {
                tracing::warn!(
                    "Failed to batch-mark {} fragments as remote: {:?}",
                    mark_remote.len(),
                    e
                );
            }
        }
    }
}
