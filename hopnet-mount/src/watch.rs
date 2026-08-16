//! The watch loop (RFC-018 S4): subscribe to pokes, sync deltas, bust
//! kernel caches.
//!
//! Platform-neutral — kernel notification goes through the
//! `KernelInvalidator` seam (fuser-backed on Linux, recording in tests),
//! so the whole loop runs against the mock with no kernel. Discipline:
//! resync from the anchor after EVERY (re)connect, so a dropped watch
//! connection never opens a divergence window; pokes coalesce (drain
//! before sync); liveness is heartbeat-bounded, reconnects back off
//! exponentially.

use std::sync::Arc;
use std::time::Duration;

use tokio_stream::StreamExt;

use crate::transport::{Height, NodeTransport};
use crate::vfs::{Invalidation, MountCore};

/// Kernel cache busting — the fuser Notifier behind a testable seam.
pub trait KernelInvalidator: Send + Sync {
    fn inval_entry(&self, parent_ino: u64, name: &str);
    fn inval_inode(&self, ino: u64);
}

/// No kernel to notify (mock demos before mount, tests of pure sync).
pub struct NullInvalidator;
impl KernelInvalidator for NullInvalidator {
    fn inval_entry(&self, _parent_ino: u64, _name: &str) {}
    fn inval_inode(&self, _ino: u64) {}
}

/// Daemon-side activation coupling (RFC-024 S2), fired from inside the
/// standardized 426 handler. Implementations MUST NOT block — the
/// watcher task calls these inline; spawn internally.
pub trait UpgradeCoupling: Send + Sync {
    /// The false→true transition of the held state: spawn ONE wrapper
    /// run. A later clear (node accepts us again) re-arms this for the
    /// next entry.
    fn entered(&self);
    /// Every subsequent refusal while held (~BACKOFF_MAX cadence):
    /// re-evaluate the exit-75 gate — a timer flip may have landed.
    fn still_held(&self);
}

/// Heartbeats arrive every ~15 s (server keepalive); ~3 missed = dead.
const LIVENESS_TIMEOUT: Duration = Duration::from_secs(45);
const BACKOFF_INITIAL: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

/// Anchor init value: strictly-greater filtering guarantees an empty
/// delta; the response still carries the current height.
///
/// `i64::MAX`, not `u64::MAX`, even though heights are u64: the server
/// maps heights onto SQLite INTEGER with the lossless bit-cast
/// (`hopnet_common::height`), so anything above `i64::MAX` lands as a
/// NEGATIVE column value and `modified_at_height > ?` would then match
/// EVERY row — the exact inverse of this sentinel. Any height at or
/// below `i64::MAX` round-trips positive and orders correctly.
///
/// A magic height is still the wrong shape for "no anchor yet"; an
/// out-of-band marker would not depend on the storage mapping at all.
/// Left as-is here because changing it is a mount wire-protocol change.
pub const ANCHOR_INIT: Height = i64::MAX as Height;

pub struct Watcher {
    core: Arc<MountCore>,
    transport: Arc<dyn NodeTransport>,
    invalidator: Arc<dyn KernelInvalidator>,
    anchor: Height,
    /// Sticky RFC-023 S4 state (the passthrough-disarm pattern): set on
    /// the first UpgradeRequired so the standardized handler logs the
    /// versions loudly ONCE and later rejections stay quiet; cleared by
    /// the next successful connect (a node rollback un-strands us).
    upgrade_required: bool,
    /// RFC-024 S2 activation coupling; None = unmanaged install, the
    /// hold stays a log-only state.
    coupling: Option<Arc<dyn UpgradeCoupling>>,
}

impl Watcher {
    pub fn new(
        core: Arc<MountCore>,
        transport: Arc<dyn NodeTransport>,
        invalidator: Arc<dyn KernelInvalidator>,
    ) -> Self {
        Watcher {
            core,
            transport,
            invalidator,
            anchor: ANCHOR_INIT,
            upgrade_required: false,
            coupling: None,
        }
    }

    /// Attach the RFC-024 activation coupling (managed deployments).
    pub fn with_upgrade_coupling(mut self, coupling: Arc<dyn UpgradeCoupling>) -> Self {
        self.coupling = Some(coupling);
        self
    }

    /// The standardized 426 handler (RFC-023 S4): loud once, quiet
    /// after — never generic transport noise. Returns true if `e` was
    /// an UpgradeRequired.
    fn note_upgrade_required(&mut self, e: &crate::transport::TransportError) -> bool {
        if !matches!(e, crate::transport::TransportError::UpgradeRequired { .. }) {
            return false;
        }
        if self.upgrade_required {
            tracing::debug!("still awaiting client upgrade: {e}");
            if let Some(coupling) = &self.coupling {
                coupling.still_held();
            }
        } else {
            self.upgrade_required = true;
            tracing::error!("{e} — hopnet-mount must be upgraded; holding until it is");
            if let Some(coupling) = &self.coupling {
                coupling.entered();
            }
        }
        true
    }

    /// One delta sync: changes(anchor) → apply → fire invalidations →
    /// advance the anchor. Errors leave the anchor untouched (the next
    /// sync retries the same window — deltas are idempotent).
    pub async fn sync(&mut self) -> Result<(), crate::transport::TransportError> {
        let changes = self.transport.changes(self.anchor).await?;
        let invalidations = self.core.apply_changes(&changes);
        if !invalidations.is_empty() {
            // Kernel notifications are SYNCHRONOUS /dev/fuse writes and can
            // block until the kernel processes them — which may require an
            // in-flight FUSE request (whose reply needs this runtime) to
            // finish first. Off the async workers, always: blocking a
            // worker here deadlocks single-threaded runtimes outright and
            // steals workers on multi-threaded ones.
            let invalidator = self.invalidator.clone();
            let _ = tokio::task::spawn_blocking(move || {
                for invalidation in &invalidations {
                    match invalidation {
                        Invalidation::Entry { parent_ino, name } => {
                            invalidator.inval_entry(*parent_ino, name)
                        }
                        Invalidation::Inode { ino } => invalidator.inval_inode(*ino),
                    }
                }
            })
            .await;
        }
        self.anchor = changes.height;
        Ok(())
    }

    /// The forever loop. Returns only if the runtime shuts down.
    pub async fn run(mut self) {
        let mut backoff = BACKOFF_INITIAL;
        loop {
            match self.transport.watch().await {
                Ok(mut stream) => {
                    backoff = BACKOFF_INITIAL;
                    if self.upgrade_required {
                        self.upgrade_required = false;
                        tracing::info!("node accepts this client again; resuming");
                    }
                    // Cover the gap between (re)connect and the first poke.
                    if let Err(e) = self.sync().await {
                        if !self.note_upgrade_required(&e) {
                            tracing::warn!("post-connect sync failed: {e}");
                        }
                    }
                    loop {
                        match tokio::time::timeout(LIVENESS_TIMEOUT, stream.next()).await {
                            Ok(Some(crate::transport::WatchEvent::Poke)) => {
                                // Coalesce a burst into one sync (extra
                                // heartbeats drained here are harmless).
                                while let Ok(Some(_)) =
                                    tokio::time::timeout(Duration::from_millis(50), stream.next())
                                        .await
                                {}
                                if let Err(e) = self.sync().await {
                                    if !self.note_upgrade_required(&e) {
                                        tracing::warn!("poke sync failed: {e}");
                                    }
                                }
                            }
                            Ok(Some(crate::transport::WatchEvent::Heartbeat)) => {}
                            Ok(None) => {
                                tracing::info!("watch stream ended; reconnecting");
                                break;
                            }
                            Err(_) => {
                                tracing::warn!("watch liveness timeout; reconnecting");
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    if self.note_upgrade_required(&e) {
                        // Hold, don't spin: re-probe at the max cadence
                        // until the wrapper (or operator) upgrades us.
                        backoff = BACKOFF_MAX;
                    } else {
                        tracing::warn!("watch connect failed: {e}; retrying in {backoff:?}");
                    }
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(BACKOFF_MAX);
                }
            }
        }
    }
}
