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
        }
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
                    // Cover the gap between (re)connect and the first poke.
                    if let Err(e) = self.sync().await {
                        tracing::warn!("post-connect sync failed: {e}");
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
                                    tracing::warn!("poke sync failed: {e}");
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
                    tracing::warn!("watch connect failed: {e}; retrying in {backoff:?}");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(BACKOFF_MAX);
                }
            }
        }
    }
}
