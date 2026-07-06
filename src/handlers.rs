//! Transaction-handler seam — the contract itself lives in
//! hopnet-projection (RFC-015) so projection crates can implement and
//! register handlers across the crate boundary. This module re-exports it
//! and holds the HOST-side ChangeNotifier implementation.

pub use hopnet_projection::{
    ChangeNotifier, HandlerCtx, HandlerResult, NullNotifier, TransactionHandler, TxMeta,
};

/// Host change notifier: owns platform gating and spawning for post-apply
/// side effects (macOS FileProvider refresh). Handlers signal intent via
/// `ctx.notifier.files_changed()`; everything platform-specific stays here.
pub struct HostNotifier {
    pub test_mode: bool,
}

impl ChangeNotifier for HostNotifier {
    fn files_changed(&self) {
        #[cfg(target_os = "macos")]
        {
            let test_mode = self.test_mode;
            tokio::spawn(async move {
                if let Err(e) =
                    crate::fileprovider::domain::signal_fileprovider_refresh(test_mode).await
                {
                    tracing::warn!("Failed to signal FileProvider refresh: {}", e);
                }
            });
        }
        #[cfg(not(target_os = "macos"))]
        {
            let _ = self.test_mode;
        }
    }
}
