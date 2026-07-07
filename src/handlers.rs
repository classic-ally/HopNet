//! Transaction-handler seam — the contract itself lives in
//! hopnet-projection (RFC-015) so projection crates can implement and
//! register handlers across the crate boundary. This module re-exports it
//! and holds the HOST-side ChangeNotifier implementation.

pub use hopnet_projection::{
    ChangeNotifier, HandlerCtx, HandlerResult, NullNotifier, NullScheduler, TransactionHandler,
    TxMeta, WorkScheduler,
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

/// Host work scheduler: routes named background work enqueued by handlers
/// during apply (`ctx.work.schedule(subsystem, key)`) onto host runtime
/// tasks. Spawns on the MAIN runtime handle (`app_state.runtime`), NOT the
/// ambient one — apply runs on the consensus shell's dedicated
/// `current_thread` runtime (time-only, no IO driver), where spawned work
/// would interleave with (and block) consensus.
///
/// Ordering note: schedule() fires DURING apply, before the block's DB
/// transaction commits. Spawned tasks must therefore tolerate not yet
/// seeing the applied rows; `takeout.materialize` retries its row lookup
/// briefly (the old in-apply `tokio::spawn` landed on the shell runtime,
/// which only polled the task after the commit — this preserves that
/// effective ordering).
pub struct HostWorkScheduler {
    pub app_state: crate::AppState,
}

impl WorkScheduler for HostWorkScheduler {
    fn schedule(&self, subsystem: &'static str, key: String) {
        match subsystem {
            "takeout.materialize" => {
                let state = self.app_state.clone();
                self.app_state.runtime.spawn(async move {
                    let takeout_id = match key.parse::<crate::db::CustomUUID>() {
                        Ok(id) => id,
                        Err(e) => {
                            tracing::error!(
                                "takeout.materialize: invalid takeout id {:?}: {:?}",
                                key,
                                e
                            );
                            return;
                        }
                    };
                    // The takeouts row lands with the block commit, which
                    // happens just after schedule(); retry briefly before
                    // giving up (the fallback job catches stragglers).
                    let mut takeout = None;
                    for _ in 0..20 {
                        match crate::db::takeout::get_takeout_by_id(
                            state.db_pool.get(),
                            &takeout_id,
                        ) {
                            Ok(Some(t)) => {
                                takeout = Some(t);
                                break;
                            }
                            Ok(None) => {
                                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                            }
                            Err(e) => {
                                tracing::error!(
                                    "takeout.materialize: lookup failed for {}: {:?}",
                                    takeout_id,
                                    e
                                );
                                return;
                            }
                        }
                    }
                    let Some(takeout) = takeout else {
                        tracing::error!(
                            "takeout.materialize: takeout {} not visible after commit window — \
                             leaving to the fallback job",
                            takeout_id
                        );
                        return;
                    };
                    if let Err(e) = hopnet_takeout::export::execute_takeout_materialization(
                        &crate::takeout_host::takeout_state(&state),
                        &takeout_id,
                        takeout.user_id,
                    )
                    .await
                    {
                        tracing::error!(
                            "Failed to trigger materialization for takeout {}: {:?}",
                            takeout_id,
                            e
                        );
                        // Don't panic - fallback job will catch this later
                    }
                });
            }
            "takeout.cleanup" => {
                let fragments_dir = self.app_state.fragments_dir.clone();
                let db_pool = self.app_state.db_pool.clone();
                self.app_state.runtime.spawn(async move {
                    let takeout_id = match key.parse::<crate::db::CustomUUID>() {
                        Ok(id) => id,
                        Err(e) => {
                            tracing::error!(
                                "takeout.cleanup: invalid takeout id {:?}: {:?}",
                                key,
                                e
                            );
                            return;
                        }
                    };
                    if let Err(e) = crate::db::takeout::cleanup_expired_takeout_files(
                        &takeout_id,
                        &fragments_dir,
                    )
                    .await
                    {
                        tracing::error!("Failed to clean up takeout files {}: {:?}", takeout_id, e);
                    }
                    if let Err(e) =
                        crate::db::takeout::cleanup_takeout_table(db_pool.get(), &takeout_id)
                    {
                        tracing::error!("Failed to clean up takeout table {}: {:?}", takeout_id, e);
                    }
                });
            }
            other => {
                // Manifest fallthrough (RFC-016 Stage 5): offer the work to
                // every registered projection; first claimant wins and its
                // future runs on the main runtime (same invariant as the
                // named takeout arms above).
                let caps = crate::drive_host::drive_state(&self.app_state);
                for projection in crate::projections::manifests() {
                    if let Some(fut) = projection.work(&caps, other, key.clone()) {
                        self.app_state.runtime.spawn(fut);
                        return;
                    }
                }
                tracing::error!(
                    "HostWorkScheduler: unknown work subsystem {:?} (key {:?})",
                    other,
                    key
                );
            }
        }
    }
}
