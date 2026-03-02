use std::time::Duration;
use crate::AppState;
use crate::db::consensus as db;
use crate::consensus::functions::issue_timeout_vote;
use crate::consensus::routes::{ensure_caught_up_and_active, CatchUpMode, NodeReadiness, SyncStatus};

const TIMEOUT_DURATION: Duration = Duration::from_secs(60);
const REISSUE_INTERVAL: Duration = Duration::from_secs(60);

/// Deterministic timeout detector. Waits exactly TIMEOUT_DURATION after the last
/// view change before issuing a timeout vote, then reissues at REISSUE_INTERVAL
/// while stuck. View changes (via Notify) reset the timer immediately.
pub async fn timeout_detector(app_state: AppState) {
    loop {
        // Read current view (one DB read per cycle)
        let current_view = match db::get_consensus(app_state.db_pool.get()) {
            Ok(state) => state.view,
            Err(_) => {
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        // Wait for timeout duration, reset if view changes
        tokio::select! {
            _ = app_state.view_changed.notified() => continue,
            _ = tokio::time::sleep(TIMEOUT_DURATION) => {}
        }

        // Timer expired. Re-read view — if it advanced, restart
        let new_view = match db::get_consensus(app_state.db_pool.get()) {
            Ok(state) => state.view,
            Err(_) => continue,
        };
        if new_view != current_view {
            continue; // View advanced during sleep (race with notification)
        }

        // Ensure caught up and active before issuing timeout vote
        match ensure_caught_up_and_active(&app_state, CatchUpMode::Convergence, true, 0, None).await {
            Ok(NodeReadiness { sync_status: SyncStatus::CaughtUp, is_active: true }) => {
                let mut conn = match app_state.db_pool.get() {
                    Ok(c) => c,
                    Err(_) => continue,
                };
                let _ = issue_timeout_vote(current_view, &app_state, None, &mut conn).await;
            }
            _ => {} // inactive or error — skip, will retry
        }

        // After voting, wait for either view change or reissue interval
        tokio::select! {
            _ = app_state.view_changed.notified() => continue,
            _ = tokio::time::sleep(REISSUE_INTERVAL) => {} // loop back to reissue
        }
    }
}
