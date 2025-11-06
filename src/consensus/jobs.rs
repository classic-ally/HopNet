use super::*;
use apalis::prelude::*;
use apalis_cron::CronContext;
use serde::{Deserialize, Serialize};
use chrono::Utc;
use std::sync::Arc;
use crate::AppState;
use crate::db::consensus as db;
use crate::consensus::routes::{apply_timeout_certificate, broadcast_timeout_certificate};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TimeoutDetectionJob;

/// Handler for timeout detection jobs (runs every minute)
pub async fn handle_timeout_detection(
    _job: TimeoutDetectionJob,
    _ctx: CronContext<Utc>,
    data: Data<AppState>,
) -> Result<(), Error> {
    let app_state = &*data;
    
    // Get current consensus state
    let consensus_state = db::get_consensus(app_state.db_pool.get())
        .map_err(|e| Error::Failed(Arc::new(Box::new(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to get consensus state: {:?}", e))))))?;
    
    let current_view = consensus_state.view;
    let last_observed = app_state.last_observed_view.load(std::sync::atomic::Ordering::SeqCst);

    // Ensure we're caught up and active before participating in consensus
    use crate::consensus::routes::{ensure_caught_up_and_active, CatchUpMode, NodeReadiness, SyncStatus};
    match ensure_caught_up_and_active(app_state, CatchUpMode::Convergence, true, 0, None).await {
        Ok(NodeReadiness { sync_status: SyncStatus::CaughtUp, is_active: true }) => {
            // We're caught up and active, proceed with timeout detection
            tracing::debug!("Node is caught up and active, proceeding with timeout detection");
        }
        Ok(NodeReadiness { is_active: false, .. }) => {
            // We're inactive (activation request submitted), skip timeout detection
            tracing::info!("Node is inactive at current height - activation requested, skipping timeout detection this cycle");
            return Ok(());
        }
        Ok(NodeReadiness { sync_status, .. }) => {
            // Should never happen with Convergence mode (always CaughtUp or error)
            tracing::warn!("Unexpected sync status after convergence: {:?}, skipping timeout detection", sync_status);
            return Ok(());
        }
        Err(e) => {
            tracing::error!("Failed to ensure caught up and active: {:?}", e);
            return Err(Error::Failed(Arc::new(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to ensure caught up and active: {:?}", e)
            )))));
        }
    }
    
    // Only create timeout votes if we're stuck in the same view
    if last_observed == current_view {
        // Check if we've already issued a timeout vote for this view
        if consensus_state.last_timeout_vote_view == current_view {
            // P2: Already issued - proactively check for higher QC and reissue for resilience
            // This prevents bucket fragmentation deadlocks and provides message loss tolerance
            tracing::debug!("View {} still stuck, checking for higher QC before reissuing timeout vote", current_view);

            // Proactively discover higher QC from quorum (prevents bucket deadlock)
            use crate::consensus::routes::ensure_intra_view_synced;
            if let Err(e) = ensure_intra_view_synced(app_state).await {
                tracing::warn!("Failed to sync before timeout vote reissuance: {:?}", e);
                // Continue anyway - rebroadcast with current QC for message loss resilience
            }

            // Reissue with current (potentially updated) QC
            // issue_timeout_vote is reissuance-safe: creates vote from current consensus state
            use crate::consensus::functions::issue_timeout_vote;
            match issue_timeout_vote(current_view, app_state, None).await {
                Ok(_) => {
                    tracing::info!("Successfully reissued timeout vote for view {} (proactive convergence + message loss resilience)", current_view);
                }
                Err(e) => {
                    tracing::error!("Failed to reissue timeout vote for view {}: {:?}", current_view, e);
                    // Don't fail the job - will retry next cycle
                }
            }
        } else {
            // First time stuck - issue new timeout vote
            tracing::info!("View {} has not progressed since last check, issuing timeout vote", current_view);

            use crate::consensus::functions::issue_timeout_vote;
            match issue_timeout_vote(current_view, app_state, None).await {
                Ok(_) => {
                    tracing::debug!("Successfully issued timeout vote for view {}", current_view);
                }
                Err(e) => {
                    tracing::error!("Failed to issue timeout vote for view {}: {:?}", current_view, e);
                    return Err(Error::Failed(Arc::new(Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Timeout vote issuance failed: {:?}", e)
                    )))));
                }
            }
        }
    } else {
        // View has progressed, update our tracking
        tracing::debug!("View progressed from {} to {}, updating tracking", last_observed, current_view);
        app_state.last_observed_view.store(current_view, std::sync::atomic::Ordering::SeqCst);
    }

    Ok(())
}