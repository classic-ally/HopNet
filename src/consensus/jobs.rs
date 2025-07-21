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
    
    // Only create timeout votes if we're stuck in the same view
    if last_observed == current_view {
        tracing::info!("View {} has not progressed since last check, creating timeout vote", current_view);
        
        // Create timeout vote for current view and process it
        match create_timeout_vote_for_view(current_view, app_state).await {
            Ok(timeout_vote) => {
                // Send to our own timeout vote handler (which will broadcast if TC is formed)
                match app_state.timeout_vote_collector.add_vote(timeout_vote.clone(), app_state).await {
                    Ok(Some(tc)) => {
                        tracing::info!("Timeout vote for view {} created TC", current_view);
                        // Apply TC locally first, then broadcast
                        if let Err(e) = apply_timeout_certificate(tc.clone(), app_state).await {
                            tracing::error!("Failed to apply our own TC locally: {:?}", e);
                        } else {
                            // Now broadcast to other nodes  
                            if let Err(e) = broadcast_timeout_certificate(tc, app_state).await {
                                tracing::warn!("Failed to broadcast our TC: {:?}", e);
                            }
                        }
                    }
                    Ok(None) => {
                        tracing::info!("Timeout vote for view {} added, waiting for more", current_view);
                        // Also broadcast our vote to other nodes
                        let _ = broadcast_our_timeout_vote(timeout_vote, app_state).await;
                    }
                    Err(e) => {
                        tracing::error!("Failed to process our own timeout vote: {:?}", e);
                    }
                }
            }
            Err(e) => {
                tracing::error!("Failed to create timeout vote for view {}: {:?}", current_view, e);
                return Err(Error::Failed(Arc::new(Box::new(std::io::Error::new(std::io::ErrorKind::Other, format!("Timeout vote creation failed: {:?}", e))))));
            }
        }
    } else {
        // View has progressed, update our tracking
        tracing::debug!("View progressed from {} to {}, updating tracking", last_observed, current_view);
        app_state.last_observed_view.store(current_view, std::sync::atomic::Ordering::SeqCst);
    }
    
    Ok(())
}

/// Create a timeout vote for the given view
async fn create_timeout_vote_for_view(
    view_number: i32, 
    app_state: &AppState
) -> Result<TimeoutVote, CertificateError> {
    // Get current consensus state
    let consensus_state = db::get_consensus(app_state.db_pool.get())
        .map_err(|_| CertificateError::DatabaseError)?;
    
    // Create timeout data for this view
    let timeout_data = TimeoutSignData::from_consensus_state(view_number, &consensus_state);
    
    // Get our node_id (only thing we need from db::get_me)
    let me = db::get_me(app_state.db_pool.get())
        .map_err(|_| CertificateError::DatabaseError)?;
    
    // Sign the timeout data using AppState's private key
    let signature = timeout_data.sign(&app_state.private_key)
        .map_err(|_| CertificateError::SigningError)?;
    
    // Create the timeout vote
    Ok(TimeoutVote {
        sender: VoteSignMessage {
            replica_id: me.node_id,
            signature,
        },
        data: timeout_data,
    })
}

/// Broadcast our timeout vote to other validators (fire and forget)
async fn broadcast_our_timeout_vote(
    timeout_vote: TimeoutVote,
    app_state: &AppState,
) -> Result<(), CertificateError> {
    // Get our node_id and all validators except ourselves
    let me = db::get_me(app_state.db_pool.get())
        .map_err(|_| CertificateError::DatabaseError)?;
    let validators = db::get_validators(app_state.db_pool.get(), timeout_vote.data.view_number)
        .map_err(|_| CertificateError::DatabaseError)?
        .into_iter()
        .filter(|node| node.node_id != me.node_id)
        .collect::<Vec<_>>();
    
    // Broadcast timeout vote to all other validators (fire and forget)
    let client = reqwest::Client::new();
    
    for validator in validators {
        let timeout_vote_clone = timeout_vote.clone();
        let client_clone = client.clone();
        let url = format!("http://{}:{}/consensus/timeout_vote", validator.ip_address, validator.port);
        
        // Spawn fire-and-forget task
        tokio::spawn(async move {
            if let Err(e) = client_clone.post(&url).json(&timeout_vote_clone).send().await {
                tracing::warn!("Failed to send timeout vote to {}: {}", url, e);
            }
        });
    }
    
    Ok(())
}