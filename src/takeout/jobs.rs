use apalis::prelude::*;
use apalis_cron::CronContext;
use serde::{Deserialize, Serialize};
use chrono::Utc;
use std::sync::Arc;
use crate::AppState;
use crate::db::{CustomUUID, takeout::TakeoutStatusPayload};
use crate::consensus::Transaction;
use hopnet_common::TakeoutStatus;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TakeoutMaintenanceJob;

/// Handler for takeout maintenance (runs every 4-6 hours with randomization)
/// Handles expiration checking and orphaned cleanup as a safety net for edge cases
pub async fn handle_takeout_maintenance(
    _job: TakeoutMaintenanceJob,
    _ctx: CronContext<Utc>,
    data: Data<AppState>,
) -> Result<(), Error> {
    let app_state = &*data;

    tracing::info!("Starting takeout maintenance job");

    // Get user ID for consensus submissions
    let user_id = match app_state.get_user_id() {
        Ok(id) => id,
        Err(_) => {
            tracing::warn!("User ID not initialized, skipping takeout maintenance");
            return Ok(());
        }
    };

    // Step 1: Find and mark expired takeouts (network-wide, not just this node's)
    let expired_takeouts = match crate::db::takeout::get_expired_takeouts_needing_status_update(
        app_state.db_pool.get()
    ) {
        Ok(takeouts) => takeouts,
        Err(e) => {
            tracing::error!("Failed to get expired takeouts: {:?}", e);
            return Err(Error::Failed(Arc::new(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to get expired takeouts: {:?}", e)
            )))));
        }
    };

    if expired_takeouts.is_empty() {
        tracing::debug!("No expired takeouts found needing status update");
    } else {
        tracing::info!("Found {} expired takeouts needing status update", expired_takeouts.len());

        // Batch all expiration updates into a single consensus submission
        let mut transactions = Vec::new();

        for takeout_id in &expired_takeouts {
            let status_payload = TakeoutStatusPayload {
                takeout_id: takeout_id.clone(),
                new_status: TakeoutStatus::Expired,
            };

            let encoded_payload = match bincode::serde::encode_to_vec(&status_payload, bincode::config::standard()) {
                Ok(data) => data,
                Err(e) => {
                    tracing::error!("Failed to encode expired status payload for takeout {}: {:?}", takeout_id, e);
                    continue; // Skip this one but continue with others
                }
            };

            transactions.push(crate::consensus::functions::create_signed_transaction(
                app_state,
                "update_takeout_status".to_string(),
                encoded_payload,
            ).map_err(|_| Error::Failed(Arc::new(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Failed to sign transaction"
            )))))?);
        }

        if transactions.is_empty() {
            tracing::warn!("No valid transactions to submit for expiration updates");
        } else {
            tracing::info!("Submitting {} expiration updates to consensus in single batch", transactions.len());

            // Submit all expiration updates in one consensus call
            // This triggers cleanup on owner nodes for all expired takeouts
            let results = app_state.consensus_queue.submit_batch(transactions).await;
            let failures: Vec<_> = results.iter().filter(|r| r.is_err()).collect();
            if failures.is_empty() {
                tracing::info!("Successfully marked {} takeouts as expired via consensus", expired_takeouts.len());
            } else {
                tracing::error!("Failed to submit expiration updates to consensus: {} failures", failures.len());
                return Err(Error::Failed(Arc::new(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to submit expiration updates: {} failures", failures.len())
                )))));
            }
        }
    }

    // TODO: Step 2: Handle stuck processing (takeouts that should have processed but didn't)
    // TODO: Step 3: Clean up orphaned files (files without corresponding database entries)

    tracing::info!("Takeout maintenance job completed");
    Ok(())
}

#[derive(Debug)]
pub enum TakeoutMaintenanceError {
    Database(crate::db::DatabaseError),
    Consensus(String),
}

impl From<crate::db::DatabaseError> for TakeoutMaintenanceError {
    fn from(e: crate::db::DatabaseError) -> Self {
        TakeoutMaintenanceError::Database(e)
    }
}

impl std::fmt::Display for TakeoutMaintenanceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TakeoutMaintenanceError::Database(e) => write!(f, "Database error: {:?}", e),
            TakeoutMaintenanceError::Consensus(e) => write!(f, "Consensus error: {}", e),
        }
    }
}

impl std::error::Error for TakeoutMaintenanceError {}