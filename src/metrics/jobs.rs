use apalis::prelude::*;
use apalis_cron::CronContext;
use serde::{Deserialize, Serialize};
use chrono::Utc;
use std::sync::Arc;
use crate::AppState;
use crate::metrics::collector::{collect_all_node_metrics, CollectionError};
use crate::consensus::Transaction;
use tokio::time::Duration as TokioDuration;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MetricsCollectionJob;

/// Handler for metrics collection jobs (runs every ~10 minutes with randomization)
pub async fn handle_metrics_collection(
    _job: MetricsCollectionJob,
    _ctx: CronContext<Utc>,
    data: Data<AppState>,
) -> Result<(), Error> {
    let app_state = &*data;
    
    tracing::info!("Starting automated metrics collection");
    
    // Use 30-second timeout per node (matching manual trigger)
    let measurement_timeout = TokioDuration::from_secs(30);
    
    match collect_all_node_metrics(app_state, measurement_timeout).await {
        Ok(metrics) => {
            if metrics.is_empty() {
                tracing::info!("No metrics collected - no nodes to measure");
                return Ok(());
            }
            
            tracing::info!("Collected {} metrics, submitting to consensus", metrics.len());
            
            // Serialize metrics for consensus submission using existing pattern
            let serialized_metrics = bincode::serde::encode_to_vec(&metrics, bincode::config::standard())
                .map_err(|e| Error::Failed(Arc::new(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData, 
                    format!("Failed to serialize metrics: {}", e)
                )))))?;
            
            // Create consensus transaction using existing pattern from routes.rs
            let tx = crate::consensus::functions::create_signed_transaction(
                app_state,
                "submit_metrics".to_string(),
                serialized_metrics,
            ).map_err(|_| Error::Failed(Arc::new(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Failed to sign transaction"
            )))))?;
            
            // Get node ID for consensus submission
            let source_node_id = app_state.get_node_id()
                .map_err(|_| Error::Failed(Arc::new(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Node not properly configured"
                )))))?;
            
            // Submit to consensus queue
            match app_state.consensus_queue.submit(tx).await {
                Ok(_) => {
                    tracing::info!("Successfully submitted metrics to consensus");
                }
                Err(e) => {
                    tracing::error!("Failed to submit metrics to consensus: {:?}", e);
                    return Err(Error::Failed(Arc::new(Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Consensus submission failed: {:?}", e)
                    )))));
                }
            }
        }
        Err(CollectionError::DatabaseError(db_err)) => {
            tracing::error!("Database error during metrics collection: {:?}", db_err);
            return Err(Error::Failed(Arc::new(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Database error: {:?}", db_err)
            )))));
        }
        Err(CollectionError::NetworkError(net_err)) => {
            tracing::warn!("Network error during metrics collection: {}", net_err);
            // Network errors are expected and shouldn't fail the job
            return Ok(());
        }
        Err(CollectionError::ConfigurationError) => {
            tracing::error!("Configuration error during metrics collection");
            return Err(Error::Failed(Arc::new(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Node configuration error"
            )))));
        }
    }
    
    Ok(())
}