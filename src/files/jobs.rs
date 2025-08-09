use crate::{AppState, db::{DatabaseError, CustomUUID, fragments::{find_orphaned_data_blocks, get_node_availability_classification, AvailabilityClass}}, consensus::{functions::consensus_middleware, types::Transaction}};
use apalis::prelude::*;
use chrono::Utc;
use uuid::{Timestamp, timestamp::context::NoContext};
use std::sync::Arc;

#[derive(Debug)]
pub enum MaintenanceError {
    Database(DatabaseError),
    Storage(String),
    Configuration(String),
}

impl From<DatabaseError> for MaintenanceError {
    fn from(e: DatabaseError) -> Self {
        MaintenanceError::Database(e)
    }
}

/// Manual trigger for orphaned data block cleanup  
/// Initially only supports manual trigger - threshold checking and scheduling to be added later
pub async fn handle_orphaned_data_block_cleanup(job: TaskId, ctx: Data<AppState>) -> Result<(), Error> {
    // Use default values for scheduled jobs
    run_orphaned_data_block_cleanup(&ctx, 50, 30).await.map(|_| ())
}

/// Core cleanup logic that can be called from job handler or manual trigger
pub async fn run_orphaned_data_block_cleanup(app_state: &AppState, batch_size: i32, retention_days: i64) -> Result<usize, Error> {
    tracing::info!("Starting orphaned data block cleanup");
    
    // Get node ID for availability classification
    let node_id = match app_state.get_node_id() {
        Ok(id) => id,
        Err(_) => {
            tracing::error!("Node ID not initialized, cannot run cleanup");
            return Err(Error::Failed(Arc::new(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Node ID not initialized")))));
        }
    };
    
    // Get database connection
    let db_connection = app_state.db_pool.get();
    
    // Determine cleanup strategy based on availability
    let (node_availability, availability_class) = match get_node_availability_classification(
        db_connection,
        node_id,
        30, // 30-day rolling average
    ) {
        Ok((avail, class)) => (avail, class),
        Err(e) => {
            tracing::error!("Failed to determine node availability: {:?}", e);
            return Err(Error::Failed(Arc::new(Box::new(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to determine node availability: {:?}", e))))));
        }
    };
    
    tracing::info!(
        "Node availability: {:.1}%, classification: {:?}", 
        node_availability * 100.0, 
        availability_class
    );
    
    // For now, implement only historical data cleanup (Stage 1 for below-average nodes)
    // Redundant copy cleanup to be implemented later
    match availability_class {
        AvailabilityClass::BelowAverage => {
            tracing::info!("Below-average availability node: cleaning historical data first");
            cleanup_orphaned_data_blocks(app_state, node_id, batch_size, retention_days).await
        }
        AvailabilityClass::AboveAverage => {
            tracing::info!("Above-average availability node: would clean redundant copies first (not implemented yet)");
            // TODO: Implement redundant copy cleanup
            // For now, also clean historical data
            cleanup_orphaned_data_blocks(app_state, node_id, batch_size, retention_days).await
        }
    }
}

async fn cleanup_orphaned_data_blocks(
    app_state: &AppState,
    _node_id: i32,
    batch_size: i32,
    retention_days: i64,
) -> Result<usize, Error> {
    let mut total_cleaned = 0;
    
    // Generate cutoff UUID for retention policy
    let cutoff_uuid = generate_cutoff_uuid(retention_days)
        .map_err(|e| {
            tracing::error!("Failed to generate cutoff UUID: {:?}", e);
            Error::Failed(Arc::new(Box::new(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to generate cutoff UUID: {:?}", e)))))
        })?;
    
    tracing::info!("Using {}-day retention policy, batch size: {}, cutoff UUID: {}", retention_days, batch_size, cutoff_uuid);
    
    loop {
        // Get database connection for this batch
        let db_connection = app_state.db_pool.get();
        
        // Find batch of orphaned data blocks
        let data_block_ids = match find_orphaned_data_blocks(db_connection, &cutoff_uuid, batch_size) {
            Ok(ids) => ids,
            Err(e) => {
                tracing::error!("Failed to find orphaned data blocks: {:?}", e);
                return Err(Error::Failed(Arc::new(Box::new(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to find orphaned data blocks: {:?}", e))))));
            }
        };
        
        if data_block_ids.is_empty() {
            tracing::info!("No more orphaned data blocks to clean");
            break;
        }
        
        tracing::info!("Found {} orphaned data blocks in this batch", data_block_ids.len());
        
        // Submit consensus transaction to delete these data blocks
        let payload = crate::files::handlers::DeleteOrphanedDataBlocksPayload {
            data_block_ids: data_block_ids.clone(),
        };
        
        let serialized_payload = match bincode::serde::encode_to_vec(&payload, bincode::config::standard()) {
            Ok(data) => data,
            Err(e) => {
                tracing::error!("Failed to serialize deletion payload: {:?}", e);
                return Err(Error::Failed(Arc::new(Box::new(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to serialize deletion payload: {:?}", e))))));
            }
        };
        
        let transaction = Transaction {
            function: "delete_orphaned_data_blocks".to_string(),
            payload: serialized_payload,
        };
        
        // Get user ID for consensus submission
        let user_id = match app_state.get_user_id() {
            Ok(id) => id,
            Err(_) => {
                tracing::error!("User ID not initialized, cannot submit consensus transaction");
                return Err(Error::Failed(Arc::new(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "User ID not initialized")))));
            }
        };
        
        // Submit to consensus
        match consensus_middleware(app_state, vec![transaction], user_id).await {
            Ok(_) => {
                tracing::info!("Successfully submitted consensus transaction to delete {} data blocks", data_block_ids.len());
                total_cleaned += data_block_ids.len();
            }
            Err(e) => {
                tracing::error!("Failed to submit consensus transaction: {:?}", e);
                return Err(Error::Failed(Arc::new(Box::new(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to submit consensus transaction: {:?}", e))))));
            }
        }
    }
    
    Ok(total_cleaned)
}

fn generate_cutoff_uuid(retention_days: i64) -> Result<CustomUUID, MaintenanceError> {
    let cutoff_time = Utc::now() - chrono::Duration::days(retention_days);
    
    let timestamp = Timestamp::from_unix(
        NoContext,
        cutoff_time.timestamp() as u64,
        0,
    );
    
    Ok(CustomUUID::new(Some(&timestamp)))
}