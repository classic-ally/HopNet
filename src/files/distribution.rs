use crate::{
    AppState,
    db::{DatabaseError, files::{PlacementHeightUpdate, get_distributable_file, mark_fragment_local_state, DistributableFileData}, consensus},
    consensus::{functions::{consensus_middleware, ConsensusError}, types::Transaction},
    db::metrics::get_all_node_metrics,
    types::Blake3Hash,
    db::types::CustomUUID,
};
use ed25519_dalek::Signer;
use reqwest::StatusCode;

#[derive(Debug)]
pub enum DistributionError {
    Database(DatabaseError),
    Consensus(ConsensusError),
    Configuration(StatusCode),
    Network(String),
    FragmentTransfer(String),
    Encoding(bincode::error::EncodeError),
}

impl std::fmt::Display for DistributionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DistributionError::Database(e) => write!(f, "Database error: {:?}", e),
            DistributionError::Consensus(e) => write!(f, "Consensus error: {:?}", e),
            DistributionError::Configuration(e) => write!(f, "Configuration error: {:?}", e),
            DistributionError::Network(e) => write!(f, "Network error: {}", e),
            DistributionError::FragmentTransfer(e) => write!(f, "Fragment transfer error: {}", e),
            DistributionError::Encoding(e) => write!(f, "Encoding error: {:?}", e),
        }
    }
}

impl std::error::Error for DistributionError {}

impl From<DatabaseError> for DistributionError {
    fn from(e: DatabaseError) -> Self {
        DistributionError::Database(e)
    }
}

impl From<ConsensusError> for DistributionError {
    fn from(e: ConsensusError) -> Self {
        DistributionError::Consensus(e)
    }
}

impl From<StatusCode> for DistributionError {
    fn from(e: StatusCode) -> Self {
        DistributionError::Configuration(e)
    }
}

impl From<bincode::error::EncodeError> for DistributionError {
    fn from(e: bincode::error::EncodeError) -> Self {
        DistributionError::Encoding(e)
    }
}

/// Run fragment distribution for a newly uploaded file
/// Called directly after file upload completion (event-driven)
pub async fn distribute_fragments_for_upload(
    app_state: &AppState,
    data_block_id: CustomUUID,
) -> Result<(), DistributionError> {
    tracing::info!("Starting fragment distribution for uploaded file {}", data_block_id);
    
    // Get current consensus height for deterministic placement
    let consensus_state = consensus::get_consensus(app_state.db_pool.get())?;
    let consensus_height = consensus_state.committed_block.data.height;
    
    // Get the file if it needs distribution
    let data_block = match get_distributable_file(app_state.db_pool.get(), data_block_id.clone())? {
        Some(block) => block,
        None => {
            tracing::debug!("File {} does not need distribution (already distributed or not fully local)", data_block_id);
            return Ok(());
        }
    };
    
    // Distribute all fragments for this file
    distribute_file_fragments(app_state, &data_block, consensus_height).await?;
    
    // Submit placement height update to consensus
    let update = PlacementHeightUpdate {
        data_block_id: data_block.id,
        placement_height: consensus_height,
    };
    submit_placement_update_to_consensus(app_state, update).await?;
    
    tracing::info!("Successfully completed fragment distribution for {}", data_block_id);
    Ok(())
}


/// Distribute all fragments for a single file
async fn distribute_file_fragments(
    app_state: &AppState,
    data_block: &DistributableFileData,
    consensus_height: i32,
) -> Result<(), DistributionError> {
    // Get our node ID to avoid sending fragments to ourselves
    let my_node_id = app_state.get_node_id().map_err(|_| DistributionError::Network("Failed to get node ID".to_string()))?;
    
    // Get all node metrics at the locked consensus height
    let node_metrics = get_all_node_metrics(app_state.db_pool.get(), consensus_height)?;
        
    // Create lookup map for node connection info to avoid separate DB calls
    let node_connections: std::collections::HashMap<i32, (&str, i32)> = node_metrics
        .iter()
        .map(|m| (m.node_id, (m.ip_address.as_str(), m.port)))
        .collect();
    
    // Memory limit for concurrent processing
    const MAX_CONCURRENT_FRAGMENTS: usize = 100;
    
    // Process fragments in batches to limit memory usage
    let total_fragments = data_block.fragment_hashes.len();
    let mut processed = 0;
    
    while processed < total_fragments {
        let batch_end = (processed + MAX_CONCURRENT_FRAGMENTS).min(total_fragments);
        let batch = &data_block.fragment_hashes[processed..batch_end];
        
        tracing::info!(
            "Processing fragment batch {}-{} of {}",
            processed + 1,
            batch_end,
            total_fragments
        );
        
        // Process this batch of fragments
        for (_index, fragment_hash, fragment_type) in batch {
            // Calculate placement for this fragment using standardized algorithm
            let scored_candidates = crate::files::discovery::get_fragment_placement_candidates(
                fragment_hash,
                *fragment_type,
                &node_metrics,
            );
            
            // Try to place fragment on nodes in preference order
            let mut placed = false;
            for candidate in scored_candidates {
                // Skip sending to ourselves - keep fragment local if we're the best placement
                if candidate.node_id == my_node_id {
                    tracing::debug!("Fragment {} best placed on local node {}, keeping locally", 
                                   fragment_hash, my_node_id);
                    // Fragment stays local (stored_locally remains TRUE)
                    placed = true;
                    break;
                }
                
                // Send to remote node
                match send_fragment_to_node(app_state, candidate.node_id, fragment_hash, &node_connections).await {
                    Ok(()) => {
                        // Mark fragment as not stored locally since it was sent elsewhere
                        mark_fragment_local_state(app_state.db_pool.get(), fragment_hash, false)?;
                        placed = true;
                        break;
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Failed to send fragment {} to node {}: {:?}",
                            fragment_hash, candidate.node_id, e
                        );
                        // Try next node
                    }
                }
            }
            
            if !placed {
                return Err(DistributionError::FragmentTransfer(
                    format!("Failed to place fragment {}", fragment_hash)
                ));
            }
        }
        
        processed = batch_end;
    }
    
    tracing::info!("Successfully distributed all {} fragments", total_fragments);
    Ok(())
}

/// Send a fragment to a specific node with retry logic and timeout
async fn send_fragment_to_node(
    app_state: &AppState,
    node_id: i32,
    fragment_hash: &Blake3Hash,
    node_connections: &std::collections::HashMap<i32, (&str, i32)>,
) -> Result<(), DistributionError> {
    // 1. Look up node address from the connections map
    let (ip_address, port) = node_connections.get(&node_id)
        .ok_or_else(|| DistributionError::Network(format!("Node {} not found in connection map", node_id)))?;
    
    // 2. Read fragment from local storage
    let fragment_data = crate::files::functions::fetch_and_verify_fragment(
        fragment_hash, 
        &app_state.fragments_dir
    ).map_err(|e| DistributionError::FragmentTransfer(format!("Failed to read fragment {}: {:?}", fragment_hash, e)))?;
    
    // 3. Get node and user credentials for inter-node authentication
    let my_node_id = app_state.get_node_id().map_err(|_| DistributionError::Network("Failed to get node ID".to_string()))?;
    let user_keys = app_state.get_user_keys().map_err(|_| DistributionError::Network("Failed to get user keys".to_string()))?;
    let user_id = app_state.get_user_id().map_err(|_| DistributionError::Network("Failed to get user ID".to_string()))?;
    
    // 4. Sign the fragment data with both node and user keys for authentication
    let node_signature = app_state.private_key.sign(&fragment_data);
    let user_signature = user_keys.private_key.sign(&fragment_data);
    
    // 5. Retry configuration
    const MAX_RETRIES: u32 = 3;
    const BASE_DELAY_MS: u64 = 500;
    const CONNECTION_TIMEOUT_SECONDS: u64 = 5;  // Fast connection establishment
    const REQUEST_TIMEOUT_SECONDS: u64 = 30;    // Generous upload time
    
    let url = format!("http://{}:{}/fragments/{}", ip_address, port, fragment_hash);
    
    // 6. Retry loop with exponential backoff
    for attempt in 0..MAX_RETRIES {
        // Create client with split timeouts for each attempt
        let client = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(CONNECTION_TIMEOUT_SECONDS))
            .timeout(std::time::Duration::from_secs(REQUEST_TIMEOUT_SECONDS))
            .build()
            .map_err(|e| DistributionError::Network(format!("Failed to create HTTP client: {}", e)))?;
        
        let response_result = client
            .post(&url)
            .header("Content-Type", "application/octet-stream")
            .header("X-Node-ID", my_node_id.to_string())
            .header("X-User-ID", user_id.to_string())
            .header("X-Node-Signature", hex::encode(node_signature.to_bytes()))
            .header("X-User-Signature", hex::encode(user_signature.to_bytes()))
            .body(fragment_data.clone())
            .send()
            .await;
        
        match response_result {
            Ok(response) => {
                if response.status().is_success() {
                    tracing::debug!("Successfully sent fragment {} to node {} ({}:{}) on attempt {}", 
                                   fragment_hash, node_id, ip_address, port, attempt + 1);
                    return Ok(());
                } else if response.status().is_client_error() {
                    // Don't retry client errors (4xx) - these are permanent failures
                    return Err(DistributionError::FragmentTransfer(
                        format!("Fragment transfer failed with permanent error: HTTP {}", response.status())
                    ));
                } else {
                    // Server errors (5xx) are retryable
                    tracing::warn!("Fragment transfer attempt {} failed with HTTP {}, will retry", 
                                  attempt + 1, response.status());
                }
            }
            Err(e) => {
                if e.is_timeout() {
                    tracing::warn!("Fragment transfer attempt {} timed out ({}s connection + {}s request), will retry", 
                                  attempt + 1, CONNECTION_TIMEOUT_SECONDS, REQUEST_TIMEOUT_SECONDS);
                } else if e.is_connect() {
                    tracing::warn!("Fragment transfer attempt {} failed to connect within {}s, will retry", 
                                  attempt + 1, CONNECTION_TIMEOUT_SECONDS);
                } else {
                    tracing::warn!("Fragment transfer attempt {} failed with network error: {}, will retry", 
                                  attempt + 1, e);
                }
            }
        }
        
        // Don't delay after the last attempt
        if attempt < MAX_RETRIES - 1 {
            let delay_ms = BASE_DELAY_MS * 2_u64.pow(attempt);
            tracing::debug!("Waiting {}ms before retry attempt {}", delay_ms, attempt + 2);
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
        }
    }
    
    // All attempts failed
    Err(DistributionError::FragmentTransfer(
        format!("Failed to send fragment {} to node {} after {} attempts", 
                fragment_hash, node_id, MAX_RETRIES)
    ))
}


/// Submit placement height update to consensus
async fn submit_placement_update_to_consensus(
    app_state: &AppState,
    update: PlacementHeightUpdate,
) -> Result<(), DistributionError> {
    let source_node_id = app_state.get_node_id()?;
    
    // Single update wrapped in vector for consistency with handler
    let updates = vec![update];
    let encoded_updates = bincode::serde::encode_to_vec(&updates, bincode::config::standard())?;
    
    let transaction = crate::consensus::functions::create_signed_transaction(
        app_state,
        "update_placement_heights".to_string(),
        encoded_updates,
    ).map_err(|_| DistributionError::Database(crate::db::DatabaseError::LockError))?;
    
    consensus_middleware(app_state, vec![transaction]).await?;
    
    tracing::info!("Submitted placement height update to consensus");
    Ok(())
}