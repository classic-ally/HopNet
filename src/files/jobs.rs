use crate::{AppState, db::{DatabaseError, CustomUUID, fragments::{find_orphaned_data_blocks, get_node_availability_classification, AvailabilityClass}}, consensus::{functions::consensus_middleware, types::Transaction}};
use apalis::prelude::*;
use chrono::Utc;
use uuid::{Timestamp, timestamp::context::NoContext};
use std::sync::Arc;
use ed25519_dalek::Signer;
use hex;

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

/// Send fragment fetch instructions to a specific node via RPC
/// Returns the list of fragment hashes that failed to fetch for retry logic
pub async fn send_fetch_instructions_to_node(
    ip_address: &str,
    port: i32,
    fragment_instructions: Vec<FragmentFetchInstruction>,
    auth: &crate::NodeAuthInfo,
) -> Result<Vec<crate::db::Blake3Hash>, MaintenanceError> {
    tracing::info!("Sending {} fragment fetch instructions to {}:{}", fragment_instructions.len(), ip_address, port);
    
    // Calculate timeout based on fragment count
    // Assumptions:
    // - Base timeout: 30 seconds for RPC overhead
    // - Transfer rate: 1GB per 30 minutes = ~570Kbps
    // - 4MB fragment = ~7 seconds transfer time
    // - Add 2 seconds for discovery/verification per fragment
    // - Add 50% buffer for Reed-Solomon reconstruction and retries
    let base_timeout = 30;
    let seconds_per_mb = 1.8; // 1GB/30min = 0.56MB/s, so ~1.8s per MB
    let mb_per_fragment = 4.0;
    let overhead_per_fragment = 2; // discovery + verification
    let per_fragment_timeout = (seconds_per_mb * mb_per_fragment) as u64 + overhead_per_fragment;
    let timeout_seconds = base_timeout + (fragment_instructions.len() as u64 * per_fragment_timeout);
    let timeout_with_buffer = (timeout_seconds as f64 * 1.5) as u64;
    let timeout = std::time::Duration::from_secs(timeout_with_buffer);
    
    tracing::info!("Using {}s timeout for {} fragments", timeout.as_secs(), fragment_instructions.len());
    
    // Create RPC request
    let request = FetchFragmentsRequest {
        fragments: fragment_instructions.iter().map(|inst| {
            crate::files::routes::FragmentFetchInfo {
                fragment_hash: inst.fragment_hash.clone(),
                placement_height: inst.placement_height,
            }
        }).collect(),
    };
    
    // Serialize request body for signing
    let request_body = match serde_json::to_vec(&request) {
        Ok(body) => body,
        Err(e) => {
            tracing::error!("Failed to serialize RPC request: {}", e);
            return Err(MaintenanceError::Storage(format!("Failed to serialize request: {}", e)));
        }
    };
    
    // Create HTTP client with calculated timeout
    let client = reqwest::Client::builder()
        .timeout(timeout)
        .build()
        .map_err(|e| MaintenanceError::Storage(format!("HTTP client error: {}", e)))?;
    
    // Sign request body for POST authentication (same pattern as fragment transfer)
    let node_signature = auth.node_private_key.sign(&request_body);
    let user_signature = auth.user_keys.private_key.sign(&request_body);
    
    // Send RPC request with dual Ed25519 signatures
    let url = format!("http://{}:{}/rpc/fetch-fragments", ip_address, port);
    let response = client
        .post(&url)
        .header("Content-Type", "application/json")
        .header("X-Node-ID", auth.node_id.to_string())
        .header("X-User-ID", auth.user_id.to_string())
        .header("X-Node-Signature", hex::encode(node_signature.to_bytes()))
        .header("X-User-Signature", hex::encode(user_signature.to_bytes()))
        .body(request_body)
        .send()
        .await
        .map_err(|e| {
            tracing::error!("Network error sending to {}:{}: {}", ip_address, port, e);
            MaintenanceError::Storage(format!("Network error: {}", e))
        })?;
    
    if !response.status().is_success() {
        tracing::error!("RPC request to {}:{} failed with status: {}", ip_address, port, response.status());
        return Err(MaintenanceError::Storage(format!("RPC request failed with status: {}", response.status())));
    }
    
    let fetch_response: FetchFragmentsResponse = response
        .json()
        .await
        .map_err(|e| {
            tracing::error!("Failed to parse response from {}:{}: {}", ip_address, port, e);
            MaintenanceError::Storage(format!("Failed to parse response: {}", e))
        })?;
    
    tracing::info!("Node {}:{} fetch result: {}/{} successful", 
                  ip_address, port, fetch_response.successful_fetches, fetch_response.total_requested);
    
    // Return the failed fragment hashes as strings
    // In the future, we could parse these back to Blake3Hash if needed for retry logic
    if !fetch_response.failed_fetches.is_empty() {
        tracing::warn!("Node {}:{} failed to fetch {} fragments: {:?}", 
                     ip_address, port, fetch_response.failed_fetches.len(), 
                     &fetch_response.failed_fetches);
    }
    
    // For now, return empty Vec since we're not implementing retry logic yet
    // TODO: Implement retry logic that parses hex strings back to Blake3Hash
    Ok(Vec::new())
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct FragmentFetchInstruction {
    pub fragment_hash: crate::db::Blake3Hash,
    pub placement_height: i32,
}

// Import types from routes for RPC communication
use crate::files::routes::{FetchFragmentsRequest, FetchFragmentsResponse};

/// Network rebalancing job to redistribute fragments to optimal nodes
/// Processes data blocks atomically - only updates placement height after all fragments migrate successfully
pub async fn run_network_rebalancing(
    app_state: &AppState,
    max_data_blocks: i32,
    min_age_heights: i32,
) -> Result<NetworkRebalancingResult, Error> {
    tracing::info!("Starting network rebalancing (max {} data blocks, min age {} heights)", 
                  max_data_blocks, min_age_heights);
    
    // Get current consensus height
    let consensus_height = match crate::db::consensus::get_consensus(app_state.db_pool.get()) {
        Ok(consensus_state) => consensus_state.committed_block.data.height,
        Err(e) => {
            tracing::error!("Failed to get consensus state for rebalancing: {:?}", e);
            return Err(Error::Failed(Arc::new(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other, 
                format!("Failed to get consensus state: {:?}", e)
            )))));
        }
    };
    
    let max_placement_height = consensus_height - min_age_heights;
    tracing::info!("Rebalancing at height {}, looking for data blocks placed before height {}", 
                  consensus_height, max_placement_height);
    
    // Get node metrics for placement computation
    let node_metrics = match crate::db::metrics::get_all_node_metrics(app_state.db_pool.get(), consensus_height) {
        Ok(metrics) => metrics,
        Err(e) => {
            tracing::error!("Failed to get node metrics: {:?}", e);
            return Err(Error::Failed(Arc::new(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to get node metrics: {:?}", e)
            )))));
        }
    };
    
    if node_metrics.is_empty() {
        tracing::warn!("No node metrics available for rebalancing");
        return Ok(NetworkRebalancingResult::default());
    }
    
    // Get all nodes with connection info
    let all_nodes = match crate::db::nodes::get_nodes(app_state.db_pool.get()) {
        Ok(nodes) => nodes,
        Err(e) => {
            tracing::error!("Failed to get nodes: {:?}", e);
            return Err(Error::Failed(Arc::new(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to get nodes: {:?}", e)
            )))));
        }
    };
    
    let node_map: std::collections::HashMap<i32, (String, i32)> = all_nodes
        .into_iter()
        .map(|node| (node.node_id, (node.ip_address, node.port)))
        .collect();
    
    // Get data blocks that need rebalancing
    let data_blocks_to_rebalance = match crate::db::fragments::get_data_blocks_for_rebalancing(
        app_state.db_pool.get(),
        max_placement_height,
        max_data_blocks,
    ) {
        Ok(blocks) => blocks,
        Err(e) => {
            tracing::error!("Failed to get data blocks for rebalancing: {:?}", e);
            return Err(Error::Failed(Arc::new(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to get data blocks: {:?}", e)
            )))));
        }
    };
    
    let total_data_blocks = data_blocks_to_rebalance.len();
    tracing::info!("Found {} data blocks to rebalance", total_data_blocks);
    
    // Create node auth for RPC calls
    let node_auth = match crate::NodeAuthInfo::from_app_state(app_state) {
        Ok(auth) => auth,
        Err(_) => {
            tracing::error!("Failed to create node auth");
            return Err(Error::Failed(Arc::new(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failed to create node auth"
            )))));
        }
    };
    
    let user_id = match app_state.get_user_id() {
        Ok(id) => id,
        Err(_) => {
            tracing::error!("User ID not initialized");
            return Err(Error::Failed(Arc::new(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "User ID not initialized"
            )))));
        }
    };
    
    // Process each data block
    let mut successful_data_blocks = 0;
    let mut failed_data_blocks = 0;
    let mut total_fragments_migrated = 0;
    
    for data_block_info in data_blocks_to_rebalance {
        tracing::info!("Processing data block {} with {} fragments (placed at height {})",
                      data_block_info.data_block_id,
                      data_block_info.fragments.len(),
                      data_block_info.placement_height);
        
        // Determine optimal placement for each fragment
        let mut node_fragments: std::collections::HashMap<i32, Vec<FragmentFetchInstruction>> = 
            std::collections::HashMap::new();
        
        for fragment_info in &data_block_info.fragments {
            let fragment_type = match fragment_info.chunk_type.as_str() {
                "original" => crate::files::placement::FragmentType::Original,
                "recovery" => crate::files::placement::FragmentType::Recovery,
                _ => {
                    tracing::error!("Unknown chunk type: {}", fragment_info.chunk_type);
                    continue;
                }
            };
            
            let candidates = crate::files::discovery::get_fragment_placement_candidates(
                &fragment_info.fragment_hash,
                fragment_type,
                &node_metrics,
            );
            
            // For now, just use the best candidate
            // TODO: In future, spread across multiple candidates based on replication factor
            if let Some(best) = candidates.first() {
                node_fragments
                    .entry(best.node_id)
                    .or_insert_with(Vec::new)
                    .push(FragmentFetchInstruction {
                        fragment_hash: fragment_info.fragment_hash.clone(),
                        placement_height: consensus_height,
                    });
            }
        }
        
        // Send fetch instructions to all nodes in parallel
        let mut fetch_tasks = Vec::new();
        
        for (node_id, fragments) in node_fragments {
            let (ip_address, port) = match node_map.get(&node_id) {
                Some(info) => info.clone(),
                None => {
                    tracing::error!("No connection info for node {}", node_id);
                    continue;
                }
            };
            
            let node_auth_clone = node_auth.clone();
            let task = tokio::spawn(async move {
                let result = send_fetch_instructions_to_node(
                    &ip_address,
                    port,
                    fragments.clone(),
                    &node_auth_clone,
                ).await;
                (node_id, result, fragments.len())
            });
            
            fetch_tasks.push(task);
        }
        
        // Wait for all fetches to complete
        let mut all_successful = true;
        let mut fragments_migrated = 0;
        
        for task in fetch_tasks {
            match task.await {
                Ok((node_id, result, fragment_count)) => {
                    match result {
                        Ok(failed_hashes) => {
                            if !failed_hashes.is_empty() {
                                tracing::error!("Node {} failed to fetch {} fragments for data block {}",
                                             node_id, failed_hashes.len(), data_block_info.data_block_id);
                                all_successful = false;
                            } else {
                                fragments_migrated += fragment_count;
                            }
                        }
                        Err(e) => {
                            tracing::error!("Failed to send instructions to node {}: {:?}", node_id, e);
                            all_successful = false;
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Task failed: {:?}", e);
                    all_successful = false;
                }
            }
        }
        
        // Only update placement height if ALL fragments migrated successfully
        if all_successful && fragments_migrated == data_block_info.fragments.len() {
            tracing::info!("All {} fragments migrated successfully for data block {}, updating placement height",
                         fragments_migrated, data_block_info.data_block_id);
            
            // Submit consensus transaction to update placement height
            let update = crate::db::files::PlacementHeightUpdate {
                data_block_id: data_block_info.data_block_id.clone(),
                placement_height: consensus_height,
            };
            
            let payload = match bincode::serde::encode_to_vec(&vec![update], bincode::config::standard()) {
                Ok(data) => data,
                Err(e) => {
                    tracing::error!("Failed to serialize placement update: {:?}", e);
                    failed_data_blocks += 1;
                    continue;
                }
            };
            
            let transaction = Transaction {
                function: "update_placement_heights".to_string(),
                payload,
            };
            
            match consensus_middleware(app_state, vec![transaction], user_id).await {
                Ok(_) => {
                    tracing::info!("Successfully updated placement height for data block {}",
                                 data_block_info.data_block_id);
                    successful_data_blocks += 1;
                    total_fragments_migrated += fragments_migrated;
                }
                Err(e) => {
                    tracing::error!("Failed to update placement height via consensus: {:?}", e);
                    failed_data_blocks += 1;
                }
            }
        } else {
            tracing::error!("Failed to migrate all fragments for data block {} ({}/{})",
                         data_block_info.data_block_id, fragments_migrated, data_block_info.fragments.len());
            failed_data_blocks += 1;
        }
    }
    
    let result = NetworkRebalancingResult {
        consensus_height,
        data_blocks_checked: total_data_blocks,
        data_blocks_rebalanced: successful_data_blocks,
        data_blocks_failed: failed_data_blocks,
        total_fragments_migrated,
    };
    
    tracing::info!("Network rebalancing completed: {:?}", result);
    Ok(result)
}

#[derive(Debug, Default, serde::Serialize)]
pub struct NetworkRebalancingResult {
    pub consensus_height: i32,
    pub data_blocks_checked: usize,
    pub data_blocks_rebalanced: usize,
    pub data_blocks_failed: usize,
    pub total_fragments_migrated: usize,
}