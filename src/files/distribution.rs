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

    // Wait for the insert_files consensus transaction to execute and set stored_locally = TRUE
    // This prevents a race condition where we try to distribute before fragments are marked local
    const MAX_WAIT_ATTEMPTS: u32 = 20;  // 10 seconds total (20 * 500ms)
    const POLL_INTERVAL_MS: u64 = 500;

    let mut attempt = 0;
    let data_block = loop {
        match get_distributable_file(app_state.db_pool.get(), data_block_id.clone())? {
            Some(block) => {
                if attempt > 0 {
                    tracing::debug!("File {} became distributable after {} attempts ({} ms)",
                                  data_block_id, attempt, attempt as u64 * POLL_INTERVAL_MS);
                }
                break block;
            }
            None => {
                attempt += 1;
                if attempt >= MAX_WAIT_ATTEMPTS {
                    tracing::warn!("File {} did not become distributable within {}s - consensus may not have executed yet or file is already distributed",
                                 data_block_id, (MAX_WAIT_ATTEMPTS as u64 * POLL_INTERVAL_MS) / 1000);
                    return Ok(());  // Exit gracefully - background job will handle orphans
                }

                if attempt % 5 == 0 {
                    tracing::debug!("Waiting for file {} to become distributable (attempt {}/{})",
                                  data_block_id, attempt, MAX_WAIT_ATTEMPTS);
                }

                tokio::time::sleep(std::time::Duration::from_millis(POLL_INTERVAL_MS)).await;
            }
        }
    };

    // Get current consensus height for deterministic placement
    let consensus_state = consensus::get_consensus(app_state.db_pool.get())?;
    let consensus_height = consensus_state.committed_block.data.height;

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

    // Get active validators at consensus height
    let validators = consensus::get_validators(app_state.db_pool.get(), consensus_height)?;

    // Get all node metrics at the locked consensus height
    let node_metrics = get_all_node_metrics(app_state.db_pool.get(), consensus_height)?;

    // Select nodes for this file using file-level node selection
    let selected_nodes = crate::files::placement::select_nodes_for_file(
        validators,
        node_metrics,
        &data_block.file_hash,
    );

    tracing::debug!("Fragment distribution using {} selected nodes at consensus height {} for file {}",
                   selected_nodes.len(), consensus_height,
                   data_block.file_hash.to_hex().chars().take(8).collect::<String>());

    // Create lookup map for node connection info to avoid separate DB calls
    let node_connections: std::sync::Arc<std::collections::HashMap<i32, (String, i32)>> =
        std::sync::Arc::new(selected_nodes
            .iter()
            .map(|n| (n.node_id, (n.ip_address.clone(), n.port)))
            .collect());

    // Parallel distribution with work queue pattern
    const NUM_WORKERS: usize = 10;
    const FAILURE_THRESHOLD_PERCENT: f64 = 10.0;  // Fail if >10% of fragments can't be placed

    let total_fragments = data_block.fragment_hashes.len();
    tracing::info!("Starting parallel distribution of {} fragments with {} workers",
                   total_fragments, NUM_WORKERS);

    // Create work queue with all fragments to distribute
    let work_queue: std::sync::Arc<tokio::sync::Mutex<Vec<(usize, Blake3Hash, crate::files::placement::FragmentType)>>> =
        std::sync::Arc::new(tokio::sync::Mutex::new(
            data_block.fragment_hashes.clone()
        ));

    // Channel to report failed fragments
    let (failure_tx, mut failure_rx) = tokio::sync::mpsc::unbounded_channel();
    let successful_placements = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

    // Spawn workers to distribute fragments
    let mut worker_handles = Vec::new();

    for worker_id in 0..NUM_WORKERS {
        let tx = failure_tx.clone();
        let queue = work_queue.clone();
        let app_state_clone = app_state.clone();
        let node_connections_clone = node_connections.clone();
        let selected_nodes_clone = selected_nodes.clone();
        let successful_placements_clone = successful_placements.clone();

        let worker_handle = tokio::spawn(async move {
            tracing::debug!("Worker {} starting fragment distribution", worker_id);

            loop {
                // Get next fragment to distribute from work queue
                let next_work = {
                    let mut queue_lock = queue.lock().await;
                    queue_lock.pop()
                };

                let (local_index, fragment_hash, _fragment_type) = match next_work {
                    Some(work) => work,
                    None => {
                        tracing::debug!("Worker {} finished - queue empty", worker_id);
                        break;
                    }
                };

                // Calculate placement using modulo distribution
                let placement_candidates = crate::files::placement::get_fragment_placement(
                    local_index as u32,
                    &selected_nodes_clone,
                );

                // Try to place fragment on nodes in preference order
                let mut placed = false;
                for candidate_node in placement_candidates {
                    // Skip sending to ourselves - keep fragment local if we're the best placement
                    if candidate_node.node_id == my_node_id {
                        tracing::debug!("Fragment {} best placed on local node {}, keeping locally",
                                       fragment_hash, my_node_id);
                        placed = true;
                        break;
                    }

                    // Send to remote node
                    match send_fragment_to_node(&app_state_clone, candidate_node.node_id, &fragment_hash, &node_connections_clone).await {
                        Ok(()) => {
                            // Mark fragment as not stored locally since it was sent elsewhere
                            match mark_fragment_local_state(app_state_clone.db_pool.get(), &fragment_hash, false) {
                                Ok(_rows) => {
                                    tracing::debug!("Successfully sent fragment {} to node {}, marked not stored locally",
                                                  fragment_hash, candidate_node.node_id);
                                }
                                Err(e) => {
                                    tracing::warn!("Failed to mark fragment {} as not local: {:?}, but fragment was sent successfully",
                                                 fragment_hash, e);
                                }
                            }
                            placed = true;
                            break;
                        }
                        Err(e) => {
                            tracing::debug!(
                                "Worker {} failed to send fragment {} to node {}: {:?}, trying next candidate",
                                worker_id, fragment_hash, candidate_node.node_id, e
                            );
                            // Try next candidate node
                        }
                    }
                }

                if placed {
                    successful_placements_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                } else {
                    // All placement attempts failed - fragment stays local
                    tracing::warn!("Worker {} failed to place fragment {} on any candidate node, keeping locally",
                                 worker_id, fragment_hash);
                    let _ = tx.send(fragment_hash);
                }
            }

            tracing::debug!("Worker {} completed", worker_id);
        });

        worker_handles.push(worker_handle);
    }

    // Drop the sender so the receiver knows when all workers are done
    drop(failure_tx);

    // Wait for all workers to complete
    for handle in worker_handles {
        let _ = handle.await;
    }

    // Collect all failed fragments
    let mut failed_fragments = Vec::new();
    while let Some(fragment_hash) = failure_rx.recv().await {
        failed_fragments.push(fragment_hash);
    }

    let successful = successful_placements.load(std::sync::atomic::Ordering::Relaxed);
    let failure_rate = (failed_fragments.len() as f64 / total_fragments as f64) * 100.0;

    if !failed_fragments.is_empty() {
        tracing::warn!(
            "Failed to distribute {} of {} fragments ({:.1}%), fragments remain stored locally",
            failed_fragments.len(),
            total_fragments,
            failure_rate
        );
    }

    // Fail only if too many fragments couldn't be distributed
    if failure_rate > FAILURE_THRESHOLD_PERCENT {
        return Err(DistributionError::FragmentTransfer(
            format!(
                "Fragment distribution failed: {}/{} fragments ({:.1}%) could not be placed (threshold: {:.1}%)",
                failed_fragments.len(), total_fragments, failure_rate, FAILURE_THRESHOLD_PERCENT
            )
        ));
    }

    tracing::info!(
        "Successfully distributed {} of {} fragments ({:.1}% success rate)",
        successful, total_fragments,
        (successful as f64 / total_fragments as f64) * 100.0
    );
    Ok(())
}

/// Send a fragment to a specific node with retry logic and timeout
async fn send_fragment_to_node(
    app_state: &AppState,
    node_id: i32,
    fragment_hash: &Blake3Hash,
    node_connections: &std::collections::HashMap<i32, (String, i32)>,
) -> Result<(), DistributionError> {
    // 1. Look up node address from the connections map
    let (ip_address, port) = node_connections.get(&node_id)
        .ok_or_else(|| DistributionError::Network(format!("Node {} not found in connection map", node_id)))?;
    let ip_address = ip_address.as_str();

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