use crate::{
    AppState,
    consensus::{queue::ConsensusSubmitError, types::Transaction},
    db::metrics::get_all_node_metrics_with_conn,
    db::types::CustomUUID,
    db::{
        DatabaseError, consensus,
        files::{DistributableFileData, PlacementHeightUpdate, get_distributable_file},
    },
    types::Blake3Hash,
};

#[derive(Debug)]
pub enum DistributionError {
    Database(DatabaseError),
    Consensus(ConsensusSubmitError),
    Network(String),
    FragmentTransfer(String),
    Encoding(bincode::error::EncodeError),
}

impl std::fmt::Display for DistributionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DistributionError::Database(e) => write!(f, "Database error: {:?}", e),
            DistributionError::Consensus(e) => write!(f, "Consensus error: {:?}", e),
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

impl From<ConsensusSubmitError> for DistributionError {
    fn from(e: ConsensusSubmitError) -> Self {
        DistributionError::Consensus(e)
    }
}

impl From<bincode::error::EncodeError> for DistributionError {
    fn from(e: bincode::error::EncodeError) -> Self {
        DistributionError::Encoding(e)
    }
}

/// Number of global distribution workers (RFC-014 engine rule: concurrency
/// tracks the mesh, not the upload count; actual sends are further bounded
/// by SEND_PERMITS).
const DISTRIBUTION_WORKERS: usize = 4;

/// Spawn the global distribution worker pool (once, at engine start) and
/// install the work-queue sender in AppState. Blob ids arrive from
/// HopNetApplication::on_decided — every node enqueues every decided blob;
/// get_distributable_file filters to the node actually holding the
/// fragments (the origin), so non-origin nodes no-op cheaply.
pub fn spawn_distribution_workers(app_state: &AppState) {
    if app_state.distribution_tx.get().is_some() {
        return;
    }
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<CustomUUID>();
    if app_state.distribution_tx.set(tx).is_err() {
        return; // lost the race — another spawner won
    }
    let rx = std::sync::Arc::new(tokio::sync::Mutex::new(rx));
    for worker in 0..DISTRIBUTION_WORKERS {
        let app_state = app_state.clone();
        let rx = rx.clone();
        tokio::spawn(async move {
            loop {
                let blob_id = {
                    let mut rx = rx.lock().await;
                    rx.recv().await
                };
                let Some(blob_id) = blob_id else { break };
                if let Err(e) = distribute_one(&app_state, blob_id.clone()).await {
                    tracing::error!(
                        "distribution worker {worker}: blob {blob_id} failed: {e:?}"
                    );
                }
            }
        });
    }
}

/// Distribute one decided blob if THIS node holds its fragments. The decided
/// blob arrives via our own apply (on_decided) — no polling, no forwarder
/// race: state is committed before the kick fires.
async fn distribute_one(
    app_state: &AppState,
    data_block_id: CustomUUID,
) -> Result<(), DistributionError> {
    let Some(data_block) = get_distributable_file(app_state.db_pool.get(), data_block_id.clone())?
    else {
        // Not ours to distribute (fragments not local), already placed, or
        // already handled — the common case on non-origin nodes.
        return Ok(());
    };

    tracing::info!("Starting fragment distribution for blob {}", data_block_id);

    // Distribute all fragments for this file (reads height/validators/metrics
    // on one scoped checkout; returns the placement height it locked).
    let consensus_height = distribute_file_fragments(app_state, &data_block).await?;

    // Enqueue the placement commit — batched with other files' placements
    // into ONE consensus tx per window (RFC-014 control-plane discipline).
    enqueue_placement_update(
        app_state,
        PlacementHeightUpdate {
            data_block_id: data_block.id,
            placement_height: consensus_height,
        },
    );

    tracing::info!(
        "Fragment distribution complete for {} (placement commit enqueued)",
        data_block_id
    );
    Ok(())
}

/// Distribute all fragments for a single file
async fn distribute_file_fragments(
    app_state: &AppState,
    data_block: &DistributableFileData,
) -> Result<i32, DistributionError> {
    // Get our node ID to avoid sending fragments to ourselves
    let my_node_id = app_state
        .get_node_id()
        .map_err(|_| DistributionError::Network("Failed to get node ID".to_string()))?;

    // One scoped checkout for all placement inputs, dropped BEFORE any
    // network send (conn-lifecycle rule: never hold a pool conn across the
    // data plane). Height, validators, and metrics are read at the same
    // instant so placement is computed against one consistent state.
    let (consensus_height, validators, node_metrics) = {
        let conn = app_state
            .db_pool
            .get()
            .map_err(|_| crate::db::DatabaseError::LockError)?;
        let height = consensus::get_current_consensus_height(&conn)?;
        let validators = consensus::get_validators_with_conn(&conn, height)?;
        let metrics = get_all_node_metrics_with_conn(&conn, height)?;
        (height, validators, metrics)
    };

    // Select nodes for this blob — placement seeds from the blob id
    // (RFC-014): deterministic, public, zero plaintext-derived input.
    let selected_nodes = crate::files::placement::select_nodes_for_blob_id(
        validators,
        node_metrics,
        &data_block.id,
    );

    tracing::debug!(
        "Fragment distribution using {} selected nodes at consensus height {} for blob {}",
        selected_nodes.len(),
        consensus_height,
        data_block.id
    );

    // Create lookup map for node pubkeys (iroh transport addresses)
    let node_pubkeys: std::sync::Arc<std::collections::HashMap<i32, iroh::PublicKey>> =
        std::sync::Arc::new(
            selected_nodes
                .iter()
                .map(|n| (n.node_id, n.pubkey.to_iroh_node_id()))
                .collect(),
        );

    // Parallel distribution with work queue pattern. Workers scale with the
    // FILE (small cap); actual sends are bounded PROCESS-WIDE by
    // SEND_PERMITS — under an upload burst, concurrency tracks mesh
    // bandwidth, not upload count (RFC-014 engine rule).
    const FAILURE_THRESHOLD_PERCENT: f64 = 10.0; // Fail if >10% of fragments can't be placed

    let total_fragments = data_block.fragment_hashes.len();
    let num_workers = total_fragments.clamp(1, 4);
    tracing::info!(
        "Starting parallel distribution of {} fragments with {} workers",
        total_fragments,
        num_workers
    );

    // Create work queue with all fragments to distribute
    let work_queue: std::sync::Arc<
        tokio::sync::Mutex<Vec<(usize, Blake3Hash, crate::files::placement::FragmentType)>>,
    > = std::sync::Arc::new(tokio::sync::Mutex::new(data_block.fragment_hashes.clone()));

    // Channels to report failed fragments and remotely-placed fragments
    let (failure_tx, mut failure_rx) = tokio::sync::mpsc::unbounded_channel();
    let (remote_tx, mut remote_rx) = tokio::sync::mpsc::unbounded_channel::<Blake3Hash>();
    let successful_placements = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

    // Spawn workers to distribute fragments
    let mut worker_handles = Vec::new();

    for worker_id in 0..num_workers {
        let tx = failure_tx.clone();
        let remote_placed_tx = remote_tx.clone();
        let queue = work_queue.clone();
        let app_state_clone = app_state.clone();
        let node_pubkeys_clone = node_pubkeys.clone();
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
                        tracing::debug!(
                            "Fragment {} best placed on local node {}, keeping locally",
                            fragment_hash,
                            my_node_id
                        );
                        placed = true;
                        break;
                    }

                    // Send to remote node via iroh
                    let peer_node_id = match node_pubkeys_clone.get(&candidate_node.node_id) {
                        Some(pk) => *pk,
                        None => {
                            tracing::warn!(
                                "No pubkey for node {}, skipping",
                                candidate_node.node_id
                            );
                            continue;
                        }
                    };
                    // Process-wide send bound (held across the send's
                    // internal retries; no DB conn is held here).
                    let _permit = SEND_PERMITS.acquire().await;
                    match send_fragment_to_node(
                        &app_state_clone,
                        candidate_node.node_id,
                        &fragment_hash,
                        peer_node_id,
                    )
                    .await
                    {
                        Ok(()) => {
                            // Collect for batch update after all workers finish
                            let _ = remote_placed_tx.send(fragment_hash);
                            tracing::debug!(
                                "Successfully sent fragment {} to node {}",
                                fragment_hash,
                                candidate_node.node_id
                            );
                            placed = true;
                            break;
                        }
                        Err(e) => {
                            tracing::debug!(
                                "Worker {} failed to send fragment {} to node {}: {:?}, trying next candidate",
                                worker_id,
                                fragment_hash,
                                candidate_node.node_id,
                                e
                            );
                            // Try next candidate node
                        }
                    }
                }

                if placed {
                    successful_placements_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                } else {
                    // All placement attempts failed - fragment stays local
                    tracing::warn!(
                        "Worker {} failed to place fragment {} on any candidate node, keeping locally",
                        worker_id,
                        fragment_hash
                    );
                    let _ = tx.send(fragment_hash);
                }
            }

            tracing::debug!("Worker {} completed", worker_id);
        });

        worker_handles.push(worker_handle);
    }

    // Drop the senders so the receivers know when all workers are done
    drop(failure_tx);
    drop(remote_tx);

    // Wait for all workers to complete
    for handle in worker_handles {
        let _ = handle.await;
    }

    // Collect all failed fragments
    let mut failed_fragments = Vec::new();
    while let Some(fragment_hash) = failure_rx.recv().await {
        failed_fragments.push(fragment_hash);
    }

    // Batch-update all remotely-placed fragments in a single transaction
    let mut remotely_placed = Vec::new();
    while let Some(fragment_hash) = remote_rx.recv().await {
        remotely_placed.push(fragment_hash);
    }
    if !remotely_placed.is_empty()
        && let Err(e) = app_state.local_state_tx.try_send(
            crate::db::write_gate::LocalStateUpdate::MarkRemoteBatch {
                fragment_hashes: remotely_placed,
            },
        )
    {
        tracing::warn!("Local state queue full, dropping mark-remote batch: {}", e);
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
        return Err(DistributionError::FragmentTransfer(format!(
            "Fragment distribution failed: {}/{} fragments ({:.1}%) could not be placed (threshold: {:.1}%)",
            failed_fragments.len(),
            total_fragments,
            failure_rate,
            FAILURE_THRESHOLD_PERCENT
        )));
    }

    tracing::info!(
        "Successfully distributed {} of {} fragments ({:.1}% success rate)",
        successful,
        total_fragments,
        (successful as f64 / total_fragments as f64) * 100.0
    );
    Ok(consensus_height)
}

/// Send a fragment to a specific node over iroh with domain-level retry
async fn send_fragment_to_node(
    app_state: &AppState,
    node_id: i32,
    fragment_hash: &Blake3Hash,
    peer_node_id: iroh::PublicKey,
) -> Result<(), DistributionError> {
    // Read fragment from local storage
    let fragment_data =
        crate::files::functions::fetch_and_verify_fragment(fragment_hash, &app_state.fragments_dir)
            .map_err(|e| {
                DistributionError::FragmentTransfer(format!(
                    "Failed to read fragment {}: {:?}",
                    fragment_hash, e
                ))
            })?;

    // 2 domain-level retries with 1s delay for server-side transient errors (IrohResponse::Error).
    // Transport errors (connection failures, timeouts) propagate directly — the transport layer
    // already handles zombie-connection retry (1 retry with fresh connection).
    const MAX_RETRIES: u32 = 2;
    const RETRY_DELAY_MS: u64 = 1000;

    for attempt in 0..MAX_RETRIES {
        match crate::files::rpc::store_fragment_remote(
            &app_state.iroh_transport,
            node_id,
            peer_node_id,
            *fragment_hash,
            fragment_data.clone(),
        )
        .await
        {
            Ok(result) => {
                if result.success {
                    tracing::debug!(
                        "Successfully sent fragment {} to node {} on attempt {}{}",
                        fragment_hash,
                        node_id,
                        attempt + 1,
                        if result.already_existed {
                            " (already existed)"
                        } else {
                            ""
                        }
                    );
                    return Ok(());
                }
                // success=false shouldn't happen (errors come via IrohResponse::Error), but handle it
                tracing::warn!(
                    "Fragment store returned success=false for {} on node {}",
                    fragment_hash,
                    node_id
                );
            }
            Err(crate::net::IrohError::Protocol(
                crate::net::transport::ProtocolError::PeerError(msg),
            )) => {
                // Server-side error (e.g., hash mismatch, size limit) — retry
                tracing::warn!(
                    "Fragment store attempt {} for {} on node {} failed: {}",
                    attempt + 1,
                    fragment_hash,
                    node_id,
                    msg
                );
                if attempt < MAX_RETRIES - 1 {
                    tokio::time::sleep(std::time::Duration::from_millis(RETRY_DELAY_MS)).await;
                    continue;
                }
                return Err(DistributionError::FragmentTransfer(format!(
                    "Fragment store failed after {} attempts: {}",
                    MAX_RETRIES, msg
                )));
            }
            Err(e) => {
                // Transport error — propagate directly (transport already did zombie retry)
                return Err(DistributionError::FragmentTransfer(format!(
                    "Transport error sending fragment {} to node {}: {}",
                    fragment_hash, node_id, e
                )));
            }
        }
    }

    Err(DistributionError::FragmentTransfer(format!(
        "Failed to send fragment {} to node {} after {} attempts",
        fragment_hash, node_id, MAX_RETRIES
    )))
}

/// Submit placement height update to consensus
/// Process-wide bound on concurrent fragment sends (RFC-014: the engine's
/// concurrency tracks mesh bandwidth, not upload count).
static SEND_PERMITS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(16);

/// How long the placement batcher collects updates before flushing them as
/// one `update_placement_heights` consensus tx, and the flush size cap
/// (aligned with the consensus queue's MAX_BATCH_SIZE).
const PLACEMENT_FLUSH_MS: u64 = 750;
const PLACEMENT_FLUSH_MAX: usize = 100;
/// Flush retry cap for transient failures (timeout / internal error).
const PLACEMENT_FLUSH_ATTEMPTS: u8 = 3;

/// Enqueue a placement commit on this AppState's batcher (lazily spawned on
/// the consensus queue runtime). Fire-and-forget: per-file distribution
/// success means "fragments placed + commit enqueued"; the batcher owns
/// delivery. A terminally-dropped batch leaves placement_height NULL — the
/// file stays downloadable via inventory discovery, matching today's
/// behavior when a per-file placement tx failed. (NULL-placement
/// reconciliation is an RFC-014 follow-up.)
fn enqueue_placement_update(app_state: &AppState, update: PlacementHeightUpdate) {
    let tx = app_state.placement_batch_tx.get_or_init(|| {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        // One parked task per AppState for process lifetime (same pattern as
        // batch_processor); in-process test nodes each get their own.
        crate::consensus::queue::queue_rt().spawn(placement_batcher(rx, app_state.clone()));
        tx
    });
    if tx.send(update).is_err() {
        tracing::error!("placement batcher gone — placement commit dropped");
    }
}

/// Collect placement updates and submit them as ONE consensus tx per window.
async fn placement_batcher(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<PlacementHeightUpdate>,
    app_state: AppState,
) {
    // (update, attempts) — retried entries carry their attempt count.
    let mut pending: Vec<(PlacementHeightUpdate, u8)> = Vec::new();
    loop {
        // Wait for the first entry of a window (or carry-over retries).
        if pending.is_empty() {
            match rx.recv().await {
                Some(u) => pending.push((u, 0)),
                None => return,
            }
        }
        // Collect until the window closes or the batch fills.
        let window = tokio::time::sleep(std::time::Duration::from_millis(PLACEMENT_FLUSH_MS));
        tokio::pin!(window);
        loop {
            tokio::select! {
                more = rx.recv() => match more {
                    Some(u) => {
                        pending.push((u, 0));
                        if pending.len() >= PLACEMENT_FLUSH_MAX {
                            break;
                        }
                    }
                    None => break,
                },
                _ = &mut window => break,
            }
        }

        // Dedup by data_block_id (keep the latest placement height).
        let mut seen = std::collections::HashMap::new();
        for (u, attempts) in pending.drain(..) {
            seen.insert(u.data_block_id.clone(), (u, attempts));
        }
        let batch: Vec<(PlacementHeightUpdate, u8)> = seen.into_values().collect();
        let updates: Vec<PlacementHeightUpdate> = batch.iter().map(|(u, _)| u.clone()).collect();

        let encoded = match bincode::serde::encode_to_vec(&updates, bincode::config::standard()) {
            Ok(b) => b,
            Err(e) => {
                tracing::error!("placement batch encode failed (dropped): {e}");
                continue;
            }
        };
        // Sign at flush time — fresh nonce per attempt.
        let transaction = match crate::consensus::dispatch::create_signed_transaction(
            &app_state,
            "update_placement_heights".to_string(),
            encoded,
        ) {
            Ok(t) => t,
            Err(e) => {
                tracing::error!("placement batch signing failed (dropped): {e:?}");
                continue;
            }
        };

        match app_state.consensus_queue.submit(transaction).await {
            Ok(()) => {
                tracing::debug!("placement batch committed ({} blobs)", updates.len());
            }
            Err(crate::consensus::queue::ConsensusSubmitError::Rejected(reason)) => {
                // Permanent business rejection — dropping is correct.
                tracing::error!("placement batch rejected (dropped): {reason}");
            }
            Err(e) => {
                // Transient (timeout / internal) — re-stage with attempt caps.
                tracing::warn!("placement batch submit failed, will retry: {e:?}");
                for (u, attempts) in batch {
                    if attempts + 1 < PLACEMENT_FLUSH_ATTEMPTS {
                        pending.push((u, attempts + 1));
                    } else {
                        tracing::error!(
                            "placement commit for {} dropped after {} attempts",
                            u.data_block_id,
                            PLACEMENT_FLUSH_ATTEMPTS
                        );
                    }
                }
            }
        }
    }
}
