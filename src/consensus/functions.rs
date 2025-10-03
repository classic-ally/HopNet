use super::*;

use crate::{db::consensus as db, handlers::HandlerResult};
use reqwest::Client;
use serde_json::Value;
use crate::db::MyNode;
use crate::types::Node;
use crate::DISPATCH_TABLE;
use tokio::sync::mpsc;
use ed25519_dalek::{SigningKey, VerifyingKey};
use rand_core::OsRng;
use std::collections::HashMap;
use tokio::sync::Mutex;
use rand::prelude::*;


pub fn generate_ed25519_key() -> (SigningKey, VerifyingKey) {
    let mut csprng = OsRng;
    let private_key= SigningKey::generate(&mut csprng);
    let public_key = private_key.verifying_key();

    return (private_key, public_key);
}

#[derive (Debug)]
pub enum ConsensusError {
    InsufficientVotes,
    BlockError,
    DatabaseError,
    SigningError,
    TimeoutError,
    MalformedReply,
    ThreadError,
    ForwardingError,
    NetworkError,
}

#[derive(Debug)]
pub enum CatchUpError {
    NetworkUnavailable,      // All validators unreachable/failing
    ValidationFailed(i32),   // View failed validation (for logging)
    Database,                // Database error
}

// Create a node-only transaction (for automated operations)
pub fn create_signed_transaction(
    app_state: &AppState,
    function: String,
    payload: Vec<u8>
) -> Result<Transaction, ConsensusError> {
    let node_id = app_state.get_node_id().map_err(|_| ConsensusError::DatabaseError)?;
    Transaction::new(function, payload, node_id, &app_state.private_key)
        .map_err(|_| ConsensusError::SigningError)
}

// Create a user-initiated transaction (for user operations)
pub fn create_signed_user_transaction(
    app_state: &AppState,
    function: String,
    payload: Vec<u8>,
    user_id: i32,
) -> Result<Transaction, ConsensusError> {
    let node_id = app_state.get_node_id().map_err(|_| ConsensusError::DatabaseError)?;
    let user_keys = app_state.get_user_keys().map_err(|_| ConsensusError::DatabaseError)?;

    Transaction::new_with_user(
        function,
        payload,
        node_id,
        &app_state.private_key,
        user_id,
        &user_keys.private_key
    ).map_err(|_| ConsensusError::SigningError)
}

pub async fn consensus_middleware(app_state: &AppState, transactions: Vec<Transaction>) -> Result<(), ConsensusError> {
    tracing::debug!("Starting consensus middleware for {} transactions", transactions.len());
    
    let my_node_id = app_state.get_node_id().map_err(|_| ConsensusError::DatabaseError)?;
    
    // Retry loop to handle view changes during consensus waits
    const MAX_RETRIES: u32 = 3;
    const MAX_WAIT_MS: u64 = 5000; // 5 second timeout for waiting
    const POLL_INTERVAL_MS: u64 = 50; // Poll every 50ms
    
    for retry_attempt in 0..MAX_RETRIES {
        // Check if we are the current leader
        let initial_consensus_state = db::get_consensus(app_state.db_pool.get()).map_err(|_| ConsensusError::DatabaseError)?;
        
        if initial_consensus_state.leader.node_id != my_node_id {
            // Forward to leader instead of initiating consensus
            tracing::info!(
                "Not the leader (node {}), forwarding transactions to leader (node {})",
                my_node_id, initial_consensus_state.leader.node_id
            );
            return forward_to_leader(initial_consensus_state.leader, transactions, app_state).await;
        }
        
        // Wait for any ongoing consensus to complete
        let initial_view = initial_consensus_state.view;
        let mut wait_attempts = 0;
        let max_wait_attempts = MAX_WAIT_MS / POLL_INTERVAL_MS;
        
        let mut current_consensus_state = initial_consensus_state;
        while current_consensus_state.prepared_block.is_some() {
            if wait_attempts >= max_wait_attempts {
                tracing::error!(
                    "Timeout waiting for consensus to complete after {}ms (view: {}, prepared_block: {:?})",
                    MAX_WAIT_MS, current_consensus_state.view, current_consensus_state.prepared_block
                );
                return Err(ConsensusError::TimeoutError);
            }
            
            tracing::debug!(
                "Consensus in progress for view {} (prepared_block: {:?}), waiting... (attempt {})",
                current_consensus_state.view, current_consensus_state.prepared_block, wait_attempts + 1
            );
            
            tokio::time::sleep(tokio::time::Duration::from_millis(POLL_INTERVAL_MS)).await;
            wait_attempts += 1;
            
            // Re-check consensus state
            current_consensus_state = db::get_consensus(app_state.db_pool.get()).map_err(|_| ConsensusError::DatabaseError)?;
        }
        
        // Check if view changed during wait
        if current_consensus_state.view != initial_view {
            tracing::debug!(
                "View changed during wait ({}→{}), retrying consensus (attempt {})",
                initial_view, current_consensus_state.view, retry_attempt + 1
            );
            continue; // Retry with new view
        }
        
        // Check if we're still the leader (leader can change during consensus)
        if current_consensus_state.leader.node_id != my_node_id {
            tracing::info!(
                "Leadership changed during wait, forwarding to new leader (node {})",
                current_consensus_state.leader.node_id
            );
            return forward_to_leader(current_consensus_state.leader, transactions, app_state).await;
        }
        
        // Safe to proceed - same view, propose phase, still leader
        tracing::info!(
            "Acting as leader (node {}) for view {}, initiating consensus",
            my_node_id, current_consensus_state.view
        );
        let block = Block::new_tip(&app_state, transactions).map_err(|_| ConsensusError::BlockError)?;
        
        // Construct MyNode from AppState
        let me = MyNode {
            node_id: my_node_id,
            privkey: app_state.private_key.clone(),
        };
        
        let validators = db::get_validators(app_state.db_pool.get(), block.data.height).map_err(|_| ConsensusError::DatabaseError)?
            .iter()
            .filter(|node| node.node_id != me.node_id)
            .cloned()
            .collect();

        let validators_elect = db::get_validators_elect(app_state.db_pool.get(), block.data.height).map_err(|_| ConsensusError::DatabaseError)?
            .iter()
            .filter(|node| node.node_id != me.node_id)
            .cloned()
            .collect();

        // Get QC1 from ballot round
        let qc1 = ballot_round(&block, &me, ConsensusPhase::Propose, &validators, &validators_elect, app_state).await?;
        // Broadcast QC1 and wait for enough confirmations
        broadcast_qc(&validators, &validators_elect, qc1).await?;
        // Get QC2 from ballot round
        let qc2 = ballot_round(&block, &me, ConsensusPhase::Lock, &validators, &validators_elect, app_state).await?;
        // Broadcast QC2 and wait for enough confirmations
        broadcast_qc(&validators, &validators_elect, qc2).await?;

        let _ = process_transactions(&block.data.transactions, app_state, true);

        return Ok(());
    }
    
    // If we've exhausted all retries, return an error
    tracing::error!(
        "Consensus failed after {} retries due to view changes or timeouts",
        MAX_RETRIES
    );
    Err(ConsensusError::ThreadError) // Using existing error variant, could add MaxRetriesExceeded later
}

async fn ballot_round(
    block: &Block,
    me: &MyNode,
    phase: ConsensusPhase,
    validators: &Vec<Node>,
    validators_elect: &Vec<Node>,
    app_state: &AppState
) -> Result<QuorumCertificate, ConsensusError> {
    let vote_data = VoteSignData::from_block(block.clone(), phase.clone());

    let my_signature = vote_data.sign(&app_state.private_key).map_err(|_| ConsensusError::SigningError)?;
    let initiator_signoff = VoteSignMessage {
        replica_id: me.node_id,
        signature: my_signature,
    };

    let ballot_proposal = Ballot::propose(vote_data, block.clone(), initiator_signoff);

    let voter_signatures = broadcast_and_collect_votes(ballot_proposal, &validators, &validators_elect).await?;

    let qc = QuorumCertificate::create(
        &block,
        phase,
        me.node_id,
        &app_state.private_key,
        voter_signatures,
    ).map_err(|_| ConsensusError::SigningError)?;

    match qc.verify(&app_state, &block) {
        Ok(_) => {
            tracing::debug!(
                "QC verification passed, inserting QC for view {} phase {:?}",
                qc.view_number, qc.phase
            );
            db::insert_qc(app_state.db_pool.get(), qc.clone()).map_err(|_| ConsensusError::DatabaseError)?;
        },
        Err(_) => return Err(ConsensusError::SigningError)
    };

    Ok(qc)
}

async fn broadcast_and_collect_votes(
    ballot: Ballot,
    validators: &Vec<Node>,
    validators_elect: &Vec<Node>,
) -> Result<Vec<VoteSignMessage>, ConsensusError> {
    // Handle single validator case (no other nodes to vote)
    if validators.is_empty() {
        return Ok(Vec::new());
    }

    let (votes_tx, mut votes_rx) = mpsc::channel::<VoteSignMessage>(100); //100 channel capacity

    // Calculate quorum threshold (2/3 majority + 1)
    let required_votes = (validators.len() * 2) / 3 + 1;
    
    // Spawn tasks for each validator
    for node in validators.clone() {
        let ballot_clone = ballot.clone();
        let votes_tx_clone = votes_tx.clone();
        
        tokio::spawn(async move {
            // Ignore errors from individual nodes - they'll just timeout or fail
            let _ = ballot_send(ballot_clone, &node, votes_tx_clone).await;
        });
    }
    
    // NON-CRITICAL PATH: Inform validators elect (fire-and-forget, don't collect votes)
    for node in validators_elect.clone() {
        let ballot_clone = ballot.clone();
        tokio::spawn(async move {
            // Create a dummy channel that we never read from
            let (dummy_votes_tx, _dummy_votes_rx) = mpsc::channel::<VoteSignMessage>(1);
            // Best effort delivery - don't care about result or votes
            let _ = ballot_send(ballot_clone, &node, dummy_votes_tx).await;
        });
    }
    
    // Drop the original sender so the channel closes when all tasks complete
    drop(votes_tx);
    
    // Collect votes until we have enough for quorum or all tasks complete
    let mut voter_signatures = Vec::new();
    let timeout = tokio::time::sleep(std::time::Duration::from_secs(30));
    tokio::pin!(timeout);
    
    loop {
        tokio::select! {
            vote_opt = votes_rx.recv() => {
                match vote_opt {
                    Some(vote) => {
                        voter_signatures.push(vote);
                        
                        // Early termination on quorum
                        if voter_signatures.len() >= required_votes {
                            return Ok(voter_signatures);
                        }
                    }
                    None => {
                        // Channel closed - all tasks completed
                        break;
                    }
                }
            }
            _ = &mut timeout => {
                // Overall timeout reached
                break;
            }
        }
    }
    
    // Check if we have enough votes after timeout or all tasks complete
    if voter_signatures.len() >= required_votes {
        Ok(voter_signatures)
    } else {
        Err(ConsensusError::InsufficientVotes)
    }
}

async fn broadcast_qc(
    validators: &Vec<Node>,
    validators_elect: &Vec<Node>,
    qc: QuorumCertificate
) -> Result<(), ConsensusError> {
    // Handle single validator case (no other nodes to broadcast to)
    if validators.is_empty() {
        return Ok(());
    }

    let (confirmations_tx, mut confirmations_rx) = mpsc::channel::<()>(100);

    // Calculate quorum threshold (2/3 majority + 1)
    let required_confirmations = (validators.len() * 2) / 3 + 1;
    
    // Spawn tasks for each validator
    for node in validators.clone() {
        let qc_clone = qc.clone();
        let confirmations_tx_clone = confirmations_tx.clone();
        
        tokio::spawn(async move {
            // Send QC and notify on success
            match qc_send(qc_clone, &node).await {
                Ok(()) => {
                    let _ = confirmations_tx_clone.send(()).await;
                }
                Err(e) => {
                    tracing::debug!("Failed to send QC to node: {:?}", &e);
                    // Don't send confirmation on failure
                }
            }
        });
    }
    
    // NON-CRITICAL PATH: Inform validators elect (fire-and-forget, don't wait)
    for node in validators_elect.clone() {
        let qc_clone = qc.clone();
        tokio::spawn(async move {
            // Best effort delivery - don't care about result
            let _ = qc_send(qc_clone, &node).await;
        });
    }
    
    // Drop the original sender so the channel closes when all tasks complete
    drop(confirmations_tx);
    
    // Collect confirmations until we have enough for quorum or all tasks complete
    let mut confirmations = 0;
    let timeout = tokio::time::sleep(std::time::Duration::from_secs(30));
    tokio::pin!(timeout);
    
    loop {
        tokio::select! {
            confirmation_opt = confirmations_rx.recv() => {
                match confirmation_opt {
                    Some(()) => {
                        confirmations += 1;
                        
                        // Early termination on quorum
                        if confirmations >= required_confirmations {
                            return Ok(());
                        }
                    }
                    None => {
                        // Channel closed - all tasks completed
                        break;
                    }
                }
            }
            _ = &mut timeout => {
                // Overall timeout reached
                break;
            }
        }
    }
    
    // Check if we have enough confirmations after timeout or all tasks complete
    if confirmations >= required_confirmations {
        Ok(())
    } else {
        Err(ConsensusError::InsufficientVotes)
    }
}

async fn ballot_send(
    ballot: Ballot,
    node: &Node,
    votes_tx: mpsc::Sender<VoteSignMessage>,
) -> Result<(), ConsensusError> {
    let client = Client::new();
    let url = format!("http://{}:{}/ballot", node.ip_address, node.port);
    match client.post(url)
        .json(&ballot)
        .send()
        .await
    {
        Ok(response) => {
            if response.status().is_success() {
                let result: Value =  response.json().await.map_err(|_| ConsensusError::MalformedReply)?;
                let vote_sign_msg: VoteSignMessage = serde_json::from_value(result).map_err(|_| ConsensusError::MalformedReply)?;
                votes_tx.send(vote_sign_msg).await.map_err(|_| ConsensusError::ThreadError)?;
                Ok(())
            } else {
                return Err(ConsensusError::MalformedReply)
            }
        }
        Err(_) => return Err(ConsensusError::TimeoutError)
    }
}

async fn qc_send(
    qc: QuorumCertificate,
    node: &Node,
) -> Result<(), ConsensusError> {
    let client = Client::new();
    let url = format!("http://{}:{}/qc", node.ip_address, node.port);
    match client.post(url)
        .json(&qc)
        .send()
        .await
    {
        Ok(response) => {
            tracing::debug!("Received response from validator");
            if response.status().is_success() {
                tracing::debug!("Validator response OK");
                Ok(())
            } else {
                tracing::warn!("Validator returned error status: {}", response.status());
                return Err(ConsensusError::MalformedReply)
            }
        }
        Err(e) => {
            tracing::warn!("Failed to reach validator: {:?}", e);
            return Err(ConsensusError::TimeoutError)
        }
    }
}

pub fn process_transactions(transactions: &Option<Transactions>, app_state: &AppState, execute: bool) -> HandlerResult {
    if let Some(transactions) = transactions {
        for tx in transactions.iter() {
            match process_transaction(tx, app_state, execute) {
                Ok(_) => {
                    tracing::debug!("Transaction {} successfully: {}", if execute { "processed" } else { "validated" }, &tx.rpc.function);
                }
                Err(e) => {
                    if execute {
                        tracing::error!("Failed to process transaction {}: {:?}", &tx.rpc.function, e);
                    } else {
                        return Err(e);
                    }
                }
            }
        }
    }
    Ok(())
}

pub fn process_transaction(tx: &Transaction, app_state: &AppState, execute: bool) -> HandlerResult {
    if let Some(handler) = DISPATCH_TABLE.get(tx.rpc.function.as_str()) {
        handler.process(app_state, tx, execute)
    } else {
        tracing::warn!("No handler found for function: {}", &tx.rpc.function);
        Err(crate::db::DatabaseError::InvalidPayload)
    }
}

// Timeout Vote Collection for distributed TC generation
pub struct TimeoutVoteCollector {
    // Map: view_number -> HashMap<timeout_data_hash, Vec<TimeoutVote>>
    pending_votes: Mutex<HashMap<i32, HashMap<Vec<u8>, Vec<TimeoutVote>>>>,
}

impl TimeoutVoteCollector {
    pub fn new() -> Self {
        Self {
            pending_votes: Mutex::new(HashMap::new()),
        }
    }
    
    pub async fn add_vote(&self, vote: TimeoutVote, app_state: &AppState) -> Result<Option<TimeoutCertificate>, CertificateError> {
        // Poll current consensus state for cleanup
        let consensus_state = db::get_consensus(app_state.db_pool.get())
            .map_err(|_| CertificateError::DatabaseError)?;
        let current_view = consensus_state.view;
        
        // Clean up old votes
        self.cleanup_old_votes(current_view).await;
        
        // Only reject timeout votes for old views (nodes might be ahead of us)
        if vote.data.view_number < current_view {
            tracing::debug!("Ignoring timeout vote for old view {} (current view: {})", vote.data.view_number, current_view);
            return Err(CertificateError::ValidationError);
        }
        
        // Verify the timeout vote signature
        self.verify_timeout_vote(&vote, app_state)?;
        
        // Add vote to pending collection
        let mut pending = self.pending_votes.lock().await;
        let view_votes = pending.entry(vote.data.view_number).or_insert_with(HashMap::new);
        
        // Group by timeout data hash
        let data_hash = vote.data.encode().map_err(|_| CertificateError::ValidationError)?;
        let data_votes = view_votes.entry(data_hash).or_insert_with(Vec::new);
        
        // Check for duplicate vote from same replica BEFORE adding
        if data_votes.iter().any(|v| v.sender.replica_id == vote.sender.replica_id) {
            return Err(CertificateError::ValidationError); // Duplicate vote
        }
        
        // Add vote to collection
        data_votes.push(vote);
        
        // Always try to create TC - let it decide if there's quorum
        let timeout_votes = data_votes.clone();
        drop(pending); // Release lock before TC creation
        
        match TimeoutCertificate::create(timeout_votes, app_state) {
            Ok(tc) => Ok(Some(tc)),
            Err(CertificateError::InsufficientVotes) => Ok(None), // Not enough votes yet
            Err(e) => Err(e), // Real error
        }
    }
    
    async fn cleanup_old_votes(&self, current_view: i32) {
        let mut pending = self.pending_votes.lock().await;
        pending.retain(|&view, _| view >= current_view);
    }
    
    fn verify_timeout_vote(&self, vote: &TimeoutVote, app_state: &AppState) -> Result<(), CertificateError> {
        // Get validators to find the public key
        let validators = db::get_validators(app_state.db_pool.get(), vote.data.view_number)
            .map_err(|_| CertificateError::DatabaseError)?;
        
        // Find validator's public key
        let validator = validators.iter()
            .find(|v| v.node_id == vote.sender.replica_id)
            .ok_or(CertificateError::SignerNotFound)?;
        
        // Verify signature
        let message = vote.data.encode().map_err(|_| CertificateError::ValidationError)?;
        
        match validator.pubkey.verify_strict(&message, &vote.sender.signature) {
            Ok(_) => Ok(()),
            Err(_) => Err(CertificateError::ValidationError),
        }
    }
    
    pub async fn get_vote_count(&self, view: i32) -> usize {
        let pending = self.pending_votes.lock().await;
        pending.get(&view)
            .map(|view_votes| view_votes.values().map(|votes| votes.len()).sum())
            .unwrap_or(0)
    }
}

// Leader forwarding function for non-leader nodes
async fn forward_to_leader(
    leader: crate::types::Node,
    transactions: Vec<Transaction>,
    app_state: &AppState
) -> Result<(), ConsensusError> {
    let my_node_id = app_state.get_node_id().map_err(|_| ConsensusError::DatabaseError)?;

    // Serialize transactions for signing
    let body = serde_json::to_vec(&transactions).map_err(|_| ConsensusError::SigningError)?;

    // Sign with node key
    let node_signature = app_state.private_key.try_sign(&body).map_err(|_| ConsensusError::SigningError)?;

    // Forward to leader
    let client = reqwest::Client::new();
    let url = format!("http://{}:{}/consensus/propose", leader.ip_address, leader.port);

    tracing::info!(
        "Forwarding {} transactions to leader at {} (node_id: {})",
        transactions.len(), &url, my_node_id
    );

    let response = client
        .post(&url)
        .header("X-Node-ID", my_node_id.to_string())
        .header("X-Node-Signature", hex::encode(node_signature.to_bytes()))
        .json(&transactions)
        .send()
        .await
        .map_err(|_| ConsensusError::NetworkError)?;
    
    match response.status() {
        reqwest::StatusCode::OK | reqwest::StatusCode::CREATED => {
            tracing::debug!("Leader successfully processed {} transactions", transactions.len());
            Ok(())
        }
        _ => {
            tracing::warn!(
                "Leader rejected transactions with status: {}",
                response.status()
            );
            Err(ConsensusError::ForwardingError)
        }
    }
}

/// Poll a random subset of validators to get the maximum view in the network
/// This avoids O(N²) bandwidth while still detecting if we're behind
pub async fn poll_subset_for_max_view(
    app_state: &AppState,
    consensus_state: &ConsensusState,
    bootstrap_validators: Option<&[Node]>,
) -> Result<i32, ConsensusError> {
    use std::collections::HashSet;

    const MAX_ATTEMPTS: u32 = 3;
    const SUBSET_SIZE: usize = 5;

    // Get our node ID from AppState (avoid DB call)
    let my_node_id = app_state.get_node_id()
        .map_err(|_| ConsensusError::DatabaseError)?;
    let user_id = app_state.get_user_id()
        .map_err(|_| ConsensusError::DatabaseError)?;
    let user_keys = app_state.get_user_keys()
        .map_err(|_| ConsensusError::DatabaseError)?;

    let mut all_validators = db::get_validators(app_state.db_pool.get(), consensus_state.committed_block.data.height)
        .map_err(|_| ConsensusError::DatabaseError)?;

    // Merge with bootstrap validators, removing duplicates by node_id
    if let Some(bootstrap) = bootstrap_validators {
        let existing_ids: HashSet<i32> = all_validators.iter().map(|v| v.node_id).collect();
        all_validators.extend(
            bootstrap.iter()
                .filter(|node| !existing_ids.contains(&node.node_id))
                .cloned()
        );
    }

    // Filter out ourselves
    let other_validators: Vec<Node> = all_validators.into_iter()
        .filter(|v| v.node_id != my_node_id)
        .collect();

    if other_validators.is_empty() {
        // We're the only validator, so we're caught up by definition
        return Ok(consensus_state.view);
    }

    // Retry loop for handling unlucky validator selection
    for attempt in 1..=MAX_ATTEMPTS {
        let selected_validators: Vec<&Node> = other_validators
            .choose_multiple(&mut rand::rng(), SUBSET_SIZE)
            .collect();

        tracing::debug!("Attempt {}/{}: Polling {} validators for max view", attempt, MAX_ATTEMPTS, selected_validators.len());

        // Prepare RPC authentication (sign empty body for GET request)
        let body = b"";
        let node_signature = app_state.private_key.try_sign(body)
            .map_err(|_| ConsensusError::SigningError)?;
        let user_signature = user_keys.private_key.try_sign(body)
            .map_err(|_| ConsensusError::SigningError)?;

        let client = Client::new();
        let mut tasks = Vec::new();

        // Create parallel requests to all selected validators
        for validator in selected_validators {
            let client = client.clone();
            let url = format!("http://{}:{}/consensus", validator.ip_address, validator.port);
            let node_id = validator.node_id;
            let my_node_id = my_node_id;
            let user_id = user_id;
            let node_signature = node_signature.clone();
            let user_signature = user_signature.clone();

            let task = tokio::spawn(async move {
                match client.get(&url)
                    .header("X-Node-ID", my_node_id.to_string())
                    .header("X-User-ID", user_id.to_string())
                    .header("X-Node-Signature", hex::encode(node_signature.to_bytes()))
                    .header("X-User-Signature", hex::encode(user_signature.to_bytes()))
                    .timeout(std::time::Duration::from_secs(5))
                    .send()
                    .await
                {
                    Ok(response) => {
                        if response.status().is_success() {
                            match response.json::<ConsensusState>().await {
                                Ok(their_state) => {
                                    tracing::debug!("Validator {} is at view {}", node_id, their_state.view);
                                    Some(their_state.view)
                                }
                                Err(e) => {
                                    tracing::debug!("Failed to parse consensus state from validator {}: {:?}", node_id, e);
                                    None
                                }
                            }
                        } else {
                            tracing::debug!("Validator {} returned status: {}", node_id, response.status());
                            None
                        }
                    }
                    Err(e) => {
                        tracing::debug!("Failed to contact validator {} at {}: {:?}", node_id, url, e);
                        None
                    }
                }
            });

            tasks.push(task);
        }

        // Wait for all requests to complete and collect views
        let mut received_views = Vec::new();

        for task in tasks {
            if let Ok(Some(view)) = task.await {
                received_views.push(view);
            }
        }

        // If we got at least one response, return the maximum view
        if !received_views.is_empty() {
            let max_view = *received_views.iter().max().unwrap();
            let max_view = std::cmp::max(max_view, consensus_state.view);
            tracing::debug!("Max view detected: {} (our view: {})", max_view, consensus_state.view);
            return Ok(max_view);
        }

        // All validators failed, try again with different subset
        tracing::debug!("Attempt {}/{}: no successful responses from validators", attempt, MAX_ATTEMPTS);
    }

    // All retry attempts exhausted
    tracing::warn!("Failed to poll network height after {} attempts", MAX_ATTEMPTS);
    Err(ConsensusError::NetworkError)
}