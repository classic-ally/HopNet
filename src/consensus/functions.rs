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

pub async fn consensus_middleware(app_state: &AppState, transactions: Vec<Transaction>, user_id: i32) -> Result<(), ConsensusError> {
    tracing::debug!("Starting consensus middleware for {} transactions", transactions.len());
    
    // Check if we are the current leader
    let consensus_state = db::get_consensus(app_state.db_pool.get()).map_err(|_| ConsensusError::DatabaseError)?;
    let my_node_id = app_state.get_node_id().map_err(|_| ConsensusError::DatabaseError)?;
    
    if consensus_state.leader.node_id != my_node_id {
        // Forward to leader instead of initiating consensus
        tracing::info!(
            "Not the leader (node {}), forwarding transactions to leader (node {})",
            my_node_id, consensus_state.leader.node_id
        );
        return forward_to_leader(consensus_state.leader, transactions, user_id, app_state).await;
    }
    
    tracing::info!(
        "Acting as leader (node {}) for view {}, initiating consensus",
        my_node_id, consensus_state.view
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

    // Get QC1 from ballot round
    let qc1 = ballot_round(&block, &me, ConsensusPhase::Propose, &validators, app_state).await?;
    // Broadcast QC1 and wait for enough confirmations
    broadcast_qc(&validators, qc1).await?;
    // Get QC2 from ballot round
    let qc2 = ballot_round(&block, &me, ConsensusPhase::Lock, &validators, app_state).await?;
    // Broadcast QC2 and wait for enough confirmations
    broadcast_qc(&validators, qc2).await?;

    process_transactions(&block.data.transactions, app_state);

    Ok(())
}

async fn ballot_round(
    block: &Block,
    me: &MyNode,
    phase: ConsensusPhase,
    validators: &Vec<Node>,
    app_state: &AppState
) -> Result<QuorumCertificate, ConsensusError> {
    let vote_data = VoteSignData::from_block(block.clone(), phase.clone());

    let my_signature = vote_data.sign(&app_state.private_key).map_err(|_| ConsensusError::SigningError)?;
    let initiator_signoff = VoteSignMessage {
        replica_id: me.node_id,
        signature: my_signature,
    };

    let ballot_proposal = Ballot::propose(vote_data, block.clone(), initiator_signoff);

    let voter_signatures = broadcast_and_collect_votes(ballot_proposal, &validators).await?;

    let qc = QuorumCertificate::create(
        &block,
        phase,
        me.node_id,
        &app_state.private_key,
        voter_signatures,
    ).map_err(|_| ConsensusError::SigningError)?;

    match qc.verify(&app_state, &block) {
        Ok(_) => {
            tracing::info!(
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
) -> Result<Vec<VoteSignMessage>, ConsensusError> {
    let (votes_tx, mut votes_rx) = mpsc::channel::<VoteSignMessage>(100); //100 channel capacity

    // Calculate quorum threshold (2/3 majority)
    let required_votes = (validators.len() * 2) / 3;
    
    // Spawn tasks for each validator
    for node in validators.clone() {
        let ballot_clone = ballot.clone();
        let votes_tx_clone = votes_tx.clone();
        
        tokio::spawn(async move {
            // Ignore errors from individual nodes - they'll just timeout or fail
            let _ = ballot_send(ballot_clone, &node, votes_tx_clone).await;
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
    qc: QuorumCertificate
) -> Result<(), ConsensusError> {
    let (confirmations_tx, mut confirmations_rx) = mpsc::channel::<()>(100);

    // Calculate quorum threshold (2/3 majority)
    let required_confirmations = (validators.len() * 2) / 3;
    
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

pub fn process_transactions(transactions: &Option<Transactions>, app_state: &AppState) {
    if let Some(transactions) = transactions {
        for tx in transactions.iter() {
            match process_transaction(tx, app_state) {
                Ok(_) => {
                    tracing::debug!("Transaction processed successfully: {}", &tx.function);
                }
                Err(e) => {
                    tracing::error!("Failed to process transaction {}: {:?}", &tx.function, e);
                    // Continue processing other transactions even if one fails
                }
            }
        }
    }
}

fn process_transaction(tx: &Transaction, app_state: &AppState) -> HandlerResult {
    // look up handler by name
    if let Some(handler) = DISPATCH_TABLE.get(tx.function.as_str()) {
        // if found, execute it with the payload
        handler.handle(app_state, &tx.payload)
    } else {
        tracing::warn!("No handler found for function: {}", &tx.function);
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
    user_id: i32,
    app_state: &AppState
) -> Result<(), ConsensusError> {
    let my_node_id = app_state.get_node_id().map_err(|_| ConsensusError::DatabaseError)?;
    let user_keys = app_state.get_user_keys().map_err(|_| ConsensusError::DatabaseError)?;
    
    // Serialize transactions for signing
    let body = serde_json::to_vec(&transactions).map_err(|_| ConsensusError::SigningError)?;
    
    // Sign with both node and user keys
    let node_signature = app_state.private_key.try_sign(&body).map_err(|_| ConsensusError::SigningError)?;
    let user_signature = user_keys.private_key.try_sign(&body).map_err(|_| ConsensusError::SigningError)?;
    
    // Forward to leader
    let client = reqwest::Client::new();
    let url = format!("http://{}:{}/consensus/propose", leader.ip_address, leader.port);
    
    tracing::info!(
        "Forwarding {} transactions to leader at {} (user_id: {}, node_id: {})",
        transactions.len(), &url, user_id, my_node_id
    );
    
    let response = client
        .post(&url)
        .header("X-Node-ID", my_node_id.to_string())
        .header("X-User-ID", user_id.to_string())
        .header("X-Node-Signature", hex::encode(node_signature.to_bytes()))
        .header("X-User-Signature", hex::encode(user_signature.to_bytes()))
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
pub async fn poll_subset_for_max_view(app_state: &AppState, consensus_state: &ConsensusState) -> Result<i32, ConsensusError> {
    // Get our node ID from AppState (avoid DB call)
    let my_node_id = app_state.get_node_id()
        .map_err(|_| ConsensusError::DatabaseError)?;
    let user_id = app_state.get_user_id()
        .map_err(|_| ConsensusError::DatabaseError)?;
    let user_keys = app_state.get_user_keys()
        .map_err(|_| ConsensusError::DatabaseError)?;
    
    let all_validators = db::get_validators(app_state.db_pool.get(), consensus_state.committed_block.data.height)
        .map_err(|_| ConsensusError::DatabaseError)?;
    
    // Filter out ourselves and select random subset of up to 3
    let other_validators: Vec<Node> = all_validators.into_iter()
        .filter(|v| v.node_id != my_node_id)
        .collect();
    
    if other_validators.is_empty() {
        // We're the only validator, so we're caught up by definition
        return Ok(consensus_state.view);
    }
    
    let subset_size = std::cmp::min(3, other_validators.len());
    let selected_validators: Vec<&Node> = other_validators
        .choose_multiple(&mut rand::rng(), subset_size)
        .collect();
    
    tracing::debug!("Polling {} validators for max view", selected_validators.len());
    
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
                                tracing::warn!("Failed to parse consensus state from validator {}: {:?}", node_id, e);
                                None
                            }
                        }
                    } else {
                        tracing::warn!("Validator {} returned status: {}", node_id, response.status());
                        None
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to contact validator {} at {}: {:?}", node_id, url, e);
                    None
                }
            }
        });
        
        tasks.push(task);
    }
    
    // Wait for all requests to complete and find max view
    let mut max_view = consensus_state.view; // Start with our own view
    
    for task in tasks {
        if let Ok(Some(view)) = task.await {
            max_view = std::cmp::max(max_view, view);
        }
    }
    
    tracing::debug!("Max view detected: {} (our view: {})", max_view, consensus_state.view);
    Ok(max_view)
}