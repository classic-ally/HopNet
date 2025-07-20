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
}

pub async fn consensus_middleware(app_state: &AppState, transactions: Vec<Transaction>) -> Result<(), ConsensusError> {
    dbg!("Begin middleware");
    let block = Block::new_tip(&app_state, transactions).map_err(|_| ConsensusError::BlockError)?;
    let me = db::get_me(app_state.db_pool.get()).map_err(|_| ConsensusError::DatabaseError)?;
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
            // save it to db
            dbg!("QC looks good, committing");
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
                    dbg!("Failed to send QC to node: {:?}", &e);
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
            dbg!("Received a response");
            if response.status().is_success() {
                dbg!("Response OK");
                Ok(())
            } else {
                dbg!("Response MalformedReply");
                return Err(ConsensusError::MalformedReply)
            }
        }
        Err(_) => {
            dbg!("TimeoutError");
            return Err(ConsensusError::TimeoutError)
        }
    }
}

pub fn process_transactions(transactions: &Option<Transactions>, app_state: &AppState) {
    if let Some(transactions) = transactions {
        for tx in transactions.iter() {
            match process_transaction(tx, app_state) {
                Ok(_) => {
                    dbg!("Transaction processed successfully: {}", &tx.function);
                }
                Err(e) => {
                    dbg!("Failed to process transaction {}: {:?}", &tx.function, e);
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
        dbg!("No handler found for function:", &tx.function);
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