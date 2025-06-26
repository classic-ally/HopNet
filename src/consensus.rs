use axum::{
    extract::State,
    response::IntoResponse,
    http::StatusCode,
    Json
};

use reqwest::Client;
use serde_json::Value;
use std::time::Duration;

use crate::types::{Blake3Hash, Block};
use crate::{
    db, types::Node, types::Transaction
};
use tokio::sync::mpsc;

/// CONSENSUS ARCHITECTURE
/// Key notes:
/// - Using ed25519 over threshold w/ distributed key generation (e.g. BLS)
///   - Verifier: O(nodes) signature verification vs O(1), but ed is ~10x faster per op
///   - Signer: both O(1), ed is ~5x faster per op
///   - ed25519_dalek library gives us batch verify, ~6-8x faster 128+ batch
///   - Conclusion: probably faster until nodes is multiple hundreds (O(nodes) dominates)
/// 
///   - Gives us audit trail (which node did what?) over BLS
///   - Simpler implementation (no secret sharing polynomial pain)
///   - But, more vulnerable to mistakes
///     - No cryptographic protection against committing undersigned changes
///   - O(nodes) sigs broadcast for changes introduces network overhead
///     - BLS: 96 bytes per sig
///     - ed: 64 bytes per sig -> 6.4kb per 100 nodes
/// 
///   - We may want to address this later based on % overhead stats

use ed25519_dalek::{SigningKey, VerifyingKey, Signer, Signature};
use rand_core::OsRng;
use serde::{Serialize,Deserialize};
use bincode::{encode_to_vec, config, Encode, Decode};

use crate::AppState;

pub fn generate_ed25519_key() -> (SigningKey, VerifyingKey) {
    let mut csprng = OsRng;
    let private_key= SigningKey::generate(&mut csprng);
    let public_key = private_key.verifying_key();

    return (private_key, public_key);
}

pub enum VoteError {
    DatabaseError,
    InitiatorError,
    ProcessingError,
    ProgressionError,
    BlockError,
}

pub enum CertificateError {
    DatabaseError,
    SigningError,
    ValidationError,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Ballot {
    // initiator of the vote, must be leader
    pub initiator: VoteSignMessage,

    // vote contents received for us to decide on
    pub data: VoteSignData,

    // associated block
    pub block: Block,
}

#[derive(Encode, Serialize, Deserialize, Clone)]
pub struct VoteSignData {
    pub block_hash: Blake3Hash,
    pub block_height: i32,
    pub view: i32,
    pub phase: ConsensusPhase,
}

impl VoteSignData {
    pub fn encode(&self) -> Result<Vec<u8>, VoteError> {
        return encode_to_vec(&self, config::standard()).map_err(|_| VoteError::ProcessingError);
    }
    pub fn from_block(block: Block, phase: ConsensusPhase) -> VoteSignData {
        return VoteSignData { block_hash: block.block_hash, block_height: block.data.height, view: block.data.view_number, phase: phase }
    }
    pub fn sign(&self, private_key: &SigningKey) -> Result<Signature, VoteError> {
        let data = &self.encode()?;
        let signature = private_key.try_sign(&data).map_err(|_| VoteError::ProcessingError)?;
        return Ok(signature);
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct VoteSignMessage {
    pub replica_id: i32,
    pub signature: Signature,
}

impl Ballot {
    pub fn propose(data: VoteSignData, block: Block, from: VoteSignMessage) -> Ballot {
        return Ballot {
            initiator: from,
            data: data,
            block: block,
        };
    }

    pub fn verify_proposal(&self, state: &AppState) -> Result<(), VoteError> {
        // Check leader signature is valid and leader is authorized for view
        let consensus_state = db::get_consensus(&state.db).map_err(|_| VoteError::DatabaseError)?;

        let leader_verifyingkey = consensus_state.leader.pubkey.to_verifying_key().map_err(|_| VoteError::DatabaseError)?;
        let message = self.data.encode().map_err(|_| VoteError::ProcessingError)?;
        match leader_verifyingkey.verify_strict(message.as_slice(), &self.initiator.signature) {
            Ok(_) => {
                // just to make sure it's the leader
                if self.initiator.replica_id != consensus_state.leader.node_id {
                    return Err(VoteError::InitiatorError);
                }
            }
            Err(_) => {return Err(VoteError::InitiatorError)}
        }

        // Check block hash is valid and actually matches the proposal's hash
        match self.block.verify() {
            Ok(_) => {
                if self.block.block_hash != self.data.block_hash {
                    return Err(VoteError::BlockError)
                }
            }
            Err(_) => {return Err(VoteError::BlockError)}
        }

        // 1. View number progression
        // Reject proposals with views less than highest QC view seen
        if self.data.view < consensus_state.highest_qc_block.data.view_number {
            return Err(VoteError::ProgressionError);
        }

        // 2. Chain validity check
        // Reject proposals that aren't listing tip of chain as parent
        match &self.block.data.parent_hash {
            Some(parent_hash) => {
                if parent_hash != &consensus_state.committed_block.block_hash {
                    return Err(VoteError::ProgressionError)
                }
            }
            None => {
                return Err(VoteError::ProgressionError)
            }
        }

        // 3. Preparation safety
        // Blocks in preparation phase should not be replaced by another block at same height
        if consensus_state.prepared_block.is_some() {
            if self.data.block_height == consensus_state.prepared_block.unwrap().data.height {
                return Err(VoteError::ProgressionError)
            }
        }

        // 4. Height validation
        // need to increase by 1 height each time
        if self.data.block_height != consensus_state.committed_block.data.height + 1 {
            return Err(VoteError::ProgressionError)
        }

        Ok(())
    }

    pub fn sign(&self, app_state: AppState) -> Result<VoteSignMessage, VoteError> {
        let me = db::get_me(&app_state.db).map_err(|_| VoteError::DatabaseError)?;
        let signature = self.data.sign(&me.privkey).map_err(|_| VoteError::ProcessingError)?;
        Ok(VoteSignMessage{
            replica_id: me.node_id,
            signature: signature
        })
    }
}

#[derive(Encode, Decode, Clone, Serialize, Deserialize, Debug)]
pub enum ConsensusPhase {
    Propose,
    Vote,
}

#[derive(Debug)]
pub struct QuorumCertificate {
    pub view_number: i32,
    pub phase: ConsensusPhase,
    pub block_hash: Blake3Hash,

    // Vote tracking
    pub proposer_signature: VoteSignMessage,
    pub voter_signatures: Vec<VoteSignMessage>,

}

impl QuorumCertificate {
    pub fn create(
        block: Block,
        phase: ConsensusPhase,
        proposer_id: i32,
        proposer_key: &SigningKey,
        voter_signatures: Vec<VoteSignMessage>,
    ) -> Result<QuorumCertificate, CertificateError> {
        // sign off ourselves
        let proposer_signature = VoteSignData::from_block(block.clone(), phase.clone()).sign(&proposer_key).map_err(|_| CertificateError::SigningError)?;
        let proposer_signature_message = VoteSignMessage {
            replica_id: proposer_id,
            signature: proposer_signature
        };
        
        Ok(QuorumCertificate { 
            view_number: block.data.view_number, 
            phase: phase, 
            block_hash: block.block_hash, 
            proposer_signature: proposer_signature_message,
            voter_signatures: voter_signatures 
        })
    }
    pub fn verify(&self, state: &AppState, block: Block) -> Result<(), CertificateError> {
        // Get validators for this height
        let validators = db::get_validators(&state.db, block.data.height).map_err(|_| CertificateError::DatabaseError)?;
        let num_validators = validators.len();
        
        // Check we have enough signatures for quorum (2/3 + 1)
        let required_signatures = (num_validators * 2) / 3 + 1;
        let total_signatures = 1 + self.voter_signatures.len(); // proposer + voters
        
        if total_signatures < required_signatures {
            return Err(CertificateError::ValidationError);
        }
        
        // Prepare data for batch verification
        let vote_data = VoteSignData::from_block(block.clone(), self.phase.clone());
        let message = vote_data.encode().map_err(|_| CertificateError::ValidationError)?;
        
        // Collect all signatures and public keys for batch verification
        let mut signatures = Vec::new();
        let mut public_keys = Vec::new();
        let mut messages = Vec::new();
        
        // Add proposer signature
        signatures.push(self.proposer_signature.signature);
        
        // Find proposer's public key
        let proposer_node = validators.iter()
            .find(|v| v.node_id == self.proposer_signature.replica_id)
            .ok_or(CertificateError::ValidationError)?;
        let proposer_pubkey = proposer_node.pubkey.to_verifying_key()
            .map_err(|_| CertificateError::ValidationError)?;
        public_keys.push(proposer_pubkey);
        messages.push(message.as_slice());
        
        // Add voter signatures
        for voter_sig in &self.voter_signatures {
            signatures.push(voter_sig.signature);
            
            // Find voter's public key
            let voter_node = validators.iter()
                .find(|v| v.node_id == voter_sig.replica_id)
                .ok_or(CertificateError::ValidationError)?;
            let voter_pubkey = voter_node.pubkey.to_verifying_key()
                .map_err(|_| CertificateError::ValidationError)?;
            public_keys.push(voter_pubkey);
            messages.push(message.as_slice());
        }
        
        // Perform batch verification
        match ed25519_dalek::verify_batch(&messages, &signatures, &public_keys) {
            Ok(_) => {
                // Additional validation: ensure block hash matches
                if self.block_hash != block.block_hash {
                    return Err(CertificateError::ValidationError);
                }
                
                // Ensure view number matches
                if self.view_number != block.data.view_number {
                    return Err(CertificateError::ValidationError);
                }
                
                Ok(())
            }
            Err(_) => Err(CertificateError::ValidationError)
        }
    }
}

// route to get the consensus status
pub async fn get_consensus(
    State(app_state): State<AppState>,
) -> impl IntoResponse {
    match db::get_consensus(&app_state.db) {
        Ok(info) => (StatusCode::OK, Json(info)).into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Failed to get leader info").into_response(),
    }
}

// route to get acceptable validators for a given view
pub async fn get_validators(
    State(app_state): State<AppState>,
    Json(height): Json<i32>,
) -> impl IntoResponse {
    match db::get_validators(&app_state.db, height) {
        Ok(nodes) => (StatusCode::OK, Json(nodes)).into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Failed to get validators").into_response(),
    }
}

// route to accept ballots and operate on them
pub async fn post_ballot(
    State(app_state): State<AppState>,
    Json(ballot): Json<Ballot>
) -> impl IntoResponse {
    // validate the ballot proposal
    match ballot.verify_proposal(&app_state) {
        Ok(()) => {
            // sign and return the response
            match ballot.sign(app_state) {
                Ok(signoff) => {
                    dbg!("Signing off on block hash {}", ballot.block.block_hash);
                    return (StatusCode::OK, Json(signoff)).into_response()
                }
                Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Error signing ballot").into_response(),
            }
        }
        Err(_) => (StatusCode::UNAUTHORIZED, "Ballot rejected").into_response(),
    }
}

pub enum ConsensusError {
    InsufficientVotes,
    BlockError,
    DatabaseError,
    SigningError,
    TimeoutError,
    MalformedReply,
    ThreadError,
}

pub async fn consensus_middleware(app_state: &AppState, transactions: Vec<Transaction>) -> Result<QuorumCertificate, ConsensusError> {
    let block = Block::new_tip(&app_state, transactions).map_err(|_| ConsensusError::BlockError)?;
    let vote_data = VoteSignData::from_block(block.clone(), ConsensusPhase::Propose);

    let me = db::get_me(&app_state.db).map_err(|_| ConsensusError::DatabaseError)?;
    let my_signature = vote_data.sign(&app_state.private_key).map_err(|_| ConsensusError::SigningError)?;
    let initiator_signoff = VoteSignMessage {
        replica_id: me.node_id,
        signature: my_signature,
    };

    let ballot_proposal = Ballot::propose(vote_data, block.clone(), initiator_signoff);
    
    // Get validators and broadcast ballot to collect votes
    let validators = db::get_validators(&app_state.db, ballot_proposal.block.data.height).map_err(|_| ConsensusError::DatabaseError)?;
    let voter_signatures = broadcast_and_collect_votes(ballot_proposal, validators).await?;
    
    // Create quorum certificate with collected signatures
    let qc = QuorumCertificate::create(
        block.clone(),
        ConsensusPhase::Propose,
        me.node_id,
        &app_state.private_key,
        voter_signatures,
    ).map_err(|_| ConsensusError::SigningError)?;

    // verify the QC for sanity check
    match qc.verify(&app_state, block) {
        Ok(_) => return Ok(qc),
        Err(_) => return Err(ConsensusError::SigningError)
    };
    // return Ok(qc)
}

async fn broadcast_and_collect_votes(
    ballot: Ballot,
    validators: Vec<Node>,
) -> Result<Vec<VoteSignMessage>, ConsensusError> {
    // make sure we don't contact ourself
    let filtered_validators: Vec<Node> = validators
        .into_iter()
        .filter(|node| node.node_id != ballot.initiator.replica_id)
        .collect();

    let (votes_tx, mut votes_rx) = mpsc::channel::<VoteSignMessage>(100); //100 channel capacity

    // Calculate quorum threshold (2/3 majority)
    let required_votes = (filtered_validators.len() * 2) / 3;
    
    // Spawn tasks for each validator
    for node in filtered_validators {
        let ballot_clone = ballot.clone();
        let votes_tx_clone = votes_tx.clone();
        
        tokio::spawn(async move {
            // Ignore errors from individual nodes - they'll just timeout or fail
            let _ = ballot_send(ballot_clone, node, votes_tx_clone).await;
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

async fn ballot_send(
    ballot: Ballot,
    node: Node,
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