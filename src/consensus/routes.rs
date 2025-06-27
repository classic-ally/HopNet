use super::*;

use axum::{
    extract::State,
    response::IntoResponse,
    http::StatusCode,
    Json
};

use reqwest::Client;
use serde_json::Value;

use crate::db::consensus as db;
use crate::types::Node;
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

use ed25519_dalek::{SigningKey, VerifyingKey};
use rand_core::OsRng;

use crate::AppState;

pub fn generate_ed25519_key() -> (SigningKey, VerifyingKey) {
    let mut csprng = OsRng;
    let private_key= SigningKey::generate(&mut csprng);
    let public_key = private_key.verifying_key();

    return (private_key, public_key);
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
            dbg!("Signing off on block hash {}", ballot.block.block_hash);
            match ballot.sign(&app_state) {
                Ok(signoff) => {
                    dbg!("Adding to database block hash {}", ballot.block.block_hash);
                    match db::insert_block(&app_state.db, &ballot.block) {
                        Ok(()) => {
                            dbg!("Block saved!");
                            return (StatusCode::OK, Json(signoff)).into_response()
                        },
                        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "Error adding block to database").into_response(),
                    }
                }
                Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Error signing ballot").into_response(),
            }
        }
        Err(_) => (StatusCode::UNAUTHORIZED, "Ballot rejected").into_response(),
    }
}

// route to accept qcs and operate on them
pub async fn post_qc(
    State(app_state): State<AppState>,
    Json(qc): Json<QuorumCertificate>
) -> impl IntoResponse {
    // validate the QC against internal block
    dbg!("Received QC");
    match db::get_block(&app_state.db, qc.block_hash) {
        Ok(block) => {
            dbg!("We have the block, verifying...");
            match qc.verify(&app_state, &block) {
                Ok(()) => {
                    // save it to db
                    dbg!("QC looks good, committing");
                    match db::insert_qc(&app_state.db, qc) {
                        Ok(()) => StatusCode::OK,
                        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
                    }
                }
                Err(e) => {
                    dbg!("Don't like the QC, printing error");
                    dbg!(e);
                    StatusCode::UNAUTHORIZED
                }
            }
        }
        Err(_) => StatusCode::NOT_FOUND
    }
    
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

pub async fn consensus_middleware(app_state: &AppState, transactions: Vec<Transaction>) -> Result<QuorumCertificate, ConsensusError> {
    dbg!("Begin middleware");
    let block = Block::new_tip(&app_state, transactions).map_err(|_| ConsensusError::BlockError)?;
    // add block to database irrespective of its acceptance
    match db::insert_block(&app_state.db, &block) {
        Ok(()) => {}
        Err(_) => return Err(ConsensusError::DatabaseError)
    }

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
    let voter_signatures = broadcast_and_collect_votes(ballot_proposal, &validators).await?;
    
    // Create quorum certificate with collected signatures
    let qc = QuorumCertificate::create(
        block.clone(),
        ConsensusPhase::Propose,
        me.node_id,
        &app_state.private_key,
        voter_signatures,
    ).map_err(|_| ConsensusError::SigningError)?;

    // verify the QC for sanity check
    match qc.verify(&app_state, &block) {
        Ok(_) => {
            // save it to db
            dbg!("QC looks good, committing");
            db::insert_qc(&app_state.db, qc.clone()).map_err(|_| ConsensusError::DatabaseError)?;
        },
        Err(_) => return Err(ConsensusError::SigningError)
    };

    dbg!("Validator filter");
    // broadcast the QC to all validators
    let filtered_validators: Vec<Node> = validators
        .iter()
        .filter(|node| node.node_id != me.node_id)
        .cloned()
        .collect();

    dbg!("Contact threads");
    for node in filtered_validators {
        let qc_clone = qc.clone();
        tokio::spawn(async move {
            match qc_send(qc_clone, &node).await {
                Ok(()) => Ok(()),
                Err(e) => {
                    dbg!(&e);
                    return Err(e)
                }
            }
        });
    }

    Ok(qc)
}

async fn broadcast_and_collect_votes(
    ballot: Ballot,
    validators: &Vec<Node>,
) -> Result<Vec<VoteSignMessage>, ConsensusError> {
    // make sure we don't contact ourself
    let filtered_validators: Vec<Node> = validators
        .iter()
        .filter(|node| node.node_id != ballot.initiator.replica_id)
        .cloned()
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