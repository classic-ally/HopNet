use std::{collections::HashMap, time::SystemTime};
use axum::{
    extract::State,
    response::IntoResponse,
    http::StatusCode,
    Json
};

use crate::{
    db, types::PubKey
};
use crate::db::ConsensusState;

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
use bincode::{encode_to_vec, decode_from_slice, config, Encode, Decode};

use crate::{AppState};

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
}


pub struct Vote {
    // initiator of the vote, must be leader
    pub initiator: VoteSignMessage,

    // vote contents received for us to decide on
    pub data: VoteSignData,

    // our reply
    pub signoff: Option<VoteSignMessage>,
}

#[derive(Encode)]
struct VoteSignData {
    pub block_hash_bytes: [u8; 32],
    pub block_height: i32,
    pub view: i32,
    pub phase: ConsensusPhase,
}

impl VoteSignData {
    pub fn encode(&self) -> Result<Vec<u8>, VoteError> {
        return encode_to_vec(&self, config::standard()).map_err(|_| VoteError::ProcessingError);
    }
}

struct VoteSignMessage {
    pub replica_id: i32,
    pub signature: Signature,
}

impl Vote {
    pub fn propose(data: VoteSignData, from: VoteSignMessage) -> Vote {
        return Vote {
            initiator: from,
            data: data,
            signoff: None,
        };
    }

    pub fn verify_proposal(&self, state: AppState) -> Result<(), VoteError> {
        // Check leader signature is valid and leader is authorized
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

        // Check block and view
        // block_height must be +1
        // View must be = current view
        

        Ok(())
    }

    pub fn sign(&mut self, replica_id: i32, private_key: SigningKey) -> Result<(), VoteError> {
        let data = self.data.encode()?;
        let signature = private_key.try_sign(&data).map_err(|_| VoteError::ProcessingError)?;
        self.signoff = Some(VoteSignMessage{
            replica_id: replica_id,
            signature: signature
        });
        Ok(())
    }
}

#[derive(Encode, Decode)]
pub enum ConsensusPhase {
    Propose,
    Prepare,
}

pub struct QuorumCertificate {
    pub block_hash: blake3::Hash,
    pub block_height: i32,
    pub view: i32,
    pub phase: ConsensusPhase,
    pub total_replicas: i32,
    pub max_faults: i32,

    // Vote tracking
    signoffs_by_replica: HashMap<i32, [u8; 64]>,

    pub is_complete: bool,
    pub created_at: SystemTime
}

impl QuorumCertificate {

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