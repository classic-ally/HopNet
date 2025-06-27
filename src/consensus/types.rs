use super::*;
use serde::{Serialize, Deserialize};
use std::ops::Deref;
use duckdb::{ToSql,types::ToSqlOutput,types::FromSql,types::FromSqlResult,types::ValueRef};
use crate::db::consensus as db;
use bincode::serde::encode_to_vec;
use ed25519_dalek::Signature;
use bincode::config;
use blake3::Hasher;

pub enum VoteError {
    DatabaseError,
    InitiatorError,
    ProcessingError,
    ProgressionError,
    BlockError,
}

#[derive(Debug)]
pub enum CertificateError {
    DatabaseError,
    SigningError,
    ValidationError,
    SignerNotFound,
}

pub enum BlockError {
    EncodingError,
    DatabaseError,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum ConsensusPhase {
    Propose,
    Vote,
}

impl ToSql for ConsensusPhase {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>, duckdb::Error> {
        let phase_str = match self {
            ConsensusPhase::Propose => "propose",
            ConsensusPhase::Vote => "vote",
        };
        return Ok(phase_str.into())
    }
}

impl FromSql for ConsensusPhase {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Text(s) => {
                match s {
                    b"propose" => Ok(ConsensusPhase::Propose),
                    b"vote" => Ok(ConsensusPhase::Vote),
                    _ => Err(duckdb::types::FromSqlError::InvalidType),
                }
            }
            ValueRef::Enum(_, index) => {
                // DuckDB stores enums as dictionary arrays with indices
                // Index 0 = "propose", Index 1 = "vote"
                match index {
                    0 => Ok(ConsensusPhase::Propose),
                    1 => Ok(ConsensusPhase::Vote),
                    _ => Err(duckdb::types::FromSqlError::InvalidType),
                }
            }
            _ => Err(duckdb::types::FromSqlError::InvalidType)
        }
    }
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

    pub fn sign(&self, app_state: &AppState) -> Result<VoteSignMessage, VoteError> {
        let me = db::get_me(&app_state.db).map_err(|_| VoteError::DatabaseError)?;
        let signature = self.data.sign(&me.privkey).map_err(|_| VoteError::ProcessingError)?;
        Ok(VoteSignMessage{
            replica_id: me.node_id,
            signature: signature
        })
    }
}

#[derive(Serialize, Deserialize, Clone)]
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VoteSignMessages(Vec<VoteSignMessage>);

impl ToSql for VoteSignMessage {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>, duckdb::Error> {
        // let's turn votesignmessage into Vec<u8>
        match bincode::serde::encode_to_vec(&self, bincode::config::standard()) {
            Ok(data) => Ok(ToSqlOutput::Owned(duckdb::types::Value::Blob(data))),
            Err(e) => Err(duckdb::Error::ToSqlConversionFailure(Box::new(e)))
        }
    }
}

impl Deref for VoteSignMessages {
    type Target = Vec<VoteSignMessage>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ToSql for VoteSignMessages {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>, duckdb::Error> {
        match bincode::serde::encode_to_vec(&self, bincode::config::standard()) {
            Ok(data) => Ok(ToSqlOutput::Owned(duckdb::types::Value::Blob(data))),
            Err(e) => Err(duckdb::Error::ToSqlConversionFailure(Box::new(e)))
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QuorumCertificate {
    pub view_number: i32,
    pub phase: ConsensusPhase,
    pub block_hash: Blake3Hash,

    // Vote tracking
    pub proposer_signature: VoteSignMessage,
    pub voter_signatures: VoteSignMessages,

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
        // cast to VoteSignMessages
        let vsm = VoteSignMessages(voter_signatures);
        
        Ok(QuorumCertificate { 
            view_number: block.data.view_number, 
            phase: phase, 
            block_hash: block.block_hash, 
            proposer_signature: proposer_signature_message,
            voter_signatures: vsm 
        })
    }
    pub fn verify(&self, state: &AppState, block: &Block) -> Result<(), CertificateError> {
        // Get validators for this height
        let validators = db::get_validators(&state.db, block.data.height).map_err(|_| CertificateError::DatabaseError)?;
        dbg!(&validators.len());
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
            .ok_or(CertificateError::SignerNotFound)?;
        let proposer_pubkey = proposer_node.pubkey.to_verifying_key()
            .map_err(|_| CertificateError::ValidationError)?;
        public_keys.push(proposer_pubkey);
        messages.push(message.as_slice());
        
        // Add voter signatures
        for voter_sig in &*self.voter_signatures {
            signatures.push(voter_sig.signature);
            
            // Find voter's public key
            let voter_node = validators.iter()
                .find(|v| v.node_id == voter_sig.replica_id)
                .ok_or(CertificateError::SignerNotFound)?;
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
            Err(_) => {
                Err(CertificateError::ValidationError)
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Block {
    // hash of this block: db key
    pub block_hash: Blake3Hash,

    // computed based on these: db value
    pub data: BlockData,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BlockData {
    pub height: i32,
    pub view_number: i32,
    pub parent_hash: Option<Blake3Hash>,
    pub transactions: Option<Transactions>
}

impl BlockData {
    pub fn encode(&self) -> Result<Vec<u8>, BlockError> {
        return encode_to_vec(&self, config::standard()).map_err(|_| BlockError::EncodingError);
    }

    pub fn compute_hash(&self) -> Result<Blake3Hash, BlockError> {
        let mut hasher = Hasher::new();
        let encoded_data = &self.encode()?;
        hasher.update(encoded_data.as_slice());
        let digest = Blake3Hash::new(hasher.finalize());
        Ok(digest)
    }
}

impl Block {
    pub fn new(data: BlockData) -> Result<Block, BlockError> {
        // compute hash over blockdata
        let digest = data.compute_hash()?;
        
        return Ok(Block {
            block_hash: digest,
            data: data
        })
    }
    
    pub fn new_tip(
        app_state: &AppState, 
        transactions: Vec<Transaction>
    ) -> Result<Block, BlockError> {
        // get the current tip
        // it is the committed_block
        match db::get_consensus(&app_state.db) {
            Ok(consensus_state) => {
                let tip_data = BlockData {
                    height: consensus_state.committed_block.data.height + 1,
                    view_number: consensus_state.view,
                    parent_hash: Some(consensus_state.committed_block.block_hash),
                    transactions: Some(Transactions(transactions))
                };
                let new_block = Block::new(tip_data)?;
                Ok(new_block)
            }
            Err(_) => Err(BlockError::DatabaseError)
        }
    }

    pub fn verify(&self) -> Result<(), BlockError> {
        // compute hash and compare to self
        let digest = self.data.compute_hash()?;
        if digest != self.block_hash {
            return Err(BlockError::EncodingError)
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Transaction {
    // function the data is passed to
    pub function: String,
    // data passed into function, with input data type
    pub payload: Vec<u8>
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Transactions(pub Vec<Transaction>);

impl Deref for Transactions {
    type Target = Vec<Transaction>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ToSql for Transactions {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>, duckdb::Error> {
        // let's turn transactions into Vec<u8>
        match bincode::serde::encode_to_vec(&self, bincode::config::standard()) {
            Ok(data) => Ok(ToSqlOutput::Owned(duckdb::types::Value::Blob(data))),
            Err(e) => Err(duckdb::Error::ToSqlConversionFailure(Box::new(e)))
        }
    }
}

impl FromSql for Transactions {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Blob(b) => {
                match bincode::serde::decode_from_slice(b, bincode::config::standard()) {
                    Ok((data, _)) => Ok(Transactions(data)),
                    Err(_) => Err(duckdb::types::FromSqlError::InvalidType),
                }
            }
            _ => Err(duckdb::types::FromSqlError::InvalidType),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ConsensusState {
    pub leader: Node,
    pub view: i32,
    pub prepared_block: Option<Block>,
    pub committed_block: Block,
    pub highest_qc_block: Block,
}

use crate::db::DatabaseError;