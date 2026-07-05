//! Application transaction types (the consensus crate owns the protocol
//! types now). The bespoke engine's Ballot/QC/TC/vote machinery was deleted
//! at Stage 5b; `hopnet_consensus::types` is the canonical block shape —
//! these Transaction types bridge to it via bincode until the main crate
//! re-points entirely.

use super::*;
use bincode::config;
use bincode::serde::encode_to_vec;
use blake3::Hasher;
use ed25519_dalek::Signature;
use hopnet_common::CustomUUID;
use rusqlite::{
    ToSql, types::FromSql, types::FromSqlResult, types::ToSqlOutput,
    types::ValueRef,
};
use serde::{Deserialize, Serialize};
use std::ops::Deref;

#[derive(Debug)]
pub enum BlockError {
    EncodingError,
    DatabaseError,
    ValidationError,
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
    pub transactions: Option<Transactions>,
}

impl BlockData {
    pub fn encode(&self) -> Result<Vec<u8>, BlockError> {
        encode_to_vec(self, config::standard()).map_err(|_| BlockError::EncodingError)
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

        Ok(Block {
            block_hash: digest,
            data,
        })
    }

    pub fn verify(&self) -> Result<(), BlockError> {
        // compute hash and compare to self
        let digest = self.data.compute_hash()?;
        if digest != self.block_hash {
            return Err(BlockError::EncodingError);
        }
        Ok(())
    }
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RpcCall {
    pub function: String,
    pub payload: Vec<u8>,
}

impl RpcCall {
    pub fn encode(&self) -> Result<Vec<u8>, TransactionError> {
        encode_to_vec(self, config::standard()).map_err(|_| TransactionError::EncodingError)
    }

    pub fn sign(&self, private_key: &PrivKey) -> Result<Signature, TransactionError> {
        let data = &self.encode()?;
        let signature = private_key
            .try_sign(data)
            .map_err(|_| TransactionError::SigningError)?;
        Ok(signature)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SignedIdentity {
    pub id: i32,
    pub signature: Signature,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Transaction {
    pub rpc: RpcCall,
    pub submitter: SignedIdentity, // Node that submitted this transaction
    pub user: Option<SignedIdentity>, // User who initiated this (if user operation)
    pub nonce: CustomUUID,         // UUIDv7 nonce for dedup (prevents stale resubmission)
}

impl Transaction {
    // Create a node-only transaction (automated operations)
    pub fn new(
        function: String,
        payload: Vec<u8>,
        submitter_id: i32,
        submitter_key: &PrivKey,
    ) -> Result<Self, TransactionError> {
        let rpc = RpcCall { function, payload };
        let signature = rpc.sign(submitter_key)?;

        Ok(Transaction {
            rpc,
            submitter: SignedIdentity {
                id: submitter_id,
                signature,
            },
            user: None,
            nonce: CustomUUID::new(None),
        })
    }

    // Create a user-initiated transaction
    pub fn new_with_user(
        function: String,
        payload: Vec<u8>,
        submitter_id: i32,
        submitter_key: &PrivKey,
        user_id: i32,
        user_key: &PrivKey,
    ) -> Result<Self, TransactionError> {
        let rpc = RpcCall { function, payload };
        let submitter_signature = rpc.sign(submitter_key)?;
        let user_signature = rpc.sign(user_key)?;

        Ok(Transaction {
            rpc,
            submitter: SignedIdentity {
                id: submitter_id,
                signature: submitter_signature,
            },
            user: Some(SignedIdentity {
                id: user_id,
                signature: user_signature,
            }),
            nonce: CustomUUID::new(None),
        })
    }

    pub fn verify_signature(&self, submitter_pubkey: &PubKey) -> Result<(), TransactionError> {
        let message = self.rpc.encode()?;
        submitter_pubkey
            .verify_strict(&message, &self.submitter.signature)
            .map_err(|_| TransactionError::InvalidSignature)
    }

    pub fn verify_user_signature(&self, user_pubkey: &PubKey) -> Result<(), TransactionError> {
        if let Some(user) = &self.user {
            let message = self.rpc.encode()?;
            user_pubkey
                .verify_strict(&message, &user.signature)
                .map_err(|_| TransactionError::InvalidSignature)
        } else {
            Err(TransactionError::InvalidSignature)
        }
    }
}

#[derive(Debug)]
pub enum TransactionError {
    SigningError,
    EncodingError,
    InvalidSignature,
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
    fn to_sql(&self) -> Result<ToSqlOutput<'_>, rusqlite::Error> {
        // let's turn transactions into Vec<u8>
        match bincode::serde::encode_to_vec(self, bincode::config::standard()) {
            Ok(data) => Ok(ToSqlOutput::Owned(rusqlite::types::Value::Blob(data))),
            Err(e) => Err(rusqlite::Error::ToSqlConversionFailure(Box::new(e))),
        }
    }
}

impl FromSql for Transactions {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Blob(b) => {
                match bincode::serde::decode_from_slice(b, bincode::config::standard()) {
                    Ok((data, _)) => Ok(Transactions(data)),
                    Err(_) => Err(rusqlite::types::FromSqlError::InvalidType),
                }
            }
            _ => Err(rusqlite::types::FromSqlError::InvalidType),
        }
    }
}

