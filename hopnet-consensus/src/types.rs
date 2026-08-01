//! Consensus value types: blocks, transactions, hashes, keys.
//!
//! These are the future-canonical versions of the main crate's types
//! (src/types.rs, src/consensus/types.rs). Differences from the bespoke
//! engine's shapes, on purpose:
//! - `BlockData.height` is `u64` (was i32) and `round: u32` replaces
//!   `view_number: i32` — Tendermint rounds reset per height.
//! - `transactions` is non-optional (empty vec instead of None).
//! - `Blake3Hash` gains `Ord` (Malachite's `Value::Id` requires it).
//! - `PubKey`/`PrivKey` carry no iroh/axum impls (transport-agnostic crate);
//!   the main crate adds those via extension traits at the Stage-5 swap.
//!
//! SQL/bincode/serde encodings are byte-identical with the main crate's
//! (raw 32-byte blobs for hashes, bincode-serde blobs for keys/transactions)
//! so persisted app data needs no migration.

use std::ops::Deref;

use bincode::config;
use bincode::serde::encode_to_vec;
use blake3::Hasher;
use ed25519_dalek::{Signature, SigningKey, VerifyingKey};
use hopnet_common::CustomUUID;
use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

// ---------------------------------------------------------------------------
// Blake3Hash

/// A wrapper around blake3::Hash with bincode/serde/SQL encodings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Blake3Hash(pub blake3::Hash);

impl Blake3Hash {
    pub fn new(hash: blake3::Hash) -> Self {
        Self(hash)
    }

    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(blake3::Hash::from(bytes))
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        self.0.as_bytes()
    }

    pub fn to_hex(&self) -> String {
        self.0.to_hex().to_string()
    }
}

impl PartialOrd for Blake3Hash {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Blake3Hash {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.as_bytes().cmp(other.0.as_bytes())
    }
}

impl From<blake3::Hash> for Blake3Hash {
    fn from(hash: blake3::Hash) -> Self {
        Self(hash)
    }
}

impl std::fmt::Display for Blake3Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Serialize for Blake3Hash {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.to_hex())
        } else {
            serializer.serialize_bytes(self.0.as_bytes())
        }
    }
}

impl<'de> Deserialize<'de> for Blake3Hash {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::{Error, Visitor};
        use std::fmt;

        struct Blake3HashVisitor;

        impl<'de> Visitor<'de> for Blake3HashVisitor {
            type Value = Blake3Hash;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a hex string or binary data representing a Blake3 hash")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                let bytes = hex::decode(value).map_err(E::custom)?;
                self.visit_bytes(&bytes)
            }

            fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
            where
                E: Error,
            {
                if value.len() != 32 {
                    return Err(E::custom("Blake3 hash must be exactly 32 bytes"));
                }
                let mut array = [0u8; 32];
                array.copy_from_slice(value);
                Ok(Blake3Hash::from_bytes(array))
            }
        }

        if deserializer.is_human_readable() {
            deserializer.deserialize_str(Blake3HashVisitor)
        } else {
            deserializer.deserialize_bytes(Blake3HashVisitor)
        }
    }
}

impl FromSql for Blake3Hash {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Blob(bytes) => {
                let array: [u8; 32] = bytes.try_into().map_err(|_| {
                    FromSqlError::Other(
                        format!("Blake3Hash must be exactly 32 bytes, got {}", bytes.len()).into(),
                    )
                })?;
                Ok(Blake3Hash::from_bytes(array))
            }
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

impl ToSql for Blake3Hash {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self.as_bytes().as_slice()))
    }
}

// ---------------------------------------------------------------------------
// Keys

/// Ed25519 verifying key. Byte/SQL encodings match the main crate's PubKey
/// (bincode-serde blob in SQL — NOT the raw 32 bytes).
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct PubKey(pub VerifyingKey);

impl PubKey {
    pub fn from_hex(hex_str: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let bytes = hex::decode(hex_str)?;
        let array: [u8; 32] = bytes
            .as_slice()
            .try_into()
            .map_err(|_| "Public key must be exactly 32 bytes")?;
        Ok(PubKey(VerifyingKey::from_bytes(&array)?))
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0.to_bytes())
    }
}

impl Deref for PubKey {
    type Target = VerifyingKey;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Serialize for PubKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.to_hex())
        } else {
            serializer.serialize_bytes(&self.to_bytes())
        }
    }
}

impl<'de> Deserialize<'de> for PubKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::{Error, Visitor};
        use std::fmt;

        struct PubKeyVisitor;

        impl<'de> Visitor<'de> for PubKeyVisitor {
            type Value = PubKey;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a hex string or binary data representing a public key")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                PubKey::from_hex(value).map_err(E::custom)
            }

            fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
            where
                E: Error,
            {
                let array: [u8; 32] = value
                    .try_into()
                    .map_err(|_| E::custom("Public key must be exactly 32 bytes"))?;
                VerifyingKey::from_bytes(&array)
                    .map(PubKey)
                    .map_err(|_| E::custom("Invalid public key bytes"))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut bytes = Vec::new();
                while let Some(byte) = seq.next_element::<u8>()? {
                    bytes.push(byte);
                }
                self.visit_bytes(&bytes)
            }
        }

        if deserializer.is_human_readable() {
            deserializer.deserialize_any(PubKeyVisitor)
        } else {
            deserializer.deserialize_bytes(PubKeyVisitor)
        }
    }
}

impl ToSql for PubKey {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        match bincode::serde::encode_to_vec(self, bincode::config::standard()) {
            Ok(data) => Ok(ToSqlOutput::Owned(rusqlite::types::Value::Blob(data))),
            Err(e) => Err(rusqlite::Error::ToSqlConversionFailure(Box::new(e))),
        }
    }
}

impl FromSql for PubKey {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Blob(b) => {
                match bincode::serde::decode_from_slice(b, bincode::config::standard()) {
                    Ok((data, _)) => Ok(PubKey(data)),
                    Err(_) => Err(FromSqlError::InvalidType),
                }
            }
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

/// Ed25519 signing key.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PrivKey(pub SigningKey);

impl PrivKey {
    pub fn public(&self) -> PubKey {
        PubKey(self.0.verifying_key())
    }
}

impl Deref for PrivKey {
    type Target = SigningKey;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ToSql for PrivKey {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        match bincode::serde::encode_to_vec(self, bincode::config::standard()) {
            Ok(data) => Ok(ToSqlOutput::Owned(rusqlite::types::Value::Blob(data))),
            Err(e) => Err(rusqlite::Error::ToSqlConversionFailure(Box::new(e))),
        }
    }
}

impl FromSql for PrivKey {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Blob(b) => {
                match bincode::serde::decode_from_slice(b, bincode::config::standard()) {
                    Ok((data, _)) => Ok(PrivKey(data)),
                    Err(_) => Err(FromSqlError::InvalidType),
                }
            }
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

// ---------------------------------------------------------------------------
// Transactions

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
        use ed25519_dalek::Signer;
        let data = &self.encode()?;
        private_key
            .try_sign(data)
            .map_err(|_| TransactionError::SigningError)
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
    /// Node that submitted this transaction.
    pub submitter: SignedIdentity,
    /// User who initiated this (if a user operation).
    pub user: Option<SignedIdentity>,
    /// UUIDv7 nonce for dedup (prevents stale resubmission).
    pub nonce: CustomUUID,
}

impl Transaction {
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

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Transactions(pub Vec<Transaction>);

impl Deref for Transactions {
    type Target = Vec<Transaction>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ToSql for Transactions {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>, rusqlite::Error> {
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
                    Err(_) => Err(FromSqlError::InvalidType),
                }
            }
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

// ---------------------------------------------------------------------------
// Blocks

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Block {
    /// blake3(bincode(data)) — content address and Malachite `Value::Id`.
    pub block_hash: Blake3Hash,
    pub data: BlockData,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BlockData {
    pub height: u64,
    /// Tendermint round the value was proposed in (rounds reset per height).
    pub round: u32,
    /// None only for the genesis block.
    pub parent_hash: Option<Blake3Hash>,
    pub transactions: Transactions,
}

impl BlockData {
    pub fn encode(&self) -> Result<Vec<u8>, BlockError> {
        encode_to_vec(self, config::standard()).map_err(|_| BlockError::EncodingError)
    }

    pub fn compute_hash(&self) -> Result<Blake3Hash, BlockError> {
        let mut hasher = Hasher::new();
        hasher.update(self.encode()?.as_slice());
        Ok(Blake3Hash::new(hasher.finalize()))
    }
}

impl Block {
    pub fn new(data: BlockData) -> Result<Block, BlockError> {
        let block_hash = data.compute_hash()?;
        Ok(Block { block_hash, data })
    }

    /// Recompute the hash and compare against the stored one.
    pub fn verify(&self) -> Result<(), BlockError> {
        if self.data.compute_hash()? != self.block_hash {
            return Err(BlockError::HashMismatch);
        }
        Ok(())
    }
}

// Identity of a block IS its content hash; comparisons go through it so Block
// can satisfy Malachite's `Value` bounds without ordering transactions.
impl PartialEq for Block {
    fn eq(&self, other: &Self) -> bool {
        self.block_hash == other.block_hash
    }
}

impl Eq for Block {}

impl PartialOrd for Block {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Block {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.block_hash.cmp(&other.block_hash)
    }
}

impl std::hash::Hash for Block {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.block_hash.hash(state);
    }
}

impl malachitebft_core_types::Value for Block {
    type Id = Blake3Hash;

    fn id(&self) -> Blake3Hash {
        self.block_hash
    }
}

#[derive(Debug)]
pub enum BlockError {
    EncodingError,
    HashMismatch,
}
