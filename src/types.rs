use serde::{Serialize, Deserialize, Deserializer, Serializer};
use ed25519_dalek::VerifyingKey;
use bincode::{encode_to_vec, decode_from_slice, config, Encode, Decode};
use blake3::Hasher;

use crate::{db, AppState};

/// A wrapper around blake3::Hash that implements bincode's Encode and Decode traits
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Blake3Hash(blake3::Hash);

impl Blake3Hash {
    /// Create a new Blake3Hash from a blake3::Hash
    pub fn new(hash: blake3::Hash) -> Self {
        Self(hash)
    }
    
    /// Get the inner blake3::Hash
    pub fn inner(&self) -> &blake3::Hash {
        &self.0
    }
    
    /// Convert into the inner blake3::Hash
    pub fn into_inner(self) -> blake3::Hash {
        self.0
    }
    
    /// Create from bytes
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(blake3::Hash::from(bytes))
    }
    
    /// Get as bytes
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
    
    /// Convert to hex string
    pub fn to_hex(&self) -> String {
        self.0.to_hex().to_string()
    }
}

impl From<blake3::Hash> for Blake3Hash {
    fn from(hash: blake3::Hash) -> Self {
        Self(hash)
    }
}

impl From<Blake3Hash> for blake3::Hash {
    fn from(wrapper: Blake3Hash) -> Self {
        wrapper.0
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
        // Serialize as hex string for JSON compatibility
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for Blake3Hash {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;
        
        let hex_str = String::deserialize(deserializer)?;
        let bytes = hex::decode(&hex_str).map_err(D::Error::custom)?;
        
        if bytes.len() != 32 {
            return Err(D::Error::custom("Blake3 hash must be exactly 32 bytes"));
        }
        
        let mut array = [0u8; 32];
        array.copy_from_slice(&bytes);
        Ok(Blake3Hash::from_bytes(array))
    }
}

// Simple wrapper that converts to/from Vec<u8> for bincode compatibility
impl Encode for Blake3Hash {
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> Result<(), bincode::error::EncodeError> {
        // Convert to Vec<u8> and encode that
        let bytes: Vec<u8> = self.as_bytes().to_vec();
        bytes.encode(encoder)
    }
}

impl<Context> Decode<Context> for Blake3Hash {
    fn decode<D: bincode::de::Decoder>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        // Decode as Vec<u8> and convert to [u8; 32]
        let bytes: Vec<u8> = Vec::decode(decoder)?;
        if bytes.len() != 32 {
            return Err(bincode::error::DecodeError::Other("Blake3 hash must be exactly 32 bytes"));
        }
        let mut array = [0u8; 32];
        array.copy_from_slice(&bytes);
        Ok(Blake3Hash::from_bytes(array))
    }
}

impl<'de, Context> bincode::BorrowDecode<'de, Context> for Blake3Hash {
    fn borrow_decode<D: bincode::de::BorrowDecoder<'de>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        // Borrow decode as a slice and convert to [u8; 32]
        let bytes: &[u8] = bincode::BorrowDecode::borrow_decode(decoder)?;
        if bytes.len() != 32 {
            return Err(bincode::error::DecodeError::Other("Blake3 hash must be exactly 32 bytes"));
        }
        let mut array = [0u8; 32];
        array.copy_from_slice(bytes);
        Ok(Blake3Hash::from_bytes(array))
    }
}

#[derive(Debug, Clone)]
pub struct PubKey(pub Vec<u8>);

impl PubKey {
    pub fn from_hex(hex_str: &str) -> Result<Self, hex::FromHexError> {
        hex::decode(hex_str).map(PubKey)
    }
    
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        PubKey(bytes)
    }
    
    /// Create PubKey from ed25519_dalek::VerifyingKey
    pub fn from_verifying_key(verifying_key: &VerifyingKey) -> Self {
        PubKey(verifying_key.to_bytes().to_vec())
    }
    
    pub fn to_hex(&self) -> String {
        hex::encode(&self.0)
    }
    
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
    
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
    
    /// Convert PubKey back to ed25519_dalek::VerifyingKey for cryptographic operations
    pub fn to_verifying_key(&self) -> Result<VerifyingKey, ed25519_dalek::SignatureError> {
        // ed25519 public keys are exactly 32 bytes
        let key_bytes: [u8; 32] = self.0.as_slice()
            .try_into()
            .map_err(|_| ed25519_dalek::SignatureError::new())?;
        
        VerifyingKey::from_bytes(&key_bytes)
    }
}

impl Serialize for PubKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Always serialize as hex string for JSON compatibility
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for PubKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;
        
        // Try to deserialize as string first (hex format)
        let value = serde_json::Value::deserialize(deserializer)?;
        
        match value {
            serde_json::Value::String(hex_str) => {
                PubKey::from_hex(&hex_str).map_err(D::Error::custom)
            }
            serde_json::Value::Array(arr) => {
                // Handle Vec<u8> format
                let bytes: Result<Vec<u8>, _> = arr.into_iter()
                    .map(|v| v.as_u64().ok_or_else(|| D::Error::custom("Invalid byte value"))
                         .and_then(|n| if n <= 255 { Ok(n as u8) } else { Err(D::Error::custom("Byte value out of range")) }))
                    .collect();
                bytes.map(PubKey::from_bytes)
            }
            _ => Err(D::Error::custom("Expected string (hex) or array (bytes) for pubkey"))
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Node {
    pub node_id: i32,
    pub name: String,
    pub ip_address: String,
    pub port: i32,
    pub owner: i32, // userid corresponding to owner
    pub pubkey: PubKey,
}

#[derive(Serialize, Deserialize, Debug, Encode, Decode, Clone)]
pub struct Block {
    // hash of this block: db key
    pub block_hash: Blake3Hash,

    // computed based on these: db value
    pub data: BlockData,
}

#[derive(Serialize, Deserialize, Debug, Encode, Decode, Clone)]
pub struct BlockData {
    pub height: i32,
    pub view_number: i32,
    pub parent_hash: Option<Blake3Hash>,
    pub transactions: Option<Vec<Transaction>>
}

pub enum BlockError {
    EncodingError,
    DatabaseError,
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
        app_state: AppState, 
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
                    transactions: Some(transactions)
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

    pub fn tx_to_db(&self) -> Result<Vec<u8>, bincode::error::EncodeError> {
        match &self.data.transactions {
            Some(transactions) => encode_to_vec(transactions, config::standard()),
            None => encode_to_vec(&Vec::<Transaction>::new(), config::standard())
        }
    }
    
    pub fn db_to_tx(&mut self, data: Vec<u8>) -> Result<(), bincode::error::DecodeError> {
        let (transactions, _): (Vec<Transaction>, usize) = decode_from_slice(&data, config::standard())?;
        self.data.transactions = Some(transactions);
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Encode, Decode, Debug, Clone)]
pub struct Transaction {
    // function the data is passed to
    pub function: String,
    // data passed into function, with input data type
    pub payload: Vec<u8>
}