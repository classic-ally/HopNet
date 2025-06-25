use serde::{Serialize, Deserialize, Deserializer, Serializer};
use ed25519_dalek::VerifyingKey;
use bincode::{encode_to_vec, decode_from_slice, config, Encode, Decode};

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

#[derive(Serialize, Deserialize, Debug)]
pub struct Block {
    // hash of this block: db key
    pub block_hash: blake3::Hash,

    // computed based on these: db value
    pub height: i32,
    pub view_number: i32,
    pub parent_hash: Option<blake3::Hash>,
    pub transactions: Option<Vec<Transaction>>
}

impl Block {
    pub fn tx_to_db(&self) -> Result<Vec<u8>, bincode::error::EncodeError> {
        match &self.transactions {
            Some(transactions) => encode_to_vec(transactions, config::standard()),
            None => encode_to_vec(&Vec::<Transaction>::new(), config::standard())
        }
    }
    
    pub fn db_to_tx(&mut self, data: Vec<u8>) -> Result<(), bincode::error::DecodeError> {
        let (transactions, _): (Vec<Transaction>, usize) = decode_from_slice(&data, config::standard())?;
        self.transactions = Some(transactions);
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Encode, Decode, Debug)]
pub struct Transaction {
    // function the data is passed to
    pub function: String,
    // data passed into function, with input data type
    pub payload: Vec<u8>
}