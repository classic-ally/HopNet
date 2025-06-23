use serde::{Serialize, Deserialize, Deserializer, Serializer};

#[derive(Debug, Clone)]
pub struct PubKey(pub Vec<u8>);

impl PubKey {
    pub fn from_hex(hex_str: &str) -> Result<Self, hex::FromHexError> {
        hex::decode(hex_str).map(PubKey)
    }
    
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        PubKey(bytes)
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