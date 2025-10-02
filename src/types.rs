use axum::response::{IntoResponse,Response};
use serde::{Serialize, Deserialize, Deserializer, Serializer};
use std::ops::Deref;
use ed25519_dalek::VerifyingKey;
use bincode::{Encode, Decode};
use duckdb::types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef};
pub use ed25519_dalek::{SigningKey,Signer};
use hex;

/// A wrapper around blake3::Hash that implements bincode's Encode and Decode traits
#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
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

impl FromSql for Blake3Hash {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Blob(bytes) => {
                if bytes.len() != 32 {
                    return Err(FromSqlError::Other(format!(
                        "Blake3Hash must be exactly 32 bytes, got {} bytes",
                        bytes.len()
                    ).into()));
                }
                let mut array = [0u8; 32];
                array.copy_from_slice(bytes);
                Ok(Blake3Hash::from_bytes(array))
            }
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

impl ToSql for Blake3Hash {
    fn to_sql(&self) -> duckdb::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self.as_bytes()))
    }
}

#[derive(Debug, Copy, Clone)]
pub struct PubKey(pub VerifyingKey);

impl Serialize for PubKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Check if we're serializing to a human-readable format (like JSON)
        if serializer.is_human_readable() {
            // Use hex string for JSON and other human-readable formats
            serializer.serialize_str(&self.to_hex())
        } else {
            // Use binary format for non-human-readable formats (like bincode)
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

            // Handle hex string format (for user input)
            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                PubKey::from_hex(value).map_err(E::custom)
            }

            // Handle binary format (for internal/database usage)
            fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
            where
                E: Error,
            {
                if value.len() != 32 {
                    return Err(E::custom("Public key must be exactly 32 bytes"));
                }
                
                let mut array = [0u8; 32];
                array.copy_from_slice(value);
                
                match VerifyingKey::from_bytes(&array) {
                    Ok(verifying_key) => Ok(PubKey(verifying_key)),
                    Err(_) => Err(E::custom("Invalid public key bytes")),
                }
            }

            // Handle sequence format (for default serde)
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
            // For human-readable formats (JSON), use deserialize_any to handle strings
            deserializer.deserialize_any(PubKeyVisitor)
        } else {
            // For binary formats (bincode), expect bytes
            deserializer.deserialize_bytes(PubKeyVisitor)
        }
    }
}

impl Deref for PubKey {
    type Target = VerifyingKey;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}


impl ToSql for PubKey {
    fn to_sql(&self) -> duckdb::Result<ToSqlOutput<'_>, duckdb::Error> {
        match bincode::serde::encode_to_vec(&self, bincode::config::standard()) {
            Ok(data) => Ok(ToSqlOutput::Owned(duckdb::types::Value::Blob(data))),
            Err(e) => Err(duckdb::Error::ToSqlConversionFailure(Box::new(e)))
        }
    }
}

impl FromSql for PubKey {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Blob(b) => {
                match bincode::serde::decode_from_slice(b, bincode::config::standard()) {
                    Ok((data, _)) => Ok(PubKey(data)),
                    Err(_) => Err(duckdb::types::FromSqlError::InvalidType)
                }
            }
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

impl IntoResponse for PubKey {
    fn into_response(self) -> Response {
        // Use the custom serializer which will output hex for JSON
        let json = serde_json::to_string(&self).unwrap();
        json.into_response()
    }
}

impl PubKey {
    /// Create a PubKey from a hex string (for parsing JSON responses)
    pub fn from_hex(hex_str: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let bytes = hex::decode(hex_str)?;
        
        if bytes.len() != 32 {
            return Err("Public key must be exactly 32 bytes".into());
        }
        
        let mut array = [0u8; 32];
        array.copy_from_slice(&bytes);
        let verifying_key = VerifyingKey::from_bytes(&array)?;
        Ok(PubKey(verifying_key))
    }
    
    /// Convert to hex string
    pub fn to_hex(&self) -> String {
        hex::encode(self.0.to_bytes())
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PrivKey(pub SigningKey);

impl Deref for PrivKey {
    type Target = SigningKey;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ToSql for PrivKey {
    fn to_sql(&self) -> duckdb::Result<ToSqlOutput<'_>, duckdb::Error> {
        match bincode::serde::encode_to_vec(&self, bincode::config::standard()) {
            Ok(data) => Ok(ToSqlOutput::Owned(duckdb::types::Value::Blob(data))),
            Err(e) => Err(duckdb::Error::ToSqlConversionFailure(Box::new(e)))
        }
    }
}

impl FromSql for PrivKey {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Blob(b) => {
                match bincode::serde::decode_from_slice(b, bincode::config::standard()) {
                    Ok((data, _)) => Ok(PrivKey(data)),
                    Err(_) => Err(duckdb::types::FromSqlError::InvalidType)
                }
            }
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Node {
    pub node_id: i32,
    pub name: String,
    pub ip_address: String,
    pub port: i32,
    pub owner: i32, // userid corresponding to owner
    pub pubkey: PubKey,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct User {
    pub user_id: i32,
    pub username: String,
    pub password: String,
    pub pubkey: PubKey,
    pub x25519_pubkey: crate::db::types::XPubKey
}

use argon2::{
    password_hash::{
        rand_core::OsRng,
        PasswordHash, PasswordHasher, SaltString
    },
    Argon2, PasswordVerifier
};

impl User {
    pub fn password_hash(&mut self) -> Result<String, argon2::password_hash::Error> {
        let salt = SaltString::generate(&mut OsRng);
        let argon2 = Argon2::default();
        let password_hash = argon2.hash_password(self.password.as_bytes(), &salt)?.to_string();
        Ok(password_hash)
    }
    pub fn verify_password(&mut self, check_password: &[u8]) -> Result<bool, argon2::password_hash::Error> {
        let parsed_hash = PasswordHash::new(&self.password)?;
        return Ok(Argon2::default().verify_password(check_password, &parsed_hash).is_ok());
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Sequence {
    pub name: String,
    pub next_id: i32,
}

/// Lightweight node connection information
/// Used for fragment discovery and network operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConnectionInfo {
    pub node_id: i32,
    pub ip_address: String,
    pub port: i32,
}
