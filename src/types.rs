use axum::response::{IntoResponse, Response};
use bincode::{Decode, Encode};
use ed25519_dalek::VerifyingKey;
pub use ed25519_dalek::{Signer, SigningKey};
use hex;
use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::ops::Deref;

/// A wrapper around blake3::Hash that implements bincode's Encode and Decode traits
#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub struct Blake3Hash(pub blake3::Hash);

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
            return Err(bincode::error::DecodeError::Other(
                "Blake3 hash must be exactly 32 bytes",
            ));
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
            return Err(bincode::error::DecodeError::Other(
                "Blake3 hash must be exactly 32 bytes",
            ));
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
                    return Err(FromSqlError::Other(
                        format!(
                            "Blake3Hash must be exactly 32 bytes, got {} bytes",
                            bytes.len()
                        )
                        .into(),
                    ));
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
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
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
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        match bincode::serde::encode_to_vec(&self, bincode::config::standard()) {
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

    /// Convert to iroh NodeId (PublicKey)
    /// Both use the same 32-byte Ed25519 key format
    pub fn to_iroh_node_id(&self) -> iroh::PublicKey {
        iroh::PublicKey::from_bytes(&self.0.to_bytes()).expect("valid ed25519 key")
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PrivKey(pub SigningKey);

impl PrivKey {
    /// Convert to iroh SecretKey
    /// Both use the same 32-byte Ed25519 key format
    pub fn to_iroh_secret_key(&self) -> iroh::SecretKey {
        iroh::SecretKey::from_bytes(&self.0.to_bytes())
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
        match bincode::serde::encode_to_vec(&self, bincode::config::standard()) {
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Node {
    pub node_id: i32,
    pub name: String,
    pub owner: i32, // userid corresponding to owner
    pub pubkey: PubKey,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct User {
    pub user_id: i32,
    pub username: String,
    pub pubkey: PubKey,
    pub x25519_pubkey: crate::db::types::XPubKey,
    pub encrypted_privkey: Vec<u8>, // nonce || ChaCha20-Poly1305 ciphertext
    pub key_salt: Vec<u8>,          // Argon2 salt
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub avatar: Option<Vec<u8>>,
    pub onboarding_flags: hopnet_common::OnboardingFlags,
}

impl User {
    pub fn new(
        user_id: i32,
        username: String,
        pubkey: PubKey,
        x25519_pubkey: crate::db::types::XPubKey,
        encrypted_privkey: Vec<u8>,
        key_salt: Vec<u8>,
    ) -> User {
        User {
            user_id,
            username,
            pubkey,
            x25519_pubkey,
            encrypted_privkey,
            key_salt,
            first_name: None,
            last_name: None,
            avatar: None,
            onboarding_flags: hopnet_common::OnboardingFlags::NONE,
        }
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
    pub pubkey: PubKey,
}

/// Bootstrap information sent from coordinator to joining node via iroh.
///
/// After receiving this, the joining node:
/// 1. Initializes this_node table with node_id
/// 2. Performs catch-up from view 0 using bootstrap_validators
/// 3. Submits activation request after catching up
/// 4. User logs in via web UI to unwrap their key from consensus-replicated encrypted_privkey
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinInfo {
    pub node_id: i32,
    pub user_id: i32,
    pub bootstrap_validators: Vec<Node>, // Full node info for catch-up
}
