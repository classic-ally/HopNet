use axum::response::{IntoResponse, Response};
use bincode::{Decode, Encode};
use ed25519_dalek::VerifyingKey;
pub use ed25519_dalek::{Signer, SigningKey};
use hex;
use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::ops::Deref;

pub use hopnet_common::Blake3Hash;

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
/// 2. Fetches the trusted genesis (height 0) from a bootstrap validator,
///    installs it, and starts the consensus engine
/// 3. Decided-value-syncs to the mesh tip, then submits an activation request
/// 4. User logs in via web UI to unwrap their key from consensus-replicated encrypted_privkey
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinInfo {
    pub node_id: i32,
    pub user_id: i32,
    pub bootstrap_validators: Vec<Node>, // Full node info for catch-up
    /// The mesh's genesis-fixed quorum profile (`QuorumProfile::as_str`).
    pub quorum_profile: String,
    /// The epoch the joiner is entering (RFC-019 S7). Epoch 1 takes the
    /// trusted height-0 bootstrap; anything later takes the epoch-join
    /// path, which fetches and verifies the lineage chain and imports the
    /// boundary snapshot.
    pub epoch: u64,
    /// The FULL anchor (epoch-1) chain id (RFC-025 S5). `anchor[..4]` IS
    /// the mesh magic / join code; the joiner pre-flights it against the
    /// operator-entered code before writing anything, and the
    /// install-time check verifies the FETCHED state against the code
    /// independently (a lying coordinator aborts there). Appended last —
    /// legal on positional bincode because the setup scope is
    /// locked-class: both ends run the same release by ALPN
    /// construction.
    pub anchor: [u8; 32],
}
