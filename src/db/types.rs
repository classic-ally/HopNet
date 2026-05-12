#[derive(Debug)]
pub enum DatabaseError {
    LockError,
    InsertError,
    RecordError,
    RecallError,
    ProcessingError,
    InvalidPayload,
    NotFound,
    ConflictError,      // Resource already exists at the specified location/identifier
    AuthorizationError, // User or node not authorized for the operation
    ValidationError, // Data validation failed (e.g., cryptographic verification, consistency checks)
}

use crate::db::{Blake3Hash, PrivKey, User};
use std::ops::Deref;
pub struct MyNode {
    pub node_id: i32,
    pub privkey: PrivKey,
}
use chacha20poly1305::{
    ChaCha20Poly1305,
    aead::{Aead, KeyInit, OsRng},
};
use chrono::{DateTime, Utc};
use either::Either;
pub use hopnet_common::CustomUUID;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef};
use serde::{Deserialize, Serialize};
use x25519_dalek::{EphemeralSecret, PublicKey as X25519PublicKey};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CustomDateTime(DateTime<Utc>);

impl CustomDateTime {
    pub fn new(dt: DateTime<Utc>) -> Self {
        CustomDateTime(dt)
    }
}

impl Deref for CustomDateTime {
    type Target = DateTime<Utc>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ToSql for CustomDateTime {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self.to_rfc3339()))
    }
}

impl FromSql for CustomDateTime {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Integer(millis) => {
                // uuid_extract_timestamp returns epoch milliseconds
                DateTime::from_timestamp_millis(millis)
                    .map(CustomDateTime)
                    .ok_or(FromSqlError::InvalidType)
            }
            ValueRef::Text(str) => match std::str::from_utf8(str) {
                Ok(utf_value) => match DateTime::parse_from_rfc3339(utf_value) {
                    Ok(dt) => Ok(CustomDateTime(dt.with_timezone(&Utc))),
                    Err(_) => Err(FromSqlError::InvalidType),
                },
                Err(_) => Err(FromSqlError::InvalidType),
            },
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum ChunkType {
    Original,
    Recovery,
}

impl ToSql for ChunkType {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        let v: i32 = match self {
            ChunkType::Original => 0,
            ChunkType::Recovery => 1,
        };
        Ok(ToSqlOutput::from(v))
    }
}

impl FromSql for ChunkType {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Integer(i) => match i as i32 {
                0 => Ok(ChunkType::Original),
                1 => Ok(ChunkType::Recovery),
                _ => Err(FromSqlError::InvalidType),
            },
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Inode {
    // stable identifier (UUIDv7 encodes creation time)
    pub id: CustomUUID,
    // the owner for this specific node:
    pub owner: Either<i32, User>,
    // path is split by /
    // each segment encrypted with AES-SIV with the owner's key
    // this way, we can compute all files in a folder quickly whilst maintaining OK privacy
    pub path: String,
    // it is either a folder or file
    pub inode_type: hopnet_common::InodeType,
    // if file, point to datablock
    // if folder, None
    pub data_id: Option<Either<CustomUUID, DataRecord>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DataRecord {
    // PK for this datablock
    // referenced by inoderecord
    // distinct from hash to allow file update without needing to update inode
    // also encodes creation time due to uuidv7
    pub id: CustomUUID,
    pub modified_at: Option<CustomDateTime>,
    pub data: Data,
    pub file_access_entries: Option<Vec<FileAccess>>,
    pub file_size: u64, // Total size of the file in bytes
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Data {
    // data hash for integrity
    pub hash: Blake3Hash,
    // list of fragment hashes with metadata
    pub fragments: Vec<FragmentHash>,
    pub added_bytes: u8,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct XPubKey(X25519PublicKey);

impl XPubKey {
    pub fn from_x25519(pubkey: X25519PublicKey) -> Self {
        XPubKey(pubkey)
    }

    pub fn as_x25519(&self) -> &X25519PublicKey {
        &self.0
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        self.0.as_bytes()
    }
}

impl From<X25519PublicKey> for XPubKey {
    fn from(pubkey: X25519PublicKey) -> Self {
        XPubKey(pubkey)
    }
}

impl From<[u8; 32]> for XPubKey {
    fn from(bytes: [u8; 32]) -> Self {
        XPubKey(X25519PublicKey::from(bytes))
    }
}

impl ToSql for XPubKey {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self.as_bytes().to_vec()))
    }
}

impl FromSql for XPubKey {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Blob(blob) => {
                if blob.len() != 32 {
                    return Err(FromSqlError::InvalidType);
                }
                let mut bytes = [0u8; 32];
                bytes.copy_from_slice(blob);
                Ok(XPubKey::from(bytes))
            }
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileAccess {
    pub data_block_id: CustomUUID,
    pub user_id: i32,
    pub ephemeral_pubkey: XPubKey,
    pub encrypted_file_key: Vec<u8>, // 48 bytes (32 key + 16 auth tag)
}

impl FileAccess {
    /// Create a new FileAccess entry by wrapping the per-file key for the specified user
    pub fn new_for_user_with_conn(
        conn: &rusqlite::Connection,
        data_block_id: CustomUUID,
        user_id: i32,
        per_file_key: &chacha20poly1305::Key,
    ) -> Result<Self, DatabaseError> {
        // Look up user from database to get their X25519 public key
        let user = match crate::db::users::get_user_by_userid_conn(conn, user_id) {
            Ok(Some(user)) => user,
            Ok(None) => return Err(DatabaseError::RecallError), // User not found
            Err(e) => return Err(e),
        };

        // Generate ephemeral key pair for this file
        let ephemeral_secret = EphemeralSecret::random_from_rng(&mut OsRng);
        let ephemeral_public = X25519PublicKey::from(&ephemeral_secret);

        // Perform ECDH with user's X25519 public key
        let shared_secret = ephemeral_secret.diffie_hellman(user.x25519_pubkey.as_x25519());

        // Derive ChaCha20Poly1305 key from shared secret using Blake3
        let mut wrap_key_bytes = [0u8; 32];
        let mut hasher = blake3::Hasher::new_derive_key("hopnet key_wrap");
        hasher.update(shared_secret.as_bytes());
        let mut xof = hasher.finalize_xof();
        xof.fill(&mut wrap_key_bytes);
        let wrap_key = chacha20poly1305::Key::from(wrap_key_bytes);

        // Derive deterministic nonce from data_block_id + user_id + ephemeral_pubkey
        let mut nonce_bytes = [0u8; 12];
        let mut nonce_hasher = blake3::Hasher::new_derive_key("hopnet wrap_nonce");
        nonce_hasher.update(data_block_id.as_bytes());
        nonce_hasher.update(&user_id.to_le_bytes());
        nonce_hasher.update(ephemeral_public.as_bytes());
        nonce_hasher.finalize_xof().fill(&mut nonce_bytes);
        let wrap_nonce = chacha20poly1305::Nonce::from(nonce_bytes);

        // Encrypt the per-file key
        let wrap_cipher = ChaCha20Poly1305::new(&wrap_key);
        let encrypted_file_key = wrap_cipher
            .encrypt(&wrap_nonce, per_file_key.as_slice())
            .map_err(|_| DatabaseError::ProcessingError)?;

        Ok(FileAccess {
            data_block_id,
            user_id,
            ephemeral_pubkey: XPubKey::from(ephemeral_public),
            encrypted_file_key,
        })
    }

    pub fn new_for_user(
        db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
        data_block_id: CustomUUID,
        user_id: i32,
        per_file_key: &chacha20poly1305::Key,
    ) -> Result<Self, DatabaseError> {
        match db_connection {
            Ok(db_lock) => {
                Self::new_for_user_with_conn(&db_lock, data_block_id, user_id, per_file_key)
            }
            Err(_) => Err(DatabaseError::LockError),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct FragmentHash {
    pub data_block_id: CustomUUID,
    pub chunk_number: u32,
    pub local_index: u32,
    pub fragment_id: CustomUUID, // UUID v7 for chunk identification and nonce derivation
    pub fragment_hash: Blake3Hash, // Hash of encrypted chunk for storage verification
    pub chunk_type: ChunkType,
    pub stored_locally: bool,
}
