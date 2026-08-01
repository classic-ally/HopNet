/// Database-layer error taxonomy — owned by the projection seam crate
/// (RFC-015) so projection handlers and DB code share it across crates.
pub use hopnet_projection::DatabaseError;

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

/// Drive-owned (RFC-015): CustomDateTime lives in hopnet-drive's model;
/// re-exported at the old path so call sites don't churn.
pub use hopnet_drive::model::CustomDateTime;

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

/// Drive-owned (RFC-015): the inode model lives in hopnet-drive; owner is
/// a wire-compatible single-variant enum (see hopnet_drive::model).
pub use hopnet_drive::{Inode, InodeOwner};

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

/// Access rows are substrate-owned (RFC-014): pubkey-keyed wraps of the
/// per-blob key. The projection maps users to pubkeys at wrap time; nothing
/// in the substrate knows user ids.
pub use hopnet_storage::BlobAccess;

/// Wrap the per-blob key to a user's X25519 pubkey — resolves the user's
/// pubkey from the DB, then delegates to the substrate's v1 wrap.
pub fn blob_access_for_user_with_conn(
    conn: &rusqlite::Connection,
    blob_id: CustomUUID,
    user_id: i32,
    per_blob_key: &chacha20poly1305::Key,
) -> Result<BlobAccess, DatabaseError> {
    let user = match crate::db::users::get_user_by_userid_conn(conn, user_id) {
        Ok(Some(user)) => user,
        Ok(None) => return Err(DatabaseError::RecallError), // User not found
        Err(e) => return Err(e),
    };
    hopnet_storage::crypto::wrap_blob_key(&blob_id, user.x25519_pubkey.as_x25519(), per_blob_key)
        .map_err(|_| DatabaseError::ProcessingError)
}

pub fn blob_access_for_user(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    blob_id: CustomUUID,
    user_id: i32,
    per_blob_key: &chacha20poly1305::Key,
) -> Result<BlobAccess, DatabaseError> {
    match db_connection {
        Ok(db_lock) => blob_access_for_user_with_conn(&db_lock, blob_id, user_id, per_blob_key),
        Err(_) => Err(DatabaseError::LockError),
    }
}
