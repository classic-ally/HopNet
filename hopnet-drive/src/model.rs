//! Drive model types (RFC-015).

use chrono::{DateTime, Utc};
use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef};
use serde::{Deserialize, Serialize};
use std::ops::Deref;

pub use hopnet_common::CustomUUID;

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

/// File metadata and access control from the database: the substrate's
/// reassembly manifest plus the caller's wrap row. `manifest` is None for
/// empty files (data_id NULL — no fragments, no encryption).
pub struct FileAccessData {
    pub manifest: Option<hopnet_storage::store::BlobManifest>,
    pub file_access_entry: Option<hopnet_storage::BlobAccess>,
    pub file_size: u64,
}

/// Inode owner — a user id.
///
/// WIRE COMPATIBILITY: the legacy field was `Either<i32, User>` and every
/// producer encoded `Left(user_id)`; handlers rejected `Right`. A single
/// tag-0 variant reproduces `Left`'s bincode bytes exactly (one varint
/// discriminant 0, then the id), and a legacy `Right` payload (tag 1) now
/// fails DECODE instead of failing authorization — same rejection, earlier.
/// Golden-byte test pins this in the main crate's files::tests.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum InodeOwner {
    Id(i32),
}

impl InodeOwner {
    pub fn id(&self) -> i32 {
        let InodeOwner::Id(id) = self;
        *id
    }
}

impl From<i32> for InodeOwner {
    fn from(id: i32) -> Self {
        InodeOwner::Id(id)
    }
}

/// A drive inode: one user's reference to a path (file or folder).
#[derive(Serialize, Deserialize, Debug)]
pub struct Inode {
    // stable identifier (UUIDv7 encodes creation time)
    pub id: CustomUUID,
    // the owner for this specific node
    pub owner: InodeOwner,
    // path is split by /
    // each segment encrypted with AES-SIV with the owner's key
    // this way, we can compute all files in a folder quickly whilst maintaining OK privacy
    pub path: String,
    // it is either a folder or file
    pub inode_type: hopnet_common::InodeType,
    // if file, point to datablock
    // if folder, None
    /// Blob reference (RFC-014): inodes reference blobs by id only. Blob
    /// registration rides the same transaction as its own sub-payload
    /// (DriveInsertPayload.blob_ops) — never embedded in the inode.
    pub data_id: Option<CustomUUID>,
}
