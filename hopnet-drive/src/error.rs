//! Drive file-operation error taxonomy (RFC-015).
//!
//! Moved verbatim from the host's `files::functions` — the host re-exports
//! `FileError` at its old path so call sites don't churn.

use std::io;

#[derive(Debug)]
pub enum FileError {
    ShardingError,
    HashingError,
    HashMismatch,
    InvalidChunkCount,
    TaskJoinError,
    EncryptionError,
    StorageError(io::Error),
    DatabaseError,
    NetworkError,
    ReconstructionTimeout,
}

impl std::fmt::Display for FileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileError::ShardingError => write!(f, "Sharding error"),
            FileError::HashingError => write!(f, "Hashing error"),
            FileError::HashMismatch => write!(f, "Hash mismatch"),
            FileError::InvalidChunkCount => write!(f, "Invalid chunk count"),
            FileError::TaskJoinError => write!(f, "Task join error"),
            FileError::EncryptionError => write!(f, "Encryption error"),
            FileError::StorageError(e) => write!(f, "Storage error: {}", e),
            FileError::DatabaseError => write!(f, "Database error"),
            FileError::NetworkError => write!(f, "Network error"),
            FileError::ReconstructionTimeout => write!(f, "Reconstruction timeout"),
        }
    }
}

impl std::error::Error for FileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            FileError::StorageError(e) => Some(e),
            _ => None,
        }
    }
}

impl From<hopnet_storage::StorageError> for FileError {
    fn from(e: hopnet_storage::StorageError) -> Self {
        match e {
            hopnet_storage::StorageError::Encryption => FileError::EncryptionError,
            // Fragment-hash mismatch mapped to HashingError, matching the
            // pre-extraction behavior of fetch_and_verify_fragment.
            hopnet_storage::StorageError::HashMismatch => FileError::HashingError,
            hopnet_storage::StorageError::Io(io) | hopnet_storage::StorageError::Read(io) => {
                FileError::StorageError(io)
            }
            hopnet_storage::StorageError::Rs => FileError::ShardingError,
            // Host seam failures (engine-side DB/signing) never reach the
            // projection's put/get delegations; map defensively.
            hopnet_storage::StorageError::Host(msg) => {
                FileError::StorageError(std::io::Error::other(msg))
            }
            // Consensus-path callers classify Transient before it reaches
            // FileError; the HTTP upload paths that land here treat it as
            // any other storage failure (retried by upload_staged).
            hopnet_storage::StorageError::Transient(code) => FileError::StorageError(
                std::io::Error::other(format!("transient database contention: {code:?}")),
            ),
        }
    }
}

impl From<FileError> for rusqlite::Error {
    fn from(err: FileError) -> Self {
        rusqlite::Error::ToSqlConversionFailure(Box::new(err))
    }
}
