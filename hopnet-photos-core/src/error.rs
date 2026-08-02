use thiserror::Error;

use crate::asset::ResourceKind;

#[derive(Debug, Error)]
pub enum PublishValidationError {
    #[error("missing byte source for declared resource {0}")]
    MissingByteSource(ResourceKind),

    #[error("duplicate byte source for {0}")]
    DuplicateByteSource(ResourceKind),

    #[error("byte source {0} not declared on asset")]
    UnexpectedByteSource(ResourceKind),

    #[error("resource {kind} stream ended at {actual} of {expected} bytes")]
    ResourceTooShort {
        kind: ResourceKind,
        expected: u64,
        actual: u64,
    },

    #[error("resource {kind} stream exceeds declared {expected} bytes")]
    ResourceTooLong { kind: ResourceKind, expected: u64 },

    #[error("resource {kind} byte_len {byte_len} exceeds platform limits")]
    ResourceTooLarge { kind: ResourceKind, byte_len: u64 },

    #[error("no recipients for publish")]
    NoRecipients,

    #[error("duplicate recipient {0}")]
    DuplicateRecipient(i32),

    #[error("personal publish must target exactly the uploader {uploaded_by}, got {member_ids:?}")]
    InvalidPersonalRecipients {
        uploaded_by: i32,
        member_ids: Vec<i32>,
    },

    #[error("uploader {uploaded_by} is not a member of the target library")]
    UploaderNotMember { uploaded_by: i32 },

    #[error("edit changes nothing: no resources, no removals, no metadata")]
    EmptyEdit,

    #[error("duplicate removal of {0}")]
    DuplicateRemoval(ResourceKind),

    #[error("resource {0} is both edited and removed")]
    EditedAndRemoved(ResourceKind),
}

#[derive(Debug, Error)]
pub enum PhotosCoreError {
    #[error("storage substrate error: {0}")]
    Storage(#[from] hopnet_storage::StorageError),

    #[error("metadata json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("payload bincode encode error: {0}")]
    BincodeEncode(#[from] bincode::error::EncodeError),

    #[error("encryption/decryption failure")]
    Encryption,

    #[cfg(feature = "sidecar")]
    #[error("sidecar db error: {0}")]
    Db(#[from] rusqlite::Error),

    #[error("dispatch error: {0}")]
    Dispatch(String),

    #[error("invalid photo asset: {0}")]
    InvalidAsset(#[from] crate::asset::AssetValidationError),

    #[error("invalid publish request: {0}")]
    InvalidPublishRequest(#[from] PublishValidationError),

    /// One or more blobs were uploaded before the publish failed. The blob IDs
    /// are reconciliation candidates, not known orphans — see the idempotency
    /// contract in `crate::publisher`.
    #[error("partial publish of photo {photo_id}: {source}")]
    PartialPublish {
        photo_id: hopnet_common::CustomUUID,
        uploaded_blob_ids: Vec<hopnet_storage::BlobId>,
        #[source]
        source: Box<PhotosCoreError>,
    },
}
