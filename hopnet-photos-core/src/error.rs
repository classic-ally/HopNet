use thiserror::Error;

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
}
