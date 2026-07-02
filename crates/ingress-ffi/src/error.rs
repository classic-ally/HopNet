//! FFI error surface. Flat (message-carrying) variants: Swift classifies and
//! displays in Phase 2; structured payloads can be added later without
//! breaking the enum.

use ingress_core::IngressError;

#[derive(Debug, thiserror::Error, uniffi::Error)]
#[uniffi(flat_error)]
pub enum FfiError {
    #[error("database: {msg}")]
    Database { msg: String },
    #[error("io: {msg}")]
    Io { msg: String },
    #[error("unmapped scope: {msg}")]
    UnmappedScope { msg: String },
    #[error("cloud_id conflict: {msg}")]
    CloudIdConflict { msg: String },
    #[error("invariant: {msg}")]
    Invariant { msg: String },
    #[error("invalid descriptor: {msg}")]
    InvalidDescriptor { msg: String },
    #[error("sink misuse: {msg}")]
    SinkState { msg: String },
}

impl From<IngressError> for FfiError {
    fn from(e: IngressError) -> Self {
        match e {
            IngressError::Db(inner) => FfiError::Database { msg: inner.to_string() },
            IngressError::Migrate(inner) => FfiError::Database { msg: inner.to_string() },
            IngressError::Json(inner) => FfiError::Invariant { msg: inner.to_string() },
            IngressError::UnsupportedSidecarSchema(s) => {
                FfiError::Invariant { msg: format!("unsupported sidecar schema {s}") }
            }
            IngressError::UnknownResourceType(t) => {
                FfiError::InvalidDescriptor { msg: format!("unknown PHAssetResourceType {t}") }
            }
            IngressError::UnmappedScope(scope) => {
                FfiError::UnmappedScope { msg: format!("{scope:?}") }
            }
            IngressError::CloudIdConflict(id) => FfiError::CloudIdConflict { msg: id },
            IngressError::Invariant(msg) => FfiError::Invariant { msg },
        }
    }
}
