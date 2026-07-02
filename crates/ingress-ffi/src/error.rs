//! FFI error surface.
//!
//! NOT `flat_error`: the fetch foreign trait requires Swift-thrown errors to
//! lift back into Rust with their payloads intact, and uniffi 0.31's flat
//! errors cannot cross Swift → Rust (the generated lift panics). Field-
//! carrying variants work in both directions.

use ingress_core::IngressError;

#[derive(Debug, thiserror::Error, uniffi::Error)]
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

    // Fetch classification (Swift classifies PhotoKit NSErrors into these;
    // the scheduler maps them to dispositions — see spec §Failure Handling).
    /// `CloudPhotoLibraryErrorDomain` code 1005: local disk pressure.
    /// Daemon-wide pause, never a per-resource failure.
    #[error("local disk pressure (1005)")]
    LocalDiskPressure,
    /// Cancellation (SIGTERM / user): rows untouched, no retry consumed.
    #[error("cancelled")]
    Cancelled,
    /// The PHAsset disappeared between seed and drain.
    #[error("asset unavailable: {msg}")]
    AssetUnavailable { msg: String },
    /// Everything else from PhotoKit: retry with backoff.
    #[error("fetch failed: {msg}")]
    FetchTransient { msg: String },
}

impl From<uniffi::UnexpectedUniFFICallbackError> for FfiError {
    fn from(e: uniffi::UnexpectedUniFFICallbackError) -> Self {
        FfiError::FetchTransient { msg: format!("unexpected callback error: {e}") }
    }
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
