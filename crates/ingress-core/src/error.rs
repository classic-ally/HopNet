use crate::descriptor::LibraryScope;

/// Crate-wide result alias.
pub type Result<T, E = IngressError> = std::result::Result<T, E>;

/// Errors surfaced by the ingress core.
///
/// Variants carry only owned simple data so a future UniFFI export is
/// mechanical.
#[derive(Debug, thiserror::Error)]
pub enum IngressError {
    #[error("database: {0}")]
    Db(#[from] sqlx::Error),

    #[error("migration: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),

    #[error("sidecar json: {0}")]
    Json(#[from] serde_json::Error),

    #[error("unsupported sidecar schema {0:?}")]
    UnsupportedSidecarSchema(String),

    #[error("unknown PHAssetResourceType {0}")]
    UnknownResourceType(i32),

    #[error("no library bound for scope {0:?}")]
    UnmappedScope(LibraryScope),

    #[error("cloud_id uniqueness violated: {0}")]
    CloudIdConflict(String),

    #[error("invariant violated: {0}")]
    Invariant(String),

    /// A blob root's filesystem is gone (network mount dropped). A pause
    /// class, not a failure class: the scheduler polls for the mount's
    /// return instead of charging retries to individual resources.
    #[error("storage unavailable: {0}")]
    StorageUnavailable(String),
}
