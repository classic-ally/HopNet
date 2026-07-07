//! Host seams for the drive's HTTP/business surface (RFC-015).
//!
//! The drive's routers and upload/download flows reach the host through
//! five capabilities: per-user session keys (`SessionAccess`), consensus
//! transaction submission with host-side signing (`TxGateway`), blob
//! reconstruction streaming (`BlobStreamer` — type-erases the storage
//! crate's generic `api::get`), post-apply change signaling (re-used
//! `ChangeNotifier` from hopnet-projection), and import write gating
//! (`WriteAdmission`). One host adapter implements them all (pattern:
//! the main crate's SubstrateHost for the RFC-014 seams).
//!
//! dyn-object style (boxed futures) deliberately: these hang off an axum
//! state struct and cross one box per REQUEST, never per byte — the
//! download body itself is already a boxed stream.

/// Generic host capabilities moved down to hopnet-projection (sessions +
/// tx submission at Stage D5a; blob streaming + write admission at
/// RFC-016 Stage 1; the full capability bundle at Stage 2) so
/// projection-agnostic services and future projections can consume them;
/// re-exported here so drive call sites are unchanged.
pub use hopnet_projection::host::{
    BlobStreamer, BoxFuture, ByteStream, HostCapabilities, SessionAccess, SessionError,
    TxGateway, TxSigner, TxSpec, TxSubmitError, UserSession, WriteAdmission, WriteCheckError,
    WriteDenied,
};

/// The drive's axum state IS the host capability bundle (RFC-016 Stage 2)
/// — the drive owns its SQL through `db_pool` and reaches the host
/// through the seam fields. The alias keeps the drive's vocabulary.
pub type DriveState = HostCapabilities;
