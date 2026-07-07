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

use std::sync::Arc;

/// Generic host capabilities moved down to hopnet-projection (sessions +
/// tx submission at Stage D5a; blob streaming + write admission at
/// RFC-016 Stage 1) so projection-agnostic services and future
/// projections can consume them; re-exported here so drive call sites
/// are unchanged.
pub use hopnet_projection::host::{
    BlobStreamer, BoxFuture, ByteStream, SessionAccess, SessionError, TxGateway, TxSigner,
    TxSpec, TxSubmitError, UserSession, WriteAdmission, WriteCheckError, WriteDenied,
};

/// The drive's axum state: concrete DB access (the drive owns its SQL)
/// plus the five host seams.
#[derive(Clone)]
pub struct DriveState {
    pub db_pool: r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
    pub fragments_dir: String,
    pub test_mode: bool,
    pub node_id: Arc<once_cell::sync::OnceCell<i32>>,
    pub sessions: Arc<dyn SessionAccess>,
    pub txs: Arc<dyn TxGateway>,
    pub blobs: Arc<dyn BlobStreamer>,
    pub notify: Arc<dyn hopnet_projection::ChangeNotifier>,
    pub write_admission: Arc<dyn WriteAdmission>,
}

impl DriveState {
    pub fn node_id(&self) -> Option<i32> {
        self.node_id.get().copied()
    }
}
