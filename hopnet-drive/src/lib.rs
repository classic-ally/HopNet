//! HopNet fs projection (RFC-015).
//!
//! The drive: inodes with deterministically-encrypted paths, shares, and
//! the FileProvider/DocumentProvider surfaces — a PROJECTION over the
//! storage substrate (hopnet-storage blobs) and the consensus state
//! machine. Extraction proceeds in stages (D1–D5); this crate currently
//! owns its schema unit, model/wire types, path crypto, DB surface
//! (Stage D2b), its consensus transaction handlers + GC reference
//! provider (Stage D3), and its HTTP/business surface — routers, upload
//! and download flows — behind the `host` seams (Stage D4).

pub mod db;
pub mod download;
pub mod exporter;
pub mod host;
pub mod http;
pub mod envelopes;
pub mod error;
pub mod handlers;
pub mod model;
pub mod paths;
pub mod reference_provider;
pub mod upload;

pub use error::FileError;
pub use model::{Inode, InodeOwner};

/// The drive's static manifest (RFC-016 Stage 3) — the host registers
/// this one value in `projections::manifests()`.
pub struct DriveProjection;

impl hopnet_projection::Projection for DriveProjection {
    fn name(&self) -> &'static str {
        "drive"
    }

    fn tx_functions(&self) -> &'static [&'static str] {
        handlers::TX_FUNCTIONS
    }

    fn install_schema(&self, conn: &rusqlite::Connection) -> Result<(), rusqlite::Error> {
        db::install_schema(conn)
    }

    fn exporter(
        &self,
        caps: &hopnet_projection::host::HostCapabilities,
    ) -> Option<std::sync::Arc<dyn hopnet_projection::ProjectionExporter>> {
        Some(exporter::drive_exporter(caps.clone()))
    }
}
