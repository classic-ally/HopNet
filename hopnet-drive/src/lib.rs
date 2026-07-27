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

    fn tables(&self) -> &'static [&'static str] {
        db::TABLES
    }

    fn exporter(
        &self,
        caps: &hopnet_projection::host::HostCapabilities,
    ) -> Option<std::sync::Arc<dyn hopnet_projection::ProjectionExporter>> {
        Some(exporter::drive_exporter(caps.clone()))
    }

    fn committed_blob_ids(
        &self,
        function: &str,
        payload: &[u8],
    ) -> Vec<hopnet_storage::BlobId> {
        // Pure decode of the drive's OWN envelopes for the host's
        // distribution kick (consensus shell thread — no DB, no IO).
        // Decode failures yield empty, matching the pre-hook behavior.
        match function {
            "insert_files" => bincode::serde::decode_from_slice::<envelopes::DriveInsertPayload, _>(
                payload,
                bincode::config::standard(),
            )
            .map(|(p, _)| p.blob_ops.into_iter().map(|op| op.blob_id).collect())
            .unwrap_or_default(),
            "modify_item" => bincode::serde::decode_from_slice::<envelopes::ModifyItemPayload, _>(
                payload,
                bincode::config::standard(),
            )
            .map(|(p, _)| {
                p.content_update
                    .and_then(|u| u.blob_op)
                    .map(|op| op.blob_id)
                    .into_iter()
                    .collect()
            })
            .unwrap_or_default(),
            _ => Vec::new(),
        }
    }

    fn user_data_size_bytes(
        &self,
        caps: &hopnet_projection::host::HostCapabilities,
        user_id: i32,
    ) -> hopnet_projection::host::BoxFuture<'static, Result<u64, String>> {
        let pool = caps.db_pool.clone();
        Box::pin(async move {
            let conn = pool.get().map_err(|e| format!("db pool: {e}"))?;
            db::files::user_data_size(&conn, user_id)
                .map_err(|e| format!("drive user data size: {e:?}"))
        })
    }

    fn mounts(
        &self,
        caps: &hopnet_projection::host::HostCapabilities,
    ) -> Vec<hopnet_projection::Mount> {
        use hopnet_projection::{AuthClass, Mount};
        vec![
            Mount {
                prefix: "/files",
                auth: AuthClass::UserJwt,
                router: http::files::router(caps.clone()),
            },
            Mount {
                prefix: "/shares",
                auth: AuthClass::UserJwt,
                router: http::shares::router(caps.clone()),
            },
            Mount {
                prefix: "/integrations/fileprovider",
                auth: AuthClass::DeviceToken,
                router: http::fileprovider::router(caps.clone()),
            },
            Mount {
                prefix: "/integrations/documentprovider",
                auth: AuthClass::DeviceToken,
                router: http::documentprovider::router(caps.clone()),
            },
        ]
    }
}
