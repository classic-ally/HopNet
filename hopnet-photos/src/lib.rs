//! HopNet photos projection (RFC-011 Phase 1).
//!
//! Photos are a distinct domain — not files in a folder hierarchy — with
//! their own encrypted-metadata model, operation history, shared library
//! coordination, and lifecycle management. The consensus layer stores only
//! opaque encrypted blobs (zero-trust metadata: photos.md:7,30-32); all
//! queryable metadata lives in a client-side sidecar, populated by
//! decrypting consensus-tracked encrypted blobs.
//!
//! Phase 1: consensus-tracked tables + `DataBlockReferenceProvider` for GC
//! integration + `photo_add`/`photo_delete`/`photo_restore` consensus
//! transaction handlers + fragment distribution hook. Routes, HTTP surface,
//! and crypto come in later phases.

pub mod db;
pub mod envelopes;
pub mod handlers;
pub mod reference_provider;

use hopnet_projection::Projection;

/// The photos projection's static manifest (RFC-016 Stage 3) — the host
/// registers this one value in `projections::manifests()`.
pub struct PhotosProjection;

impl Projection for PhotosProjection {
    fn name(&self) -> &'static str {
        "photos"
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

    /// Blob ids committed by this projection's transactions — the host
    /// feeds them to the storage engine's distribution kick. Without this
    /// override, photo fragments would never replicate beyond the upload
    /// node. Pure decode (no DB, no IO — runs on the consensus shell
    /// thread post-decide).
    fn committed_blob_ids(
        &self,
        function: &str,
        payload: &[u8],
    ) -> Vec<hopnet_storage::BlobId> {
        match function {
            "photo_add" => {
                bincode::serde::decode_from_slice::<envelopes::PhotoAddPayload, _>(
                    payload,
                    bincode::config::standard(),
                )
                .map(|(p, _)| {
                    p.entries
                        .iter()
                        .flat_map(|e| &e.resources)
                        .map(|r| r.op.blob_id.clone())
                        .collect()
                })
                .unwrap_or_default()
            }
            _ => Vec::new(),
        }
    }
}
