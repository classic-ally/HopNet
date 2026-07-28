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
//! integration + `photo_add`/`photo_delete`/`photo_restore`/`photo_cleanup`
//! consensus transaction handlers + fragment distribution hook.
//!
//! The `projection` feature (default-on) gates the server half — handlers,
//! DB, GC, cron, and the `Projection` trait impl. Without it (photos-core
//! builds), only the envelope wire types and `METADATA_KEY_WRAP_DOMAIN`
//! are exported.

pub mod envelopes;

/// Domain-separation constant for per-photo metadata key wrapping.
/// New context strings give clean separation from the substrate's blob-key
/// domain (BLOB_WRAP_DOMAIN), preventing cross-domain key transplantation.
pub const METADATA_KEY_WRAP_DOMAIN: hopnet_storage::WrapDomain = hopnet_storage::WrapDomain {
    key_context: "hopnet-photos metadata_key v1",
    nonce_context: "hopnet-photos metadata_nonce v1",
};

#[cfg(feature = "projection")]
pub mod db;
#[cfg(feature = "projection")]
pub mod handlers;
#[cfg(feature = "projection")]
pub mod jobs;
#[cfg(feature = "projection")]
pub mod reference_provider;

#[cfg(feature = "projection")]
use hopnet_projection::Projection;

#[cfg(feature = "projection")]
/// The photos projection's static manifest (RFC-016 Stage 3) — the host
/// registers this one value in `projections::manifests()`.
pub struct PhotosProjection;

#[cfg(feature = "projection")]
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
                    payload, bincode::config::standard(),
                )
                .map(|(p, _)| {
                    p.entries.iter().flat_map(|e| &e.resources).map(|r| r.op.blob_id.clone()).collect()
                })
                .unwrap_or_default()
            }
            "photo_edit_content" => {
                bincode::serde::decode_from_slice::<envelopes::PhotoEditContentPayload, _>(
                    payload, bincode::config::standard(),
                )
                .map(|(p, _)| {
                    p.entries.iter()
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
