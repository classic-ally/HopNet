//! HopNet photos projection (RFC-011 Phase 1).
//!
//! Photos are a distinct domain — not files in a folder hierarchy — with
//! their own encrypted-metadata model, operation history, shared library
//! coordination, and lifecycle management. The consensus layer stores only
//! opaque encrypted blobs (zero-trust metadata: photos.md:7,30-32); all
//! queryable metadata lives in a client-side sidecar, populated by
//! decrypting consensus-tracked encrypted blobs.
//!
//! Phase 1 (this crate, schema-only): the consensus-tracked tables +
//! `DataBlockReferenceProvider` for GC integration. Handlers, routes,
//! and crypto come in later phases.

pub mod db;
pub mod reference_provider;

/// The photos projection's static manifest (RFC-016 Stage 3) — the host
/// registers this one value in `projections::manifests()`.
pub struct PhotosProjection;

impl hopnet_projection::Projection for PhotosProjection {
    fn name(&self) -> &'static str {
        "photos"
    }

    fn tx_functions(&self) -> &'static [&'static str] {
        // Phase 2: photo_add, photo_delete, photo_edit_content,
        // photo_edit_metadata, photo_restore, photo_undo,
        // create_shared_library, join_shared_library, leave_shared_library,
        // album_create, album_add_photo, album_remove_photo.
        // Empty for Phase 1 — the boot tripwire accepts an empty slice
        // (src/lib.rs::assert_projection_registrations loops zero times).
        &[]
    }

    fn install_schema(&self, conn: &rusqlite::Connection) -> Result<(), rusqlite::Error> {
        db::install_schema(conn)
    }

    fn tables(&self) -> &'static [&'static str] {
        db::TABLES
    }
}
