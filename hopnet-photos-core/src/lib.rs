//! HopNet photos core (RFC-011 Track B).
//!
//! Pure, sync, fully testable photos primitives: metadata-key + blob-key
//! crypto, the encrypted PhotoMetadata JSON shape, payload builders over
//! the photos projection's frozen envelope wire types, the PhotoDispatch
//! trait, the source-independent asset model, and the local sidecar SQLite
//! for decrypted gallery queries.

pub mod asset;
pub mod crypto;
pub mod dispatch;
pub mod error;
pub mod metadata;
pub mod payloads;
pub mod publisher;
#[cfg(feature = "sidecar")]
pub mod sidecar;

use hopnet_storage::WrapDomain;

/// Domain-separation constant for per-photo metadata key wrapping.
/// New context strings give clean separation from the substrate's blob-key
/// domain (BLOB_WRAP_DOMAIN), preventing cross-domain key transplantation.
pub const METADATA_KEY_WRAP_DOMAIN: WrapDomain = WrapDomain {
    key_context: "hopnet-photos metadata_key v1",
    nonce_context: "hopnet-photos metadata_nonce v1",
};

/// Domain-separation constant for shared-library key wrapping. Must
/// byte-match the copy in hopnet-photos/src/lib.rs (same duplication
/// convention as METADATA_KEY_WRAP_DOMAIN — the two crates share no dep).
/// Wrap id = library id bytes, so a wrap cannot move between libraries.
pub const LIBRARY_KEY_WRAP_DOMAIN: WrapDomain = WrapDomain {
    key_context: "hopnet-photos library_key v1",
    nonce_context: "hopnet-photos library_nonce v1",
};

pub use asset::{
    AssetValidationError, PhotoAsset, PhotoResource, ResourceContent, ResourceKind, SourceIdentity,
};
pub use error::{PhotosCoreError, PublishValidationError};
pub use metadata::PhotoMetadata;
pub use publisher::{ByteSource, IngestOutcome, PublishRequest, publish_photo_add};
#[cfg(feature = "sidecar")]
pub use sidecar::install_schema;
