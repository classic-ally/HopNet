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

pub use asset::{
    AssetValidationError, PhotoAsset, PhotoResource, ResourceContent, ResourceKind, SourceIdentity,
};
pub use error::PhotosCoreError;
pub use metadata::PhotoMetadata;
#[cfg(feature = "sidecar")]
pub use sidecar::install_schema;
