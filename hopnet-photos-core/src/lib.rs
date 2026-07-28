//! HopNet photos core (RFC-011 Track B).
//!
//! Pure, sync, fully testable photos primitives: metadata-key + blob-key
//! crypto, the encrypted PhotoMetadata JSON shape, payload builders over
//! the photos projection's frozen envelope wire types, and the
//! PhotoDispatch trait (Commit 2 — unimplemented here; impls live
//! downstream). The sidecar DB is deferred to Commit 3.

pub mod crypto;
pub mod dispatch;
pub mod error;
pub mod metadata;
pub mod payloads;

use hopnet_storage::WrapDomain;

/// Domain-separation constant for per-photo metadata key wrapping.
/// New context strings give clean separation from the substrate's blob-key
/// domain (BLOB_WRAP_DOMAIN), preventing cross-domain key transplantation.
pub const METADATA_KEY_WRAP_DOMAIN: WrapDomain = WrapDomain {
    key_context: "hopnet-photos metadata_key v1",
    nonce_context: "hopnet-photos metadata_nonce v1",
};

pub use error::PhotosCoreError;
pub use metadata::PhotoMetadata;
