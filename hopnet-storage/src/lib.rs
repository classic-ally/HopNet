//! HopNet distribution substrate (RFC-014).
//!
//! Durable, location-transparent, content-verified, ENCRYPTED blobs on top of
//! the consensus state machine (RFC-013). Projections (filesystem, photos)
//! reference blobs by id and express access as recipient-pubkey sets; this
//! crate owns the ciphers, the Reed-Solomon fragment format, deterministic
//! placement, and (from Stage E) the distribution engine.
//!
//! Two temperatures: everything here is pure/sync and testable without tokio;
//! the `engine` feature (Stage E) adds the tokio distribution engine, whose
//! decisions delegate back to the pure modules.

#[cfg(feature = "engine")]
pub mod api;
pub mod crypto;
#[cfg(feature = "engine")]
pub mod engine;
pub mod error;
pub mod eviction;
pub mod fragstore;
pub mod maintenance;
pub mod membership;
pub mod pins;
pub mod placement;
pub mod rpc;
pub mod rs;
pub mod serve;
pub mod store;
pub mod traits;
pub mod types;

pub use error::StorageError;
pub use hopnet_common::Blake3Hash;
pub use hopnet_common::CustomUUID;
pub use types::{
    BlobAccess, BlobId, DeleteOrphanedDataBlocksPayload, MeshKeyGrant, PlacementUpdate,
    SelfCheckFragments,
};

// Public crypto seam — consumers (photos-core, future projections) wrap
// keys through these without touching the private primitives.
pub use crypto::{
    unwrap_key_v1_in_domain, wrap_key_v1_in_domain, RecipientKey, StaticRecipient, WrapDomain,
    BLOB_WRAP_DOMAIN,
};
// The dalek vocabulary types are part of the crypto seam's public API
// (RecipientKey references them); re-export so consumers don't drift onto
// an incompatible version.
pub use x25519_dalek;
