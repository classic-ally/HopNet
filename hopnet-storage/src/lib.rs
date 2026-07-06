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

pub mod crypto;
pub mod error;
pub mod fragstore;
pub mod placement;
pub mod rs;

pub use error::StorageError;
pub use hopnet_common::Blake3Hash;
pub use hopnet_common::CustomUUID;
