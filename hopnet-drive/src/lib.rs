//! HopNet fs projection (RFC-015).
//!
//! The drive: inodes with deterministically-encrypted paths, shares, and
//! the FileProvider/DocumentProvider surfaces — a PROJECTION over the
//! storage substrate (hopnet-storage blobs) and the consensus state
//! machine. Extraction proceeds in stages (D1–D5); this crate currently
//! owns its schema unit, model/wire types, path crypto, and DB surface
//! (Stage D2b) — route/consensus handlers remain host-side until D3.

pub mod db;
pub mod envelopes;
pub mod error;
pub mod model;
pub mod paths;

pub use error::FileError;
pub use model::{Inode, InodeOwner};
