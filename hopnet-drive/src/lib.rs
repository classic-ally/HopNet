//! HopNet fs projection (RFC-015).
//!
//! The drive: inodes with deterministically-encrypted paths, shares, and
//! the FileProvider/DocumentProvider surfaces — a PROJECTION over the
//! storage substrate (hopnet-storage blobs) and the consensus state
//! machine. Extraction proceeds in stages (D1–D5); this crate currently
//! owns its schema unit only.

pub mod db;
pub mod model;

pub use model::{Inode, InodeOwner};
