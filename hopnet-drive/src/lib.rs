//! HopNet fs projection (RFC-015).
//!
//! The drive: inodes with deterministically-encrypted paths, shares, and
//! the FileProvider/DocumentProvider surfaces — a PROJECTION over the
//! storage substrate (hopnet-storage blobs) and the consensus state
//! machine. Extraction proceeds in stages (D1–D5); this crate currently
//! owns its schema unit, model/wire types, path crypto, DB surface
//! (Stage D2b), its consensus transaction handlers + GC reference
//! provider (Stage D3), and its HTTP/business surface — routers, upload
//! and download flows — behind the `host` seams (Stage D4).

pub mod db;
pub mod download;
pub mod host;
pub mod http;
pub mod envelopes;
pub mod error;
pub mod handlers;
pub mod model;
pub mod paths;
pub mod reference_provider;
pub mod upload;

pub use error::FileError;
pub use model::{Inode, InodeOwner};
