//! The daemon⇄node boundary (RFC-018 S1): everything the mount core may
//! ask of a HopNet node, and nothing else.
//!
//! Dyn-object seam in the hopnet-projection style — boxed futures cross
//! one box per FUSE request, never per byte. Implementations:
//! `mock::MockTransport` (tests + `--mock` demo, S1) and the HTTP
//! transport (S3). The trait grows in later slices (changes/watch S4,
//! content S5, mutations S7); S1 freezes only the namespace surface.

use std::future::Future;
use std::pin::Pin;
use std::time::SystemTime;

use hopnet_common::CustomUUID;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Consensus height an item state was read at — the sync/version anchor.
pub type Height = i64;

/// Identity of a drive item. The root is modelled explicitly so node-side
/// sentinel strings never leak into the daemon.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ItemId {
    Root,
    Inode(CustomUUID),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ItemKind {
    File { size: u64 },
    Folder,
}

/// One drive item's state as read from the node. Mirrors what the node's
/// existing enumerate/item queries return, so the HTTP transport (S2/S3)
/// is a thin skin.
#[derive(Debug, Clone)]
pub struct Item {
    pub id: ItemId,
    pub parent: ItemId,
    pub name: String,
    pub kind: ItemKind,
    pub created: SystemTime,
    pub modified: SystemTime,
    pub height: Height,
    /// Backing blob for files (None for folders and empty files — nothing
    /// to download when absent). Load-bearing for S5 snapshot-at-open.
    pub blob: Option<CustomUUID>,
}

/// Opaque enumeration cursor, defined by the node (last-seen-id per
/// RFC-018). The daemon only threads it back unchanged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cursor(pub String);

#[derive(Debug, Clone)]
pub struct Page {
    pub items: Vec<Item>,
    pub next: Option<Cursor>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Health {
    Ready,
    NotReady,
}

#[derive(Debug)]
pub enum TransportError {
    /// Node unreachable or the connection failed mid-request.
    Unavailable(String),
    /// Node reachable but the response was not understood.
    Protocol(String),
    /// Credentials rejected.
    Unauthorized,
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransportError::Unavailable(why) => write!(f, "node unavailable: {why}"),
            TransportError::Protocol(why) => write!(f, "protocol error: {why}"),
            TransportError::Unauthorized => write!(f, "credentials rejected"),
        }
    }
}

impl std::error::Error for TransportError {}

pub trait NodeTransport: Send + Sync {
    /// Resolve one child of `parent` by name. `Ok(None)` = no such child.
    fn lookup(
        &self,
        parent: ItemId,
        name: String,
    ) -> BoxFuture<'_, Result<Option<Item>, TransportError>>;

    /// Fetch a single item's current state. `item(Root)` returns the
    /// synthesized root folder; `Ok(None)` = the item is gone.
    fn item(&self, id: ItemId) -> BoxFuture<'_, Result<Option<Item>, TransportError>>;

    /// One page of `parent`'s children; passing the previous page's
    /// cursor resumes where it left off.
    fn enumerate(
        &self,
        parent: ItemId,
        cursor: Option<Cursor>,
    ) -> BoxFuture<'_, Result<Page, TransportError>>;

    /// Node readiness — distinguishes "not running" from "not set up".
    fn health(&self) -> BoxFuture<'_, Result<Health, TransportError>>;
}
