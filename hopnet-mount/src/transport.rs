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
    /// Mutation conflicts with current state (name taken, folder not
    /// empty) — maps to EEXIST/ENOTEMPTY by op context.
    Conflict,
    /// Consensus wait timed out — outcome UNKNOWN; callers must not
    /// assume either applied or not.
    OutcomeUnknown,
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransportError::Unavailable(why) => write!(f, "node unavailable: {why}"),
            TransportError::Protocol(why) => write!(f, "protocol error: {why}"),
            TransportError::Unauthorized => write!(f, "credentials rejected"),
            TransportError::Conflict => write!(f, "conflicts with current state"),
            TransportError::OutcomeUnknown => write!(f, "consensus wait timed out"),
        }
    }
}

impl std::error::Error for TransportError {}

/// One delta batch: latest state per touched item strictly after the
/// anchor, plus ids that no longer exist, plus the new anchor height.
#[derive(Debug, Clone)]
pub struct Changes {
    pub items: Vec<Item>,
    pub deleted: Vec<CustomUUID>,
    pub height: Height,
}

/// What a live watch connection yields. Heartbeats carry no meaning
/// beyond "the connection is alive" — the watch loop's liveness timeout
/// resets on ANY item, so an idle-but-healthy connection survives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchEvent {
    /// Something changed — run changes from your anchor.
    Poke,
    /// Server keepalive.
    Heartbeat,
}

/// Stream of watch events. Stream end = the connection dropped.
pub type WatchStream = Pin<Box<dyn tokio_stream::Stream<Item = WatchEvent> + Send>>;

/// Streaming upload source (staged files never buffer whole in memory).
pub type ByteSource =
    Pin<Box<dyn tokio_stream::Stream<Item = std::io::Result<bytes::Bytes>> + Send>>;

/// Result of a strict mutation: fresh post-apply state + read anchor.
#[derive(Debug, Clone)]
pub struct Mutated {
    pub item: Option<Item>,
    pub height: Height,
}

/// Node-side statfs numbers (RFC-018 S8): total = user-data capacity
/// while the mesh tolerates >= 2 node failures, used = observed bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatfsInfo {
    pub total_bytes: u64,
    pub used_bytes: u64,
}

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

    /// Everything that changed strictly after `since` (RFC-018 S4).
    /// `since = i32::MAX as Height` is the cheap anchor-init: guaranteed
    /// empty rows, current height returned.
    fn changes(&self, since: Height) -> BoxFuture<'_, Result<Changes, TransportError>>;

    /// Subscribe to change pokes. The returned stream ends when the
    /// connection drops; callers reconnect and resync from their anchor.
    fn watch(&self) -> BoxFuture<'_, Result<WatchStream, TransportError>>;

    /// One ranged read of a blob's plaintext (RFC-018 S5). Blob-addressed
    /// so open handles keep snapshot-at-open semantics. Callers keep
    /// ranges within one cache segment.
    fn read_blob(
        &self,
        blob: CustomUUID,
        offset: u64,
        len: u64,
    ) -> BoxFuture<'_, Result<Vec<u8>, TransportError>>;

    // ---- mutations (RFC-018 S7): strict — resolve only after the
    // transaction is decided AND applied on the node; `Mutated.height`
    // is the read anchor, `item` the fresh post-apply state. ----

    fn create_folder(
        &self,
        parent: ItemId,
        name: String,
    ) -> BoxFuture<'_, Result<Mutated, TransportError>>;

    fn create_file(
        &self,
        parent: ItemId,
        name: String,
        size: u64,
        content: ByteSource,
    ) -> BoxFuture<'_, Result<Mutated, TransportError>>;

    /// Whole-file content replacement (mints a new blob node-side).
    fn update_content(
        &self,
        id: CustomUUID,
        size: u64,
        content: ByteSource,
    ) -> BoxFuture<'_, Result<Mutated, TransportError>>;

    /// Rename and/or move. `new_parent: None` = unchanged parent;
    /// `new_name: None` = unchanged name.
    fn rename(
        &self,
        id: CustomUUID,
        new_parent: Option<ItemId>,
        new_name: Option<String>,
    ) -> BoxFuture<'_, Result<Mutated, TransportError>>;

    fn delete(
        &self,
        id: CustomUUID,
        recursive: bool,
    ) -> BoxFuture<'_, Result<Height, TransportError>>;

    /// Node readiness — distinguishes "not running" from "not set up".
    fn health(&self) -> BoxFuture<'_, Result<Health, TransportError>>;

    /// Mesh-level capacity numbers for statfs — node-side definitions
    /// (tolerance-constrained total, observed used), never local disk.
    fn statfs(&self) -> BoxFuture<'_, Result<StatfsInfo, TransportError>>;
}
