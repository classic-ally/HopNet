//! HopNet projection seam (RFC-015).
//!
//! The contracts a PROJECTION crate (hopnet-drive, photos, …) implements to
//! plug into the host: consensus transaction handlers, post-apply change
//! notification, the GC reference registry, and (Stage D5) the takeout
//! export/import translator. This crate sits BELOW the host and every
//! projection — it owns the `inventory` registries so registration works
//! across crate boundaries (the typetag pattern).
//!
//! Handlers receive a NARROWED view of a transaction ([`TxMeta`]) and of the
//! host ([`HandlerCtx`]) — never the host's AppState or full transaction
//! type. Signature verification happened host-side before dispatch; the ids
//! carried here are trusted.

use serde::{Deserialize, Serialize};

/// Database-layer error taxonomy shared by handlers and projection DB code.
/// (Moved verbatim from the main crate's db/types.rs — the host re-exports.)
#[derive(Debug)]
pub enum DatabaseError {
    LockError,
    InsertError,
    RecordError,
    RecallError,
    ProcessingError,
    InvalidPayload,
    NotFound,
    ConflictError,      // Resource already exists at the specified location/identifier
    AuthorizationError, // User or node not authorized for the operation
    ValidationError, // Data validation failed (e.g., cryptographic verification, consistency checks)
}

pub type HandlerResult = Result<(), DatabaseError>;

/// Narrowed, signature-verified view of a consensus transaction.
///
/// Verification happened host-side before dispatch — `submitter_node` and
/// `user_id` are cryptographically attested identities, safe to authorize
/// against.
pub struct TxMeta<'a> {
    /// The transaction's function name (dispatch key).
    pub function: &'a str,
    /// The operation payload to decode (bincode).
    pub payload: &'a [u8],
    /// Node that submitted the transaction.
    pub submitter_node: i32,
    /// Authenticated user, when the transaction is user-signed.
    pub user_id: Option<i32>,
}

/// Host-provided post-apply side effects. Fire-and-forget — the host owns
/// spawning and any platform specifics (e.g. macOS FileProvider refresh).
pub trait ChangeNotifier: Send + Sync {
    /// A drive mutation was EXECUTED (not just validated) — OS integrations
    /// should refresh their views.
    fn files_changed(&self);
}

/// No-op notifier for tests and non-interactive contexts.
pub struct NullNotifier;
impl ChangeNotifier for NullNotifier {
    fn files_changed(&self) {}
}

/// The slice of host state a handler may need. Deliberately minimal —
/// verified against every existing handler (only fragments_dir and the
/// change signal were ever read from the host).
pub struct HandlerCtx<'a> {
    /// Local fragment store root (blob applies probe stored_locally).
    pub fragments_dir: &'a str,
    /// This node's consensus id, when initialized.
    pub node_id: Option<i32>,
    /// Post-apply change signal (host impl owns test_mode/platform gating).
    pub notifier: &'a dyn ChangeNotifier,
    /// TEMPORARY escape hatch (RFC-015; dies at Stage D5): the host's full
    /// state for the two takeout handlers that still spawn host-side
    /// background work from apply. Projection crates MUST NOT touch this —
    /// it does not survive the takeout reshape.
    pub host: Option<&'a (dyn std::any::Any + Send + Sync)>,
}

/// A consensus transaction handler. Implementations register themselves via
/// `inventory::submit!` in their own crate; the host builds its dispatch
/// table from the collected registry.
pub trait TransactionHandler: Send + Sync {
    /// Stable lookup name for the function this handles.
    fn name(&self) -> &'static str;

    /// Apply (execute=true) or validate (execute=false) the transaction
    /// inside the host's shared block transaction.
    fn process(
        &self,
        tx: &TxMeta<'_>,
        execute: bool,
        ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult;
}

inventory::collect!(&'static dyn TransactionHandler);

/// GC reference registry: a projection declares which data blocks (blobs)
/// it still references, so orphan cleanup never collects a referenced blob.
pub trait DataBlockReferenceProvider: Send + Sync {
    /// Provider name for logging ("filesystem", "photos", etc.)
    fn name(&self) -> &'static str;

    /// Per-row check: does this provider reference the given data block?
    /// Used during consensus validation in delete_orphaned_data_blocks.
    fn references_data_block(
        &self,
        db_tx: &rusqlite::Transaction,
        data_block_id: &str,
    ) -> Result<bool, DatabaseError>;

    /// SQL subquery selecting all data_block_ids claimed by this provider.
    /// Used for bulk candidate identification in find_orphaned_data_blocks.
    /// Must return rows with a column named `data_block_id`.
    fn referenced_data_blocks_subquery(&self) -> &'static str;
}

inventory::collect!(&'static dyn DataBlockReferenceProvider);

/// One exportable unit of a projection's state (takeout translator vocab —
/// finalized at Stage D5; declared here so projections and hopnet-takeout
/// share it). Sidecar principle: `metadata` must carry everything a fresh
/// mesh needs to reconstruct the projection's state from entries alone.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportEntry {
    /// Namespaced logical path within the projection's section.
    pub logical_path: String,
    /// Content blob, when the entry has content (None = folder/container).
    pub blob_id: Option<hopnet_common::CustomUUID>,
    pub size: u64,
    /// Projection-specific metadata (self-describing, versioned by the
    /// projection).
    pub metadata: serde_json::Value,
}
