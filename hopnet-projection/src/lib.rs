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

pub mod barriers;
pub mod dbstats;
pub mod host;

pub use host::{
    BoxFuture, SessionAccess, SessionError, TxGateway, TxSigner, TxSpec, TxSubmitError, UserSession,
};

use serde::{Deserialize, Serialize};
use std::pin::Pin;

/// RFC-3339-in-SQLite datetime newtype. Moved down from hopnet-drive's model
/// at Stage D5b — both drive and takeout serialize it into consensus
/// payloads (bincode wire shape unchanged by the move; the serde impl is
/// derived on the newtype either way). Drive re-exports it.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CustomDateTime(chrono::DateTime<chrono::Utc>);

impl CustomDateTime {
    pub fn new(dt: chrono::DateTime<chrono::Utc>) -> Self {
        CustomDateTime(dt)
    }
}

impl std::ops::Deref for CustomDateTime {
    type Target = chrono::DateTime<chrono::Utc>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl rusqlite::types::ToSql for CustomDateTime {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(rusqlite::types::ToSqlOutput::from(self.to_rfc3339()))
    }
}

impl rusqlite::types::FromSql for CustomDateTime {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        use chrono::DateTime;
        use rusqlite::types::{FromSqlError, ValueRef};
        match value {
            ValueRef::Integer(millis) => {
                // uuid_extract_timestamp returns epoch milliseconds
                DateTime::from_timestamp_millis(millis)
                    .map(CustomDateTime)
                    .ok_or(FromSqlError::InvalidType)
            }
            ValueRef::Text(str) => match std::str::from_utf8(str) {
                Ok(utf_value) => match DateTime::parse_from_rfc3339(utf_value) {
                    Ok(dt) => Ok(CustomDateTime(dt.with_timezone(&chrono::Utc))),
                    Err(_) => Err(FromSqlError::InvalidType),
                },
                Err(_) => Err(FromSqlError::InvalidType),
            },
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

/// Database-layer error taxonomy shared by handlers and projection DB code.
/// (Moved verbatim from the main crate's db/types.rs — the host re-exports.)
#[derive(Debug, PartialEq, Eq)]
pub enum DatabaseError {
    LockError,
    InsertError,
    RecordError,
    RecallError,
    ProcessingError,
    InvalidPayload,
    NotFound,
    ConflictError,      // Resource already exists at the specified location/identifier
    /// Replace-rename onto a non-empty folder — the POSIX ENOTEMPTY verdict.
    /// Distinct from ConflictError so the mount surface can answer a coded
    /// 409 the daemon maps to ENOTEMPTY instead of EEXIST.
    NotEmpty,
    AuthorizationError, // User or node not authorized for the operation
    ValidationError, // Data validation failed (e.g., cryptographic verification, consistency checks)
    /// Transient, node-local storage contention (SQLITE_BUSY / SQLITE_LOCKED).
    /// Not a verdict on the operation: consumers must retry or restage, never
    /// treat it as a permanent semantic failure (it is non-deterministic and
    /// would otherwise leak into consensus verdicts and client-visible 409s).
    Transient(rusqlite::ErrorCode),
}

impl DatabaseError {
    /// Classify a rusqlite failure at the seam where it is still typed:
    /// retryable lock contention becomes [`DatabaseError::Transient`];
    /// anything else keeps the caller's chosen verdict.
    pub fn classified(e: &rusqlite::Error, fallback: DatabaseError) -> DatabaseError {
        match e.sqlite_error_code() {
            Some(
                code @ (rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked),
            ) => DatabaseError::Transient(code),
            _ => fallback,
        }
    }
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

    /// Subscribe to change pokes (RFC-018 S4). Every `files_changed` fires
    /// one content-free poke; lagged receivers coalesce (a poke is
    /// idempotent — subscribers follow with their own delta query). The
    /// macOS FileProvider signal is conceptually just another subscriber.
    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<()>;
}

/// No-op notifier for tests and non-interactive contexts.
pub struct NullNotifier;
impl ChangeNotifier for NullNotifier {
    fn files_changed(&self) {}
    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<()> {
        // Dead receiver: sender drops immediately, subscribers see Closed.
        tokio::sync::broadcast::channel(1).1
    }
}

/// Post-apply background-work scheduling. Handlers may only ENQUEUE named
/// work (fire-and-forget, non-blocking); the host routes subsystem keys to
/// its runtime tasks. This is how consensus handlers trigger host-side
/// work without ever holding host state.
pub trait WorkScheduler: Send + Sync {
    fn schedule(&self, subsystem: &'static str, key: String);
}

/// No-op scheduler for tests and validate-only contexts.
pub struct NullScheduler;
impl WorkScheduler for NullScheduler {
    fn schedule(&self, _subsystem: &'static str, _key: String) {}
}

/// The slice of host state a handler may need. Deliberately minimal —
/// verified against every existing handler (only fragments_dir and the
/// change signal were ever read from the host).
pub struct HandlerCtx<'a> {
    /// Local fragment store root (blob applies probe stored_locally).
    pub fragments_dir: &'a str,
    /// This node's consensus id, when initialized.
    pub node_id: Option<i32>,
    /// The block height this transaction is being processed under: the
    /// DECIDING height during apply, the candidate height during
    /// validation, 0 in non-block contexts (mempool preflight). Writers
    /// stamping modification heights MUST use this, not
    /// `last_decided_height` — the meta row lags the block being applied,
    /// and rows stamped with a lagging height fall behind anchors already
    /// handed to /changes clients (silent divergence; caught by the
    /// RFC-018 S4 stack test).
    pub height: u64,
    /// Post-apply change signal (host impl owns test_mode/platform gating).
    pub notifier: &'a dyn ChangeNotifier,
    /// Named background-work scheduler (host impl owns spawning/routing).
    /// Handlers schedule under `execute` only — validation must be pure.
    pub work: &'a dyn WorkScheduler,
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
    /// Exporter-private reference so `open()` needs no re-resolve (drive:
    /// the encrypted path). Never serialized into the manifest.
    #[serde(skip)]
    pub export_handle: Option<String>,
}

/// Content bytes for one export entry, as streamed by `open()`.
pub type ExportByteStream =
    Pin<Box<dyn tokio_stream::Stream<Item = Result<bytes::Bytes, std::io::Error>> + Send>>;

/// A projection's enumeration of its exportable state (paged/streaming —
/// photos-scale; never materialize the whole listing in memory).
pub type ExportEntryStream =
    Pin<Box<dyn tokio_stream::Stream<Item = Result<ExportEntry, ExportError>> + Send>>;

/// Export-side failure (enumerate/open).
#[derive(Debug)]
pub struct ExportError(pub String);

/// Import-side failure classification: `Permanent` marks the entry Failed
/// with a stable code (surfaced in status counts); `Transient` is retried
/// by the core's resume machinery.
#[derive(Debug)]
pub enum ImportEntryError {
    Permanent { code: &'static str, message: String },
    Transient(String),
}

/// Per-projection takeout translator (RFC-015 Stage D5). hopnet-takeout's
/// core drives export and import; the projection only translates between
/// its own state and [`ExportEntry`] rows. Registration is
/// host-constructed (`Vec<Arc<dyn ProjectionExporter>>`) — impls hold
/// runtime state (drive's holds its DriveState for sessions/blobs).
///
/// Sidecar principle (acceptance bar): each entry's `metadata` must carry
/// everything a FRESH mesh needs to reconstruct the projection's state
/// from the entries alone — no side lookups into the source mesh.
///
/// Import contract: the core stages content and calls `import_entry` once
/// PER ENTRY (uniform resume/progress per row), then `flush` at the end of
/// the projection's section — the projection's batch boundary (drive may
/// batch consensus transactions underneath and settle them at flush).
pub trait ProjectionExporter: Send + Sync {
    /// Manifest section name ("drive", "photos", …).
    fn name(&self) -> &'static str;

    /// Stream every exportable entry for the user. Paths in entries are
    /// DECRYPTED logical paths.
    fn enumerate(&self, user_id: i32) -> BoxFuture<'_, Result<ExportEntryStream, ExportError>>;

    /// Open one entry's content for streaming (entries with `blob_id`).
    fn open(
        &self,
        user_id: i32,
        entry: &ExportEntry,
    ) -> BoxFuture<'_, Result<ExportByteStream, ExportError>>;

    /// Apply one entry to this projection on import. `staged_content` is
    /// the core-staged plaintext file (None for folders/containers).
    fn import_entry<'a>(
        &'a self,
        user_id: i32,
        entry: &'a ExportEntry,
        staged_content: Option<&'a std::path::Path>,
    ) -> BoxFuture<'a, Result<(), ImportEntryError>>;

    /// Section-end batch boundary: settle anything `import_entry` deferred.
    fn flush(&self, user_id: i32) -> BoxFuture<'_, Result<(), ImportEntryError>>;
}

/// Which host auth middleware wraps a projection's mounted router
/// (RFC-016 Stage 4). The host owns the middleware implementations; a
/// projection only declares the class. Both classes insert
/// `Extension<i32>` (the authenticated user id) that projection routers
/// and the write gate read.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AuthClass {
    /// The host's JWT session auth (browser/API surface).
    UserJwt,
    /// The host's device-token auth (FileProvider/DocumentProvider
    /// integrations; bootstraps a short-lived session).
    DeviceToken,
}

/// One mounted router of a projection's HTTP surface. `prefix` is the
/// FULL path the host nests at; routers own their internal layers (write
/// gate, body limits) — the host adds only the declared auth class and
/// its global layers (overload shedding, tracing).
pub struct Mount {
    pub prefix: &'static str,
    pub auth: AuthClass,
    pub router: axum::Router,
    /// Per-surface override of the projection-wide minimum (RFC-023):
    /// the effective minimum client version code for this surface is
    /// `self.min_client.or(projection.min_client())`.
    pub min_client: Option<u32>,
}

/// A projection's static manifest (RFC-016 Stage 3): the single object a
/// projection crate exports and the host registers — ONE line in the
/// host's `projections::manifests()` is the whole host diff for adding a
/// projection.
///
/// Manifests are unit structs (`&'static dyn Projection`): schema install
/// and the boot tripwire run BEFORE the host's AppState (and therefore
/// [`host::HostCapabilities`]) exists, so the manifest never stores
/// capabilities — runtime methods take `&HostCapabilities` per call
/// (construction is cheap Arc clones, precedented by the host's
/// per-request state builders).
pub trait Projection: Send + Sync {
    /// Manifest/section name ("drive", "takeout", "photos", …).
    fn name(&self) -> &'static str;

    /// Every consensus tx function this projection's handlers register
    /// via inventory — the host's boot tripwire asserts each is present
    /// in the dispatch table (linker-drop guard).
    fn tx_functions(&self) -> &'static [&'static str];

    /// The tables this projection owns and that are consensus-tracked
    /// (mutations replicated across all nodes). The host's divergence
    /// checker hashes each in addition to the host-owned
    /// `CONSENSUS_TABLES` list. Single source of truth: the projection's
    /// `db::TABLES` const feeds both the uninstall symmetry test and
    /// this method, so the schema and the divergence coverage cannot
    /// drift. (Schema installation itself is `chain()` replay — RFC-020
    /// S2 removed the per-projection installer.)
    /// Default: empty (a projection with no consensus tables).
    fn tables(&self) -> &'static [&'static str] {
        &[]
    }

    /// The projection's takeout translator, if it has one. Default: none.
    fn exporter(
        &self,
        _caps: &host::HostCapabilities,
    ) -> Option<std::sync::Arc<dyn ProjectionExporter>> {
        None
    }

    /// The projection's HTTP surface as (prefix, auth class, router)
    /// mounts; the host nests each under its declared auth middleware and
    /// global layers. Default: no HTTP surface.
    fn mounts(&self, _caps: &host::HostCapabilities) -> Vec<Mount> {
        Vec::new()
    }

    /// Claim one unit of named background work enqueued from consensus
    /// apply via [`WorkScheduler::schedule`]. Return `Some(future)` to
    /// claim; the host spawns it on the MAIN runtime (apply runs on the
    /// consensus shell's time-only runtime, where IO work must not land).
    /// schedule() fires before the block's DB transaction commits — the
    /// future must tolerate not yet seeing applied rows (brief retry).
    /// Default: claims nothing.
    fn work(
        &self,
        _caps: &host::HostCapabilities,
        _subsystem: &str,
        _key: String,
    ) -> Option<host::BoxFuture<'static, ()>> {
        None
    }

    /// Blob ids whose content this DECIDED transaction commits — the host
    /// feeds them to the storage engine's distribution kick (RFC-017
    /// Stage 4; previously the host decoded drive envelopes itself). Pure
    /// decode: runs on the consensus shell thread post-decide — no DB, no
    /// IO, no awaits. Decode failures must yield an empty vec, never
    /// panic. Default: no blobs.
    fn committed_blob_ids(&self, _function: &str, _payload: &[u8]) -> Vec<hopnet_storage::BlobId> {
        Vec::new()
    }

    /// Bytes of user content this projection stores for `user_id`, summed
    /// across all manifests by the host for takeout/import quota sizing
    /// (RFC-017 Stage 6; previously host SQL read the drive's inodes
    /// directly). Capabilities are cloned into the future (precedent:
    /// [`Projection::work`]). Default: none.
    fn user_data_size_bytes(
        &self,
        _caps: &host::HostCapabilities,
        _user_id: i32,
    ) -> host::BoxFuture<'static, Result<u64, String>> {
        Box::pin(std::future::ready(Ok(0)))
    }

    // RFC-019 additions:

    /// This projection's section of the canonical state snapshot: the
    /// covered tables the divergence manifest hashes and the epoch
    /// artifact exports (the mesh-scoped sibling of [`ProjectionExporter`],
    /// which is per-user). The host assembles sections after the
    /// substrate's, in registration order. The `'static` return is
    /// deliberate — the host builds a `'static` section list from unit
    /// structs. Default: no covered tables.
    fn snapshot_section(&self) -> Option<&'static hopnet_common::SectionSpec> {
        None
    }

    /// This projection's node-local tables — outside the snapshot
    /// universe entirely; the host's registry test pins
    /// covered ∪ node-local == sqlite_master, so every table this
    /// projection installs must appear in exactly one of the two.
    /// Default: none.
    fn node_local_tables(&self) -> &'static [&'static str] {
        &[]
    }

    // RFC-020 additions:

    /// This projection's schema chain (RFC-020): the append-only
    /// migration steps whose replay IS the module's schema. REQUIRED,
    /// not defaulted — replay is the only installer, and a
    /// defaulted-empty chain would let a projection silently install
    /// nothing. The chain's module name must equal the snapshot
    /// section's name, and its head ordinal the section's
    /// `format_version` (both pinned by host registry tests).
    fn chain(&self) -> &'static hopnet_common::Chain;

    // RFC-023 additions:

    /// Projection-wide default: the oldest client version code every
    /// surface of this projection supports — the coverage backstop; a
    /// [`Mount::min_client`] overrides it per surface. None = no
    /// declaration, which is legal only for surfaces that are not
    /// `DeviceToken`-authed: a `DeviceToken` mount resolving to None at
    /// both levels fails the host's coverage assertion at boot.
    fn min_client(&self) -> Option<u32> {
        None
    }
}

/// Current decided consensus height off any connection to the shared DB
/// (RFC-017 Stage 3 — the single replacement for three identical copies in
/// the host, drive, and takeout). Conn-based rather than a HostCapabilities
/// method because projections read the height INSIDE consensus-apply
/// transactions, where an async request-scoped seam would break
/// read-your-writes and the sync context. Pre-genesis (missing row) reads
/// as 0; errors map to `RecallError` — the previous copies' semantics, with
/// hopnet-consensus's SQL as the single source of truth.
pub fn current_height(conn: &rusqlite::Connection) -> Result<u64, DatabaseError> {
    hopnet_consensus::store::last_decided_height(conn)
        .map(|h| h.map_or(0, |h| h.0))
        .map_err(|_| DatabaseError::RecallError)
}

#[cfg(test)]
mod classify_tests {
    use super::*;

    fn sqlite_failure(extended_code: std::ffi::c_int) -> rusqlite::Error {
        rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(extended_code),
            Some("database is locked".into()),
        )
    }

    // Impact: this classification is what keeps node-local lock contention out
    // of consensus verdicts and client-visible 409s; misclassifying either way
    // reintroduces the EEXIST data-loss path or masks real failures.
    // Should: classify SQLITE_BUSY as transient, preserving the error code.
    // Should: classify the snapshot-promotion flavour of busy as transient.
    // Should: classify SQLITE_LOCKED as transient.
    #[test]
    fn lock_contention_is_transient() {
        for code in [
            rusqlite::ffi::SQLITE_BUSY,
            rusqlite::ffi::SQLITE_BUSY_SNAPSHOT,
            rusqlite::ffi::SQLITE_LOCKED,
        ] {
            let got = DatabaseError::classified(&sqlite_failure(code), DatabaseError::InsertError);
            assert!(
                matches!(got, DatabaseError::Transient(_)),
                "extended code {code} should classify as Transient, got {got:?}"
            );
        }
    }

    // Should: keep the caller's verdict for non-contention sqlite failures.
    // Should not: classify a constraint violation as transient.
    #[test]
    fn semantic_failures_keep_the_callers_verdict() {
        let constraint = sqlite_failure(rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE);
        assert_eq!(
            DatabaseError::classified(&constraint, DatabaseError::ConflictError),
            DatabaseError::ConflictError
        );
    }

    // Should: keep the caller's verdict for rusqlite errors with no sqlite code.
    #[test]
    fn non_sqlite_errors_keep_the_callers_verdict() {
        assert_eq!(
            DatabaseError::classified(
                &rusqlite::Error::QueryReturnedNoRows,
                DatabaseError::RecordError
            ),
            DatabaseError::RecordError
        );
    }
}
