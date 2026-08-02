//! HopNet takeout/import service (RFC-015 Stage D5).
//!
//! Projection-AGNOSTIC: this crate owns the archive/manifest-v2 format, the
//! takeout/import consensus handlers, the work tables, the HTTP surface, and
//! the export/import pipelines — but knows nothing about any particular
//! projection. Each projection registers a [`ProjectionExporter`] translator
//! (host-constructed `Vec<Arc<dyn ProjectionExporter>>`); manifest sections
//! are namespaced per projection, and importing a section with no registered
//! translator SKIPS it (reported, never failed) — the forward/backward-compat
//! contract.

pub mod archive;
pub mod db;
pub mod export;
pub mod handlers;
pub mod import;
pub mod jobs;
pub mod manifest;
pub mod routes;

use std::sync::Arc;

use hopnet_projection::host::BoxFuture;
use hopnet_projection::{ProjectionExporter, SessionAccess, TxGateway};

/// Headroom reserved on top of the manifest's total bytes × 3 when checking
/// import quota against summed validator capacity. Guards against concurrent
/// imports + general write traffic eating into the same budget.
pub const STORAGE_SAFETY_MARGIN_BYTES: u64 = 10 * 1024 * 1024 * 1024;

/// Takeout's static manifest (RFC-016 Stage 3): only the static trio —
/// takeout genuinely has consensus handlers and a schema unit, so the
/// host's tripwire and install chain cover it through the same loop as
/// real projections. Its runtime surface (routers, cron, resume hooks,
/// barriers, TakeoutRuntime) deliberately stays NAMED host wiring: it is
/// a projection-agnostic service whose state (exporters collected from
/// OTHER projections, host SQL hooks) is not expressible from generic
/// capabilities.
pub struct TakeoutProjection;

impl hopnet_projection::Projection for TakeoutProjection {
    fn name(&self) -> &'static str {
        "takeout"
    }

    fn tx_functions(&self) -> &'static [&'static str] {
        handlers::TX_FUNCTIONS
    }

    fn install_schema(&self, conn: &rusqlite::Connection) -> Result<(), rusqlite::Error> {
        db::install_schema(conn)
    }

    fn tables(&self) -> &'static [&'static str] {
        db::TABLES
    }
}

/// Takeout/import test barriers. The name registry lives here with the
/// runtime; the host keeps the HTTP test routes + the `BarrierRegistration`
/// inventory shim pointing into [`TakeoutRuntime::barriers`].
pub mod barriers {
    pub mod names {
        /// Held by the import-resume scenario: pauses the creation walk after
        /// extraction completes (status flipped to Importing, path table
        /// seeded) so the test can stop the owner mid-import and verify
        /// resume.
        pub const BEFORE_IMPORT_CREATION_WALK: &str = "before_import_creation_walk";
    }

    pub const ALL_BARRIER_NAMES: &[&str] = &[names::BEFORE_IMPORT_CREATION_WALK];

    pub fn new() -> hopnet_projection::barriers::Barriers {
        hopnet_projection::barriers::Barriers::new(ALL_BARRIER_NAMES)
    }
}

/// Module-owned mutable runtime state. Lives behind a single `Arc` on the
/// host's AppState (and inside every [`TakeoutState`]) so all on-the-fly
/// state constructions share one resume registry + one barrier set.
pub struct TakeoutRuntime {
    /// Owner-restart import resume registry. Populated by
    /// `jobs::scan_at_startup`; drained by `jobs::maybe_resume_for_user` as
    /// users re-authenticate after an owner-process restart.
    pub resume_registry:
        tokio::sync::Mutex<std::collections::HashMap<i32, hopnet_common::CustomUUID>>,
    /// Test-only barriers gating points in the takeout/import lifecycle.
    /// The host registers them with its central barrier HTTP routes.
    pub barriers: Arc<hopnet_projection::barriers::Barriers>,
}

impl Default for TakeoutRuntime {
    fn default() -> Self {
        Self {
            resume_registry: tokio::sync::Mutex::new(std::collections::HashMap::new()),
            barriers: Arc::new(barriers::new()),
        }
    }
}

/// Takeout's host contract (NOT a projection seam — this is what the host
/// owes the takeout service): domains that stay host-side (users onboarding,
/// storage metrics) reached through one adapter.
pub trait TakeoutHooks: Send + Sync {
    /// An import reached `Completed` for `user_id`. Host impl submits the
    /// onboarding-flags consensus transaction exactly as before (best-effort
    /// — the core logs and continues on error).
    fn import_completed(&self, user_id: i32) -> BoxFuture<'_, Result<(), String>>;

    /// Network-wide validator storage sum in bytes (import quota input).
    /// Host impl wraps its metrics aggregation (+ bootstrap fs fallback);
    /// the ×3 + safety-margin FORMULA stays in core.
    fn available_storage_bytes(&self) -> BoxFuture<'_, Result<u64, String>>;

    /// This node's available storage in bytes (takeout-initiate headroom
    /// input). `Ok(None)` = could not be determined (maps to 500, matching
    /// the pre-split route). Host impl wraps its metrics row + fs fallback.
    fn node_available_storage_bytes(&self) -> BoxFuture<'_, Result<Option<u64>, String>>;

    /// Total bytes of the user's exportable data (takeout-initiate sizing
    /// input). Host impl keeps the pre-split projection SQL; a future
    /// refactor may fold this into the exporter contract.
    fn user_data_size_bytes(&self, user_id: i32) -> BoxFuture<'_, Result<u64, String>>;
}

/// The takeout service's state: concrete DB access (core owns the takeout/
/// import SQL) plus host seams and the registered projection translators.
#[derive(Clone)]
pub struct TakeoutState {
    pub db_pool: r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
    pub fragments_dir: String,
    pub node_id: Arc<once_cell::sync::OnceCell<i32>>,
    pub sessions: Arc<dyn SessionAccess>,
    pub txs: Arc<dyn TxGateway>,
    /// Host-constructed translator registry (RFC-015 D5 decision 1) —
    /// impls hold runtime state, so no static inventory here.
    pub exporters: Arc<[Arc<dyn ProjectionExporter>]>,
    pub runtime: Arc<TakeoutRuntime>,
    pub hooks: Arc<dyn TakeoutHooks>,
}

impl TakeoutState {
    pub fn node_id(&self) -> Option<i32> {
        self.node_id.get().copied()
    }

    /// Look up a registered translator by manifest section name.
    pub fn exporter(&self, name: &str) -> Option<&Arc<dyn ProjectionExporter>> {
        self.exporters.iter().find(|e| e.name() == name)
    }
}
