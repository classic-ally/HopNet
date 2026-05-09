pub mod routes;
pub mod handlers;
pub mod materialization;
pub mod archive;
pub mod barriers;
pub mod import;
pub mod import_gate;
pub mod jobs;
pub mod manifest;

pub use routes::takeout_routes;

/// Headroom reserved on top of `manifest.total_bytes × 3` when checking
/// import quota against summed validator capacity. Guards against concurrent
/// imports + general write traffic eating into the same budget.
pub const STORAGE_SAFETY_MARGIN_BYTES: u64 = 10 * 1024 * 1024 * 1024;

/// Module-owned mutable runtime state. Lives behind a single `Arc` field on
/// `AppState`, so adding takeout/import-side state in future phases doesn't
/// expand `AppState`'s already-bloated flat field list. Mirrors the
/// per-module-runtime grouping that consensus/net/files will adopt in a
/// follow-up refactor.
pub struct TakeoutRuntime {
    /// Owner-restart import resume registry. Populated by
    /// `jobs::scan_at_startup`; drained by `jobs::maybe_resume_for_user` as
    /// users re-authenticate after an owner-process restart.
    pub resume_registry:
        tokio::sync::Mutex<std::collections::HashMap<i32, crate::db::CustomUUID>>,
    /// Test-only barriers gating points in the takeout/import lifecycle.
    /// Registered with the central `barriers` registry via `inventory`.
    pub barriers: std::sync::Arc<crate::barriers::Barriers>,
}

impl Default for TakeoutRuntime {
    fn default() -> Self {
        Self {
            resume_registry: tokio::sync::Mutex::new(std::collections::HashMap::new()),
            barriers: std::sync::Arc::new(barriers::new()),
        }
    }
}