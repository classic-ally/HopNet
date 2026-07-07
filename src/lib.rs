use jsonwebtoken::{DecodingKey, EncodingKey};
use once_cell::sync::{Lazy, OnceCell};
use std::collections::HashMap;
use std::sync::Arc;

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;

use crate::db::{PrivKey, PubKey};
use crate::handlers::TransactionHandler;

pub mod admin;
pub mod auth;
pub mod barriers;
pub mod consensus;
pub mod db;
pub mod devices;
// Drive-owned (RFC-015, Stage D4): the DocumentProvider routes live in
// hopnet_drive::http::documentprovider; the host mounts them in main.rs.
pub mod drive_host;
pub mod fileprovider;
pub mod files;
pub mod handlers;
pub mod metrics;
pub mod net;
pub mod nodes;
pub mod passphrase;
pub mod reference_providers;
pub mod setup;
pub mod shares;
pub mod takeout;
pub mod types;
pub mod users;

#[derive(Clone, Debug)]
pub struct UserKeys {
    pub private_key: PrivKey,
    pub public_key: PubKey,
}

#[derive(Clone)]
pub struct AppState {
    pub db_pool: Pool<SqliteConnectionManager>,
    pub encoding_key: EncodingKey,
    pub decoding_key: DecodingKey,
    pub private_key: PrivKey,
    pub public_key: PubKey,
    pub node_id: Arc<OnceCell<i32>>,
    pub user_id: Arc<OnceCell<i32>>,
    pub fragments_dir: String,
    pub port: u16,
    pub test_mode: bool,
    pub orphaned_fragment_scan: Arc<std::sync::Mutex<Option<files::jobs::OrphanedFragmentScan>>>,
    pub iroh_transport: net::IrohTransport,
    pub consensus_barriers: Arc<barriers::Barriers>,
    pub dedup_cache: Arc<net::DedupCache>,
    pub session_store: Arc<auth::SessionStore>,
    /// Module-owned takeout/import runtime state (resume registry today;
    /// future Phase 5 worker pool etc.). Single Arc field instead of growing
    /// AppState's flat list; mirrors the per-module-runtime grouping that
    /// consensus/net/files will adopt in a follow-up refactor.
    pub takeout_runtime: Arc<takeout::TakeoutRuntime>,
    pub consensus_queue: consensus::queue::ConsensusQueue,
    pub write_gate: Arc<db::write_gate::WriteGate>,
    pub local_state_tx: tokio::sync::mpsc::Sender<db::write_gate::LocalStateUpdate>,
    /// Malachite engine handle — set by `spawn_engine` (startup restart path,
    /// genesis setup, or join bootstrap). Empty until the node is initialized;
    /// consensus dispatch answers "not active" meanwhile.
    pub malachite: Arc<OnceCell<consensus::malachite::EngineHandle>>,
    /// Storage distribution engine handle (RFC-014) — set by
    /// `files::substrate_host::spawn_storage_engine` at consensus engine
    /// start (mirrors `.malachite`). HopNetApplication::on_decided kicks it
    /// with decided blob ids (non-blocking); the engine's workers and
    /// placement batcher run behind the host seams.
    pub storage: Arc<OnceCell<hopnet_storage::engine::EngineHandle>>,
}

impl AppState {
    pub fn get_node_id(&self) -> Result<i32, axum::http::StatusCode> {
        self.node_id
            .get()
            .copied()
            .ok_or(axum::http::StatusCode::PRECONDITION_REQUIRED)
    }

    pub fn get_user_id(&self) -> Result<i32, axum::http::StatusCode> {
        self.user_id
            .get()
            .copied()
            .ok_or(axum::http::StatusCode::PRECONDITION_REQUIRED)
    }

    pub async fn get_session(
        &self,
        user_id: i32,
    ) -> Result<auth::SessionEntry, axum::http::StatusCode> {
        let store = self.session_store.read().await;
        match store.get(&user_id) {
            Some(entry) if entry.expires_at > chrono::Utc::now() => Ok(entry.clone()),
            Some(_) => Err(axum::http::StatusCode::UNAUTHORIZED),
            None => Err(axum::http::StatusCode::PRECONDITION_REQUIRED),
        }
    }
}

pub static DISPATCH_TABLE: Lazy<HashMap<&'static str, &'static dyn TransactionHandler>> =
    Lazy::new(|| {
        tracing::debug!("Building dispatch table from registered handlers");
        let mut table = HashMap::new();
        for handler in inventory::iter::<&'static dyn TransactionHandler> {
            tracing::debug!("Registering handler: {}", handler.name());
            table.insert(handler.name(), *handler);
        }
        table
    });

/// RFC-015 boot tripwire: cross-crate inventory registrations are a
/// link-time property — a silently dropped registration would make every
/// node reject the projection's transactions (fail-stop drive outage).
/// Call at startup, after DISPATCH_TABLE is built.
pub fn assert_projection_registrations() {
    for f in hopnet_drive::handlers::TX_FUNCTIONS {
        assert!(
            DISPATCH_TABLE.contains_key(f),
            "projection handler '{f}' missing from dispatch table — inventory registration dropped at link time"
        );
    }
    let providers =
        inventory::iter::<&'static dyn crate::reference_providers::DataBlockReferenceProvider>
            .into_iter()
            .count();
    assert!(
        providers >= 1,
        "no DataBlockReferenceProvider registered — GC would collect referenced blobs"
    );
}
