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
pub mod capabilities;
pub mod fileprovider;
pub mod storage_host;
pub mod handlers;
pub mod metrics;
pub mod net;
pub mod nodes;
pub mod passphrase;
pub mod projections;
pub mod photos_host;
pub mod reference_providers;
pub mod setup;
pub mod shares;
pub mod takeout_host;
pub mod types;
pub mod users;
pub mod views;

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
    pub orphaned_fragment_scan:
        Arc<std::sync::Mutex<Option<hopnet_storage::maintenance::OrphanedFragmentScan>>>,
    pub comms: hopnet_comms::IrohComms,
    /// Whether this node has completed setup (genesis or JoinInfo received).
    /// Shared with the comms peer directory — when false, all incoming
    /// connections are allowed (the brief pre-init window).
    pub setup_complete: Arc<std::sync::atomic::AtomicBool>,
    pub consensus_barriers: Arc<barriers::Barriers>,
    pub session_store: Arc<auth::SessionStore>,
    /// Module-owned takeout/import runtime state (resume registry +
    /// barriers), crate-owned since RFC-015 Stage D5b. Single Arc shared by
    /// every on-the-fly `takeout_host::takeout_state` construction.
    pub takeout_runtime: Arc<hopnet_takeout::TakeoutRuntime>,
    pub consensus_queue: consensus::queue::ConsensusQueue,
    pub write_gate: Arc<db::write_gate::WriteGate>,
    pub local_state_tx: tokio::sync::mpsc::Sender<db::write_gate::LocalStateUpdate>,
    /// Malachite engine handle — set by `spawn_engine` (startup restart path,
    /// genesis setup, or join bootstrap). Empty until the node is initialized;
    /// consensus dispatch answers "not active" meanwhile.
    pub malachite: Arc<OnceCell<consensus::malachite::EngineHandle>>,
    /// Storage distribution engine handle (RFC-014) — set by
    /// `storage_host::substrate_host::spawn_storage_engine` at consensus engine
    /// start (mirrors `.malachite`). HopNetApplication::on_decided kicks it
    /// with decided blob ids (non-blocking); the engine's workers and
    /// placement batcher run behind the host seams.
    pub storage: Arc<OnceCell<hopnet_storage::engine::EngineHandle>>,
    /// Per-peer liveness evidence (RFC-CONSENSUS-002 S3). One writer lock
    /// per authenticated exchange; classification is pure over snapshots.
    pub evidence: Arc<consensus::evidence::EvidenceMap>,
    /// The MAIN multi-thread runtime's handle, captured at startup.
    /// Host-side background work scheduled from consensus apply
    /// (`HostWorkScheduler`) spawns here — apply runs on the consensus
    /// shell's dedicated `current_thread` runtime (no IO driver, and any
    /// blocking work there stalls consensus), so `tokio::spawn` from apply
    /// must not land on the ambient runtime.
    pub runtime: tokio::runtime::Handle,
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

/// RFC-015/016 boot tripwire: cross-crate inventory registrations are a
/// link-time property — a silently dropped registration would make every
/// node reject the projection's transactions (fail-stop outage). Loops
/// the projection registry, plus the host's own storage-substrate
/// handlers (below the projection seam, so not in a manifest).
/// Call at startup, after DISPATCH_TABLE is built.
pub fn assert_projection_registrations() {
    for projection in crate::projections::manifests() {
        for f in projection.tx_functions() {
            assert!(
                DISPATCH_TABLE.contains_key(f),
                "{} handler '{f}' missing from dispatch table — inventory registration dropped at link time",
                projection.name()
            );
        }
    }
    for f in crate::storage_host::handlers::TX_FUNCTIONS {
        assert!(
            DISPATCH_TABLE.contains_key(f),
            "storage handler '{f}' missing from dispatch table — inventory registration dropped at link time"
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
