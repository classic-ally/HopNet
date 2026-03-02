use jsonwebtoken::{DecodingKey, EncodingKey};
use once_cell::sync::{Lazy, OnceCell};
use std::collections::HashMap;
use std::sync::Arc;

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;

use crate::db::{PrivKey, PubKey};
use crate::handlers::TransactionHandler;

pub mod nodes;
pub mod setup;
pub mod users;
pub mod metrics;
pub mod db;
pub mod auth;
pub mod consensus;
pub mod types;
pub mod handlers;
pub mod files;
pub mod fileprovider;
pub mod documentprovider;
pub mod takeout;
pub mod admin;
pub mod devices;
pub mod net;
pub mod passphrase;
pub mod shares;
pub mod reference_providers;

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
    pub timeout_vote_collector: Arc<consensus::functions::TimeoutVoteCollector>,
    pub catch_up_state: Arc<consensus::catch_up_state::CatchUpState>,
    pub consensus_lock: Arc<tokio::sync::Mutex<()>>,
    pub fileprovider_api_key: String,
    pub port: u16,
    pub test_mode: bool,
    pub orphaned_fragment_scan: Arc<std::sync::Mutex<Option<files::jobs::OrphanedFragmentScan>>>,
    pub iroh_transport: net::IrohTransport,
    pub consensus_barriers: Arc<consensus::barriers::ConsensusBarriers>,
    pub dedup_cache: Arc<net::DedupCache>,
    pub lock_vote_evidence: Arc<std::sync::Mutex<Option<consensus::types::LockVoteEvidence>>>,
    pub session_store: Arc<auth::SessionStore>,
    pub consensus_queue: consensus::queue::ConsensusQueue,
    pub view_changed: Arc<tokio::sync::Notify>,
    pub write_gate: Arc<db::write_gate::WriteGate>,
    pub local_state_tx: tokio::sync::mpsc::Sender<db::write_gate::LocalStateUpdate>,
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
