use aes_siv::{siv::Aes256Siv, Key, Nonce};
use axum::{
    extract::DefaultBodyLimit, http::{HeaderValue, Method, StatusCode}, middleware, routing::{get,post,put,delete}, serve, Router
};
use jsonwebtoken::{DecodingKey, EncodingKey};
use tower_serve_static::ServeDir;
use tower_http::cors::CorsLayer;
use include_dir::{Dir, include_dir};
use once_cell::sync::{Lazy, OnceCell};
use std::sync::Arc;
use std::collections::HashMap;
use duckdb::DuckdbConnectionManager;
use r2d2::Pool;
use apalis::prelude::*;
use std::str::FromStr;

use crate::{db::{PrivKey, PubKey}, handlers::TransactionHandler};

mod nodes;
mod setup;
mod users;
mod metrics;
mod db;
mod interfaces;
mod auth;
mod consensus;
mod types;
mod handlers;
mod files;

static ASSETS_DIR: Dir<'static> = include_dir!("$CARGO_MANIFEST_DIR/frontend/dist");

#[derive(Clone)]
pub struct UserKeys {
    pub private_key: PrivKey,
    pub public_key: PubKey,
}

#[derive(Clone)]
pub struct AppState {
    db_pool: Pool<DuckdbConnectionManager>,
    encoding_key: EncodingKey,
    decoding_key: DecodingKey,
    private_key: PrivKey,
    public_key: PubKey,
    node_id: Arc<OnceCell<i32>>,
    user_id: Arc<OnceCell<i32>>,
    user_keys: Arc<OnceCell<UserKeys>>,
    siv_key: Arc<OnceCell<Key<Aes256Siv>>>,
    siv_nonce: Arc<OnceCell<Nonce>>,
    fragments_dir: String,
    timeout_vote_collector: Arc<consensus::functions::TimeoutVoteCollector>,
    last_observed_view: Arc<std::sync::atomic::AtomicI32>,
}

impl AppState {
    pub fn get_user_keys(&self) -> Result<&UserKeys, StatusCode> {
        self.user_keys.get().ok_or(StatusCode::PRECONDITION_REQUIRED)
    }
    
    pub fn get_node_id(&self) -> Result<i32, StatusCode> {
        self.node_id.get().copied().ok_or(StatusCode::PRECONDITION_REQUIRED)
    }
    
    pub fn get_user_id(&self) -> Result<i32, StatusCode> {
        self.user_id.get().copied().ok_or(StatusCode::PRECONDITION_REQUIRED)
    }
    
    pub fn get_siv_key(&self) -> Result<&Key<Aes256Siv>, StatusCode> {
        self.siv_key.get().ok_or(StatusCode::PRECONDITION_REQUIRED)
    }
    
    pub fn get_siv_nonce(&self) -> Result<&Nonce, StatusCode> {
        self.siv_nonce.get().ok_or(StatusCode::PRECONDITION_REQUIRED)
    }
    
    pub fn initialize_siv_keys(&self) -> Result<(), StatusCode> {
        let user_keys = self.get_user_keys()?;
        let (siv_key, siv_nonce) = auth::derive_siv_key_from_user(&user_keys.private_key, "file_path");
        
        self.siv_key.set(siv_key).map_err(|_| StatusCode::CONFLICT)?;
        self.siv_nonce.set(siv_nonce).map_err(|_| StatusCode::CONFLICT)?;
        
        Ok(())
    }
}

static DISPATCH_TABLE: Lazy<HashMap<&'static str, &'static dyn TransactionHandler>> = Lazy::new(|| {
    tracing::debug!("Building dispatch table from registered handlers");
    let mut table = HashMap::new();
    // iterate over the globally collected handlers
    for handler in inventory::iter::<&'static dyn TransactionHandler> {
        tracing::debug!("Registering handler: {}", handler.name());
        table.insert(handler.name(), *handler);
    }
    table
});

#[tokio::main]
async fn main() {
    // tracing
    tracing_subscriber::fmt::init();

    let admin_service = ServeDir::new(&ASSETS_DIR);

    // port selection by system
    let mut port = 34632;
    let os = std::env::consts::OS;
    if os == "linux" {
        port = port + 1;
        tracing::info!("Running on Linux on port {}", port);
    }

    let bindurl = format!("0.0.0.0:{}", port);

    let (encodingkey, decodingkey) = auth::generate_jwt_key();
    let (privatekey, publickey) = consensus::functions::generate_ed25519_key();

    // Create database connection pool
    // Unwrapping since unsuccessful means failed startup anyway
    let manager = DuckdbConnectionManager::memory().unwrap();
    let pool = Pool::builder()
        .max_size(8)         // 8 concurrent connections
        .min_idle(Some(2))   // Keep 2 connections warm
        .build(manager).unwrap();
    
    // Initialize database schema
    let conn = pool.get().unwrap();
    match db::shared::initialize(conn) {
        Ok(()) => {
            // Initialize fragments directory
            let fragments_dir = files::functions::get_fragments_dir().unwrap_or_else(|_| {
                eprintln!("Failed to get fragments directory, using current directory");
                "./hopnet/fragments".to_string()
            });
            
            let app_state = AppState {
                db_pool: pool,
                encoding_key: encodingkey,
                decoding_key: decodingkey,
                private_key: PrivKey(privatekey),
                public_key: PubKey(publickey),
                node_id: Arc::new(OnceCell::new()),
                user_id: Arc::new(OnceCell::new()),
                user_keys: Arc::new(OnceCell::new()),
                siv_key: Arc::new(OnceCell::new()),
                siv_nonce: Arc::new(OnceCell::new()),
                fragments_dir,
                timeout_vote_collector: Arc::new(consensus::functions::TimeoutVoteCollector::new()),
                last_observed_view: Arc::new(std::sync::atomic::AtomicI32::new(-1)),
            };

            // Start timeout detection worker with cron schedule (every minute)
            let schedule = apalis_cron::Schedule::from_str("0 * * * * *").unwrap(); // Every minute
            let cron_stream = apalis_cron::CronStream::new(schedule);
            
            let timeout_worker = WorkerBuilder::new("timeout-detection")
                .data(app_state.clone())
                .backend(cron_stream)
                .build_fn(consensus::jobs::handle_timeout_detection);
            
            tokio::spawn(async move {
                timeout_worker.run().await;
            });

            // Protected routes that require authentication
            let protected_routes = Router::new()
                .route("/users", get(users::routes::get_users))
                .route("/users", post(users::routes::post_users))
                .route("/nodes", get(nodes::get_nodes))
                .route("/nodes", post(nodes::post_nodes))
                .route("/files", get(files::routes::get_files))
                .route("/files", post(files::routes::post_files)).layer(DefaultBodyLimit::max(5000*1_000_000))
                .route("/files", delete(files::routes::delete_files))
                .route("/files/{*path}", get(files::routes::get_file_fragments))
                .route("/validators", get(consensus::routes::get_validators))
                .layer(middleware::from_fn_with_state(app_state.clone(), auth::auth_middleware));

            // Routes that accept either JWT (users) or RPC (nodes) authentication
            let jwt_or_rpc_routes = Router::new()
                .route("/consensus", get(consensus::routes::get_consensus))
                .layer(middleware::from_fn_with_state(app_state.clone(), consensus::routes::jwt_or_rpc_auth_middleware));

            // RPC routes for inter-node communication with dual signature authentication
            let rpc_routes = Router::new()
                .route("/consensus/propose", post(consensus::routes::post_propose))
                .route("/consensus/view/{view}", get(consensus::routes::get_view_consensus_data))
                .layer(middleware::from_fn_with_state(app_state.clone(), consensus::routes::rpc_auth_middleware));

            // Strict catch-up routes (must be fully caught up)
            let strict_consensus_routes = Router::new()
                .route("/ballot", post(consensus::routes::post_ballot))
                .layer(middleware::from_fn_with_state(app_state.clone(), consensus::routes::ensure_caught_up_middleware))
                .layer(axum::Extension(consensus::routes::CatchUpStrictness::Strict));

            // Lenient catch-up routes (allow 1 view behind)
            let lenient_consensus_routes = Router::new()
                .route("/qc", post(consensus::routes::post_qc))
                .route("/consensus/tc", post(consensus::routes::post_tc))
                .layer(middleware::from_fn_with_state(app_state.clone(), consensus::routes::ensure_caught_up_middleware))
                .layer(axum::Extension(consensus::routes::CatchUpStrictness::Lenient));

            let base_app = Router::new()
                .fallback_service(admin_service) // routes we don't have get sent to vite frontend
                .route("/metrics/get-all", get(metrics::routes::get_metrics))
                .merge(protected_routes)
                .merge(jwt_or_rpc_routes)
                .merge(rpc_routes)
                .merge(strict_consensus_routes)
                .merge(lenient_consensus_routes)
                .route("/setup", get(setup::get_setup))
                .route("/setup", post(setup::post_setup))
                .route("/setup", put(setup::put_setup))
                .route("/interfaces", get(interfaces::get_interfaces))
                .route("/rpc/latency-server", get(metrics::routes::get_latency_server))
                .route("/rpc/get-remote-latency", get(metrics::routes::get_remote_latency_handler))
                .route("/login", post(auth::sign_in))
                .route("/consensus/timeout_vote", post(consensus::routes::post_timeout_vote));

            let app = if cfg!(debug_assertions) {
                let cors = CorsLayer::new()
                    .allow_origin("http://localhost:5173".parse::<HeaderValue>().unwrap()) // allow vite dev
                    .allow_methods([Method::GET, Method::POST, Method::DELETE])
                    .allow_headers([axum::http::header::CONTENT_TYPE, axum::http::header::AUTHORIZATION])
                    .max_age(std::time::Duration::from_secs(3600))
                    .allow_credentials(false);

                base_app
                    .layer(cors)
                    .with_state(app_state)
            } else {
                base_app // no CORS in prod
                    .with_state(app_state)
            };

            match tokio::net::TcpListener::bind(&bindurl).await {
                Ok(listener) => {
                    tracing::info!("Server starting on {}", bindurl);
                    serve(listener, app).await.unwrap();
                }
                Err(error) => {panic!("{}", error)}
            }
        }
        Err(error) => {panic!{"{}", error}}
    }
    

}
