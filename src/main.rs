use axum::{
    extract::DefaultBodyLimit, http::{HeaderValue, Method, StatusCode}, middleware, routing::{get,post,patch,delete}, serve, Router
};
use jsonwebtoken::{DecodingKey, EncodingKey};
use tower_serve_static::ServeDir;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use include_dir::{Dir, include_dir};
use once_cell::sync::{Lazy, OnceCell};
use std::sync::Arc;
use std::collections::HashMap;
use duckdb::{Config, DuckdbConnectionManager};
use r2d2::Pool;
use apalis::prelude::*;
use std::str::FromStr;

use chrono::Utc;
use crate::{db::{PrivKey, PubKey}, handlers::TransactionHandler};

mod nodes;
mod setup;
mod users;
mod metrics;
mod db;
mod auth;
mod consensus;
mod types;
mod handlers;
mod files;
mod fileprovider;
mod documentprovider;
mod takeout;
mod admin;
mod devices;
mod net;
mod passphrase;
mod shares;

static ASSETS_DIR: Dir<'static> = include_dir!("$CARGO_MANIFEST_DIR/frontend/dist");

/// Single source of truth for the backend HTTP port.
const BACKEND_PORT: u16 = 34632;

/// Global AppState accessible to Tauri IPC commands (GUI mode only).
/// Set by run_server() after AppState creation; read by Tauri commands.
#[cfg(feature = "gui")]
static GUI_APP_STATE: Lazy<tokio::sync::RwLock<Option<AppState>>> =
    Lazy::new(|| tokio::sync::RwLock::new(None));

#[derive(Clone, Debug)]
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
    fragments_dir: String,
    timeout_vote_collector: Arc<consensus::functions::TimeoutVoteCollector>,
    last_observed_view: Arc<std::sync::atomic::AtomicI32>,
    consensus_lock: Arc<tokio::sync::Mutex<()>>,
    fileprovider_api_key: String,
    port: u16,
    test_mode: bool,
    orphaned_fragment_scan: Arc<std::sync::Mutex<Option<files::jobs::OrphanedFragmentScan>>>,
    iroh_transport: net::IrohTransport,
    consensus_barriers: Arc<consensus::barriers::ConsensusBarriers>,
    dedup_cache: Arc<net::DedupCache>,
    lock_vote_evidence: Arc<std::sync::Mutex<Option<consensus::types::LockVoteEvidence>>>,
    session_store: Arc<auth::SessionStore>,
}

impl AppState {
    pub fn get_node_id(&self) -> Result<i32, StatusCode> {
        self.node_id.get().copied().ok_or(StatusCode::PRECONDITION_REQUIRED)
    }

    pub fn get_user_id(&self) -> Result<i32, StatusCode> {
        self.user_id.get().copied().ok_or(StatusCode::PRECONDITION_REQUIRED)
    }

    pub async fn get_session(&self, user_id: i32) -> Result<auth::SessionEntry, StatusCode> {
        let store = self.session_store.read().await;
        match store.get(&user_id) {
            Some(entry) if entry.expires_at > Utc::now() => Ok(entry.clone()),
            Some(_) => Err(StatusCode::UNAUTHORIZED),
            None => Err(StatusCode::PRECONDITION_REQUIRED),
        }
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

async fn run_server() -> Result<(), Box<dyn std::error::Error>> {
    // tracing
    tracing_subscriber::fmt::init();

    let admin_service = ServeDir::new(&ASSETS_DIR);

    let port = BACKEND_PORT;
    let bindurl = format!("0.0.0.0:{}", port);

    let (encodingkey, decodingkey) = auth::generate_jwt_key();

    // Check if FileProvider API key already exists in keychain (release builds only)
    let fileprovider_api_key = {
        #[cfg(all(target_os = "macos", not(debug_assertions)))]
        {
            // Only try to load from keychain in release builds to avoid permission prompts in development/CI
            match fileprovider::keychain::load_config(fileprovider::keychain::KeychainEnvironment::Production) {
                Ok(existing_config) => {
                    tracing::info!("Using existing FileProvider API key from keychain");
                    existing_config.api_key
                }
                Err(e) => {
                    tracing::info!("No existing FileProvider API key found ({}), generating new one", e);
                    auth::generate_fileprovider_api_key()
                }
            }
        }
        #[cfg(any(not(target_os = "macos"), debug_assertions))]
        {
            // Always generate fresh API key in debug builds or non-macOS to avoid keychain prompts
            tracing::info!("Generating fresh FileProvider API key (debug build or non-macOS)");
            auth::generate_fileprovider_api_key()
        }
    };

    // Check if ephemeral database mode is requested (for testing)
    let use_ephemeral_db = std::env::var("HOPNET_EPHEMERAL_DB").is_ok();

    // Create DuckDB config with extension autoloading disabled
    // We manually load signed extensions from the bundle instead
    let config = Config::default()
        .enable_autoload_extension(false)
        .expect("Failed to disable autoload extensions");

    let pool = if use_ephemeral_db {
        tracing::info!("Using ephemeral in-memory database (HOPNET_EPHEMERAL_DB set)");
        let manager = DuckdbConnectionManager::memory_with_flags(config).unwrap();
        Pool::builder()
            .max_size(8)
            .min_idle(Some(0))  // Don't keep idle connections - prevents stale reads after checkpoint
            .build(manager).unwrap()
    } else {
        // Get database path and ensure directory exists
        let db_path = db::shared::get_database_path();
        let db_file_exists = db::shared::database_exists(&db_path);

        if !db_file_exists {
            tracing::info!("Creating new database at {}", db_path);
            db::shared::ensure_database_dir(&db_path)
                .expect("Failed to create database directory");
        } else {
            tracing::info!("Found existing database file at {}", db_path);
        }

        // Create database connection pool (file-based)
        // WAL mode is automatically enabled for file-based DuckDB databases
        // Config disables extension autoloading to prevent macOS code signing issues
        let manager = DuckdbConnectionManager::file_with_flags(&db_path, config).unwrap();
        let pool = Pool::builder()
            .max_size(8)         // 8 concurrent connections
            .min_idle(Some(0))   // Don't keep idle connections - prevents stale reads after checkpoint
            .build(manager).unwrap();

        tracing::info!("Database connection pool established (WAL mode enabled, extension autoloading disabled)");
        pool
    };

    // Check if database schema is initialized
    let conn = pool.get().unwrap();

    let schema_initialized = if use_ephemeral_db {
        false  // Ephemeral database always needs initialization
    } else {
        db::shared::is_schema_initialized(&conn)
            .expect("Failed to check schema status")
    };

    let init_result = if schema_initialized {
        tracing::info!("Loading existing database schema");
        Ok(())
    } else {
        tracing::info!("Initializing new database schema");
        db::shared::initialize(conn)
    };

    match init_result {
        Ok(()) => {
            // Try to load existing state from database, or generate new keys if this is a new node
            let startup_state_opt = match db::consensus::get_startup_state(pool.get()) {
                Ok(state) => {
                    tracing::info!("Loaded existing state from database (node_id: {}, user_id: {})",
                        state.node_id, state.user_id);
                    Some(state)
                }
                Err(db::DatabaseError::RecallError) => {
                    tracing::info!("No existing state found, generating new Ed25519 key pairs");
                    None
                }
                Err(e) => {
                    panic!("Failed to check for existing state: {:?}", e);
                }
            };

            // Get node keys (either from loaded state or generate new)
            let (privatekey, publickey) = if let Some(ref state) = startup_state_opt {
                let pubkey = state.node_privkey.verifying_key();
                (state.node_privkey.0.clone(), pubkey)
            } else {
                consensus::functions::generate_ed25519_key()
            };

            // Initialize fragments directory
            let fragments_dir = files::functions::get_fragments_dir().unwrap_or_else(|_| {
                eprintln!("Failed to get fragments directory, using current directory");
                "./hopnet/fragments".to_string()
            });
            std::fs::create_dir_all(&fragments_dir)
                .expect("Failed to create fragments directory");

            // Create iroh transport
            let iroh_secret = PrivKey(privatekey.clone()).to_iroh_secret_key();
            let iroh_transport = net::IrohTransport::new(iroh_secret, pool.clone(), startup_state_opt.is_some()).await
                .expect("Failed to create iroh transport");
            tracing::info!("iroh endpoint ready, node_id: {}", iroh_transport.node_id());

            let app_state = AppState {
                db_pool: pool,
                encoding_key: encodingkey,
                decoding_key: decodingkey,
                private_key: PrivKey(privatekey),
                public_key: PubKey(publickey),
                node_id: Arc::new(OnceCell::new()),
                user_id: Arc::new(OnceCell::new()),
                fragments_dir,
                timeout_vote_collector: Arc::new(consensus::functions::TimeoutVoteCollector::new()),
                last_observed_view: Arc::new(std::sync::atomic::AtomicI32::new(-1)),
                consensus_lock: Arc::new(tokio::sync::Mutex::new(())),
                fileprovider_api_key: fileprovider_api_key.clone(),
                port,
                test_mode: cfg!(debug_assertions) || std::env::var("HOPNET_TEST_MODE").is_ok(),
                orphaned_fragment_scan: Arc::new(std::sync::Mutex::new(None)),
                iroh_transport: iroh_transport.clone(),
                consensus_barriers: Arc::new(consensus::barriers::ConsensusBarriers::new()),
                dedup_cache: Arc::new(net::DedupCache::default()),
                lock_vote_evidence: Arc::new(std::sync::Mutex::new(None)),
                session_store: Arc::new(auth::SessionStore::default()),
            };

            // If we loaded state from database, populate the OnceCell fields
            if let Some(state) = startup_state_opt {
                app_state.node_id.set(state.node_id)
                    .expect("Failed to set node_id in AppState");
                app_state.user_id.set(state.user_id)
                    .expect("Failed to set user_id in AppState");

                tracing::info!("AppState initialized from persisted database (user keys require login)");

                // GUI auto-login: load owner session from keychain if available
                #[cfg(all(target_os = "macos", feature = "gui", not(debug_assertions)))]
                {
                    if let Ok((kc_user_id, privkey_bytes)) = fileprovider::keychain::load_session_key(
                        fileprovider::keychain::KeychainEnvironment::Production,
                    ) {
                        if let Ok(key_bytes) = <[u8; 32]>::try_from(privkey_bytes.as_slice()) {
                            let privkey = db::PrivKey(ed25519_dalek::SigningKey::from_bytes(&key_bytes));
                            let pubkey = db::PubKey(privkey.verifying_key());
                            let (siv_key, siv_nonce) = auth::derive_siv_key_from_user(&privkey, "file_path");
                            let session = auth::SessionEntry {
                                user_keys: UserKeys { private_key: privkey, public_key: pubkey },
                                siv_key, siv_nonce,
                                expires_at: chrono::Utc::now() + chrono::Duration::hours(876000),
                            };
                            app_state.session_store.blocking_write().insert(kc_user_id, session);
                            tracing::info!("Loaded owner session from keychain (auto-login ready)");
                        }
                    }
                }
            }

            // Publish AppState for Tauri IPC commands (GUI mode)
            #[cfg(feature = "gui")]
            {
                *GUI_APP_STATE.write().await = Some(app_state.clone());
            }

            tracing::info!("FileProvider API key: {}", fileprovider_api_key);

            // Warn about test mode being enabled
            if cfg!(debug_assertions) {
                tracing::warn!("⚠️  TEST MODE ENABLED: Test endpoints are exposed and will return API credentials");
                tracing::warn!("⚠️  This is a SECURITY RISK in production - test mode is automatically disabled in release builds");
            }

            // Store FileProvider configuration in keychain for Swift extension (release builds only)
            #[cfg(all(target_os = "macos", not(debug_assertions)))]
            {
                let base_url = format!("http://localhost:{}", port);
                let keychain_config = fileprovider::keychain::FileProviderConfig::new(
                    fileprovider_api_key.clone(),
                    base_url,
                );
                
                if let Err(e) = fileprovider::keychain::store_config(&keychain_config, fileprovider::keychain::KeychainEnvironment::Production) {
                    tracing::warn!("Failed to store FileProvider configuration in keychain: {}", e);
                } else {
                    tracing::info!("FileProvider configuration stored/updated in keychain");
                }
            }
            
            #[cfg(all(target_os = "macos", debug_assertions))]
            {
                tracing::info!("Skipping keychain storage in debug build - use environment variables or run release build for keychain support");
            }

            // Initialize FileProvider on startup - only cleans up if app is uninitialized (release builds only)
            #[cfg(all(target_os = "macos", not(debug_assertions)))]
            {
                if let Err(e) = fileprovider::domain::initialize_fileprovider_on_startup(&app_state).await {
                    tracing::warn!("Failed to initialize FileProvider on startup: {}", e);
                } else {
                    tracing::info!("FileProvider startup initialization completed");
                }
            }
            
            #[cfg(all(target_os = "macos", debug_assertions))]
            {
                tracing::info!("Skipping FileProvider domain initialization in debug build - testing API endpoints only");
            }

            // Start timeout detection worker with randomized cron schedule (every minute at random second)
            use rand::Rng;
            let random_second = rand::rng().random_range(5..55);
            let cron_expression = format!("{} * * * * *", random_second);
            let schedule = apalis_cron::Schedule::from_str(&cron_expression).unwrap();
            let cron_stream = apalis_cron::CronStream::new(schedule);
            
            let timeout_worker = WorkerBuilder::new("timeout-detection")
                .data(app_state.clone())
                .backend(cron_stream)
                .build_fn(consensus::jobs::handle_timeout_detection);
            
            tokio::spawn(async move {
                timeout_worker.run().await;
            });

            // Start metrics collection worker with randomized 10-minute schedule
            let random_second = rand::rng().random_range(5..55);
            let random_minute = rand::rng().random_range(0..10); // 0-9 minutes offset within each 10-minute window
            let metrics_cron_expression = format!("{} {}/10 * * * *", random_second, random_minute);
            let metrics_schedule = apalis_cron::Schedule::from_str(&metrics_cron_expression).unwrap();
            let metrics_cron_stream = apalis_cron::CronStream::new(metrics_schedule);
            
            let metrics_worker = WorkerBuilder::new("metrics-collection")
                .data(app_state.clone())
                .backend(metrics_cron_stream)
                .build_fn(metrics::jobs::handle_metrics_collection);
            
            tokio::spawn(async move {
                metrics_worker.run().await;
            });

            // Start takeout maintenance worker with randomized 4-6 hour schedule
            let random_second = rand::rng().random_range(5..55);
            let random_minute = rand::rng().random_range(0..60);
            let random_hour_offset = rand::rng().random_range(4..7); // 4-6 hours
            let takeout_cron_expression = format!("{} {} */{} * * *", random_second, random_minute, random_hour_offset);
            let takeout_schedule = apalis_cron::Schedule::from_str(&takeout_cron_expression).unwrap();
            let takeout_cron_stream = apalis_cron::CronStream::new(takeout_schedule);

            let takeout_worker = WorkerBuilder::new("takeout-maintenance")
                .data(app_state.clone())
                .backend(takeout_cron_stream)
                .build_fn(takeout::jobs::handle_takeout_maintenance);

            tokio::spawn(async move {
                takeout_worker.run().await;
            });

            // Start fragment inventory self-check worker with randomized 20-30 minute schedule
            let random_second = rand::rng().random_range(5..55);
            let random_minute = rand::rng().random_range(0..30); // 0-29 minutes offset within each 30-minute window
            let self_check_cron_expression = format!("{} {}/30 * * * *", random_second, random_minute);
            let self_check_schedule = apalis_cron::Schedule::from_str(&self_check_cron_expression).unwrap();
            let self_check_cron_stream = apalis_cron::CronStream::new(self_check_schedule);

            let self_check_worker = WorkerBuilder::new("fragment-inventory-self-check")
                .data(app_state.clone())
                .backend(self_check_cron_stream)
                .build_fn(files::jobs::handle_fragment_inventory_self_check);

            tokio::spawn(async move {
                self_check_worker.run().await;
            });

            // Start iroh accept loop for incoming connections
            {
                let endpoint = iroh_transport.endpoint().clone();
                let app_state_clone = app_state.clone();
                tokio::spawn(async move {
                    net::handler::handle_incoming_connections(endpoint, app_state_clone).await;
                });
            }

            // Protected routes that require authentication
            let protected_routes = Router::new()
                .route("/users", get(users::routes::get_users))
                .route("/users", post(users::routes::post_users))
                .route("/nodes", get(nodes::routes::get_nodes))
                .route("/nodes", post(nodes::routes::post_nodes))
                .route("/files", get(files::routes::get_files).patch(files::routes::patch_files))
                .route("/files", post(files::routes::post_files)).layer(DefaultBodyLimit::max(5000*1_000_000))
                .route("/files", delete(files::routes::delete_files))
                .route("/files/{*path}", get(files::routes::get_file_fragments))
                .route("/fragments", get(files::routes::get_fragments_count))
                .route("/maintenance/cleanup-orphaned", post(files::routes::post_cleanup_orphaned_data_blocks))
                .route("/maintenance/rebalance", post(files::routes::post_rebalance_network))
                .route("/maintenance/takeout", post(takeout::routes::post_takeout_maintenance))
                .route("/maintenance/fragment-inventory-self-check", post(files::routes::post_fragment_inventory_self_check))
                .route("/maintenance/orphaned-fragments", get(files::routes::get_orphaned_fragments_scan).delete(files::routes::delete_orphaned_fragments))
                .route("/diagnostics/fragment-inventory-differential", get(files::routes::get_fragment_inventory_differential))
                .route("/diagnostics/file-fragments", get(files::routes::get_file_fragment_distribution))
                .route("/diagnostics/network-resilience", get(files::routes::get_network_resilience_stats))
                .route("/debug/iroh-ping", get(net::routes::debug_iroh_ping))
                .route("/validators", get(consensus::routes::get_validators))
                .route("/metrics", get(metrics::routes::get_metrics))
                .route("/metrics/trigger", get(metrics::routes::get_metrics_trigger))
                .route("/metrics/scores", get(metrics::routes::get_placement_scores))
                .nest("/takeout", takeout::takeout_routes())
                .nest("/admin", admin::routes::admin_routes())
                .nest("/shares", shares::routes::router())
                .route("/logout", post(auth::sign_out))
                .layer(middleware::from_fn_with_state(app_state.clone(), auth::auth_middleware));

            // State snapshot endpoint requires DuckDB JSON extension (not codesigned for macOS release)
            #[cfg(any(not(target_os = "macos"), debug_assertions))]
            let protected_routes = protected_routes.route("/debug/state", get(consensus::routes::get_state_snapshot));

            // Routes that accept either JWT (users) or RPC (nodes) authentication
            let jwt_or_rpc_routes = Router::new()
                .route("/consensus", get(consensus::routes::get_consensus))
                .route("/consensus/history", get(consensus::routes::get_consensus_history))
                .route("/consensus/view", post(consensus::routes::debug_view_state))
                .layer(middleware::from_fn_with_state(app_state.clone(), consensus::routes::jwt_or_rpc_auth_middleware));

            // FileProvider routes with scoped authentication (all routes require auth)
            let fileprovider_routes = Router::new()
                .route("/health", get(fileprovider::routes::get_health))
                .route("/enumerate", get(fileprovider::routes::get_enumerate))
                .route("/changes", get(fileprovider::routes::get_changes))
                .route("/delete", delete(fileprovider::routes::delete_item))
                .route("/download", get(fileprovider::routes::download_file))
                .route("/item", get(fileprovider::routes::get_item))
                .route("/create", post(fileprovider::routes::create_item))
                .route("/modify", patch(fileprovider::routes::modify_item))
                .layer(DefaultBodyLimit::max(5000*1_000_000)) // 5GB limit for file uploads
                .layer(middleware::from_fn_with_state(app_state.clone(), fileprovider::auth::fileprovider_auth_middleware));

            // Test routes - only available in test mode
            let test_routes = if app_state.test_mode {
                Router::new()
                    .route("/integrations/fileprovider/test", get(fileprovider::routes::get_test))
                    .route("/integrations/fileprovider/test/signals", get(fileprovider::routes::get_test_signals))
                    .route("/test/fragment-health-check/{fragment_hash}", get(files::test_routes::get_fragment_health_check))
                    .nest("/test", consensus::barriers::test_routes())
            } else {
                Router::new() // Empty router when not in test mode
            };

            let base_app = Router::new()
                .fallback_service(admin_service) // routes we don't have get sent to vite frontend
                .merge(protected_routes)
                .merge(jwt_or_rpc_routes)
                .nest("/integrations/fileprovider", fileprovider_routes)
                .nest("/integrations/documentprovider", documentprovider::routes::router(app_state.clone()))
                .nest("/devices", devices::routes::router(app_state.clone()))
                .merge(test_routes)
                .route("/setup", get(setup::get_setup))
                .route("/setup", post(setup::post_setup))
                .route("/login", post(auth::sign_in));

            // Create trace layer with request IDs
            let trace_layer = TraceLayer::new_for_http()
                .make_span_with(|request: &axum::http::Request<_>| {
                    let id = hopnet_common::CustomUUID::new(None);
                    tracing::info_span!(
                        "api_req",
                        id = &id.to_string()[28..],
                        method = %request.method(),
                        uri = %request.uri(),
                    )
                });

            let app = if cfg!(debug_assertions) {
                let cors = CorsLayer::new()
                    .allow_origin("http://localhost:5173".parse::<HeaderValue>().unwrap()) // allow vite dev
                    .allow_methods([Method::GET, Method::POST, Method::DELETE])
                    .allow_headers([axum::http::header::CONTENT_TYPE, axum::http::header::AUTHORIZATION])
                    .max_age(std::time::Duration::from_secs(3600))
                    .allow_credentials(false);

                base_app
                    .layer(cors)
                    .layer(trace_layer)
                    .with_state(app_state)
            } else {
                base_app // no CORS in prod
                    .layer(trace_layer)
                    .with_state(app_state)
            };

            let listener = tokio::net::TcpListener::bind(&bindurl).await?;
            tracing::info!("Server starting on {}", bindurl);
            serve(listener, app).await?;
        }
        Err(error) => return Err(error.into()),
    }
    
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "gui")]
    {
        run_with_gui().await
    }
    
    #[cfg(not(feature = "gui"))]
    {
        run_server().await
    }
}

/// Tauri IPC command for GUI auto-login.
/// Only callable from the Tauri webview — not exposed over HTTP.
/// Issues a JWT if the node owner has a pre-loaded session (from keychain).
#[cfg(feature = "gui")]
#[tauri::command]
async fn auto_login() -> Result<auth::SignInResponse, String> {
    let state_guard = GUI_APP_STATE.read().await;
    let app_state = state_guard.as_ref().ok_or("Server not ready")?;

    let node_id = app_state.get_node_id().map_err(|_| "Node not initialized".to_string())?;
    let user_id = app_state.get_user_id().map_err(|_| "Node not initialized".to_string())?;

    // Verify owner session exists (pre-loaded from keychain at startup)
    app_state.get_session(user_id).await.map_err(|_| "No owner session available".to_string())?;

    let token = auth::encode_jwt_with_duration(
        node_id.to_string(), user_id.to_string(),
        app_state.encoding_key.clone(), 24,
    ).map_err(|_| "JWT encoding failed".to_string())?;

    Ok(auth::SignInResponse { token })
}

#[cfg(feature = "gui")]
async fn run_with_gui() -> Result<(), Box<dyn std::error::Error>> {
    use tauri::{Manager, menu::{Menu, MenuItem, PredefinedMenuItem}, tray::TrayIconBuilder, TitleBarStyle, WebviewWindowBuilder};
    
    // Helper function to create and configure the main window
    fn create_main_window(app: &tauri::AppHandle, port: u16) -> Result<tauri::WebviewWindow, Box<dyn std::error::Error>> {
        let win_builder = WebviewWindowBuilder::new(app, "main", tauri::WebviewUrl::default())
            .title("HopNet")
            .inner_size(1200.0, 800.0);
        
        // Set transparent title bar only when building for macOS
        #[cfg(target_os = "macos")]
        let win_builder = win_builder.title_bar_style(TitleBarStyle::Transparent);
        
        let window = win_builder.build()?;
        
        // Set background color only when building for macOS
        #[cfg(target_os = "macos")]
        {
            use cocoa::appkit::{NSColor, NSWindow};
            use cocoa::base::{id, nil};
            
            let ns_window = window.ns_window().unwrap() as id;
            unsafe {
                let bg_color = NSColor::colorWithRed_green_blue_alpha_(
                    nil,
                    17.0 / 255.0,   // #11111b red component (crust)
                    17.0 / 255.0,   // #11111b green component (crust)
                    27.0 / 255.0,   // #11111b blue component (crust)
                    1.0,
                );
                ns_window.setBackgroundColor_(bg_color);
            }
        }
        
        // Load the local server
        let url = format!("http://localhost:{}", port);
        window.navigate(url.parse()?)?;
        
        Ok(window)
    }
    
    // Start the server in a background task
    let server_handle = tokio::spawn(async {
        if let Err(e) = run_server().await {
            tracing::error!("Server error: {}", e);
        }
    });
    
    // Give the server a moment to start
    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
    
    let port = BACKEND_PORT;
    
    // Create Tauri app
    let context = tauri::generate_context!();
    let app = tauri::Builder::default()
        .invoke_handler(tauri::generate_handler![auto_login])
        .setup(move |app| {
            // Use Accessory policy to never show in dock
            #[cfg(target_os = "macos")]
            app.set_activation_policy(tauri::ActivationPolicy::Accessory);
            // Create tray menu with toggle item (text will be updated dynamically)
            let toggle_item = MenuItem::with_id(app, "toggle", "Toggle window", true, None::<&str>)?;
            let separator = PredefinedMenuItem::separator(app)?;
            let quit_item = MenuItem::with_id(app, "quit", "Quit", true, None::<&str>)?;
            
            let menu = Menu::with_items(app, &[
                &toggle_item,
                &separator,
                &quit_item,
            ])?;
            
            // Create tray icon with proper error handling
            let mut tray_builder = TrayIconBuilder::with_id("main_tray")
                .menu(&menu);

            // Load tray icon from icon.png (different from app icon)
            if let Some(resource_path) = app.path().resource_dir().ok() {
                let tray_icon_path = resource_path.join("icons/icon.png");
                if let Ok(icon) = tauri::image::Image::from_path(&tray_icon_path) {
                    tray_builder = tray_builder.icon(icon);
                }
            }
            
            let toggle_item_ref = toggle_item.clone();
            let port_for_toggle = port;
            let _tray = tray_builder
                .on_menu_event(move |app, event| {
                    match event.id.0.as_str() {
                        "toggle" => {
                            if let Some(window) = app.get_webview_window("main") {
                                // Window exists, close it to free resources
                                if let Err(e) = window.close() {
                                    tracing::error!("Failed to close window: {}", e);
                                }
                            } else {
                                // Window doesn't exist, create it
                                match create_main_window(app, port_for_toggle) {
                                    Ok(window) => {
                                        // Show and focus
                                        if let Err(e) = window.show() {
                                            tracing::error!("Failed to show window: {}", e);
                                        }
                                        if let Err(e) = window.set_focus() {
                                            tracing::error!("Failed to focus window: {}", e);
                                        }
                                    }
                                    Err(e) => {
                                        tracing::error!("Failed to create window: {}", e);
                                    }
                                }
                            }
                        }
                        "quit" => {
                            app.exit(0);
                        }
                        _ => {}
                    }
                })
                .on_tray_icon_event({
                    let app_handle = app.handle().clone();
                    move |_tray, event| {
                        // Update menu text when tray is right-clicked (before menu shows)
                        if let tauri::tray::TrayIconEvent::Click { button: tauri::tray::MouseButton::Right, .. } = event {
                            let text = if app_handle.get_webview_window("main").is_some() {
                                "Close window"
                            } else {
                                "Open window"  
                            };
                            if let Err(e) = toggle_item_ref.set_text(text) {
                                tracing::error!("Failed to update menu text: {}", e);
                            }
                        }
                    }
                })
                .build(app)?;
            
            // Create the main window using the helper function
            let window = create_main_window(&app.handle(), port)?;
            
            // Start visible (no dock icon due to Accessory policy)
            window.show()?;
            window.set_focus()?;
            
            Ok(())
        })
        .build(context)?;
    
    // Run the Tauri app (this blocks until the app is closed)
    app.run(|app_handle, event| {
        match event {
            tauri::RunEvent::ExitRequested { api, code, .. } => {
                // Only allow exit if explicitly requested (e.g., from tray menu)
                if code != Some(0) {
                    api.prevent_exit();
                }
            }
            tauri::RunEvent::WindowEvent { 
                label, 
                event: tauri::WindowEvent::CloseRequested { .. }, 
                .. 
            } => {
                // Window close requested - let it close but don't exit app
                if label == "main" {
                    // Window will be destroyed, app continues running
                }
            }
            tauri::RunEvent::MainEventsCleared => {
                // Check if we should exit (no windows and user chose quit)
                // App will keep running as long as tray is active
            }
            _ => {}
        }
    });
    
    // If we get here, the GUI was closed, so stop the server
    server_handle.abort();
    
    Ok(())
}
