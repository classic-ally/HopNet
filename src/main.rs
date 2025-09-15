use aes_siv::{siv::Aes256Siv, Key, Nonce};
use axum::{
    extract::DefaultBodyLimit, http::{HeaderValue, Method, StatusCode}, middleware, routing::{get,post,put,patch,delete}, serve, Router
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
mod fileprovider;

static ASSETS_DIR: Dir<'static> = include_dir!("$CARGO_MANIFEST_DIR/frontend/dist");

#[derive(Clone)]
pub struct UserKeys {
    pub private_key: PrivKey,
    pub public_key: PubKey,
}

/// Authentication info for inter-node requests
/// Populated once and reused to avoid repeated AppState getter calls
#[derive(Clone)]
pub struct NodeAuthInfo {
    pub node_id: i32,
    pub user_id: i32,
    pub user_keys: UserKeys,
    pub node_private_key: PrivKey,
}

impl NodeAuthInfo {
    /// Extract authentication info from AppState once for reuse
    pub fn from_app_state(app_state: &AppState) -> Result<Self, StatusCode> {
        let node_id = app_state.get_node_id()?;
        let user_id = app_state.get_user_id()?;
        let user_keys = app_state.get_user_keys()?.clone();
        let node_private_key = app_state.private_key.clone();
        
        Ok(NodeAuthInfo {
            node_id,
            user_id,
            user_keys,
            node_private_key,
        })
    }
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
    throughput_result_collector: Arc<metrics::functions::ThroughputResultCollector>,
    last_observed_view: Arc<std::sync::atomic::AtomicI32>,
    fileprovider_api_key: String,
    port: u16,
    test_mode: bool,
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

async fn run_server() -> Result<(), Box<dyn std::error::Error>> {
    // tracing
    tracing_subscriber::fmt::init();

    let admin_service = ServeDir::new(&ASSETS_DIR);

    // port selection by system and mode
    let mut port = 34632;
    let os = std::env::consts::OS;
    if os == "linux" {
        port = port + 1;
        tracing::info!("Running on Linux on port {}", port);
    }
    
    // Use dedicated test port in debug mode to avoid collisions
    if cfg!(debug_assertions) {
        port = 34634;
        tracing::info!("Running in test mode on dedicated port {}", port);
    }

    let bindurl = format!("0.0.0.0:{}", port);

    let (encodingkey, decodingkey) = auth::generate_jwt_key();
    let (privatekey, publickey) = consensus::functions::generate_ed25519_key();
    
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
                throughput_result_collector: Arc::new(metrics::functions::ThroughputResultCollector::new()),
                last_observed_view: Arc::new(std::sync::atomic::AtomicI32::new(-1)),
                fileprovider_api_key: fileprovider_api_key.clone(),
                port,
                test_mode: cfg!(debug_assertions), // Enable test routes in debug builds only
            };
            
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

            // Protected routes that require authentication
            let protected_routes = Router::new()
                .route("/users", get(users::routes::get_users))
                .route("/users", post(users::routes::post_users))
                .route("/nodes", get(nodes::routes::get_nodes))
                .route("/nodes", post(nodes::routes::post_nodes))
                .route("/files", get(files::routes::get_files))
                .route("/files", post(files::routes::post_files)).layer(DefaultBodyLimit::max(5000*1_000_000))
                .route("/files", delete(files::routes::delete_files))
                .route("/files/{*path}", get(files::routes::get_file_fragments))
                .route("/fragments", get(files::routes::get_fragments_count))
                .route("/maintenance/cleanup-orphaned", post(files::routes::post_cleanup_orphaned_data_blocks))
                .route("/maintenance/rebalance", post(files::routes::post_rebalance_network))
                .route("/validators", get(consensus::routes::get_validators))
                .route("/metrics", get(metrics::routes::get_metrics))
                .route("/metrics/trigger", get(metrics::routes::get_metrics_trigger))
                .route("/metrics/scores", get(metrics::routes::get_placement_scores))
                .layer(middleware::from_fn_with_state(app_state.clone(), auth::auth_middleware));

            // Routes that accept either JWT (users) or RPC (nodes) authentication
            let jwt_or_rpc_routes = Router::new()
                .route("/consensus", get(consensus::routes::get_consensus))
                .route("/rpc/storage-server", get(metrics::routes::get_storage_server))
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

            // RPC routes for inter-node communication with dual signature authentication
            let rpc_routes = Router::new()
                .route("/consensus/propose", post(consensus::routes::post_propose))
                .route("/consensus/view/{view}", get(consensus::routes::get_view_consensus_data))
                .route("/fragments/{fragment_hash}", get(files::routes::get_fragment))
                .route("/fragments/{fragment_hash}", post(files::routes::post_fragment))
                .route("/fragments/{fragment_hash}/health", get(files::routes::get_fragment_health))
                .route("/rpc/fetch-fragments", post(files::routes::post_fetch_fragments))
                .route("/rpc/throughput-server", get(metrics::routes::get_throughput_server))
                .route("/rpc/throughput-result/{session_id}", get(metrics::routes::get_throughput_result))
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

            // Test routes - only available in test mode
            let test_routes = if app_state.test_mode {
                Router::new()
                    .route("/integrations/fileprovider/test", get(fileprovider::routes::get_test))
                    .route("/integrations/fileprovider/test/signals", get(fileprovider::routes::get_test_signals))
            } else {
                Router::new() // Empty router when not in test mode
            };

            let base_app = Router::new()
                .fallback_service(admin_service) // routes we don't have get sent to vite frontend
                .merge(protected_routes)
                .merge(jwt_or_rpc_routes)
                .merge(rpc_routes)
                .merge(strict_consensus_routes)
                .merge(lenient_consensus_routes)
                .nest("/integrations/fileprovider", fileprovider_routes)
                .merge(test_routes)
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
    
    // Determine the port (same logic as in run_server)
    let mut port = 34632;
    if std::env::consts::OS == "linux" {
        port = 34633;
    }
    
    // Create Tauri app
    let context = tauri::generate_context!();
    let app = tauri::Builder::default()
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
            
            // Set icon if available
            if let Some(icon) = app.default_window_icon() {
                tray_builder = tray_builder.icon(icon.clone());
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
