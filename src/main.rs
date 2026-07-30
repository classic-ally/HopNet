use apalis::prelude::*;
use axum::{
    Router,
    body::Body,
    extract::{DefaultBodyLimit, Request},
    http::{header, HeaderValue, Method, StatusCode},
    middleware,
    response::Response,
    routing::{get, post},
    serve,
};
use include_dir::{Dir, include_dir};

/// Maximum concurrently-executing HTTP requests before the load-shed layer
/// starts refusing with 503 + Retry-After. Sized so a burst cannot pile
/// hundreds of handlers onto the DB pool (32 conns, 2s checkout timeout) —
/// beyond this, executing more requests only converts them into slow 500s.
const API_CONCURRENCY_LIMIT: usize = 128;

/// The DB-capacity gate sheds new API requests while the pool has fewer than
/// this many idle connections, keeping a trickle available for background
/// tasks that aren't behind the gate (settler retries, metrics, peer refresh).
const DB_GATE_IDLE_HEADROOM: u32 = 2;
use once_cell::sync::{Lazy, OnceCell};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use std::str::FromStr;
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use bytes::Bytes;
use hopnet::db::{PrivKey, PubKey};
use hopnet::*;

static ASSETS_DIR: Dir<'static> = include_dir!("$CARGO_MANIFEST_DIR/frontend/dist");

/// Default HTTP port for headless mode (browser-served deployments).
/// GUI mode binds an ephemeral loopback port — see `run_server`.
const HEADLESS_BACKEND_PORT: u16 = 34632;

/// Actual bound port. Populated by `run_server` after `TcpListener::bind`
/// returns — needed in GUI mode because we bind `127.0.0.1:0` and let the
/// kernel pick a free port, so two HopNet processes never clash.
static ACTUAL_BACKEND_PORT: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(0);

/// Global AppState accessible to Tauri IPC commands (GUI mode only).
/// Set by run_server() after AppState creation; read by Tauri commands.
#[cfg(feature = "gui")]
static GUI_APP_STATE: Lazy<tokio::sync::RwLock<Option<AppState>>> =
    Lazy::new(|| tokio::sync::RwLock::new(None));

/// SPA-aware static file server. Serves files from the embedded
/// frontend/dist/ directory when they exist, falling back to index.html for
/// any unknown path so client-side routing works (e.g. /browse, /settings).
async fn serve_spa(request: Request) -> Response<Body> {
    let path = request.uri().path().trim_start_matches('/');
    let path = if path.is_empty() { "index.html" } else { path };

    if let Some(file) = ASSETS_DIR.get_file(path) {
        let mime = mime_guess::from_path(path)
            .first_raw()
            .unwrap_or("application/octet-stream");
        return Response::builder()
            .header(header::CONTENT_TYPE, HeaderValue::from_static(mime))
            .body(Body::from(Bytes::copy_from_slice(file.contents())))
            .unwrap();
    }

    let index = ASSETS_DIR.get_file("index.html").unwrap();
    Response::builder()
        .header(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/html; charset=utf-8"),
        )
        .body(Body::from(Bytes::copy_from_slice(index.contents())))
        .unwrap()
}

/// Run the axum server.
///
/// `bind_addr` is the address to bind. Headless: `0.0.0.0:34632` (publicly
/// reachable). GUI: `127.0.0.1:0` (loopback, kernel-assigned port — prevents
/// collisions between concurrent HopNet processes).
async fn run_server(bind_addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    // tracing
    tracing_subscriber::fmt::init();

    // RFC-015 boot tripwire: fail-stop immediately if the linker dropped a
    // projection's cross-crate inventory registrations.
    assert_projection_registrations();

    // Bind before anything else. With `127.0.0.1:0` the kernel assigns a
    // free port; we need to capture the result and thread it into AppState
    // so things like the FileProvider keychain config point at the correct
    // loopback URL.
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    let port = listener.local_addr()?.port();
    ACTUAL_BACKEND_PORT.store(port, std::sync::atomic::Ordering::SeqCst);
    tracing::info!("Server listening on {} (port {})", bind_addr, port);

    // GUI release builds: refresh the FileProvider keychain entry with the
    // freshly-bound loopback URL. The extension reads `base_url` from the
    // keychain to find this process, and the port changes every launch.
    #[cfg(all(target_os = "macos", feature = "gui", not(debug_assertions)))]
    {
        use fileprovider::keychain::{self, KeychainEnvironment};
        if keychain::load_config(KeychainEnvironment::Production).is_ok() {
            let url = format!("http://127.0.0.1:{}", port);
            if let Err(e) = keychain::update_base_url(&url, KeychainEnvironment::Production) {
                tracing::warn!("FileProvider keychain base_url refresh failed: {:?}", e);
            }
        }
    }

    let (encodingkey, decodingkey) = auth::generate_jwt_key();

    // Check if ephemeral database mode is requested (for testing)
    let use_ephemeral_db = std::env::var("HOPNET_EPHEMERAL_DB").is_ok();

    let pool = if use_ephemeral_db {
        tracing::info!("Using ephemeral in-memory database (HOPNET_EPHEMERAL_DB set)");
        // Use shared-cache URI so all pool connections see the same in-memory DB
        let manager = SqliteConnectionManager::file("file::memory:?cache=shared");
        Pool::builder()
            .max_size(db::DB_POOL_MAX_SIZE)
            // Checkout waits are sub-ms in normal operation; r2d2's 30s
            // default turns burst overload into a runtime livelock (a blocking
            // get() parks a tokio worker — enough concurrent waiters park ALL
            // workers, and each freed conn goes to another parked waiter).
            // Fail fast instead so overload sheds as 500s and the runtime
            // keeps polling accept loops and the consensus queue.
            .connection_timeout(std::time::Duration::from_secs(2))
            .connection_customizer(Box::new(db::shared::SqliteInitializer))
            .build(manager)
            .unwrap()
    } else {
        // Get database path and ensure directory exists
        let db_path = db::shared::get_database_path();
        let db_file_exists = db::shared::database_exists(&db_path);

        if !db_file_exists {
            tracing::info!("Creating new database at {}", db_path);
            db::shared::ensure_database_dir(&db_path).expect("Failed to create database directory");
        } else {
            tracing::info!("Found existing database file at {}", db_path);
        }

        let manager = SqliteConnectionManager::file(&db_path);
        let pool = Pool::builder()
            .max_size(db::DB_POOL_MAX_SIZE)
            // See the ephemeral-pool builder above: fail checkout fast so
            // burst overload cannot park every tokio worker for 30s waves.
            .connection_timeout(std::time::Duration::from_secs(2))
            .connection_customizer(Box::new(db::shared::SqliteInitializer))
            .build(manager)
            .unwrap();

        tracing::info!("Database connection pool established (WAL mode)");
        pool
    };

    // Check if database schema is initialized
    let conn = pool.get().unwrap();

    let schema_initialized = if use_ephemeral_db {
        false // Ephemeral database always needs initialization
    } else {
        db::shared::is_schema_initialized(&conn).expect("Failed to check schema status")
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
                    tracing::info!(
                        "Loaded existing state from database (node_id: {}, user_id: {})",
                        state.node_id,
                        state.user_id
                    );
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
                consensus::dispatch::generate_ed25519_key()
            };

            // Initialize fragments directory
            let fragments_dir = storage_host::functions::get_fragments_dir().unwrap_or_else(|_| {
                eprintln!("Failed to get fragments directory, using current directory");
                "./hopnet/fragments".to_string()
            });
            std::fs::create_dir_all(&fragments_dir).expect("Failed to create fragments directory");

            // Create the comms transport: host-injected peer directory (the
            // nodes table + setup-mode bypass) over the node's Ed25519 key.
            let setup_complete = Arc::new(std::sync::atomic::AtomicBool::new(
                startup_state_opt.is_some(),
            ));
            let directory = Arc::new(net::directory::HostPeerDirectory::new(
                pool.clone(),
                setup_complete.clone(),
            ));
            let comms = hopnet_comms::IrohComms::bind(
                privatekey.to_bytes(),
                directory,
                std::env::var("HOPNET_RELAY_URL").ok(),
            )
            .await
            .expect("Failed to create iroh comms");
            tracing::info!(
                "iroh endpoint ready, node_id: {}",
                hex::encode(comms.local_pubkey())
            );

            // Create consensus queue (channel + submit handle)
            let (consensus_queue, consensus_queue_rx) =
                consensus::queue::ConsensusQueue::new(pool.clone(), 300);

            // Create write gate + local-state channel
            let write_gate = Arc::new(db::write_gate::WriteGate::new());
            let (local_state_tx, local_state_rx) = tokio::sync::mpsc::channel(1024);

            let app_state = AppState {
                db_pool: pool,
                encoding_key: encodingkey,
                decoding_key: decodingkey,
                private_key: PrivKey(privatekey),
                public_key: PubKey(publickey),
                node_id: Arc::new(OnceCell::new()),
                user_id: Arc::new(OnceCell::new()),
                fragments_dir,
                port,
                test_mode: cfg!(debug_assertions) || std::env::var("HOPNET_TEST_MODE").is_ok(),
                orphaned_fragment_scan: Arc::new(std::sync::Mutex::new(None)),
                comms,
                setup_complete,
                consensus_barriers: Arc::new(consensus::barriers::new()),
                session_store: Arc::new(auth::SessionStore::default()),
                takeout_runtime: Arc::new(hopnet_takeout::TakeoutRuntime::default()),
                consensus_queue,
                write_gate: write_gate.clone(),
                local_state_tx,
                malachite: Arc::new(OnceCell::new()),
                evidence: std::sync::Arc::new(consensus::evidence::EvidenceMap::new()),
                storage: Arc::new(OnceCell::new()),
                runtime: tokio::runtime::Handle::current(),
            };

            // If we loaded state from database, populate the OnceCell fields
            if let Some(state) = startup_state_opt {
                app_state
                    .node_id
                    .set(state.node_id)
                    .expect("Failed to set node_id in AppState");
                app_state
                    .user_id
                    .set(state.user_id)
                    .expect("Failed to set user_id in AppState");

                tracing::info!(
                    "AppState initialized from persisted database (user keys require login)"
                );

                // Scan for stranded imports owned by this node so that on the
                // next user authentication event the resume hook can finish them.
                match hopnet_takeout::jobs::scan_at_startup(&takeout_host::takeout_state(
                    &app_state,
                ))
                .await
                {
                    Ok(0) => {}
                    Ok(n) => tracing::info!("Import resume registry: {} stranded import(s)", n),
                    Err(e) => tracing::warn!("Import resume scan failed: {:?}", e),
                }

                // GUI auto-login: load owner session from keychain if available
                #[cfg(all(target_os = "macos", feature = "gui", not(debug_assertions)))]
                {
                    if let Ok((kc_user_id, privkey_bytes)) =
                        fileprovider::keychain::load_session_key(
                            fileprovider::keychain::KeychainEnvironment::Production,
                        )
                    {
                        if let Ok(key_bytes) = <[u8; 32]>::try_from(privkey_bytes.as_slice()) {
                            let privkey =
                                db::PrivKey(ed25519_dalek::SigningKey::from_bytes(&key_bytes));
                            let pubkey = db::PubKey(privkey.verifying_key());
                            let (siv_key, siv_nonce) =
                                auth::derive_siv_key_from_user(&privkey, "file_path");
                            let session = auth::SessionEntry {
                                user_keys: UserKeys {
                                    private_key: privkey,
                                    public_key: pubkey,
                                },
                                siv_key,
                                siv_nonce,
                                expires_at: chrono::Utc::now() + chrono::Duration::hours(876000),
                            };
                            app_state
                                .session_store
                                .blocking_write()
                                .insert(kc_user_id, session);
                            tracing::info!("Loaded owner session from keychain (auto-login ready)");
                            tokio::spawn(hopnet_takeout::jobs::maybe_resume_for_user(
                                takeout_host::takeout_state(&app_state),
                                kc_user_id,
                            ));
                        }
                    }
                }
            }

            // Publish AppState for Tauri IPC commands (GUI mode)
            #[cfg(feature = "gui")]
            {
                *GUI_APP_STATE.write().await = Some(app_state.clone());
            }

            // Warn about test mode being enabled
            if cfg!(debug_assertions) {
                tracing::warn!(
                    "⚠️  TEST MODE ENABLED: Test endpoints are exposed and will return API credentials"
                );
                tracing::warn!(
                    "⚠️  This is a SECURITY RISK in production - test mode is automatically disabled in release builds"
                );
            }

            // Initialize FileProvider on startup - only cleans up if app is uninitialized (release builds only)
            #[cfg(all(target_os = "macos", not(debug_assertions)))]
            {
                if let Err(e) =
                    fileprovider::domain::initialize_fileprovider_on_startup(&app_state).await
                {
                    tracing::warn!("Failed to initialize FileProvider on startup: {}", e);
                } else {
                    tracing::info!("FileProvider startup initialization completed");
                }
            }

            #[cfg(all(target_os = "macos", debug_assertions))]
            {
                tracing::info!(
                    "Skipping FileProvider domain initialization in debug build - testing API endpoints only"
                );
            }

            // Start metrics collection worker with randomized 10-minute schedule
            use rand::RngExt;
            let random_second = rand::rng().random_range(5..55);
            let random_minute = rand::rng().random_range(0..10); // 0-9 minutes offset within each 10-minute window
            let metrics_cron_expression = format!("{} {}/10 * * * *", random_second, random_minute);
            let metrics_schedule =
                apalis_cron::Schedule::from_str(&metrics_cron_expression).unwrap();
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
            let takeout_cron_expression = format!(
                "{} {} */{} * * *",
                random_second, random_minute, random_hour_offset
            );
            let takeout_schedule =
                apalis_cron::Schedule::from_str(&takeout_cron_expression).unwrap();
            let takeout_cron_stream = apalis_cron::CronStream::new(takeout_schedule);

            let takeout_worker = WorkerBuilder::new("takeout-maintenance")
                .data(app_state.clone())
                .backend(takeout_cron_stream)
                .build_fn(takeout_host::handle_takeout_maintenance);

            tokio::spawn(async move {
                takeout_worker.run().await;
            });

            // Start fragment inventory self-check worker with randomized 20-30 minute schedule
            let random_second = rand::rng().random_range(5..55);
            let random_minute = rand::rng().random_range(0..30); // 0-29 minutes offset within each 30-minute window
            let self_check_cron_expression =
                format!("{} {}/30 * * * *", random_second, random_minute);
            let self_check_schedule =
                apalis_cron::Schedule::from_str(&self_check_cron_expression).unwrap();
            let self_check_cron_stream = apalis_cron::CronStream::new(self_check_schedule);

            let self_check_worker = WorkerBuilder::new("fragment-inventory-self-check")
                .data(app_state.clone())
                .backend(self_check_cron_stream)
                .build_fn(storage_host::jobs::handle_fragment_inventory_self_check);

            tokio::spawn(async move {
                self_check_worker.run().await;
            });

            // Storage policy tick (RFC-STORAGE-002 S6): view sync, repair
            // scan (urgent + one lazy re-encode), one migration pull,
            // eviction check, daily scrub slice. Randomized ~5 min cadence.
            let random_second = rand::rng().random_range(5..55);
            let random_minute = rand::rng().random_range(0..5);
            let policy_tick_cron_expression =
                format!("{} {}/5 * * * *", random_second, random_minute);
            let policy_tick_schedule =
                apalis_cron::Schedule::from_str(&policy_tick_cron_expression).unwrap();
            let policy_tick_cron_stream = apalis_cron::CronStream::new(policy_tick_schedule);

            let policy_tick_worker = WorkerBuilder::new("storage-policy-tick")
                .data(app_state.clone())
                .backend(policy_tick_cron_stream)
                .build_fn(storage_host::jobs::handle_storage_policy_tick);

            tokio::spawn(async move {
                policy_tick_worker.run().await;
            });

            // Spawn consensus queue batch processor — on the dedicated queue
            // runtime (see consensus::queue::queue_rt) so consensus keeps
            // draining when API load starves the main runtime.
            {
                let app_state_clone = app_state.clone();
                consensus::queue::queue_rt().spawn(async move {
                    consensus::queue::batch_processor(consensus_queue_rx, app_state_clone).await;
                });
            }

            // Spawn local-state drain task (batches background fragment DB writes)
            {
                let db_pool_clone = app_state.db_pool.clone();
                let write_gate_clone = write_gate;
                tokio::spawn(async move {
                    db::write_gate::drain_local_state_queue(
                        local_state_rx,
                        db_pool_clone,
                        write_gate_clone,
                    )
                    .await;
                });
            }

            // Install the scope handlers and start the comms accept loop —
            // comms spawns it on its dedicated net runtime (see
            // hopnet_comms::net_rt); each scope handler hops DB-touching work
            // to its own runtime. Mesh liveness must not depend on API load.
            app_state.comms.start(net::scopes::build_registry(&app_state));

            // Restart path: an initialized node starts the consensus engine
            // now — AFTER the accept loop (QUIC handshakes only complete under
            // a polled accept; consensus participation must not precede it).
            // Fresh nodes spawn the engine from the setup/join flows instead.
            if app_state.node_id.get().is_some() {
                if let Err(e) = consensus::malachite::engine::spawn_engine(&app_state) {
                    tracing::error!("failed to start consensus engine: {e}");
                }
            }

            // Host capabilities (RFC-016): one seam bundle handed to every
            // projection's mounts()/exporter(); built once, cheap clones.
            let host_caps = capabilities::build_capabilities(&app_state);

            // Takeout service state (RFC-015 Stage D5b): registered
            // projection translators (drive today; photos registers here
            // later) + host hooks, over the same DriveHost seam impls.
            let takeout_state = takeout_host::takeout_state(&app_state);

            // Protected routes that require authentication
            let protected_routes = Router::new()
                .nest("/users", users::routes::router())
                .route("/nodes", get(nodes::routes::get_nodes))
                .route("/nodes", post(nodes::routes::post_nodes))
                .route("/fragments", get(storage_host::routes::get_fragments_count))
                .route(
                    "/maintenance/cleanup-orphaned",
                    post(storage_host::routes::post_cleanup_orphaned_data_blocks),
                )
                .route(
                    "/maintenance/rebalance",
                    post(storage_host::routes::post_rebalance_network),
                )
                .merge(hopnet_takeout::routes::maintenance_router(
                    takeout_state.clone(),
                ))
                .route(
                    "/maintenance/fragment-inventory-self-check",
                    post(storage_host::routes::post_fragment_inventory_self_check),
                )
                .route(
                    "/maintenance/orphaned-fragments",
                    get(storage_host::routes::get_orphaned_fragments_scan)
                        .delete(storage_host::routes::delete_orphaned_fragments),
                )
                .route(
                    "/maintenance/watermark-eviction",
                    post(storage_host::routes::post_watermark_eviction),
                )
                .route(
                    "/maintenance/policy-tick",
                    post(storage_host::routes::post_policy_tick),
                )
                .route(
                    "/diagnostics/fragment-inventory-differential",
                    get(storage_host::routes::get_fragment_inventory_differential),
                )
                .route(
                    "/diagnostics/file-fragments",
                    get(storage_host::routes::get_file_fragment_distribution),
                )
                .route("/debug/iroh-ping", get(net::routes::debug_iroh_ping))
                .route("/debug/db-stats", get(consensus::routes::get_db_stats))
                .route("/storage/view", get(storage_host::routes::get_storage_view))
                .route("/validators", get(consensus::routes::get_validators))
                .route("/metrics", get(metrics::routes::get_metrics))
                .route(
                    "/metrics/trigger",
                    get(metrics::routes::get_metrics_trigger),
                )
                .route(
                    "/metrics/scores",
                    get(metrics::routes::get_placement_scores),
                )
                .nest("/takeout", hopnet_takeout::routes::router(takeout_state.clone()))
                .nest("/admin", admin::routes::admin_routes())
                .nest("/views", views::routes::router())
                .route("/logout", post(auth::sign_out))
                .layer(middleware::from_fn_with_state(
                    app_state.clone(),
                    auth::auth_middleware,
                ));

            // State snapshot endpoint requires DuckDB JSON extension (not codesigned for macOS release)
            #[cfg(any(not(target_os = "macos"), debug_assertions))]
            let protected_routes =
                protected_routes.route("/debug/state", get(consensus::routes::get_state_snapshot));

            // Routes that accept either JWT (users) or RPC (nodes) authentication
            let jwt_or_rpc_routes = Router::new()
                .route("/consensus", get(consensus::routes::get_consensus))
                .route(
                    "/consensus/history",
                    get(consensus::routes::get_consensus_history),
                )
                .route("/consensus/view", post(consensus::routes::debug_view_state))
                .route("/consensus/leave", post(consensus::routes::post_leave))
                .route("/consensus/evidence", get(consensus::evidence::get_evidence))
                .layer(middleware::from_fn_with_state(
                    app_state.clone(),
                    consensus::routes::jwt_or_rpc_auth_middleware,
                ));

            // Test routes - only available in test mode
            let test_routes = if app_state.test_mode {
                Router::new()
                    .route(
                        "/integrations/fileprovider/test",
                        get(fileprovider::routes::get_test),
                    )
                    .route(
                        "/integrations/fileprovider/test/signals",
                        get(fileprovider::routes::get_test_signals),
                    )
                    .route(
                        "/test/fragment-health-check/{fragment_hash}",
                        get(storage_host::test_routes::get_fragment_health_check),
                    )
                    .nest("/test/barriers", barriers::test_routes())
            } else {
                Router::new() // Empty router when not in test mode
            };

            let api_routes = Router::new()
                .merge(protected_routes)
                .merge(jwt_or_rpc_routes)
                .nest("/integrations", fileprovider::routes::health_router())
                .nest("/devices", devices::routes::router(app_state.clone()))
                .merge(test_routes)
                .route("/setup", get(setup::get_setup))
                .route("/setup", post(setup::post_setup))
                .route("/login", post(auth::sign_in));

            // Projection mounts (RFC-016 Stage 4). Host routes close over
            // AppState first; each projection's routers are Router<()> and
            // get nested under their declared auth class. Everything —
            // host and projection alike — then goes under the /api nest
            // below.
            let mut api_routes: Router<()> = api_routes.with_state(app_state.clone());
            for mount in projections::manifests()
                .iter()
                .flat_map(|m| m.mounts(&host_caps))
            {
                let routed = match mount.auth {
                    hopnet_projection::AuthClass::UserJwt => {
                        mount.router.layer(middleware::from_fn_with_state(
                            app_state.clone(),
                            auth::auth_middleware,
                        ))
                    }
                    hopnet_projection::AuthClass::DeviceToken => {
                        mount.router.layer(middleware::from_fn_with_state(
                            app_state.clone(),
                            devices::auth::device_token_auth_middleware,
                        ))
                    }
                };
                api_routes = api_routes.nest(mount.prefix, routed);
            }

            // Overload shedding, two gates, both answering 503 + Retry-After
            // so CLIENTS own the retry and the server never converts overload
            // into slow 500s:
            //  1. DB-capacity gate (precise): refuse on entry when the pool
            //     has (almost) no idle connections — the actual scarce
            //     resource. Snapshot-based: occasionally wrong in both
            //     directions, which retries absorb; the headroom keeps
            //     non-gated paths (settler, metrics, peer refresh) supplied.
            //  2. Concurrency limit (coarse): catastrophic upper bound on
            //     in-flight requests, far above the DB gate's trip point.
            // Applied inside the /api nest so the SPA fallback (index.html)
            // is never shed — a 503 on the bootstrap HTML would brick the app.
            let api_routes = api_routes.layer(middleware::from_fn_with_state(
                app_state.clone(),
                |axum::extract::State(state): axum::extract::State<AppState>,
                 req: axum::extract::Request,
                 next: middleware::Next| async move {
                    use axum::response::IntoResponse;
                    let pool_state = state.db_pool.state();
                    if pool_state.idle_connections < DB_GATE_IDLE_HEADROOM {
                        return (
                            StatusCode::SERVICE_UNAVAILABLE,
                            [(axum::http::header::RETRY_AFTER, "1")],
                            "db capacity exhausted, retry shortly",
                        )
                            .into_response();
                    }
                    next.run(req).await
                },
            ));
            let api_routes = api_routes.layer(
                tower::ServiceBuilder::new()
                    .layer(axum::error_handling::HandleErrorLayer::new(
                        |_e: tower::BoxError| async {
                            (
                                StatusCode::SERVICE_UNAVAILABLE,
                                [(axum::http::header::RETRY_AFTER, "1")],
                                "server overloaded, retry shortly",
                            )
                        },
                    ))
                    .load_shed()
                    .concurrency_limit(API_CONCURRENCY_LIMIT),
            );

            // Create trace layer with request IDs. Failure logging: a 503 is
            // deliberate load shedding (the DB-capacity gate / concurrency
            // limit doing their job) — logging each at ERROR floods the logs
            // exactly when the system is busiest, so sheds log at DEBUG;
            // genuine 5xx failures stay ERROR.
            let trace_layer = TraceLayer::new_for_http()
                .make_span_with(|request: &axum::http::Request<_>| {
                    let id = hopnet_common::CustomUUID::new(None);
                    tracing::info_span!(
                        "api_req",
                        id = &id.to_string()[28..],
                        method = %request.method(),
                        uri = %request.uri(),
                    )
                })
                .on_failure(
                    |class: tower_http::classify::ServerErrorsFailureClass,
                     latency: std::time::Duration,
                     _span: &tracing::Span| {
                        use tower_http::classify::ServerErrorsFailureClass as C;
                        match class {
                            C::StatusCode(StatusCode::SERVICE_UNAVAILABLE) => {
                                tracing::debug!(latency_ms = latency.as_millis() as u64, "request shed (503)");
                            }
                            other => {
                                tracing::error!(
                                    classification = %other,
                                    latency_ms = latency.as_millis() as u64,
                                    "response failed"
                                );
                            }
                        }
                    },
                );

            // Root router: /api for all endpoints, SPA fallback serves
            // index.html for any remaining path so client-side routing works.
            let app = Router::new()
                .nest("/api", api_routes)
                .fallback(serve_spa);

            let app = if cfg!(debug_assertions) {
                let cors = CorsLayer::new()
                    .allow_origin("http://localhost:5173".parse::<HeaderValue>().unwrap()) // allow vite dev
                    .allow_methods([Method::GET, Method::POST, Method::PUT, Method::DELETE])
                    .allow_headers([
                        axum::http::header::CONTENT_TYPE,
                        axum::http::header::AUTHORIZATION,
                    ])
                    .max_age(std::time::Duration::from_secs(3600))
                    .allow_credentials(false);

                app.layer(cors).layer(trace_layer)
            } else {
                app // no CORS in prod
                    .layer(trace_layer)
            };

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
        // HOPNET_HTTP_PORT lets a dev copy run alongside a real node on the
        // same machine (pair with XDG_DATA_HOME for storage isolation).
        let port = std::env::var("HOPNET_HTTP_PORT")
            .ok()
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(HEADLESS_BACKEND_PORT);
        run_server(&format!("0.0.0.0:{}", port)).await
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

    let node_id = app_state
        .get_node_id()
        .map_err(|_| "Node not initialized".to_string())?;
    let user_id = app_state
        .get_user_id()
        .map_err(|_| "Node not initialized".to_string())?;

    // Verify owner session exists (pre-loaded from keychain at startup)
    app_state
        .get_session(user_id)
        .await
        .map_err(|_| "No owner session available".to_string())?;

    let token = auth::encode_jwt_with_duration(
        node_id.to_string(),
        user_id.to_string(),
        app_state.encoding_key.clone(),
        24,
    )
    .map_err(|_| "JWT encoding failed".to_string())?;

    Ok(auth::SignInResponse { token })
}

#[cfg(feature = "gui")]
async fn run_with_gui() -> Result<(), Box<dyn std::error::Error>> {
    use tauri::{
        Manager, TitleBarStyle, WebviewWindowBuilder,
        menu::{Menu, MenuItem, PredefinedMenuItem},
        tray::TrayIconBuilder,
    };

    // Helper function to create and configure the main window
    fn create_main_window(
        app: &tauri::AppHandle,
        port: u16,
    ) -> Result<tauri::WebviewWindow, Box<dyn std::error::Error>> {
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
                    17.0 / 255.0, // #11111b red component (crust)
                    17.0 / 255.0, // #11111b green component (crust)
                    27.0 / 255.0, // #11111b blue component (crust)
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

    // Start the server in a background task. Bind loopback only with an
    // ephemeral port so multiple HopNet processes can coexist (e.g. dev
    // `cargo run` alongside the installed .app).
    let server_handle = tokio::spawn(async {
        if let Err(e) = run_server("127.0.0.1:0").await {
            tracing::error!("Server error: {}", e);
        }
    });

    // Wait for the listener to publish its kernel-assigned port before
    // navigating the webview, otherwise the load races the bind and fails.
    let port = {
        let mut waited_ms = 0;
        loop {
            let p = ACTUAL_BACKEND_PORT.load(std::sync::atomic::Ordering::SeqCst);
            if p != 0 {
                break p;
            }
            if waited_ms >= 5_000 {
                return Err("server failed to bind within 5s".into());
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
            waited_ms += 20;
        }
    };
    tracing::info!("Tauri webview loading backend on port {}", port);

    // Create Tauri app
    let context = tauri::generate_context!();
    let app = tauri::Builder::default()
        .invoke_handler(tauri::generate_handler![auto_login])
        .setup(move |app| {
            // Use Accessory policy to never show in dock
            #[cfg(target_os = "macos")]
            app.set_activation_policy(tauri::ActivationPolicy::Accessory);
            // Create tray menu with toggle item (text will be updated dynamically)
            let toggle_item =
                MenuItem::with_id(app, "toggle", "Toggle window", true, None::<&str>)?;
            let separator = PredefinedMenuItem::separator(app)?;
            let quit_item = MenuItem::with_id(app, "quit", "Quit", true, None::<&str>)?;

            let menu = Menu::with_items(app, &[&toggle_item, &separator, &quit_item])?;

            // Create tray icon with proper error handling
            let mut tray_builder = TrayIconBuilder::with_id("main_tray").menu(&menu);

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
                        if let tauri::tray::TrayIconEvent::Click {
                            button: tauri::tray::MouseButton::Right,
                            ..
                        } = event
                        {
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
