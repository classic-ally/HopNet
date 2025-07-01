use axum::{
    http::{HeaderValue, Method}, middleware, routing::{get,post,put}, serve, Router
};
use jsonwebtoken::{DecodingKey, EncodingKey};
use tower_serve_static::ServeDir;
use tower_http::cors::CorsLayer;
use include_dir::{Dir, include_dir};
use once_cell::sync::Lazy;
use std::collections::HashMap;

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

static ASSETS_DIR: Dir<'static> = include_dir!("$CARGO_MANIFEST_DIR/frontend/dist");

#[derive(Clone)]
pub struct AppState {
    db: std::sync::Arc<std::sync::Mutex<duckdb::Connection>>,
    encoding_key: EncodingKey,
    decoding_key: DecodingKey,
    private_key: PrivKey,
    public_key: PubKey
}

static DISPATCH_TABLE: Lazy<HashMap<&'static str, &'static dyn TransactionHandler>> = Lazy::new(|| {
    dbg!("Building dispatch table from registered handlers...");
    let mut table = HashMap::new();
    // iterate over the globally collected handlers
    for handler in inventory::iter::<&'static dyn TransactionHandler> {
        dbg!(" - Registering handler: {}", handler.name());
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
        dbg!("Running on Linux on port {}", port);
    }

    let bindurl = format!("0.0.0.0:{}", port);

    let (encodingkey, decodingkey) = auth::generate_jwt_key();
    let (privatekey, publickey) = consensus::functions::generate_ed25519_key();

    match db::shared::initialize() {
        Ok(database) => {
            let app_state = AppState {
                db: database,
                encoding_key: encodingkey,
                decoding_key: decodingkey,
                private_key: PrivKey(privatekey),
                public_key: PubKey(publickey)
            };

            // Protected routes that require authentication
            let protected_routes = Router::new()
                .route("/users", get(users::routes::get_users))
                .route("/users", post(users::routes::post_users))
                .route("/nodes", get(nodes::get_nodes))
                .route("/nodes", post(nodes::post_nodes))
                .route("/validators", get(consensus::routes::get_validators))
                .route("/consensus", get(consensus::routes::get_consensus))
                .layer(middleware::from_fn_with_state(app_state.clone(), auth::auth_middleware));

            let base_app = Router::new()
                .fallback_service(admin_service) // routes we don't have get sent to vite frontend
                .route("/metrics/get-all", get(metrics::routes::get_metrics))
                .merge(protected_routes)
                .route("/setup", get(setup::get_setup))
                .route("/setup", post(setup::post_setup))
                .route("/setup", put(setup::put_setup))
                .route("/interfaces", get(interfaces::get_interfaces))
                .route("/rpc/latency-server", get(metrics::routes::get_latency_server))
                .route("/rpc/get-remote-latency", get(metrics::routes::get_remote_latency_handler))
                .route("/login", post(auth::sign_in))
                .route("/ballot", post(consensus::routes::post_ballot))
                .route("/qc", post(consensus::routes::post_qc));

            let app = if cfg!(debug_assertions) {
                let cors = CorsLayer::new()
                    .allow_origin("http://localhost:5173".parse::<HeaderValue>().unwrap()) // allow vite dev
                    .allow_methods([Method::GET, Method::POST])
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

            match tokio::net::TcpListener::bind(bindurl).await {
                Ok(listener) => {
                    dbg!("beginning server");
                    serve(listener, app).await.unwrap();
                }
                Err(error) => {panic!("{}", error)}
            }
        }
        Err(error) => {panic!{"{}", error}}
    }
    

}