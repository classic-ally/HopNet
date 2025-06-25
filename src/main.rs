use axum::{
    extract::{Query, State}, http::{HeaderValue, Method, StatusCode}, middleware, response::IntoResponse, routing::{get,post,put}, serve, Json, Router
};
use ed25519_dalek::{SigningKey, VerifyingKey};
use jsonwebtoken::{DecodingKey, EncodingKey};
use std::net::IpAddr;
use serde::{Serialize, Deserialize};
use tower_serve_static::ServeDir;
use tower_http::cors::CorsLayer;
use include_dir::{Dir, include_dir};

use duckdb::Connection;

mod nodes;
mod setup;
mod users;
mod metrics;
mod db;
mod interfaces;
mod auth;
mod consensus;
mod types;

static ASSETS_DIR: Dir<'static> = include_dir!("$CARGO_MANIFEST_DIR/frontend/dist");

#[derive(Clone)]
pub struct AppState {
    db: std::sync::Arc<std::sync::Mutex<duckdb::Connection>>,
    encoding_key: EncodingKey,
    decoding_key: DecodingKey,
    private_key: SigningKey,
    public_key: VerifyingKey
}

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
    let (privatekey, publickey) = consensus::generate_ed25519_key();

    match db::initialize() {
        Ok(database) => {
            let app_state = AppState {
                db: database,
                encoding_key: encodingkey,
                decoding_key: decodingkey,
                private_key: privatekey,
                public_key: publickey
            };

            // Protected routes that require authentication
            let protected_routes = Router::new()
                .route("/users", get(users::get_users))
                .route("/users", post(users::post_users))
                .route("/nodes", get(nodes::get_nodes))
                .route("/nodes", post(nodes::post_nodes))
                .route("/consensus", get(consensus::get_consensus))
                .layer(middleware::from_fn_with_state(app_state.clone(), auth::auth_middleware));

            let base_app = Router::new()
                .fallback_service(admin_service) // routes we don't have get sent to vite frontend
                .route("/metrics/get-all", get(get_metrics))
                .merge(protected_routes)
                .route("/setup", get(setup::get_setup))
                .route("/setup", post(setup::post_setup))
                .route("/setup", put(setup::put_setup))
                .route("/interfaces", get(interfaces::get_interfaces))
                .route("/rpc/latency-server", get(get_latency_server))
                .route("/rpc/get-remote-latency", get(get_remote_latency_handler))
                .route("/login", post(auth::sign_in));

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

async fn get_latency_server() -> impl IntoResponse {
    match metrics::latency::listener().await {
        Ok((_, latency_port)) => {
            (StatusCode::CREATED, Json(latency_port))
        }
        Err(_error) => {
            (StatusCode::INTERNAL_SERVER_ERROR, Json(0))
        }
    }
}

#[derive(Deserialize)]
struct RemoteLatencyQuery {
    ip: String,
}

async fn get_metrics(
    State(app_state): State<AppState>,
) -> impl IntoResponse {
    match db::get_metric(&app_state.db) {
        Ok(metrics) => {
            println!("{:?}", metrics);    
            (StatusCode::OK, Json(metrics))
        }
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(Vec::<db::Metric>::new())),
    }
}

async fn get_remote_latency_handler(
    Query(params): Query<RemoteLatencyQuery>,
    State(app_state): State<AppState>,
) -> impl IntoResponse {
    let (status, response) = get_remote_latency(&app_state.db, &params.ip).await;
    match response {
        Some(latency_response) => (status, Json(LatencyResponseWrapper::Success(latency_response))),
        None => (status, Json(LatencyResponseWrapper::Error(ErrorResponse { error: "Failed to get remote latency".to_string() })))
    }
}

async fn get_remote_latency(
    db: &std::sync::Arc<std::sync::Mutex<Connection>>, 
    str_ip: &str
) -> (StatusCode, Option<LatencyResponse>) {
    // let's hit the remote
    let url = format!("http://{}:34632/rpc/latency-server", str_ip);
    match reqwest::get(&url).await {
        Ok(response) => {
            if response.status().is_success() {
                match response.text().await {
                    Ok(str) => {
                        match str.parse::<u16>() {
                            // yes it's a u16
                            Ok(port) => {
                                match str_ip.parse::<IpAddr>() {
                                    Ok(ip) => {
                                        match metrics::latency::send_latency(db, ip, port).await {
                                            Ok((average_rtt, variance, jitter)) => {
                                                let response = LatencyResponse {
                                                    address: str_ip.to_string() + ":" + &str,
                                                    average_rtt: average_rtt,
                                                    variance: variance,
                                                    jitter: jitter,
                                                };
                                                return (StatusCode::OK, Some(response));
                                            }
                                            Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, None)
                                        }
                                    }
                                    Err(_) => (StatusCode::UNPROCESSABLE_ENTITY, None)
                                }
                            }
                            // port isn't a u16-> gateway is naughty
                            Err(_) => (StatusCode::BAD_GATEWAY, None)
                        }
                    }
                    Err(_) => (StatusCode::BAD_GATEWAY, None)
                }
            } else {
                return (response.status(), None)
            }
        }
        Err(e) => {
            // handle reqwest errors
            match e.status() {
                Some(status) => (status, None),
                None => (StatusCode::GATEWAY_TIMEOUT, None)
            }
        }
    }
}

// response for remote latency
#[derive(Serialize)]
struct LatencyResponse {
    address: String,
    average_rtt: f64,
    variance: f64,
    jitter: f64,
}

// error response
#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

// unified response wrapper
#[derive(Serialize)]
#[serde(untagged)]
enum LatencyResponseWrapper {
    Success(LatencyResponse),
    Error(ErrorResponse),
}