use axum::{
    extract::{Query, State}, http::{HeaderValue, Method, StatusCode}, response::IntoResponse, routing::{get,post}, serve, Json, Router
};
use std::net::IpAddr;
use serde::{Serialize, Deserialize};
use tower_serve_static::ServeDir;
use tower_http::cors::CorsLayer;
use include_dir::{Dir, include_dir};

use duckdb::{Connection, Error};

mod metrics;
mod db;
mod interfaces;

static ASSETS_DIR: Dir<'static> = include_dir!("$CARGO_MANIFEST_DIR/frontend/dist");

#[tokio::main]
async fn main() {
    // tracing
    tracing_subscriber::fmt::init();

    let admin_service = ServeDir::new(&ASSETS_DIR);

    match db::initialize() {
        Ok(database) => {
            let base_app = Router::new()
                .fallback_service(admin_service) // routes we don't have get sent to vite frontend
                .route("/metrics/get-all", get(get_metrics))
                .route("/users", get(get_users))
                .route("/users", post(post_users))
                .route("/interfaces", get(interfaces::get_interfaces))
                .route("/rpc/latency-server", get(get_latency_server))
                .route("/rpc/get-remote-latency", get(get_remote_latency_handler));

            let app = if cfg!(debug_assertions) {
                let cors = CorsLayer::new()
                    .allow_origin("http://localhost:5173".parse::<HeaderValue>().unwrap()) // allow vite dev
                    .allow_methods([Method::GET])
                    .max_age(std::time::Duration::from_secs(3600))
                    .allow_credentials(false);

                base_app
                    .layer(cors)
                    .with_state(database)
            } else {
                base_app // no CORS in prod
                    .with_state(database)
            };

            match tokio::net::TcpListener::bind("0.0.0.0:34632").await {
                Ok(listener) => {
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
    State(db): State<std::sync::Arc<std::sync::Mutex<duckdb::Connection>>>,
) -> impl IntoResponse {
    match db::get_metric(&db) {
        Ok(metrics) => {
            println!("{:?}", metrics);    
            (StatusCode::OK, Json(metrics))
        }
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(Vec::<db::Metric>::new())),
    }
}

async fn get_users(
    State(db): State<std::sync::Arc<std::sync::Mutex<duckdb::Connection>>>,
) -> impl IntoResponse {
    match db::get_users(&db) {
        Ok(users) => {
            (StatusCode::OK, Json(users))
        }
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(Vec::<db::User>::new())
        ),
    }
}

#[derive(Deserialize)]
struct UserRequest {
    username: String,
    password_hash: String,
}

async fn post_users (
    State(db): State<std::sync::Arc<std::sync::Mutex<duckdb::Connection>>>,
    Json(payload): Json<UserRequest>
) -> impl IntoResponse {
    let user = db::User {
        user_id: 0,
        username: payload.username,
        password_hash: payload.password_hash,
    };

    match db::insert_user(&db, user) {
        Ok(()) => {
            (StatusCode::CREATED)
        },
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR
        ),
    }
}


async fn get_remote_latency_handler(
    Query(params): Query<RemoteLatencyQuery>,
    State(db): State<std::sync::Arc<std::sync::Mutex<duckdb::Connection>>>,
) -> impl IntoResponse {
    let (status, response) = get_remote_latency(&db, &params.ip).await;
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