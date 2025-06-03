use axum::{
    http::StatusCode,
    routing::get,
    serve,
    Router,
    Json,
    response::IntoResponse
};

mod metrics;

#[tokio::main]
async fn main() {
    // tracing
    tracing_subscriber::fmt::init();

    let app = Router::new()
        .route("/", get(root))
        .route("/rpc/start-latency", get(start_latency));

    match tokio::net::TcpListener::bind("0.0.0.0:6000").await {
        Ok(listener) => {
            serve(listener, app).await.unwrap();
        }
        Err(error) => {panic!("{}", error)}
    }
}

async fn root() -> &'static str {
    "Version 2025-06-03"
}

async fn start_latency() -> impl IntoResponse {
    match metrics::latency::listener().await {
        Ok((_, latency_port)) => {
            (StatusCode::CREATED, Json(latency_port))
        }
        Err(_error) => {
            (StatusCode::INTERNAL_SERVER_ERROR, Json(0))
        }
    }
}