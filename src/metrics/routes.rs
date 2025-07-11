use std::net::IpAddr;
use axum::{extract::{State, Query}, response::IntoResponse, http::StatusCode, Json};
use crate::AppState;
use crate::db::metrics::get_metric;
use crate::metrics::{
    latency::{
        listener,
        send_latency
    },
    types::{
        LatencyResponseWrapper,
        LatencyResponse,
        RemoteLatencyQuery,
        Metric,
        ErrorResponse,
    },
};
use duckdb::DuckdbConnectionManager;

pub async fn get_metrics(
    State(app_state): State<AppState>,
) -> impl IntoResponse {
    match get_metric(app_state.db_pool.get()) {
        Ok(metrics) => {
            println!("{:?}", metrics);    
            (StatusCode::OK, Json(metrics))
        }
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(Vec::<Metric>::new())),
    }
}

pub async fn get_remote_latency_handler(
    Query(params): Query<RemoteLatencyQuery>,
    State(app_state): State<AppState>,
) -> impl IntoResponse {
    let (status, response) = get_remote_latency(app_state.db_pool.get(), &params.ip).await;
    match response {
        Some(latency_response) => (status, Json(LatencyResponseWrapper::Success(latency_response))),
        None => (status, Json(LatencyResponseWrapper::Error(ErrorResponse { error: "Failed to get remote latency".to_string() })))
    }
}

async fn get_remote_latency(
    db_conn: Result<r2d2::PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
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
                                        match send_latency(db_conn, ip, port).await {
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

pub async fn get_latency_server() -> impl IntoResponse {
    match listener().await {
        Ok((_, latency_port)) => {
            (StatusCode::CREATED, Json(latency_port))
        }
        Err(_error) => {
            (StatusCode::INTERNAL_SERVER_ERROR, Json(0))
        }
    }
}