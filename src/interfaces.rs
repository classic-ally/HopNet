use axum::{http::StatusCode, response::IntoResponse, Json};
use network_interface::NetworkInterface;
use network_interface::NetworkInterfaceConfig;

pub async fn get_interfaces() -> impl IntoResponse {
    let network_interfaces = NetworkInterface::show();
    match network_interfaces {
        Ok(interfaces) => {
            // Filter out interfaces with empty addr
            let filtered_interfaces = interfaces.into_iter().filter(|iface| !iface.addr.is_empty()).collect::<Vec<_>>();
            (StatusCode::OK, Json(filtered_interfaces))
        }
        Err(_) => {
            (StatusCode::INTERNAL_SERVER_ERROR, Json(Vec::<NetworkInterface>::new()))
        }
    }
}