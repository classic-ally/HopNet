// Shared setup types
use serde::{Deserialize, Serialize};
use typeshare::typeshare;

/// Initial setup payload for creating a new HopNet node from scratch
#[derive(Debug, Deserialize, Serialize)]
#[typeshare]
pub struct InitialSetupPayload {
    pub username: String,
    pub password: String,
    pub node_name: String,
    pub ip_address: String,
    pub port: i32,
}