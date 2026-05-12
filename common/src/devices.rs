// Device management types for Android DocumentProvider and macOS FileProvider
use super::db::CustomUUID;
use serde::{Deserialize, Serialize};
use typeshare::typeshare;

/// API request for device registration
#[derive(Debug, Deserialize, Serialize)]
#[typeshare]
pub struct RegisterDeviceRequest {
    pub device_name: String,
}

/// API response for device registration (shows API key once)
#[derive(Debug, Deserialize, Serialize)]
#[typeshare]
pub struct RegisterDeviceResponse {
    #[typeshare(serialized_as = "String")]
    pub device_id: CustomUUID,
    pub api_key: String, // Full token: {device_id}.{secret} - only shown once
}

/// API response for listing devices (decrypted device name, no API key)
#[derive(Debug, Deserialize, Serialize)]
#[typeshare]
pub struct DeviceInfo {
    #[typeshare(serialized_as = "String")]
    pub id: CustomUUID,
    pub device_name: String, // Decrypted for display
    #[typeshare(serialized_as = "number")]
    pub created_at: i64, // Unix timestamp extracted from UUIDv7
}
