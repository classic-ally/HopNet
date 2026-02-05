use serde::{Deserialize, Serialize};
use crate::db::{CustomUUID, Blake3Hash};

// Re-export API types from common for use elsewhere in this crate
pub use hopnet_common::{RegisterDeviceRequest, RegisterDeviceResponse, DeviceInfo};

/// Payload for RegisterDevice consensus transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterDevicePayload {
    pub id: CustomUUID,
    pub user_id: i32,
    pub api_key_hash: Blake3Hash,           // Blake3 hash of the secret portion
    pub encrypted_device_name: String,      // SIV-encrypted, hex-encoded
}

/// Payload for RevokeDevice consensus transaction (deletes the token)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RevokeDevicePayload {
    pub device_id: CustomUUID,
    pub user_id: i32,  // For authorization check
}
