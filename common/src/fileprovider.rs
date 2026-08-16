// Shared FileProvider types
use super::db::InodeType;
use serde::{Deserialize, Serialize};
use typeshare::typeshare;

/// Health status for FileProvider - using snake_case to match server behavior
#[derive(Debug, Deserialize, Serialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
#[typeshare]
pub enum HealthStatus {
    Ready,
    NotReady,
}

/// Health check response
#[derive(Debug, Deserialize, Serialize)]
#[typeshare]
pub struct HealthResponse {
    pub status: HealthStatus,
    /// The node's CalVer code (RFC-022 S3) — what a client's `min_node`
    /// check reads at its probe. `0` = a pre-RFC-022 node that never
    /// sent the field.
    #[serde(default)]
    pub node_version: u32,
}

/// FileProvider item metadata
#[derive(Debug, Deserialize, Serialize, Clone)]
#[typeshare]
pub struct FileProviderItem {
    pub identifier: String, // "file:uuid" or "folder:hex"
    pub filename: String,
    pub parent_item_identifier: String,
    pub item_type: InodeType,
    pub file_size: Option<String>, // File size in bytes as string (None for folders) - String for typeshare compatibility
    pub creation_date: Option<String>, // ISO 8601 timestamp extracted from UUIDv7 or folder creation
    pub content_modification_date: Option<String>, // ISO 8601 timestamp from modified_at column
    #[typeshare(serialized_as = "Option<U64Height>")]
    pub modification_height: Option<u64>, // Consensus height when item was last modified
}

/// Directory enumeration response
#[derive(Debug, Deserialize, Serialize)]
#[typeshare]
pub struct EnumerateResponse {
    pub items: Vec<FileProviderItem>,
    pub next_page: Option<String>,
    #[typeshare(serialized_as = "U64Height")]
    pub current_consensus_height: u64,
}

/// Changes response for incremental sync
#[derive(Debug, Deserialize, Serialize)]
#[typeshare]
pub struct ChangesResponse {
    pub items: Vec<FileProviderItem>,
    pub deleted_identifiers: Vec<String>, // List of identifiers for deleted items
    #[typeshare(serialized_as = "U64Height")]
    pub current_consensus_height: u64,
}

/// Query parameters for changes endpoint
#[derive(Debug, Deserialize, Serialize)]
#[typeshare]
pub struct ChangesQuery {
    pub parent_path: Option<String>,
    #[typeshare(serialized_as = "Option<U64Height>")]
    pub since_height: Option<u64>,
}

/// Delete request for FileProvider
#[derive(Debug, Deserialize, Serialize)]
#[typeshare]
pub struct DeleteItemRequest {
    pub identifier: String, // The item identifier to delete
    pub recursive: bool,    // Whether to allow recursive deletion of non-empty folders
}

/// Download query for FileProvider
#[derive(Debug, Deserialize, Serialize)]
#[typeshare]
pub struct DownloadQuery {
    pub identifier: String, // The file identifier to download (e.g., "file:uuid")
}

/// Item query for FileProvider
#[derive(Debug, Deserialize, Serialize)]
#[typeshare]
pub struct ItemQuery {
    pub identifier: String, // The item identifier to lookup (e.g., "file:uuid" or "folder:hex")
}

/// Modify item response - returns new identifier after successful modification
#[derive(Debug, Deserialize, Serialize)]
#[typeshare]
pub struct ModifyItemResponse {
    pub new_identifier: String, // Updated identifier after path change (for folder renames/moves)
}

/// Test mode response for FileProvider testing
#[derive(Debug, Deserialize, Serialize)]
#[typeshare]
pub struct TestResponse {
    pub api_key: String,
    pub backend_url: String,
}
