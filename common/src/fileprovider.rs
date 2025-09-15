// Shared FileProvider types
use serde::{Deserialize, Serialize};
use typeshare::typeshare;
use super::db::InodeType;

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
}

/// FileProvider item metadata
#[derive(Debug, Deserialize, Serialize, Clone)]
#[typeshare]
pub struct FileProviderItem {
    pub identifier: String,           // "file:uuid" or "folder:hex"
    pub filename: String,
    pub parent_item_identifier: String,
    pub item_type: InodeType,
    pub file_size: Option<String>,    // File size in bytes as string (None for folders) - String for typeshare compatibility
    pub creation_date: Option<String>,           // ISO 8601 timestamp extracted from UUIDv7 or folder creation
    pub content_modification_date: Option<String>, // ISO 8601 timestamp from modified_at column
    pub modification_height: Option<i32>,        // Consensus height when item was last modified
}

/// Directory enumeration response
#[derive(Debug, Deserialize, Serialize)]
#[typeshare]
pub struct EnumerateResponse {
    pub items: Vec<FileProviderItem>,
    pub next_page: Option<String>,
    pub current_consensus_height: i32,
}

/// Changes response for incremental sync
#[derive(Debug, Deserialize, Serialize)]
#[typeshare]
pub struct ChangesResponse {
    pub items: Vec<FileProviderItem>,
    pub deleted_identifiers: Vec<String>,  // List of identifiers for deleted items
    pub current_consensus_height: i32,
}

/// Query parameters for changes endpoint
#[derive(Debug, Deserialize, Serialize)]
#[typeshare]
pub struct ChangesQuery {
    pub parent_path: Option<String>,
    pub since_height: Option<i32>,
}

/// Delete request for FileProvider
#[derive(Debug, Deserialize, Serialize)]
#[typeshare]
pub struct DeleteItemRequest {
    pub identifier: String,           // The item identifier to delete
    pub recursive: bool,              // Whether to allow recursive deletion of non-empty folders
}

/// Download query for FileProvider
#[derive(Debug, Deserialize, Serialize)]
#[typeshare]
pub struct DownloadQuery {
    pub identifier: String,           // The file identifier to download (e.g., "file:uuid")
}

/// Item query for FileProvider
#[derive(Debug, Deserialize, Serialize)]
#[typeshare]
pub struct ItemQuery {
    pub identifier: String,           // The item identifier to lookup (e.g., "file:uuid" or "folder:hex")
}

/// Modify item response - returns new identifier after successful modification
#[derive(Debug, Deserialize, Serialize)]
#[typeshare]
pub struct ModifyItemResponse {
    pub new_identifier: String,  // Updated identifier after path change (for folder renames/moves)
}

/// Test mode response for FileProvider testing
#[derive(Debug, Deserialize, Serialize)]
#[typeshare]
pub struct TestResponse {
    pub api_key: String,
    pub backend_url: String,
}