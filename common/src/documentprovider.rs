// Android DocumentProvider types
use super::db::CustomUUID;
use serde::{Deserialize, Serialize};
use typeshare::typeshare;

/// Document item for Android DocumentProvider
/// Represents a file or folder in the HopNet storage
#[derive(Debug, Deserialize, Serialize, Clone)]
#[typeshare]
#[serde(rename_all = "camelCase")]
pub struct DocumentProviderItem {
    /// Unique identifier (UUIDv7)
    pub id: CustomUUID,
    /// Display name
    pub name: String,
    /// MIME type ("vnd.android.document/directory" for folders, or actual MIME for files)
    pub mime_type: String,
    /// File size in bytes (0 for folders)
    #[typeshare(serialized_as = "number")]
    pub size: i64,
    /// Last modified timestamp (epoch milliseconds)
    #[typeshare(serialized_as = "number")]
    pub last_modified: i64,
    /// Parent folder ID (None for root-level items)
    pub parent_id: Option<CustomUUID>,
}

/// Response for enumerate endpoint
#[derive(Debug, Deserialize, Serialize)]
#[typeshare]
#[serde(rename_all = "camelCase")]
pub struct DocumentProviderEnumerateResponse {
    pub items: Vec<DocumentProviderItem>,
}

/// Request body for modify (rename/move) operation
#[derive(Debug, Deserialize, Serialize)]
#[typeshare]
#[serde(rename_all = "camelCase")]
pub struct ModifyDocumentProviderRequest {
    /// Inode UUID to modify
    pub id: String,
    /// New filename (for rename operation)
    pub name: Option<String>,
    /// New parent UUID or "root" (for move operation)
    pub parent_id: Option<String>,
}

/// Response for modify operation
#[derive(Debug, Deserialize, Serialize)]
#[typeshare]
#[serde(rename_all = "camelCase")]
pub struct ModifyDocumentProviderResponse {
    /// The identifier after modification (unchanged since inode ID is stable)
    pub new_identifier: String,
}
