// Android DocumentProvider types
use serde::{Deserialize, Serialize};
use typeshare::typeshare;
use super::db::CustomUUID;

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
    pub size: i64,
    /// Last modified timestamp (epoch milliseconds)
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
