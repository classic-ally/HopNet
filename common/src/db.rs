// Shared database types for FileProvider
use serde::{Deserialize, Serialize};
use typeshare::typeshare;
use chrono::{DateTime, Utc};

/// Inode type - file or folder
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[typeshare]
pub enum InodeType {
    File,
    Folder,
}

/// Takeout status - tracks progress of user data export
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[typeshare]
pub enum TakeoutStatus {
    Pending,
    Materializing,
    Ready,
    Expired,
    Cancelled,
}

/// Takeout record for user data export requests
#[derive(Serialize, Deserialize, Debug, Clone)]
#[typeshare]
pub struct TakeoutRecord {
    pub id: String, // UUID as string for frontend compatibility
    pub user_id: i32,
    pub owner_node_id: i32,  // Node that owns and processes this takeout
    pub status: TakeoutStatus,
    #[typeshare(serialized_as = "String")]
    pub created_at: DateTime<Utc>,
    #[typeshare(serialized_as = "String")]
    pub expires_at: DateTime<Utc>,
    pub consensus_height: i32,
}