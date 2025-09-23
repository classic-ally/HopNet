// Shared database types for FileProvider
use serde::{Deserialize, Serialize};
use typeshare::typeshare;
use chrono::{DateTime, Utc};
use uuid::{Timestamp, Uuid};
use std::ops::Deref;

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

/// Custom UUID wrapper with v7 support and timestamp extraction
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
#[typeshare(serialized_as = "String")]
pub struct CustomUUID(Uuid);

impl CustomUUID {
    pub fn new(timestamp: Option<&Timestamp>) -> CustomUUID {
        match timestamp {
            Some(timestamp) => CustomUUID(Uuid::new_v7(*timestamp)),
            None => CustomUUID(Uuid::now_v7())
        }
    }

    pub fn from_str(uuid_str: &str) -> Result<CustomUUID, uuid::Error> {
        match Uuid::parse_str(uuid_str) {
            Ok(uuid) => Ok(CustomUUID(uuid)),
            Err(e) => Err(e),
        }
    }

    pub fn to_string(&self) -> String {
        self.0.to_string()
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }

    /// Extract timestamp from UUIDv7
    pub fn extract_timestamp(&self) -> Option<DateTime<Utc>> {
        // UUIDv7 encodes timestamp in the first 48 bits
        if self.0.get_version_num() == 7 {
            if let Some(timestamp) = self.0.get_timestamp() {
                let (seconds, nanos) = timestamp.to_unix();
                return DateTime::from_timestamp(seconds as i64, nanos);
            }
        }
        None
    }
}

impl Deref for CustomUUID {
    type Target = Uuid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for CustomUUID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// File item for frontend display with timestamps and metadata
#[derive(Serialize, Deserialize, Debug, Clone)]
#[typeshare]
pub struct FileItem {
    #[typeshare(serialized_as = "String")]
    pub id: CustomUUID, // Inode ID (UUIDv7)
    pub path: String, // Decrypted path
    pub inode_type: InodeType,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[typeshare(serialized_as = "String")]
    pub file_size: Option<u64>, // File size in bytes (None for folders)
    #[typeshare(serialized_as = "String")]
    pub creation_date: DateTime<Utc>, // From inode.id UUIDv7
    #[serde(skip_serializing_if = "Option::is_none")]
    #[typeshare(serialized_as = "String")]
    pub modification_date: Option<DateTime<Utc>>, // From data_id UUIDv7 for files
}