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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shared_with_count: Option<u32>,
}

/// Network resilience statistics for cliff chart display
#[derive(Serialize, Deserialize, Debug, Clone)]
#[typeshare]
pub struct NetworkResilienceStats {
    pub unknown: ResilienceLevel,          // -2: Files without attestation data
    pub unrecoverable: ResilienceLevel,    // -1: Files that cannot be recovered
    pub critical: ResilienceLevel,         //  0: No fault tolerance (single point of failure)
    pub good: ResilienceLevel,             //  1: Can survive 1 node failure
    pub excellent: ResilienceLevel,        //  2: Can survive 2 node failures
    pub exceptional: ResilienceLevel,      // 3+: Can survive 3+ node failures
    pub total_files: u32,
    #[typeshare(serialized_as = "number")]
    pub computation_time_ms: u64,
}

/// Individual resilience level statistics
#[derive(Serialize, Deserialize, Debug, Clone)]
#[typeshare]
pub struct ResilienceLevel {
    pub file_count: u32,
    pub percentage: f64,
}

/// Data source for node storage baseline
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[typeshare]
pub enum NodeSource {
    System,    // Real system data
    Modified,  // Modified from system data
    Added,     // User-added hypothetical node
}

/// Original values for tracking modifications
#[derive(Serialize, Deserialize, Debug, Clone)]
#[typeshare]
pub struct OriginalNodeValues {
    #[typeshare(serialized_as = "number")]
    pub storage_total_gb: f64,
    #[typeshare(serialized_as = "number")]
    pub baseline_storage_gb: f64,
}

/// Node storage baseline data for fault tolerance curve generation
#[derive(Serialize, Deserialize, Debug, Clone)]
#[typeshare]
pub struct NodeStorageBaseline {
    pub node_id: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub display_name: String,
    #[typeshare(serialized_as = "number")]
    pub storage_total_gb: f64,
    #[typeshare(serialized_as = "number")]
    pub baseline_storage_gb: f64,
    #[serde(default = "default_node_source")]
    pub source: NodeSource,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original_values: Option<OriginalNodeValues>,
}

fn default_node_source() -> NodeSource {
    NodeSource::System
}

/// Point on the fault tolerance curve showing network resilience vs user data
#[derive(Serialize, Deserialize, Debug, Clone)]
#[typeshare]
pub struct FaultToleranceCurvePoint {
    #[typeshare(serialized_as = "number")]
    pub user_data_gb: f64,
    #[typeshare(serialized_as = "number")]
    pub active_nodes: usize,
    pub nodes_can_fail: i32,
    pub participating_nodes: Vec<NodeStorageBaseline>,
}