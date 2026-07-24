// Shared database types for FileProvider
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::ops::Deref;
use typeshare::typeshare;
use uuid::{Timestamp, Uuid};

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
    pub owner_node_id: i32, // Node that owns and processes this takeout
    pub status: TakeoutStatus,
    #[typeshare(serialized_as = "String")]
    pub created_at: DateTime<Utc>,
    #[typeshare(serialized_as = "String")]
    pub expires_at: DateTime<Utc>,
    pub consensus_height: i32,
}

/// Import status — tracks progress of user data ingest
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[typeshare]
pub enum ImportStatus {
    Pending,
    Importing,
    Completed,
    Failed,
}

/// Import record for user data ingest requests
#[derive(Serialize, Deserialize, Debug, Clone)]
#[typeshare]
pub struct ImportRecord {
    pub id: CustomUUID,
    pub user_id: i32,
    pub owner_node_id: i32,
    pub status: ImportStatus,
    #[typeshare(serialized_as = "String")]
    pub created_at: DateTime<Utc>,
}

/// Aggregate counts for a single import's per-path table. Owner-node-local;
/// surfaced via `GET /takeout/import/status`. Phase 4 frontend uses these for
/// progress display.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[typeshare]
pub struct ImportPathCounts {
    pub total: u32,
    pub pending: u32,
    pub imported: u32,
    pub skipped: u32,
    pub failed: u32,
}

/// Per-import path lifecycle status. `Pending` rows wait for 3.5 creation;
/// `Imported` are committed; `Failed` carries an `error_code`; `Skipped` is
/// reserved for collisions in future login→import flows.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[typeshare]
pub enum ImportPathStatus {
    Pending,
    Imported,
    Skipped,
    Failed,
}

/// Per-import path row, owner-node-local. Surfaced via the debug
/// `GET /takeout/import/paths` route in 3.4 for orchestrator observability;
/// 3.7 supersedes with the aggregate status route. Reuses `InodeType` for the
/// file/folder discriminator.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[typeshare]
pub struct ImportPathRow {
    pub path: String,
    pub path_type: InodeType,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[typeshare(serialized_as = "String")]
    pub size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_data_block_id: Option<CustomUUID>,
    pub status: ImportPathStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[typeshare(serialized_as = "String")]
    pub processed_at: Option<DateTime<Utc>>,
}

/// Custom UUID wrapper with v7 support and timestamp extraction
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
#[typeshare(serialized_as = "String")]
pub struct CustomUUID(Uuid);

impl CustomUUID {
    pub fn new(timestamp: Option<&Timestamp>) -> CustomUUID {
        match timestamp {
            Some(timestamp) => CustomUUID(Uuid::new_v7(*timestamp)),
            None => CustomUUID(Uuid::now_v7()),
        }
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }

    /// UUIDv7 cutoff for retention scans: `days` before now.
    ///
    /// Preserves sub-second precision: UUIDv7 ordering is millisecond-granular,
    /// so a seconds-truncated cutoff makes anything created in the current
    /// second invisible to `days = 0` scans.
    pub fn retention_cutoff(days: i64) -> CustomUUID {
        let cutoff_time = Utc::now() - chrono::Duration::days(days);
        let timestamp = Timestamp::from_unix(
            uuid::timestamp::context::NoContext,
            cutoff_time.timestamp() as u64,
            cutoff_time.timestamp_subsec_nanos(),
        );
        CustomUUID::new(Some(&timestamp))
    }

    /// UUIDv7 cutoff `ago` before now, at whatever granularity the caller needs.
    ///
    /// Deliberately NOT the implementation behind `retention_cutoff`, despite the
    /// overlap: that one selects rows for deletion, and those deletions are
    /// transactions, so a drift in its rounding changes what gets removed.
    /// Diagnostics wanting sub-day windows use this; retention keeps its own.
    pub fn cutoff_before(ago: chrono::Duration) -> CustomUUID {
        let cutoff_time = Utc::now() - ago;
        let timestamp = Timestamp::from_unix(
            uuid::timestamp::context::NoContext,
            cutoff_time.timestamp() as u64,
            cutoff_time.timestamp_subsec_nanos(),
        );
        CustomUUID::new(Some(&timestamp))
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

impl std::str::FromStr for CustomUUID {
    type Err = uuid::Error;

    fn from_str(uuid_str: &str) -> Result<CustomUUID, uuid::Error> {
        Uuid::parse_str(uuid_str).map(CustomUUID)
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

/// Data source for node storage baseline
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[typeshare]
pub enum NodeSource {
    System,   // Real system data
    Modified, // Modified from system data
    Added,    // User-added hypothetical node
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
