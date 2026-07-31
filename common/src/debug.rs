use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Snapshot of consensus-tracked database state for divergence detection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateSnapshot {
    pub consensus_height: u64,
    pub committed_view: u64,
    pub table_hashes: HashMap<String, TableHashInfo>,
}

/// Hash and metadata for a single consensus-tracked table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableHashInfo {
    pub hash: String, // Blake3 hash as hex string
    pub row_count: usize,
    pub excluded_columns: Vec<String>,
}
