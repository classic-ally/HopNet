use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub version: u32,
    pub git_commit: String,
    pub git_dirty: bool,
    pub captured_at: DateTime<Utc>,
    pub fixture_version: String,
    pub functions: BTreeMap<String, FunctionResult>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status")]
pub enum FunctionResult {
    #[serde(rename = "ok")]
    Ok { value: serde_json::Value },
    #[serde(rename = "error")]
    Error { error_variant: String },
}
