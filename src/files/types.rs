use crate::types::Blake3Hash;
use serde::{Deserialize, Serialize};

/// Differential self-attestation report for fragment inventory synchronization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelfCheckFragments {
    /// Node performing the self-check
    pub node_id: i32,

    /// Consensus height when this check was performed
    pub self_verified_height: i32,

    /// Previous fragment count for state sync verification
    /// Other nodes verify this matches their view of the inventory
    pub previous_count: u32,

    /// Fragments found locally but not in consensus inventory
    pub fragments_added: Vec<Blake3Hash>,

    /// Fragments in consensus inventory but not found locally
    pub fragments_removed: Vec<Blake3Hash>,
}

impl SelfCheckFragments {
    /// Check if this is an empty report (no changes)
    pub fn is_empty(&self) -> bool {
        self.fragments_added.is_empty() && self.fragments_removed.is_empty()
    }
}
