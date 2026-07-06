//! Adapter over the substrate's placement module (hopnet-storage::placement).
//!
//! The placement logic (RFC-004 scoring + seeded Fisher-Yates + modulo
//! distribution) moved to the substrate crate; this module maps the main
//! crate's Node/NodeMetrics types onto the crate's seams and preserves the
//! original call-site signatures. Unit tests moved with the logic.

use crate::db::Node;
use crate::db::metrics::NodeMetrics;
use crate::types::Blake3Hash;

pub use hopnet_storage::placement::{FragmentType, MetricsRow, Phase2Candidate, PlacementNode};

impl PlacementNode for Node {
    fn node_id(&self) -> i32 {
        self.node_id
    }
}

impl From<NodeMetrics> for MetricsRow {
    fn from(m: NodeMetrics) -> Self {
        MetricsRow {
            node_id: m.node_id,
            trust_factor: m.trust_factor,
            availability_score: m.availability_score,
            throughput_score: m.throughput_score,
            latency_score: m.latency_score,
            stability_score: m.stability_score,
            storage_multiplier: m.storage_multiplier,
        }
    }
}

/// Select nodes for a specific file's fragment placement.
/// Seeded by file_hash today; Stage B re-seeds from blob_id.
pub fn select_nodes_for_file(
    validators: Vec<Node>,
    all_metrics: Vec<NodeMetrics>,
    file_hash: &Blake3Hash,
) -> Vec<Node> {
    let metrics: Vec<MetricsRow> = all_metrics.into_iter().map(MetricsRow::from).collect();
    hopnet_storage::placement::select_nodes_for_blob(validators, metrics, file_hash.0.as_bytes())
}

/// Get fragment placement candidates using modulo distribution
/// (primary + 2 backups; see hopnet-storage::placement).
pub fn get_fragment_placement(local_index: u32, selected_nodes: &[Node]) -> Vec<&Node> {
    hopnet_storage::placement::get_fragment_placement(local_index, selected_nodes)
}

/// Score node metrics for placement ranking (RFC-004 weights).
pub fn calculate_final_placement_scores(
    node_metrics: Vec<NodeMetrics>,
    fragment_type: FragmentType,
) -> Vec<Phase2Candidate> {
    let metrics: Vec<MetricsRow> = node_metrics.into_iter().map(MetricsRow::from).collect();
    hopnet_storage::placement::calculate_final_placement_scores(metrics, fragment_type)
}
