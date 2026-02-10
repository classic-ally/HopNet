use crate::types::Blake3Hash;
use crate::db::metrics::NodeMetrics;
use crate::db::Node;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FragmentType {
    Original,
    Recovery,
}

/// Select nodes for a specific file's fragment placement
///
/// This function implements file-level node selection with two strategies:
/// - Networks with ≤30 validators: Use ALL validators (maximum failure tolerance)
/// - Networks with >30 validators: Select best 30 using scoring + deterministic shuffle
///
/// The deterministic shuffle ensures that the same file_hash always selects the same
/// 30 nodes, which guarantees that fragments with the same local_index (but different
/// chunk_number) are placed on the same nodes - critical for Reed-Solomon properties.
///
/// # Arguments
/// * `validators` - Active validators at consensus height (from get_validators())
/// * `all_metrics` - All node metrics at consensus height
/// * `file_hash` - Hash of the file's data (used as deterministic seed)
///
/// # Returns
/// * Vector of selected nodes for placement (length ≤ min(validators.len(), 30))
pub fn select_nodes_for_file(
    validators: Vec<Node>,
    all_metrics: Vec<NodeMetrics>,
    file_hash: &Blake3Hash,
) -> Vec<Node> {
    // Strategy 1: Small network (≤30 validators) - use ALL for maximum failure tolerance
    if validators.len() <= 30 {
        tracing::debug!("Small network ({}≤30): using all validators", validators.len());
        return validators;  // Early exit - no metrics filtering needed
    }

    // Strategy 2: Large network (>30 validators) - select best 30
    // Step 1: Filter metrics to only active validators
    let validator_ids: std::collections::HashSet<i32> = validators.iter().map(|v| v.node_id).collect();
    let validator_metrics: Vec<NodeMetrics> = all_metrics
        .into_iter()
        .filter(|m| validator_ids.contains(&m.node_id))
        .collect();

    tracing::debug!(
        "Large network ({}): {} validators with metrics",
        validators.len(),
        validator_metrics.len()
    );

    // Step 2: Score all validator metrics (using FragmentType::Original for base scoring)
    let mut scored_candidates = calculate_final_placement_scores(
        validator_metrics,
        FragmentType::Original,
    );

    // Step 3: Take top 60 candidates (2× target for diversity)
    let top_count = 60.min(scored_candidates.len());
    scored_candidates.truncate(top_count);

    tracing::debug!("Scored top {} candidates", top_count);

    // Step 4: Convert Phase2Candidates back to Nodes for shuffle
    let mut top_nodes: Vec<Node> = scored_candidates
        .into_iter()
        .filter_map(|candidate| {
            validators.iter().find(|v| v.node_id == candidate.node_id).cloned()
        })
        .collect();

    // Step 5: Deterministic shuffle using file_hash as seed (Fisher-Yates with Blake3)
    deterministic_shuffle(&mut top_nodes, file_hash);

    // Step 6: Return top 30 after shuffle
    let target_count = 30.min(top_nodes.len());
    top_nodes.truncate(target_count);

    tracing::debug!(
        "Selected {} nodes for file {}",
        target_count,
        file_hash.to_hex().chars().take(8).collect::<String>()
    );

    top_nodes
}

/// Deterministic Fisher-Yates shuffle using Blake3 hash as entropy source
///
/// This ensures the same seed (file_hash) produces the same shuffle every time,
/// which is critical for consistent fragment placement across all nodes in the network.
///
/// The modulo bias here is negligible (~10^-9%) and has no practical impact.
///
/// # Arguments
/// * `nodes` - Slice to shuffle in-place
/// * `seed` - Blake3Hash used as deterministic seed
fn deterministic_shuffle(nodes: &mut [Node], seed: &Blake3Hash) {
    let len = nodes.len();

    for i in 0..len.saturating_sub(1) {
        // Hash: seed || iteration_index to get deterministic "random" value
        let mut input = Vec::with_capacity(32 + 8);
        input.extend_from_slice(seed.as_bytes());
        input.extend_from_slice(&i.to_be_bytes());
        let hash = blake3::hash(&input);

        // Use first 8 bytes of hash as u64 for swap index
        let random_value = u64::from_be_bytes([
            hash.as_bytes()[0], hash.as_bytes()[1], hash.as_bytes()[2], hash.as_bytes()[3],
            hash.as_bytes()[4], hash.as_bytes()[5], hash.as_bytes()[6], hash.as_bytes()[7],
        ]);

        // Fisher-Yates: swap current with random element from remaining
        let swap_index = i + (random_value as usize % (len - i));
        nodes.swap(i, swap_index);
    }
}

/// Get fragment placement candidates using modulo distribution
///
/// Returns primary placement node plus 2 backup nodes in preference order.
/// Uses simple modulo arithmetic to ensure:
/// - Same local_index always maps to same node (across all chunks)
/// - Even distribution across all selected nodes (±1 fragment max imbalance)
/// - Deterministic backup selection with wraparound
///
/// # Arguments
/// * `local_index` - Fragment's position within its chunk (0-29 for 30-fragment chunks)
/// * `selected_nodes` - Nodes selected for this file (from select_nodes_for_file)
///
/// # Returns
/// * Vector of up to 3 node references: [primary, backup1, backup2]
pub fn get_fragment_placement(local_index: u32, selected_nodes: &[Node]) -> Vec<&Node> {
    if selected_nodes.is_empty() {
        return Vec::new();
    }

    let len = selected_nodes.len();
    let primary_idx = (local_index as usize) % len;
    let backup1_idx = (local_index as usize + 1) % len;
    let backup2_idx = (local_index as usize + 2) % len;

    vec![
        &selected_nodes[primary_idx],
        &selected_nodes[backup1_idx],
        &selected_nodes[backup2_idx],
    ]
}

/// Phase 2: Scored candidate ready for placement decision
#[derive(Debug, Clone, serde::Serialize)]
pub struct Phase2Candidate {
    pub node_id: i32,
    pub final_score: f64,  // Weighted score for placement ranking
    pub ip_address: String,
    pub port: i32,
}

/// RFC-004 scoring weights: availability, throughput, latency, stability
const PLACEMENT_WEIGHTS: (f64, f64, f64, f64) = (0.4, 0.3, 0.2, 0.1);

/// Phase 2: Calculate final placement scores using RFC-compliant weights
/// Takes NodeMetrics from database and applies fragment-type-specific scoring
pub fn calculate_final_placement_scores(
    node_metrics: Vec<NodeMetrics>,
    fragment_type: FragmentType,
) -> Vec<Phase2Candidate> {
    let mut candidates: Vec<Phase2Candidate> = node_metrics
        .into_iter()
        .map(|metrics| {
            // Calculate base score using fragment-type-specific weights
            let base_score = match fragment_type {
                FragmentType::Original => {
                    // Standard scoring: higher performance = higher score
                    metrics.availability_score * PLACEMENT_WEIGHTS.0 +
                    metrics.throughput_score * PLACEMENT_WEIGHTS.1 +
                    metrics.latency_score * PLACEMENT_WEIGHTS.2 +
                    metrics.stability_score * PLACEMENT_WEIGHTS.3
                }
                FragmentType::Recovery => {
                    // Inverse scoring for geographic diversity and load distribution
                    metrics.availability_score * PLACEMENT_WEIGHTS.0 +
                    (1.0 - metrics.throughput_score) * PLACEMENT_WEIGHTS.1 + // Prefer lower throughput nodes
                    (1.0 - metrics.latency_score) * PLACEMENT_WEIGHTS.2 +    // Prefer higher latency (distant) nodes
                    (1.0 - metrics.stability_score) * PLACEMENT_WEIGHTS.3     // Prefer less stable nodes for diversity
                }
            };
            
            // Apply trust factor blending for new nodes
            let trusted_score = if metrics.trust_factor < 1.0 {
                // Blend measured score with conservative fallback (0.5)
                base_score * metrics.trust_factor + 0.5 * (1.0 - metrics.trust_factor)
            } else {
                base_score
            };
            
            // Apply storage multiplier (e^(-5 * utilization))
            let final_score = trusted_score * metrics.storage_multiplier;
            
            Phase2Candidate {
                node_id: metrics.node_id,
                final_score,
                ip_address: metrics.ip_address,
                port: metrics.port,
            }
        })
        .collect();
    
    // Sort by final score (descending - best first)
    candidates.sort_by(|a, b| b.final_score.partial_cmp(&a.final_score).unwrap());

    candidates
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{DuckdbConnectionManager, PubKey};
    use ed25519_dalek::SigningKey;
    use rand_core::OsRng;

    fn setup_test_db() -> r2d2::Pool<DuckdbConnectionManager> {
        let manager = DuckdbConnectionManager::memory().unwrap();
        let pool = r2d2::Pool::builder()
            .max_size(1)
            .build(manager)
            .unwrap();

        crate::db::shared::initialize(pool.get().unwrap()).unwrap();
        pool
    }

    fn generate_test_pubkey() -> PubKey {
        let signing_key = SigningKey::generate(&mut OsRng);
        PubKey(signing_key.verifying_key())
    }

    fn create_test_nodes(count: usize) -> Vec<Node> {
        (1..=count)
            .map(|i| Node {
                node_id: i as i32,
                name: format!("node{}", i),
                ip_address: format!("127.0.0.{}", i),
                port: 8000 + i as i32,
                owner: 1,
                pubkey: generate_test_pubkey(),
            })
            .collect()
    }

    fn create_test_metrics(count: usize) -> Vec<NodeMetrics> {
        (1..=count)
            .map(|i| NodeMetrics {
                node_id: i as i32,
                ip_address: format!("127.0.0.{}", i),
                port: 8000 + i as i32,
                pubkey: generate_test_pubkey(),
                sample_count_7d: 100,
                trust_factor: 1.0,
                availability_score: 0.9 + (i as f64 * 0.01),
                throughput_score: 0.8 + (i as f64 * 0.01),
                latency_score: 0.7 + (i as f64 * 0.01),
                stability_score: 0.6 + (i as f64 * 0.01),
                storage_utilization: 0.5,
                storage_multiplier: 1.0,
            })
            .collect()
    }

    #[test]
    fn test_select_nodes_small_network_uses_all() {
        // With ≤30 validators, should return all validators without filtering
        let validators = create_test_nodes(20);
        let metrics = create_test_metrics(20);
        let file_hash = Blake3Hash::from_bytes([1u8; 32]);

        let selected = select_nodes_for_file(validators.clone(), metrics, &file_hash);

        // Should return all 20 validators
        assert_eq!(selected.len(), 20);

        // Should contain all original validators (order may differ due to no shuffle in small network)
        for validator in &validators {
            assert!(selected.iter().any(|n| n.node_id == validator.node_id));
        }
    }

    #[test]
    fn test_select_nodes_large_network_filters_to_30() {
        // With >30 validators, should filter to best 30
        let validators = create_test_nodes(50);
        let metrics = create_test_metrics(50);
        let file_hash = Blake3Hash::from_bytes([2u8; 32]);

        let selected = select_nodes_for_file(validators, metrics, &file_hash);

        // Should return exactly 30 nodes
        assert_eq!(selected.len(), 30);

        // All selected nodes should have unique node_ids
        let mut node_ids: Vec<i32> = selected.iter().map(|n| n.node_id).collect();
        node_ids.sort();
        node_ids.dedup();
        assert_eq!(node_ids.len(), 30);
    }

    #[test]
    fn test_select_nodes_deterministic_same_hash() {
        // Same file_hash should always select the same nodes
        let validators = create_test_nodes(50);
        let metrics = create_test_metrics(50);
        let file_hash = Blake3Hash::from_bytes([3u8; 32]);

        let selected1 = select_nodes_for_file(validators.clone(), metrics.clone(), &file_hash);
        let selected2 = select_nodes_for_file(validators, metrics, &file_hash);

        // Should be identical
        assert_eq!(selected1.len(), selected2.len());
        for (n1, n2) in selected1.iter().zip(selected2.iter()) {
            assert_eq!(n1.node_id, n2.node_id);
            assert_eq!(n1.ip_address, n2.ip_address);
            assert_eq!(n1.port, n2.port);
        }
    }

    #[test]
    fn test_select_nodes_different_hash_different_selection() {
        // Different file_hash should select different nodes (with high probability)
        let validators = create_test_nodes(50);
        let metrics = create_test_metrics(50);
        let file_hash1 = Blake3Hash::from_bytes([4u8; 32]);
        let file_hash2 = Blake3Hash::from_bytes([5u8; 32]);

        let selected1 = select_nodes_for_file(validators.clone(), metrics.clone(), &file_hash1);
        let selected2 = select_nodes_for_file(validators, metrics, &file_hash2);

        // Should have 30 nodes each
        assert_eq!(selected1.len(), 30);
        assert_eq!(selected2.len(), 30);

        // Extract node_ids
        let ids1: Vec<i32> = selected1.iter().map(|n| n.node_id).collect();
        let ids2: Vec<i32> = selected2.iter().map(|n| n.node_id).collect();

        // Order should be different (deterministic shuffle with different seeds)
        // At least some positions should differ
        let differences = ids1.iter().zip(ids2.iter()).filter(|(a, b)| a != b).count();
        assert!(differences > 0, "Expected different orderings for different file hashes");
    }

    #[test]
    fn test_select_nodes_filters_inactive_validators() {
        // Metrics for 50 nodes, but only 40 are active validators
        let validators = create_test_nodes(40);
        let metrics = create_test_metrics(50); // Extra metrics for nodes not in validator set
        let file_hash = Blake3Hash::from_bytes([6u8; 32]);

        let selected = select_nodes_for_file(validators, metrics, &file_hash);

        // Should return 30 nodes (filtered from active validators only)
        assert_eq!(selected.len(), 30);

        // All selected nodes should be from the active validator set (node_id 1-40)
        for node in &selected {
            assert!(node.node_id >= 1 && node.node_id <= 40);
        }
    }

    #[test]
    fn test_get_fragment_placement_basic() {
        let nodes = create_test_nodes(10);

        // Fragment 0 should go to node 0 (primary), 1 (backup1), 2 (backup2)
        let placement = get_fragment_placement(0, &nodes);
        assert_eq!(placement.len(), 3);
        assert_eq!(placement[0].node_id, 1); // nodes[0]
        assert_eq!(placement[1].node_id, 2); // nodes[1]
        assert_eq!(placement[2].node_id, 3); // nodes[2]

        // Fragment 5 should go to node 5 (primary), 6 (backup1), 7 (backup2)
        let placement = get_fragment_placement(5, &nodes);
        assert_eq!(placement.len(), 3);
        assert_eq!(placement[0].node_id, 6); // nodes[5]
        assert_eq!(placement[1].node_id, 7); // nodes[6]
        assert_eq!(placement[2].node_id, 8); // nodes[7]
    }

    #[test]
    fn test_get_fragment_placement_wraparound() {
        let nodes = create_test_nodes(10);

        // Fragment 9 (last) should wrap around for backups
        let placement = get_fragment_placement(9, &nodes);
        assert_eq!(placement.len(), 3);
        assert_eq!(placement[0].node_id, 10); // nodes[9] - primary
        assert_eq!(placement[1].node_id, 1);  // nodes[0] - wraparound backup1
        assert_eq!(placement[2].node_id, 2);  // nodes[1] - wraparound backup2

        // Fragment 8 should have one wraparound backup
        let placement = get_fragment_placement(8, &nodes);
        assert_eq!(placement.len(), 3);
        assert_eq!(placement[0].node_id, 9);  // nodes[8] - primary
        assert_eq!(placement[1].node_id, 10); // nodes[9] - backup1
        assert_eq!(placement[2].node_id, 1);  // nodes[0] - wraparound backup2
    }

    #[test]
    fn test_get_fragment_placement_deterministic() {
        // Same local_index should always map to same node (critical for RS properties)
        let nodes = create_test_nodes(30);

        let placement1 = get_fragment_placement(15, &nodes);
        let placement2 = get_fragment_placement(15, &nodes);

        assert_eq!(placement1.len(), placement2.len());
        for (p1, p2) in placement1.iter().zip(placement2.iter()) {
            assert_eq!(p1.node_id, p2.node_id);
        }
    }

    #[test]
    fn test_get_fragment_placement_even_distribution() {
        // With 30 fragments and 30 nodes, each node should get exactly 1 primary placement
        let nodes = create_test_nodes(30);
        let mut primary_counts = vec![0; 30];

        for local_index in 0..30 {
            let placement = get_fragment_placement(local_index, &nodes);
            let primary_node_id = placement[0].node_id;
            primary_counts[(primary_node_id - 1) as usize] += 1;
        }

        // Each node should be primary for exactly 1 fragment
        for count in primary_counts {
            assert_eq!(count, 1);
        }
    }

    #[test]
    fn test_get_fragment_placement_imbalance_non_divisible() {
        // With 30 fragments and 7 nodes, distribution should be ±1 fragment
        let nodes = create_test_nodes(7);
        let mut primary_counts = vec![0; 7];

        for local_index in 0..30 {
            let placement = get_fragment_placement(local_index, &nodes);
            let primary_node_id = placement[0].node_id;
            primary_counts[(primary_node_id - 1) as usize] += 1;
        }

        // Expected: 30/7 = 4.28, so nodes get 4 or 5 fragments
        let min_count = *primary_counts.iter().min().unwrap();
        let max_count = *primary_counts.iter().max().unwrap();
        assert!(max_count - min_count <= 1, "Max imbalance should be ±1 fragment");

        // Verify total is 30
        let total: i32 = primary_counts.iter().sum();
        assert_eq!(total, 30);
    }

    #[test]
    fn test_get_fragment_placement_empty_nodes() {
        let nodes: Vec<Node> = vec![];
        let placement = get_fragment_placement(0, &nodes);
        assert_eq!(placement.len(), 0);
    }

    #[test]
    fn test_get_fragment_placement_single_node() {
        // With only 1 node, all fragments (primary + backups) go to same node
        let nodes = create_test_nodes(1);

        let placement = get_fragment_placement(0, &nodes);
        assert_eq!(placement.len(), 3);
        assert_eq!(placement[0].node_id, 1);
        assert_eq!(placement[1].node_id, 1); // Wraparound to same node
        assert_eq!(placement[2].node_id, 1); // Wraparound to same node
    }
}