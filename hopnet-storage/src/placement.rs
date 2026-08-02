//! Deterministic fragment placement (RFC-004 scoring + seeded shuffle).
//!
//! Moved from the main crate's files/placement.rs, generalized over the host's
//! node type: the substrate never sees the host's Node/NodeMetrics structs —
//! callers implement [`PlacementNode`] and map their metrics into
//! [`MetricsRow`]. Logic is unchanged.
//!
//! The placement seed is currently the caller's choice (the fs projection
//! passes file_hash bytes); Stage B re-seeds from blob_id.

/// Placement seed derived from the blob id (v1 contract): public, random,
/// zero plaintext-derived input — every node computes placement from
/// replicated state alone. (Replaces the plaintext-derived file_hash seed.)
pub fn placement_seed(blob_id: &crate::types::BlobId) -> [u8; 32] {
    *blake3::hash(blob_id.as_bytes()).as_bytes()
}

/// Anything placeable: the substrate only needs a stable node id.
pub trait PlacementNode {
    fn node_id(&self) -> i32;
}

/// Node quality metrics at a consensus height — the substrate-owned mirror of
/// the host's replicated metrics row (score fields only).
#[derive(Debug, Clone)]
pub struct MetricsRow {
    pub node_id: i32,
    pub trust_factor: f64,
    pub availability_score: f64,
    pub throughput_score: f64,
    pub latency_score: f64,
    pub stability_score: f64,
    pub storage_multiplier: f64,
}

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
/// The deterministic shuffle ensures that the same seed always selects the same
/// 30 nodes, which guarantees that fragments with the same local_index (but different
/// chunk_number) are placed on the same nodes - critical for Reed-Solomon properties.
///
/// # Arguments
/// * `validators` - Active validators at consensus height
/// * `all_metrics` - All node metrics at consensus height
/// * `placement_seed` - 32-byte deterministic seed (same seed → same selection)
///
/// # Returns
/// * Vector of selected nodes for placement (length ≤ min(validators.len(), 30))
pub fn select_nodes_for_blob<N: PlacementNode + Clone>(
    validators: Vec<N>,
    all_metrics: Vec<MetricsRow>,
    placement_seed: &[u8; 32],
) -> Vec<N> {
    // Strategy 1: Small network (≤30 validators) - use ALL for maximum failure tolerance
    if validators.len() <= 30 {
        tracing::debug!(
            "Small network ({}≤30): using all validators",
            validators.len()
        );
        return validators; // Early exit - no metrics filtering needed
    }

    // Strategy 2: Large network (>30 validators) - select best 30
    // Step 1: Filter metrics to only active validators
    let validator_ids: std::collections::HashSet<i32> =
        validators.iter().map(|v| v.node_id()).collect();
    let validator_metrics: Vec<MetricsRow> = all_metrics
        .into_iter()
        .filter(|m| validator_ids.contains(&m.node_id))
        .collect();

    tracing::debug!(
        "Large network ({}): {} validators with metrics",
        validators.len(),
        validator_metrics.len()
    );

    // Step 2: Score all validator metrics (using FragmentType::Original for base scoring)
    let mut scored_candidates =
        calculate_final_placement_scores(validator_metrics, FragmentType::Original);

    // Step 3: Take top 60 candidates (2× target for diversity)
    let top_count = 60.min(scored_candidates.len());
    scored_candidates.truncate(top_count);

    tracing::debug!("Scored top {} candidates", top_count);

    // Step 4: Convert scored candidates back to nodes for shuffle
    let mut top_nodes: Vec<N> = scored_candidates
        .into_iter()
        .filter_map(|candidate| {
            validators
                .iter()
                .find(|v| v.node_id() == candidate.node_id)
                .cloned()
        })
        .collect();

    // Step 5: Deterministic shuffle using the seed (Fisher-Yates with Blake3)
    deterministic_shuffle(&mut top_nodes, placement_seed);

    // Step 6: Return top 30 after shuffle
    let target_count = 30.min(top_nodes.len());
    top_nodes.truncate(target_count);

    tracing::debug!(
        "Selected {} nodes for seed {}",
        target_count,
        placement_seed[..4]
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    );

    top_nodes
}

/// Deterministic Fisher-Yates shuffle using Blake3 hash as entropy source
///
/// This ensures the same seed produces the same shuffle every time,
/// which is critical for consistent fragment placement across all nodes in the network.
///
/// The modulo bias here is negligible (~10^-9%) and has no practical impact.
fn deterministic_shuffle<N>(nodes: &mut [N], seed: &[u8; 32]) {
    let len = nodes.len();

    for i in 0..len.saturating_sub(1) {
        // Hash: seed || iteration_index to get deterministic "random" value
        let mut input = Vec::with_capacity(32 + 8);
        input.extend_from_slice(seed);
        input.extend_from_slice(&i.to_be_bytes());
        let hash = blake3::hash(&input);

        // Use first 8 bytes of hash as u64 for swap index
        let random_value = u64::from_be_bytes([
            hash.as_bytes()[0],
            hash.as_bytes()[1],
            hash.as_bytes()[2],
            hash.as_bytes()[3],
            hash.as_bytes()[4],
            hash.as_bytes()[5],
            hash.as_bytes()[6],
            hash.as_bytes()[7],
        ]);

        // Fisher-Yates: swap current with random element from remaining
        let swap_index = i + (random_value as usize % (len - i));
        nodes.swap(i, swap_index);
    }
}

/// Phase 2: Scored candidate ready for placement decision
#[derive(Debug, Clone, serde::Serialize)]
pub struct Phase2Candidate {
    pub node_id: i32,
    pub final_score: f64, // Weighted score for placement ranking
}

/// RFC-004 scoring weights: availability, throughput, latency, stability
const PLACEMENT_WEIGHTS: (f64, f64, f64, f64) = (0.4, 0.3, 0.2, 0.1);

/// One row's final placement score in [0, 1]: RFC-004 weighted base,
/// trust-factor blend for new nodes, storage multiplier.
fn score_row(metrics: &MetricsRow, fragment_type: FragmentType) -> f64 {
    let base_score = match fragment_type {
        FragmentType::Original => {
            // Standard scoring: higher performance = higher score
            metrics.availability_score * PLACEMENT_WEIGHTS.0
                + metrics.throughput_score * PLACEMENT_WEIGHTS.1
                + metrics.latency_score * PLACEMENT_WEIGHTS.2
                + metrics.stability_score * PLACEMENT_WEIGHTS.3
        }
        FragmentType::Recovery => {
            // Inverse scoring for geographic diversity and load distribution
            metrics.availability_score * PLACEMENT_WEIGHTS.0 +
            (1.0 - metrics.throughput_score) * PLACEMENT_WEIGHTS.1 + // Prefer lower throughput nodes
            (1.0 - metrics.latency_score) * PLACEMENT_WEIGHTS.2 +    // Prefer higher latency (distant) nodes
            (1.0 - metrics.stability_score) * PLACEMENT_WEIGHTS.3 // Prefer less stable nodes for diversity
        }
    };

    // Blend measured score with a conservative fallback (0.5) for new nodes
    let trusted_score = if metrics.trust_factor < 1.0 {
        base_score * metrics.trust_factor + 0.5 * (1.0 - metrics.trust_factor)
    } else {
        base_score
    };

    // Storage multiplier (e^(-5 * utilization))
    trusted_score * metrics.storage_multiplier
}

/// Quantized placement weight (RFC-STORAGE-001 Placement): the RFC-004
/// Original-path score bucketed to 16 levels, never zero (rendezvous
/// scores divide by it). Coarse buckets mean placement shifts only on real
/// metric change, and every node derives the same weight from the same
/// replicated rows.
pub fn quantized_weight(metrics: &MetricsRow) -> u64 {
    let score = score_row(metrics, FragmentType::Original);
    ((score * 16.0).round() as i64).clamp(1, 16) as u64
}

/// Balanced capped rendezvous over an arbitrary score function
/// (RFC-STORAGE-001 Placement; mirrors `placeBalancedAll` in
/// `spec/storage_policy.qnt` — the drift-guard test reproduces the model's
/// literal tables through this fold).
///
/// Classes in index order; each goes to the lowest-scoring member among
/// those at the CURRENT minimum load, ties to the smaller node id. Loads
/// end at the tightest integer split (max − min ≤ 1, no zero-load members
/// while classes ≥ members). Member order cannot matter: selection is a
/// minimum over a totally ordered key.
///
/// Score contract: lower wins, already weight-divided.
pub fn assign_classes_by_score(
    members: &[i32],
    n_classes: u32,
    score: impl Fn(i32, u32) -> u64,
) -> Vec<i32> {
    if members.is_empty() {
        return Vec::new();
    }
    let mut loads: std::collections::HashMap<i32, u32> = members.iter().map(|&m| (m, 0)).collect();
    let mut assignment = Vec::with_capacity(n_classes as usize);
    for class in 0..n_classes {
        let min_load = *loads.values().min().expect("members non-empty");
        let chosen = members
            .iter()
            .copied()
            .filter(|m| loads[m] == min_load)
            .min_by_key(|&m| (score(m, class), m))
            .expect("min-load tier non-empty");
        *loads.get_mut(&chosen).expect("chosen is a member") += 1;
        assignment.push(chosen);
    }
    assignment
}

/// Production placement: class → responsible node for one blob.
/// Score = first 8 bytes (BE) of blake3(seed ‖ class ‖ node_id), divided
/// by the node's quantized weight (integer division, like the model's
/// `hrwScore = mix / w`). Weights default to 1 when absent.
pub fn assign_fragment_classes(
    seed: &[u8; 32],
    members: &[i32],
    weights: &std::collections::HashMap<i32, u64>,
    n_classes: u32,
) -> Vec<i32> {
    assign_classes_by_score(members, n_classes, |node, class| {
        let mut input = [0u8; 32 + 4 + 4];
        input[..32].copy_from_slice(seed);
        input[32..36].copy_from_slice(&class.to_be_bytes());
        input[36..].copy_from_slice(&node.to_be_bytes());
        let hash = blake3::hash(&input);
        let raw = u64::from_be_bytes(hash.as_bytes()[..8].try_into().expect("8 bytes"));
        raw / weights.get(&node).copied().unwrap_or(1).max(1)
    })
}

/// Single responsible node for one class (None on an empty member set).
pub fn responsible_node(
    seed: &[u8; 32],
    class: u32,
    members: &[i32],
    weights: &std::collections::HashMap<i32, u64>,
    n_classes: u32,
) -> Option<i32> {
    assign_fragment_classes(seed, members, weights, n_classes)
        .get(class as usize)
        .copied()
}

/// Phase 2: Calculate final placement scores using RFC-compliant weights
/// Takes metrics rows and applies fragment-type-specific scoring
pub fn calculate_final_placement_scores(
    node_metrics: Vec<MetricsRow>,
    fragment_type: FragmentType,
) -> Vec<Phase2Candidate> {
    let mut candidates: Vec<Phase2Candidate> = node_metrics
        .into_iter()
        .map(|metrics| Phase2Candidate {
            node_id: metrics.node_id,
            final_score: score_row(&metrics, fragment_type),
        })
        .collect();

    // Sort by final score (descending - best first)
    candidates.sort_by(|a, b| b.final_score.partial_cmp(&a.final_score).unwrap());

    candidates
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq)]
    struct TestNode {
        node_id: i32,
    }

    impl PlacementNode for TestNode {
        fn node_id(&self) -> i32 {
            self.node_id
        }
    }

    fn create_test_nodes(count: usize) -> Vec<TestNode> {
        (1..=count)
            .map(|i| TestNode { node_id: i as i32 })
            .collect()
    }

    fn create_test_metrics(count: usize) -> Vec<MetricsRow> {
        (1..=count)
            .map(|i| MetricsRow {
                node_id: i as i32,
                trust_factor: 1.0,
                availability_score: 0.9 + (i as f64 * 0.01),
                throughput_score: 0.8 + (i as f64 * 0.01),
                latency_score: 0.7 + (i as f64 * 0.01),
                stability_score: 0.6 + (i as f64 * 0.01),
                storage_multiplier: 1.0,
            })
            .collect()
    }

    fn seed(byte: u8) -> [u8; 32] {
        [byte; 32]
    }

    #[test]
    fn test_select_nodes_small_network_uses_all() {
        // With ≤30 validators, should return all validators without filtering
        let validators = create_test_nodes(20);
        let metrics = create_test_metrics(20);

        let selected = select_nodes_for_blob(validators.clone(), metrics, &seed(1));

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

        let selected = select_nodes_for_blob(validators, metrics, &seed(2));

        // Should return exactly 30 nodes
        assert_eq!(selected.len(), 30);

        // All selected nodes should have unique node_ids
        let mut node_ids: Vec<i32> = selected.iter().map(|n| n.node_id).collect();
        node_ids.sort();
        node_ids.dedup();
        assert_eq!(node_ids.len(), 30);
    }

    #[test]
    fn test_select_nodes_deterministic_same_seed() {
        // Same seed should always select the same nodes
        let validators = create_test_nodes(50);
        let metrics = create_test_metrics(50);

        let selected1 = select_nodes_for_blob(validators.clone(), metrics.clone(), &seed(3));
        let selected2 = select_nodes_for_blob(validators, metrics, &seed(3));

        // Should be identical
        assert_eq!(selected1.len(), selected2.len());
        for (n1, n2) in selected1.iter().zip(selected2.iter()) {
            assert_eq!(n1.node_id, n2.node_id);
        }
    }

    #[test]
    fn test_select_nodes_different_seed_different_selection() {
        // Different seed should select different nodes (with high probability)
        let validators = create_test_nodes(50);
        let metrics = create_test_metrics(50);

        let selected1 = select_nodes_for_blob(validators.clone(), metrics.clone(), &seed(4));
        let selected2 = select_nodes_for_blob(validators, metrics, &seed(5));

        // Should have 30 nodes each
        assert_eq!(selected1.len(), 30);
        assert_eq!(selected2.len(), 30);

        // Extract node_ids
        let ids1: Vec<i32> = selected1.iter().map(|n| n.node_id).collect();
        let ids2: Vec<i32> = selected2.iter().map(|n| n.node_id).collect();

        // Order should be different (deterministic shuffle with different seeds)
        // At least some positions should differ
        let differences = ids1.iter().zip(ids2.iter()).filter(|(a, b)| a != b).count();
        assert!(
            differences > 0,
            "Expected different orderings for different seeds"
        );
    }

    #[test]
    fn test_select_nodes_filters_inactive_validators() {
        // Metrics for 50 nodes, but only 40 are active validators
        let validators = create_test_nodes(40);
        let metrics = create_test_metrics(50); // Extra metrics for nodes not in validator set

        let selected = select_nodes_for_blob(validators, metrics, &seed(6));

        // Should return 30 nodes (filtered from active validators only)
        assert_eq!(selected.len(), 30);

        // All selected nodes should be from the active validator set (node_id 1-40)
        for node in &selected {
            assert!(node.node_id >= 1 && node.node_id <= 40);
        }
    }

    // Should: land the tightest integer split at every member count —
    // max − min ≤ 1, every member responsible for ≥ ⌊N/v⌋ ≥ 1 classes,
    // total N — for uniform AND skewed weights (the cap binds; weights
    // choose which classes, never the count).
    // Should not: leave a zero-load member (the greedy capped-HRW failure
    // the model witnessed at v=9).
    // Impact: INV-SPREAD is what makes the derived watermark's burst
    // bound two-sided; a zero-load member is wasted fault tolerance.
    #[test]
    fn balanced_spread_tight_at_every_size() {
        for v in 3..=30i32 {
            let members: Vec<i32> = (1..=v).collect();
            for weights in [
                std::collections::HashMap::new(),
                members.iter().map(|&m| (m, (m as u64 % 3) + 1)).collect(),
            ] {
                let assignment = assign_fragment_classes(&seed(7), &members, &weights, 30);
                let mut counts = std::collections::HashMap::new();
                for node in &assignment {
                    *counts.entry(*node).or_insert(0u32) += 1;
                }
                assert_eq!(counts.len(), v as usize, "v={v}: zero-load member");
                let max = counts.values().max().unwrap();
                let min = counts.values().min().unwrap();
                assert!(max - min <= 1, "v={v}: loads {counts:?}");
                assert_eq!(assignment.len(), 30);
            }
        }
    }

    // Should: produce the identical assignment regardless of member input
    // order (selection is a minimum over a total order).
    // Impact: nodes build their member lists from independently ordered
    // queries; order sensitivity would silently diverge placement
    // mesh-wide.
    #[test]
    fn balanced_member_order_independent() {
        let forward: Vec<i32> = (1..=9).collect();
        let mut reversed = forward.clone();
        reversed.reverse();
        let shuffled = vec![4, 9, 1, 7, 3, 8, 2, 6, 5];
        let weights = forward.iter().map(|&m| (m, (m as u64 % 4) + 1)).collect();
        let a = assign_fragment_classes(&seed(9), &forward, &weights, 30);
        let b = assign_fragment_classes(&seed(9), &reversed, &weights, 30);
        let c = assign_fragment_classes(&seed(9), &shuffled, &weights, 30);
        assert_eq!(a, b);
        assert_eq!(a, c);
    }

    /// The model's deterministic integer hash stand-in, ported verbatim
    /// from spec/storage_policy.qnt `mix` (staged mod-multiply).
    fn qnt_mix(n: i64, f: i64) -> i64 {
        let a = (n * 1103515245 + 12345) % 2147483647;
        let b = (a * 31 + f * 1103515245 + 54321) % 2147483647;
        (b * 65539 + a * (f + 7)) % 2147483647
    }

    // Should: reproduce the literal BAL_TABLE from the verified model
    // (spec/storage_policy.qnt module verify_table — the exact table
    // Apalache exhaustively checked INV-DURABLE/INV-SPREAD against) when
    // driven by the model's own mix()/weight scoring, for every member
    // subset.
    // Should not: differ in fold order, min-load tier selection, or
    // tie-break — those ARE the verified algorithm.
    // Impact: this is the drift guard that transfers the model's
    // verification to the Rust fold; blake3 vs mix is outside the verified
    // properties (the model states hash quality is non-load-bearing).
    #[test]
    fn balanced_matches_qnt_verify_table() {
        let weights: std::collections::HashMap<i32, i64> = [(1, 3), (2, 2), (3, 1), (4, 1)].into();
        let table: &[(&[i32], [i32; 6])] = &[
            (&[1], [1, 1, 1, 1, 1, 1]),
            (&[2], [2, 2, 2, 2, 2, 2]),
            (&[3], [3, 3, 3, 3, 3, 3]),
            (&[4], [4, 4, 4, 4, 4, 4]),
            (&[1, 2], [2, 1, 1, 2, 1, 2]),
            (&[1, 3], [1, 3, 1, 3, 1, 3]),
            (&[1, 4], [1, 4, 1, 4, 1, 4]),
            (&[2, 3], [2, 3, 2, 3, 3, 2]),
            (&[2, 4], [2, 4, 2, 4, 2, 4]),
            (&[3, 4], [3, 4, 4, 3, 3, 4]),
            (&[1, 2, 3], [2, 1, 3, 1, 3, 2]),
            (&[1, 2, 4], [2, 4, 1, 1, 2, 4]),
            (&[1, 3, 4], [1, 4, 3, 1, 3, 4]),
            (&[2, 3, 4], [2, 4, 3, 2, 3, 4]),
            (&[1, 2, 3, 4], [2, 4, 1, 3, 1, 2]),
        ];
        for (members, expected) in table {
            let got = assign_classes_by_score(members, 6, |n, f| {
                (qnt_mix(n as i64, f as i64) / weights[&n]) as u64
            });
            assert_eq!(&got, expected, "members {members:?}");
        }
    }

    // Should: bucket the RFC-004 score into 1..=16, never zero, and move
    // only when the underlying score crosses a bucket edge.
    // Impact: a zero weight would divide-by-zero the rendezvous score; a
    // fine-grained weight would re-place blobs on every metrics jitter.
    #[test]
    fn quantized_weight_bounds_and_coarseness() {
        let mut row = MetricsRow {
            node_id: 1,
            trust_factor: 1.0,
            availability_score: 0.0,
            throughput_score: 0.0,
            latency_score: 0.0,
            stability_score: 0.0,
            storage_multiplier: 1.0,
        };
        assert_eq!(quantized_weight(&row), 1); // floor never 0
        row.availability_score = 1.0;
        row.throughput_score = 1.0;
        row.latency_score = 1.0;
        row.stability_score = 1.0;
        assert_eq!(quantized_weight(&row), 16); // ceiling clamped
                                                // Jitter within one bucket does not move the weight.
        row.availability_score = 0.80;
        let w1 = quantized_weight(&row);
        row.availability_score = 0.81;
        assert_eq!(quantized_weight(&row), w1);
    }

    // Should: return an empty assignment for an empty member set and put
    // every class on the sole member of a one-node mesh.
    // Impact: degenerate views occur during bootstrap; a panic here would
    // take the distribution worker down with it.
    #[test]
    fn balanced_degenerate_member_sets() {
        let weights = std::collections::HashMap::new();
        assert!(assign_fragment_classes(&seed(1), &[], &weights, 30).is_empty());
        let solo = assign_fragment_classes(&seed(1), &[7], &weights, 30);
        assert_eq!(solo.len(), 30);
        assert!(solo.iter().all(|&n| n == 7));
    }
}
