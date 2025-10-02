use crate::types::Blake3Hash;
use crate::db::metrics::NodeMetrics;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FragmentType {
    Original,
    Recovery,
}

/// Phase 1: Rendezvous hashing candidate with XOR distance
#[derive(Debug, Clone)]
pub struct Phase1Candidate {
    pub node_id: i32,
    pub distance: u64,  // XOR distance for sorting
}

/// Phase 2: Scored candidate ready for placement decision
#[derive(Debug, Clone, serde::Serialize)]
pub struct Phase2Candidate {
    pub node_id: i32,
    pub final_score: f64,  // Weighted score for placement ranking
    pub ip_address: String,
    pub port: i32,
}

/// Calculate XOR distance between fragment hash and node IDs
/// Uses on-demand Blake3 hashing for simplicity and flexibility
/// 
/// Performance characteristics:
/// - Sequential: ~3-5ns per node for Blake3 hash + XOR
/// - 100 nodes: ~300-500ns total (well under 100ms target)
/// - Cache-friendly sequential access pattern
/// 
/// Future optimization opportunities:
/// - Rayon parallelization for networks >1000 nodes
/// - Pre-computed node hash caching in database
/// - SIMD-optimized XOR operations for bulk distance calculation
pub fn calculate_rendezvous_distances(
    fragment_hash: &Blake3Hash,
    node_ids: &[i32],
) -> Vec<Phase1Candidate> {
    // Convert fragment hash to u64 for XOR distance calculation
    let fragment_key = u64::from_be_bytes([
        fragment_hash.as_bytes()[0], fragment_hash.as_bytes()[1],
        fragment_hash.as_bytes()[2], fragment_hash.as_bytes()[3],
        fragment_hash.as_bytes()[4], fragment_hash.as_bytes()[5],
        fragment_hash.as_bytes()[6], fragment_hash.as_bytes()[7],
    ]);
    
    let mut candidates: Vec<Phase1Candidate> = node_ids
        .iter()
        .map(|&node_id| {
            // On-demand Blake3 hash of node_id (4 bytes input, ~3-5ns)
            let node_hash = blake3::hash(&node_id.to_be_bytes());
            let node_key_u64 = u64::from_be_bytes([
                node_hash.as_bytes()[0], node_hash.as_bytes()[1],
                node_hash.as_bytes()[2], node_hash.as_bytes()[3],
                node_hash.as_bytes()[4], node_hash.as_bytes()[5],
                node_hash.as_bytes()[6], node_hash.as_bytes()[7],
            ]);
            
            let distance = fragment_key ^ node_key_u64;
            
            Phase1Candidate {
                node_id,
                distance,
            }
        })
        .collect();
    
    // Sort by distance (closest first for rendezvous hashing)
    candidates.sort_by_key(|c| c.distance);
    
    candidates
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