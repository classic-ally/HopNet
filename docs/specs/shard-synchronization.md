# Shard Synchronization System Requirements

## Overview

This document outlines the requirements for implementing distributed shard synchronization in HopNet. The system must distribute Reed-Solomon encoded file fragments across network nodes while optimizing for performance, reliability, and fault tolerance.

## Core Design Principles

### 1. Deterministic Placement with Local Discovery
- **Requirement**: Fragment placement must be deterministic across all nodes
- **Rationale**: Enables consistent fragment location calculation without requiring consensus for individual placement decisions
- **Implementation**: Use consensus-tracked metrics with versioning to ensure all nodes make identical placement decisions
- **Discovery**: Parallel query approach to find fragments, avoiding DHT complexity for validator-sized networks

### 2. Consensus Avoidance for Individual Operations
- **Requirement**: Individual fragment placement and retrieval operations must not require consensus
- **Rationale**: Consensus overhead would create performance bottlenecks for file operations
- **Scope**: Consensus only for fragment and file existence registration and updating, node metrics updates, and validator set changes

### 3. Consensus Height-Based Versioning
- **Requirement**: Use consensus block height for deterministic versioning instead of timestamps
- **Rationale**: Consensus height provides guaranteed monotonic progression that all nodes agree upon, avoiding clock synchronization issues inherent in timestamp-based approaches
- **Implementation Requirements**:
  - Add `placement_height` column to `data_blocks` table to track when fragment placement was determined
  - Add `effective_height` column to metrics tables to version node reliability data
  - Add `measurement_height` column to node health tracking for consensus-synchronized measurements
  - Use `committed_block_height` from consensus state for all placement calculations
- **Benefits**: Eliminates clock drift, NTP adjustment, and system time inconsistency issues between nodes

## Fragment Placement Requirements

### 3. Deterministic Node Selection Strategy [UPDATED: Modulo Placement]
- **File-Level Selection**: Select 30 nodes per file using metrics-based deterministic shuffle
- **Fragment-Level Placement**: Use `local_index % num_selected_nodes` for primary placement
- **Backup Nodes**: Add 2 backup nodes at `(local_index + 1) % N` and `(local_index + 2) % N`
- **Key Insight**: Use `local_index` (0-29 position within chunk) instead of global fragment index
  - Ensures fragment[0] from chunk 1 goes to same node as fragment[0] from chunk 0
  - Creates predictable, evenly-distributed placement patterns across chunks
  - Crucial for maintaining Reed-Solomon redundancy characteristics
- **Even Distribution**: Modulo placement guarantees ±1 max imbalance across nodes
- **Determinism**: Same file_hash always selects same 30 nodes; same local_index always maps to same node
- **Implementation Details**:
  - **Phase 1 - File-Level Node Selection**: `select_nodes_for_file(validators, metrics, file_hash) -> Vec<Node>`
    - Early-exit: If ≤30 validators, return all (no filtering needed)
    - Filter validators by active status at consensus height
    - Score using weighted metrics: 40% availability, 30% throughput, 20% latency, 10% stability
    - Take top 60 candidates (2× for diversity)
    - Deterministic shuffle using Blake3-based Fisher-Yates with file_hash as seed
    - Return top 30 nodes after shuffle
    - **Performance**: ~10-50ms for 100 nodes using DuckDB analytics
  - **Phase 2 - Fragment Placement**: `get_fragment_placement(local_index, selected_nodes) -> Vec<&Node>`
    - Primary: `local_index % selected_nodes.len()`
    - Backup 1: `(local_index + 1) % selected_nodes.len()`
    - Backup 2: `(local_index + 2) % selected_nodes.len()`
    - **Performance**: O(1) calculation, deterministic wraparound
  - **Distribution Properties**:
    - With 30 nodes: each node gets exactly 1 fragment (perfect balance)
    - With 10 nodes: each node gets 3 fragments (±0 imbalance)
    - With 9 nodes: max 4 fragments per node, min 3 (±1 imbalance)
    - Failure tolerance: `floor(20 / max_fragments_per_node)` node failures tolerated
  - **Metric Calculation**:
    - **Availability**: `availability_24h * 0.7 + availability_7d * 0.3`
    - **Throughput**: Percentile-based ranking with logarithmic scaling
    - **Latency**: `1 / (1 + normalized_latency)` with exponential time decay
    - **Stability**: `1 / (1 + latency_variance_7d)` to prefer predictable nodes
  - **New Node Handling**:
    - Probationary period using trust factor: `(sample_count / 100).min(1.0)`
    - Blend measured metrics with network statistics
    - Natural progression from conservative to trusted over ~16 hours

### 4. Erasure-Code Aware Placement
- **Requirement**: Different erasure code sets (original vs recovery fragments) must prefer different node sets
- **Background**: HopNet uses 2:1 redundancy (N original + 2N recovery fragments)
- **Goal**: Prevent single node failure from eliminating entire erasure capability
- **Implementation**: Natural separation through fragment indices (0 to N-1 for original, N to 3N-1 for recovery) combined with metrics-based weighting in Phase 2

### 5. Performance-Optimized Original Fragment Placement
- **Requirement**: Original fragments must be placed on most reliable and performant nodes
- **Rationale**: Loss of original fragments forces Reed-Solomon reconstruction (N fragment downloads vs 1), severely impacting streaming performance
- **Metrics Priority**:
  - Node uptime/availability (highest weight)
  - Response time consistency (stability over speed)
  - User proximity (minimize latency to file owners)

### 6. Geographic Redundancy for Recovery Fragments
- **Requirement**: Recovery fragments should be distributed to maximize geographic diversity
- **Rationale**: Provides resilience against regional outages or natural disasters
- **Implementation Strategy**: 
  - **Phase 1A**: Implicit geographic distribution through inverse performance scoring
    - High latency nodes (geographically distant) naturally preferred for recovery
    - Low throughput nodes (different networks) provide network diversity
  - **Phase 3**: Add user-provided geographic regions for compliance requirements (regulatory constraints, data sovereignty)
  - **Future**: Advanced jurisdiction restrictions and cross-border data flow controls

### 7. User-Aware Proximity Optimization
- **Requirement**: Fragment placement should consider proximity to file owners
- **Approach**: Equal weighting for all users with access to shared files
- **Metric**: Minimize RTT from user-owned nodes to fragment storage locations
- **Scope**: Primary consideration for original fragments, secondary for recovery fragments
- **Implementation**: Phase 2 enhancement (10% weight allows deferral without major impact)

## Node Reliability Requirements

### 8. Consensus-Tracked Node Metrics
- **Requirement**: Node reliability metrics must be tracked via consensus to ensure deterministic placement
- **Implementation Strategy**: Automated background collection with consensus batching
  - Each node measures all other active validators every 10 minutes (randomized scheduling)
  - RTT latency, variance, jitter measurements using existing metrics infrastructure
  - Node availability tracking via boolean flag (successful vs failed measurements)
  - Batch all measurements into single consensus transaction per collection cycle
- **Metrics to Track**:
  - RTT latency and variance (existing infrastructure)
  - Node availability boolean (uptime calculation)
  - Consensus participation rate (from validator tracking)
  - Storage capacity and utilization in GB (UINTEGER storage_total_gb, storage_used_gb)
- **Update Frequency**: 10-minute randomized intervals to prevent thundering herd
- **Versioning**: All metrics stored with consensus height for deterministic placement consistency
- **Database Schema Changes**:
  - Add `height` column to metrics table for consensus versioning
  - Add `available` boolean column for explicit uptime tracking
  - Add `storage_total_gb`, `storage_used_gb` UINTEGER columns for capacity tracking
  - Create `submit_metrics` consensus transaction handler for batched submissions
- **Storage Metrics Collection**:
  - `/rpc/storage-server` endpoint with JWT or RPC authentication for direct user monitoring
  - Cross-platform filesystem queries using `fs4::statvfs` for fast capacity calculations
  - Storage utilization: `used_gb = total_gb - available_gb` (no slow directory traversal)
  - Integrated into existing metrics collection workflow with timeout handling

### 9. Roaming Device Detection and Penalization
- **Requirement**: Mobile/roaming devices should be discouraged from storing original fragments
- **Detection Method**: High RTT variance and frequent IP address changes
- **Penalty Strategy**: Exponential penalties for variance in placement scoring
- **Rationale**: Roaming devices have unpredictable connectivity, unsuitable for performance-critical original fragments

### 10. Stability-Weighted Reliability Scoring
- **Requirement**: Consistency must be weighted higher than absolute performance
- **Implementation**: Variance penalties more severe than high-latency penalties
- **Goal**: Prefer predictably good nodes over occasionally excellent but unreliable nodes
- **Storage Multiplier Design**: Exponential decay ensures storage capacity acts as a strong filter
  - Prevents overloading nodes regardless of other performance metrics
  - Creates natural load balancing across network
  - Nodes at same utilization percentage contribute equally regardless of absolute size

## System Architecture Requirements

### 11. Fragment Discovery and Retrieval Protocol
- **Primary Method**: Local database check first (fragments may be cached from previous requests)
- **Deterministic Placement**: Query preference-ordered list of 1/3 of storage nodes for missing fragments
- **Accelerated Fallback**: Best-candidate-first with reactive parallel discovery
  - Try highest-ranked deterministic candidate immediately for optimal case performance
  - Launch parallel health checks on remaining deterministic candidates (no waiting)
  - Spawn download tasks reactively as nodes report having fragments
  - Network-wide gossip fallback if deterministic candidates exhausted
- **Gossip Fallback**: Health-check-first gossip across all nodes when triggered
  - Parallel `/fragments/{hash}/health` queries to all nodes with 200ms timeout
  - Fetch from first node to respond positively (don't wait for all responses)
  - If first fetch fails, try next available node from remaining health responses
- **Reed-Solomon Reconstruction**: Final fallback if both deterministic placement and gossip fail
- **Fragment Verification**: Verify hash after retrieval; retry from other nodes if corrupted
- **No Discovery Queries**: No separate discovery step for deterministic placement
- **Performance Requirements**:
  - Fragment placement decision must complete in <100ms for responsive file access
  - Leverage DuckDB query result caching to amortize metrics calculation cost
  - Pre-compute node hashes at startup for O(1) lookup during placement
  - Batch fragment placement decisions when possible to reuse metrics queries

### 12. Fragment Placement Lifecycle
- **Placement Scheduling**: Event-driven only (triggered by each upload completion)
- **Job Scope**: Each event processes only the specific uploaded file, no batching or queuing
- **Job Concurrency**: Unlimited - each upload triggers independent distribution
- **Non-blocking Upload**: Fragment distribution does not block user upload process
- **Processing Limits**: Cap at ~100 fragments in memory (400MB) processed concurrently per file
- **Consensus Height**: Lock height at start of distribution for deterministic placement
- **Storage Efficiency**: Update `stored_locally = FALSE` immediately per fragment after successful distribution
- **Consensus Updates**: Submit single `PlacementHeightUpdate` per file after all fragments distributed
- **Database Schema**: `placement_height` column tracks distribution state (NULL = local-only, value = distributed at height)
- **Failure Handling**: No retries within distribution job; orphan recovery handles all failure cases
- **Orphan Recovery**: Separate job finds stuck files using dynamic threshold: 30min + (fragment_count × 4MB / median_network_throughput)

### 13. Rebalancing and Node Lifecycle
- **Rebalancing Algorithm**: Recompute placement at current consensus height, transmit fragments, update `placement_height`
- **Cleanup Process**: Nodes cleanup fragments where current placement differs from stored `placement_height`
- **Node Lifecycle Events**: Manual rebalancing trigger for graceful node removal, automatic detection of storage expansion
- **Fragment Health Verification**: Use existing `/fragments/{hash}/health` endpoint for integrity checks during rebalancing
- **Future Enhancements**: Hierarchical metrics sampling for networks >1000 nodes

### 14. Configuration Parameters
- **Storage Capacity Decay**: k=5 in `e^(-k * utilization)` storage multiplier formula
- **New Node Trust Building**: 100 samples required for full trust factor (approximately 16 hours)
- **Metrics Query Caching**: Cache results within background job execution (per consensus height)
- **Rebalancing Frequency**: Configurable interval (value to be determined during implementation)

### 15. Existing System Integration
- **Requirement**: Leverage existing HopNet infrastructure
- **Consensus System**: Use existing BFT consensus for metrics and policy coordination
- **Authentication**: Integrate with existing Ed25519 node authentication
- **Network Layer**: Build on existing HTTP-based inter-node communication
- **Database**: Extend existing DuckDB schema with height-based versioning columns
- **Height Integration**: Utilize existing `committed_block_height` tracking from consensus state

### 16. Multiple User Support
- **Requirement**: Handle shared files across multiple users efficiently
- **Approach**: Equal weighting of all users with file access
- **Avoid**: Complex access frequency tracking or weighted user preferences
- **Integration**: Work with existing inode -> data block sharing architecture

## Performance Requirements

### 17. Network Efficiency
- **Requirement**: Minimize network overhead for fragment operations
- **Target**: O(replication_factor) queries for fragment discovery
- **Constraint**: Avoid O(log N) multi-hop routing complexity of DHTs
- **Optimization**: Parallel queries to reduce latency

### 18. Streaming Performance
- **Requirement**: Optimize for low-latency file access
- **Priority**: Original fragment availability critical for streaming use cases
- **Backup Strategy**: Reed-Solomon reconstruction acceptable for rare original fragment loss
- **Metric**: Minimize probability of reconstruction requirement

## Future Extensibility Requirements

### 19. Compliance Framework Readiness
- **Requirement**: Architecture must support future jurisdiction-based placement restrictions
- **Design**: Extensible location tracking system
- **Implementation**: Pluggable location detection methods (RTT proxy -> IP geolocation -> user-declared)
- **Constraint Checking**: Framework for validating placement against compliance rules

### 20. Scalable Metrics Collection
- **Requirement**: Metrics system must scale with network growth
- **Current Scope**: Validator-sized networks (< 100 nodes)
- **Future**: Support larger networks without consensus bottlenecks
- **Strategy**: Hierarchical or sampling-based metrics for large networks

## Success Criteria

### 21. Performance Targets
- Deterministic consistency: 100% placement agreement across nodes using height-based versioning

### 22. Reliability Goals
- Single node failure: No data loss, minimal performance impact
- Regional outage: Full data availability through geographic redundancy
- Roaming device issues: No impact on network performance or availability

### 23. Integration Requirements
- Zero downtime deployment of shard synchronization
- Backward compatibility with existing file operations
- Minimal changes to existing consensus and database schemas (add height columns)
- Preserve existing security and encryption properties
- Seamless integration with existing consensus height tracking

## Implementation Priorities

### Phase 1A: Foundation (Depends on Background Metrics Collection) [x]
- [x] **BLOCKER RESOLVED**: Extended metrics table with height and availability columns for reliable node scoring
- [x] **INFRASTRUCTURE**: Implemented reusable metrics collection infrastructure with timeout handling  
- [x] **CONSENSUS INTEGRATION**: Implemented consensus transaction batching for metrics submissions ("submit_metrics" handler)
- [x] **API ENDPOINTS**: Added manual metrics trigger API endpoint with consensus integration for debugging
- [x] **DATABASE COMPATIBILITY**: Fixed metrics retrieval API with proper DuckDB timestamp handling (GET /metrics)  
- [x] **COMPLETED**: Background metrics collection worker with randomized 10-minute scheduling
- [x] **COMPLETED**: Integrate throughput measurement using existing infrastructure
- [x] **COMPLETED**: Storage capacity metrics collection (storage_total_gb, storage_used_gb columns)
- [x] **COMPLETED**: Cross-platform storage metrics endpoint (/rpc/storage-server) with dual authentication
- [x] **COMPLETED**: Modulo placement algorithm with file-level node selection
- [x] **COMPLETED**: Two-phase placement: File-level metrics-based selection → Fragment-level modulo mapping
- [x] **COMPLETED**: Local-index-aware placement ensuring consistent chunk distribution across files
- [x] **COMPLETED**: Placement scores debugging API (/metrics/scores) with raw metrics and weighted scoring
- [x] Database schema extensions for height tracking (placement_height column added)
- [x] Integration with fragment transfer protocols (RFC-003 complete)
- [x] **COMPLETED**: Consensus transaction handler for placement_height updates ("update_placement_heights")

### Phase 1B: Fragment Distribution Implementation [x]
- [x] **COMPLETED**: Event-driven fragment distribution system
  - Triggers automatically after upload consensus completion
  - Memory-limited batch processing (100 fragments per batch)
  - Self-skip optimization (avoids HTTP calls to local node)
  - Retry logic with exponential backoff (3 attempts, 5s connection + 30s request timeouts)
  - Inter-node authentication with dual Ed25519 signatures
  - Consensus integration for placement_height updates
  - Database state tracking (stored_locally flags)
- [x] **COMPLETED**: Download logic integration with accelerated fragment discovery system
- [ ] Orphan recovery job with adaptive thresholds based on network throughput

### Phase 1C: Fragment Discovery and Basic Geographic Distribution [x]
- [x] **COMPLETED**: Accelerated fragment discovery with reactive tokio-based fallback
  - Standardized placement algorithm shared between distribution and discovery systems
  - Best-candidate-first strategy with immediate parallel health checks on remaining candidates
  - Reactive downloading: spawn download tasks as soon as nodes report having fragments
  - Network-wide gossip fallback using health-check-first pattern with 200ms timeouts
  - Pure tokio implementation (tasks + channels) with automatic cleanup on success
- [x] **COMPLETED**: Download logic integration - reassemble_file() calls discovery functions when stored_locally=false
  - Work queue pattern with thread reuse for efficient concurrent fragment retrieval
  - Database consistency management with automatic state correction when fragments missing from disk
  - Ed25519 cryptographic authentication for all inter-node fragment requests
  - Robust Reed-Solomon reconstruction handling missing/corrupted fragments
- [ ] Basic node reliability scoring using performance metrics  
- [ ] Geographic distribution using RTT clustering + IP geolocation (early implementation)

### Phase 2: Performance Optimization [ ]
- [ ] Performance-optimized original fragment placement
- [ ] Advanced node reliability scoring with predictive capabilities
- [ ] User proximity optimization for shared files
- [ ] Automated fragment rebalancing triggers

### Phase 3: Enterprise Geographic Features [ ]
- [ ] User-provided geographic regions for compliance requirements
- [ ] Advanced geographic redundancy with regulatory constraints
- [ ] Background rebalancing and migration automation
- [ ] Compliance framework integration

### Phase 4: Advanced Features [ ]
- [ ] Roaming device detection and special handling
- [ ] Machine learning-based placement optimization
- [ ] Large network scaling optimizations
- [ ] Advanced rebalancing algorithms with minimal data movement