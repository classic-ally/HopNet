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

### 3. Deterministic Node Selection Strategy
- **Algorithm Output**: Returns preference-ordered list of 1/3 of total storage nodes for each fragment
- **Cascading Failure Protection**: 1/3 limit prevents same nodes handling original + recovery fragments during failures
- **Sequential Retry Logic**: Try first node → second node → third node, etc. until successful or exhausted
- **Minimum Network Size**: Below 3 nodes, cascading protection unavoidable but system still functional
- **Preference Ordering**: Based on node reliability metrics, geographic distribution, and user proximity

### 4. Erasure-Code Aware Placement
- **Requirement**: Different erasure code sets (original vs recovery fragments) must prefer different node sets
- **Background**: HopNet uses 2:1 redundancy (N original + 2N recovery fragments)
- **Goal**: Prevent single node failure from eliminating entire erasure capability
- **Implementation**: Use different type seeds in deterministic algorithm for original vs recovery placement

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
  - **Phase 1B**: Use RTT clustering as distance proxy for basic geographic distribution
  - **Phase 1B**: Integrate IP geolocation services for improved geographic awareness
  - **Phase 3**: Add user-provided geographic regions for compliance requirements (regulatory constraints, data sovereignty)
  - **Future**: Advanced jurisdiction restrictions and cross-border data flow controls

### 7. User-Aware Proximity Optimization
- **Requirement**: Fragment placement should consider proximity to file owners
- **Approach**: Equal weighting for all users with access to shared files
- **Metric**: Minimize RTT from user-owned nodes to fragment storage locations
- **Scope**: Primary consideration for original fragments, secondary for recovery fragments

## Node Reliability Requirements

### 8. Consensus-Tracked Node Metrics
- **Requirement**: Node reliability metrics must be tracked via consensus to ensure deterministic placement
- **Metrics to Track**:
  - Uptime percentage (30-day rolling window)
  - Average response time and variance
  - Consensus participation rate
  - Storage capacity and utilization
- **Update Frequency**: Periodic consensus operations (not per-fragment)
- **Versioning**: Metrics must be versioned using consensus height for deterministic placement consistency
- **Database Schema Changes**:
  - Extend metrics tables with `effective_height` columns
  - Create consensus-triggered metric update transactions
  - Implement height-based metric lookup for placement decisions

### 9. Roaming Device Detection and Penalization
- **Requirement**: Mobile/roaming devices should be discouraged from storing original fragments
- **Detection Method**: High RTT variance and frequent IP address changes
- **Penalty Strategy**: Exponential penalties for variance in placement scoring
- **Rationale**: Roaming devices have unpredictable connectivity, unsuitable for performance-critical original fragments

### 10. Stability-Weighted Reliability Scoring
- **Requirement**: Consistency must be weighted higher than absolute performance
- **Implementation**: Variance penalties more severe than high-latency penalties
- **Goal**: Prefer predictably good nodes over occasionally excellent but unreliable nodes

## System Architecture Requirements

### 11. Fragment Discovery and Retrieval Protocol
- **Primary Method**: Local database check first (fragments may be cached from previous requests)
- **Deterministic Placement**: Query preference-ordered list of 1/3 of storage nodes for missing fragments
- **Sequential Fallback**: Try nodes in preference order (first → second → third, etc.) to handle node failures
- **Broadcast Fallback**: If deterministic placement fails, query all nodes before expensive Reed-Solomon reconstruction
- **No Discovery Queries**: No separate discovery step - deterministic placement tells us which nodes to try
- **Health Monitoring Exception**: Health checks do require separate discovery queries with disk verification and checksum validation

### 12. Existing System Integration
- **Requirement**: Leverage existing HopNet infrastructure
- **Consensus System**: Use existing BFT consensus for metrics and policy coordination
- **Authentication**: Integrate with existing Ed25519 node authentication
- **Network Layer**: Build on existing HTTP-based inter-node communication
- **Database**: Extend existing DuckDB schema with height-based versioning columns
- **Height Integration**: Utilize existing `committed_block_height` tracking from consensus state

### 13. Multiple User Support
- **Requirement**: Handle shared files across multiple users efficiently
- **Approach**: Equal weighting of all users with file access
- **Avoid**: Complex access frequency tracking or weighted user preferences
- **Integration**: Work with existing inode -> data block sharing architecture

## Performance Requirements

### 14. Network Efficiency
- **Requirement**: Minimize network overhead for fragment operations
- **Target**: O(replication_factor) queries for fragment discovery
- **Constraint**: Avoid O(log N) multi-hop routing complexity of DHTs
- **Optimization**: Parallel queries to reduce latency

### 15. Streaming Performance
- **Requirement**: Optimize for low-latency file access
- **Priority**: Original fragment availability critical for streaming use cases
- **Backup Strategy**: Reed-Solomon reconstruction acceptable for rare original fragment loss
- **Metric**: Minimize probability of reconstruction requirement

## Future Extensibility Requirements

### 16. Compliance Framework Readiness
- **Requirement**: Architecture must support future jurisdiction-based placement restrictions
- **Design**: Extensible location tracking system
- **Implementation**: Pluggable location detection methods (RTT proxy -> IP geolocation -> user-declared)
- **Constraint Checking**: Framework for validating placement against compliance rules

### 17. Scalable Metrics Collection
- **Requirement**: Metrics system must scale with network growth
- **Current Scope**: Validator-sized networks (< 100 nodes)
- **Future**: Support larger networks without consensus bottlenecks
- **Strategy**: Hierarchical or sampling-based metrics for large networks

## Success Criteria

### 18. Performance Targets
- Deterministic consistency: 100% placement agreement across nodes using height-based versioning

### 19. Reliability Goals
- Single node failure: No data loss, minimal performance impact
- Regional outage: Full data availability through geographic redundancy
- Roaming device issues: No impact on network performance or availability

### 20. Integration Requirements
- Zero downtime deployment of shard synchronization
- Backward compatibility with existing file operations
- Minimal changes to existing consensus and database schemas (add height columns)
- Preserve existing security and encryption properties
- Seamless integration with existing consensus height tracking

## Implementation Priorities

### Phase 1A: Foundation (Depends on RFC-003 Fragment Transfer) [ ]
- [~] Consensus height-based versioning implementation
- [~] Rendezvous hashing placement algorithm
- [~] Erasure-code aware placement logic
- [ ] Database schema extensions for height tracking
- [ ] Integration with fragment transfer protocols (blocked by RFC-003 Phase 1A)

### Phase 1B: Fragment Discovery and Basic Geographic Distribution [ ]
- [ ] Parallel fragment query implementation using RFC-003 fragment discovery endpoints
- [ ] Caching layer for fragment locations
- [ ] Basic node reliability scoring using performance metrics
- [ ] Geographic distribution using RTT clustering + IP geolocation (early implementation)
- [ ] Fallback search expansion algorithm

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