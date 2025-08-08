# RFC-007: Maintenance and Operations System

## Overview

The HopNet Maintenance and Operations System provides automated background processes to ensure network health, storage efficiency, and operational reliability. Built on the existing `apalis` job infrastructure with randomized scheduling, the system handles fragment lifecycle management, network rebalancing, capacity monitoring, and historical data retention while preserving Time Machine functionality.

## Design Philosophy

### Threshold-Based Operations
- **Storage-Aware**: Cleanup operations respect storage capacity thresholds rather than aggressive time-based deletion
- **Retention-First**: Preserve historical data access capabilities through consensus height-based retention policies
- **Network Health**: Prioritize network stability and performance over storage optimization

### Non-Disruptive Processing  
- **Background Operations**: All maintenance runs during low-activity periods with minimal performance impact
- **Consensus Integration**: Critical operations coordinated through consensus to ensure network-wide consistency
- **Graceful Degradation**: System continues operating even if maintenance jobs fail or are delayed

## Core Systems

### 1. Fragment Lifecycle Management

#### Threshold-Based Cleanup System
**Purpose**: Manage storage capacity while preserving Time Machine functionality

**Triggers**:
- Will be based on storage capacity thresholds
- Separate thresholds for local node and network wide
- Initially only manual cleanup request through admin interface
- Never triggers based purely on time/age

**Cleanup Algorithm with UUIDv7 Timestamps**:
1. **Timestamp Extraction**: Extract creation timestamp from fragment_id UUIDv7 for efficient age-based sorting
2. **Safety Checks**: Verify fragment not needed for active files
3. **UUIDv7-Based Prioritization**: Sort fragments by embedded timestamp, prioritizing cleanup of oldest eligible fragments first
4. **Emergency Mode**: More aggressive cleanup when critical storage threshold reached

**UUIDv7 Integration Benefits**:
```rust
impl CustomUUID {
    /// Extract the timestamp from a UUIDv7 fragment_id
    pub fn get_timestamp(&self) -> Result<DateTime<Utc>, TimeError> {
        match self.0.get_timestamp() {
            Some(timestamp) => Ok(DateTime::from_timestamp_millis(timestamp.to_unix(Timestamp::UNIX_EPOCH) as i64).unwrap()),
            None => Err(TimeError::InvalidUUID)
        }
    }
    
    /// Check if fragment is older than retention policy
    pub fn is_cleanup_eligible(&self, retention_policy: &RetentionPolicy) -> bool {
        if let Ok(creation_time) = self.get_timestamp() {
            let age = Utc::now() - creation_time;
            age.num_days() > retention_policy.minimum_age_days as i64
        } else {
            false // If we can't extract timestamp, don't clean up
        }
    }
}
```

**Efficient Cleanup Query using UUIDv7**:
```sql
-- Get fragments eligible for cleanup, ordered by age (oldest first)  
-- UUIDv7 allows direct lexicographical sorting by age
SELECT fragment_id, fragment_hash, data_block_id
FROM fragment_hashes fh
JOIN data_blocks db ON fh.data_block_id = db.id  
WHERE fh.stored_locally = TRUE
  AND db.deleted_at IS NOT NULL  -- Only consider fragments from deleted files
  AND fragment_id < ? -- UUIDv7 cutoff timestamp for minimum_age_days
ORDER BY fragment_id ASC -- Oldest fragments first (UUIDv7 lexicographical = chronological)
LIMIT ?; -- Process in batches to avoid overwhelming storage I/O
```

#### Lost Shard Recovery System
**Purpose**: Ensure expected fragments exist and are retrievable across the network

**Detection Methods and Triggers**:
- Database indicates fragment should exist locally but local storage missing
- Database indicates fragment should exist remotely but we fail to fetch it
- Fragment health checks reveal gaps in network coverage
- Unclean removal of node(s) in network (e.g. without graceful computing of post-removal placement and relevant rebalancing)

**Recovery Process**:
1. **Fragment Discovery**: Use existing discovery system to locate the other fragments relevant to this particular data block
2. **Authenticated and Verified Transfer**: Retrieve enough fragments from other nodes to reconstruct missing fragment, verifying integrity with hash during process
3. **Local Regeneration**: Use Reed-Solomon to regenerate the missing fragment, storing it locally
4. **In-Place Recovery**: If possible, upload the missing fragment to the node that was meant to be responsible for it at the given consensus height. No consensus operation required in this case (consensus state remains the same)
5. **Automatic Rebalance if Node Unsuitable**: If unable to upload missing fragment to the responsible node, begin rebalance for this data block such that the fragment will become the responsibility of another node, using existing network rebalance routine

#### Redundant Copy Cleanup
**Purpose**: Ensure redundant copies of fragments are cleaned up safely while preserving network redundancy

**Potential Causes**:
- **Download-Induced Redundancy**: Fragments cached locally during file downloads but not optimal storage locations
- **Rebalancing Artifacts**: Source fragments remaining after successful migration to new optimal nodes
- **Recovery Overage**: Multiple nodes storing same fragments after concurrent recovery operations
- **Placement Algorithm Evolution**: Fragments stored under older placement decisions that are no longer optimal

**Availability-Aware Cleanup Strategy**:
```rust
pub enum CleanupPriority {
    // For high-availability nodes (servers, always-on devices)
    HighAvailability {
        stage1: RedundantCopies,    // Clean non-optimal placements first
        stage2: HistoricalFragments, // Then old deleted files if needed
    },
    // For low-availability nodes (laptops, mobile devices)  
    LowAvailability {
        stage1: HistoricalFragments, // Clean old deleted files first
        stage2: RedundantCopies,     // Preserve redundant copies for offline access
    }
}
```

**Priority Modulation Based on Node Availability**:
- **Availability threshold**: Nodes with <80% average availability are considered "low-availability"
- **High-availability nodes**: Follow standard priority (redundant → historical) since network can provide fragments
- **Low-availability nodes**: Inverted priority (historical → redundant) to maintain local copies for offline operation
- **Detection**: Rolling 30-day average of successful health check responses

**Triggers**:
- Manually to free up local space
- Automatically when storage capacity thresholds exceeded
- Priority strategy selected based on node's current availability classification

**Safety Algorithm**:
```rust
pub async fn is_fragment_safely_removable(
    fragment_hash: &Blake3Hash,
    current_node_id: i32,
    placement_height: Option<i32>,
    node_metrics: &[NodeMetrics],
) -> Result<bool, MaintenanceError> {
    // 1. Check if this node should be storing this fragment according to current placement
    let optimal_nodes = get_fragment_placement_candidates(fragment_hash, FragmentType::Original, node_metrics);
    let is_optimal_placement = optimal_nodes.iter().any(|n| n.node_id == current_node_id);
    
    if is_optimal_placement {
        return Ok(false); // Never remove if we're the optimal node
    }
    
    // 2. Verify sufficient other nodes have the fragment
    let mut verified_copies = 0;
    for candidate in optimal_nodes.iter().take(3) { // Check top 3 candidates
        if candidate.node_id != current_node_id {
            match try_ask_node_for_fragment(fragment_hash, candidate.node_id, &candidate.ip_address, candidate.port, auth).await {
                Ok(true) => verified_copies += 1,
                _ => continue,
            }
        }
    }
    
    // 3. Safe to remove if an optimal node has a verified copy
    Ok(verified_copies >= 1)
}
```

**Cleanup Process**:
1. **Placement Verification**: Check if current node is optimal for fragment storage according to the marked placement height
2. **Network Redundancy Check**: Verify one of the optimal nodes have accessible copies of the fragment  
3. **Consensus Height Consideration**: Prioritize cleanup of fragments placed under older consensus heights
4. **Safe Removal**: Remove fragment from local storage and update `stored_locally = FALSE` in database

**Scheduling**: TBD

### 2. Network Rebalancing System

#### Dynamic Node Assessment
**Purpose**: Adapt fragment placement as network topology and node performance changes

**Rebalancing Triggers**:
- New node joins network (detected via consensus validator set changes)
- Node leaves network (detected via extended unavailability or explicit departure)
- Significant node performance changes (% threshold change in reliability score TBD)
- Network imbalance detected (% disparity in storage consumption TBD)
- Scheduled job which checks oldest placement height and rebalances files placed a long time ago (consensus height delta threshold TBD)
- Manual rebalance request through admin interface

**Prioritization**:
- Operate on oldest placement height data blocks first
- When ran in an automated fashion, limited on execution time and/or minimum age of height to rebalance (e.g. don't waste resources moving recently placed file) 
- Node should prioritize data blocks which hash closest to our node ID, so that multiple nodes are less likely to request rebalance of the same block at the same time

**Migration Process**:
1. **Placement Recalculation**: Run current placement algorithm with updated node metrics
2. **Gap Analysis**: Identify fragments that should migrate based on new optimal placement
3. **No Middleman Transfer**: Inform newly responsible nodes that they should request fragment transfer from previously responsible nodes; await success
4. **Consistency Update**: Update fragment location metadata through consensus
5. **Defer Source Cleanup**: Don't remove fragment from source node after successful migration as this is outside job scope; cleaned up automatically by redundant copy cleanup job

### 3. Health Monitoring System

#### Fragment Health Assessment
**Purpose**: Ensure fragments remain accessible and intact across the network

**Health Check Types**:
- **Availability Check**: Verify fragment exists on expected nodes (lightweight HTTP HEAD-style request)
- **Integrity Check**: Validate Blake3 hash matches stored content (full download and verification)
- **Accessibility Check**: Ensure fragment can be retrieved with proper authentication
- **Redundancy Check**: Verify sufficient copies exist for Reed-Solomon reconstruction

**Health Monitoring Schedule**:
```rust
pub struct HealthMonitoringConfig {
    pub availability_check_interval: Duration,  // Every 6 hours
    pub integrity_check_interval: Duration,     // Every 24 hours for random sample
    pub full_network_scan_interval: Duration,   // Weekly comprehensive check
    pub critical_fragment_priority: Duration,   // Hourly for recently uploaded files
}
```

**Remediation Actions**:
- **Missing Fragment**: Trigger orphan recovery process
- **Corrupted Fragment**: Mark as bad, trigger regeneration from other fragments
- **Inaccessible Node**: Trigger rebalancing if needed
- **Insufficient Redundancy**: Create additional copies on high-reliability nodes

#### Node Reliability Scoring Updates
**Purpose**: Maintain current node performance assessments for optimal placement decisions

**Scoring Factors**:
- **Availability**: Percentage of successful health check responses over time window
- **Latency**: Average response time for fragment requests and health checks
- **Throughput**: Data transfer rates during fragment operations
- **Storage Capacity**: Available storage space and utilization trends
- **Network Stability**: Connection reliability and error rates

**Update Frequency**:
- Continuous collection during normal operations
- Consensus submission with other metrics every 10 minutes
- Emergency updates for critical node state changes

### 4. Consensus State Management

#### Historical Data Archival
**Purpose**: Manage consensus database growth while preserving Time Machine functionality

**Archival Strategy**:
- **Checkpoint Creation**: Periodic consensus state snapshots for fast synchronization
- **Transaction Pruning**: Remove redundant or obsolete consensus transactions beyond retention period
- **Metadata Preservation**: Maintain file/fragment metadata for historical queries
- **Genesis Reset**: Coordinated network-wide state reset using latest checkpoint

## Job Infrastructure

### Existing Framework Integration
Built on the current `apalis` + cron infrastructure with proper randomization:

```rust
// Example maintenance job configuration
let maintenance_schedule = format!("{} */6 * * * *", random_second); // Every 6 hours
let maintenance_worker = WorkerBuilder::new("fragment-health-monitoring")
    .data(app_state.clone())
    .backend(apalis_cron::CronStream::new(maintenance_schedule))
    .build_fn(files::jobs::handle_fragment_health_check);
```

### Job Coordination
- **Distributed Coordination**: Nodes prioritize work on data that hashes close to their node ID to minimize duplicate work
- **Destructive Operations Minimized**: Only specific cleanup jobs result in fragment deletion from a given node, and care is taken to not arbitrarily run these cleanup jobs
- **Leader Election and Height Tracking**: Some jobs run only per N heights, and only ran by the leader at that height, to avoid duplication of work
- **Failure Recovery**: Job failures logged and retried with exponential backoff
- **Resource Limiting**: Jobs respect system resource limits and can be throttled

## Implementation Phases

### Phase 1: Core Fragment Lifecycle [Pending]
- [ ] Implement threshold-based fragment cleanup with retention policy
- [ ] Add orphan recovery system with distributed fragment retrieval
- [ ] Create fragment health monitoring with automated remediation
- [ ] Build network rebalancing system for dynamic node changes

### Phase 2: Advanced Operations [Future]
- [ ] Add predictive rebalancing based on historical patterns
- [ ] Implement intelligent archival with ML-based data access prediction
- [ ] Create advanced monitoring dashboard for operations visibility
- [ ] Add automated capacity planning and scaling recommendations

### Phase 3: Enterprise Features [Future]
- [ ] Multi-region coordination for geographic redundancy
- [ ] Advanced compliance features (data residency, audit trails)
- [ ] Integration with external monitoring systems (Prometheus, etc.)
- [ ] Automated incident response and self-healing capabilities

### Operational Monitoring
- **Job Execution Metrics**: Success rates, execution times, resource usage
- **System Health Indicators**: Storage utilization, fragment health ratios, node availability
- **Performance Metrics**: Cleanup efficiency, rebalancing impact, recovery success rates
- **Alert Conditions**: Critical storage thresholds, job failures, network imbalances

## Security Considerations

### Authentication Integration
- All inter-node requests use existing dual Ed25519 signature authentication
- Job execution logged with cryptographic audit trail for accountability

### Data Privacy
- Fragment cleanup respects encryption and access controls
- No maintenance operation can expose encrypted file content

### Network Security
- Maintenance traffic uses same security model as regular network communication
- Rebalancing operations verified through consensus to prevent malicious migrations
- Health monitoring requests rate-limited to prevent DoS attacks

## Performance Targets

### Availability Impact  
- Zero downtime for maintenance operations during normal conditions
- Emergency maintenance operations do not get interrupted
- System remains fully operational even if maintenance jobs fail

## Reliability Goals

### Data Safety
- **Zero Data Loss**: Maintenance never deletes data needed for current access; historical data deletion minimized to only as needed
- **Consistency Guarantees**: All operations maintain consensus state consistency
- **Recovery Capabilities**: Failed maintenance operations can be safely retried

### Operational Continuity
- **Graceful Degradation**: System operates normally even with maintenance job failures
- **Self-Healing**: Automated detection and recovery from common operational issues
- **Monitoring Integration**: Clear visibility into maintenance operation status and health
- **Manual Override**: Administrative controls to pause, resume, or modify maintenance operations

## Technology Integration

### Database Layer
- **DuckDB Analytics**: Complex maintenance queries use analytical SQL capabilities
- **Transaction Safety**: All maintenance database changes use proper transaction boundaries, regularly committed to consensus such that a late-failing batch jobs doesn't result in significant progress loss
- **Performance Optimization**: Maintenance queries designed for minimal impact on regular operations

### Consensus Integration
- **State Coordination**: Critical maintenance state changes coordinated through consensus
- **Leader Election**: Some maintenance jobs use consensus leader election for coordination
- **Height Tracking**: All maintenance operations aware of current consensus height for versioning

### Network Layer
- **HTTP API Extensions**: New maintenance endpoints for manual operations and monitoring
- **Authentication**: Full integration with existing node and user authentication systems
- **Rate Limiting**: Maintenance network traffic respects existing rate limiting infrastructure

## Future Considerations

### Scalability Enhancements
- **Distributed Job Execution**: Scale maintenance across multiple coordinator nodes
- **Parallel Processing**: Execute maintenance jobs in parallel when resource permits
- **Geographic Distribution**: Coordinate maintenance across multiple regions/datacenters

### Advanced Features
- **Machine Learning Integration**: Predictive maintenance based on historical patterns
- **Third-Party Monitoring**: Integration with enterprise monitoring and alerting systems