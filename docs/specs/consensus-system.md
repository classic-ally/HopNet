# RFC-001: Consensus System

## Overview

The HopNet Consensus System provides Byzantine fault-tolerant distributed agreement for network coordination, file system operations, and system configuration. Built on a pragmatic 2-phase HotStuff-style protocol, the system prioritizes operational reliability and deterministic behavior for private network deployments while maintaining strong safety and liveness properties.

## Design Philosophy

### Pragmatic Byzantine Fault Tolerance
- **Private Network Optimization**: Designed for networks where participants have aligned incentives
- **Operational Reliability**: Prioritize consistent operation over theoretical optimizations
- **Deterministic Behavior**: Height-based progression avoiding timestamp synchronization complexity
- **Immediate Execution**: Simple state machine with direct transaction execution

### Performance Through Architecture
- **Variable Consensus**: Critical operations consensus-tracked, performance-sensitive operations (shard rebalancing, etc) handled externally to consensus system
- **Efficient Cryptography**: Ed25519 signatures for speed and individual accountability
- **Database Persistence**: Robust state management with automatic recovery capabilities
- **Network Pragmatism**: HTTP-based coordination for development velocity and debugging

### Long-Term Operational Sustainability
- **Height Management**: Checkpoint and genesis reset mechanisms for sustainable operation
- **Maintenance Integration**: Built-in framework for scheduled system maintenance tasks
- **Monitoring Foundation**: Comprehensive observability for consensus health and performance
- **Graceful Failure Handling**: Automatic detection and recovery from common failure modes

## Core Consensus Protocol

### Two-Phase HotStuff Algorithm

#### Phase Structure
1. **Propose Phase**: Leader proposes block, validators vote after transaction validation; quorum leads to Propose QC formation
2. **Lock Phase**: Validators vote to commit block after observing a quorum-signed Propose QC formation; quorum leads to Lock QC formation
3. **Commit**: Block execution and state advancement after Lock QC observation

#### Safety and Liveness Properties
- **Safety**: Conflicting blocks cannot both achieve Lock QC (highest QC tracking)
- **Liveness**: Progress guaranteed with 2/3+1 honest validators and eventual network synchrony
- **Agreement**: All honest nodes agree on committed block sequence
- **Validity**: Only valid transactions included in committed blocks

### Block Structure and Chain Management

#### Block Data Model
```rust
pub struct Block {
    pub block_hash: Blake3Hash,
    pub data: BlockData {
        pub height: i32,                    // Sequential height progression
        pub view_number: i32,               // Consensus view for leader rotation
        pub parent_hash: Option<Blake3Hash>, // Chain linkage for integrity
        pub transactions: Option<Transactions>, // Batched transaction payload  
    }
}
```

#### Chain Integrity Requirements
- **Height Progression**: Strict sequential increment (new_height = parent_height + 1)
- **Parent Hash Validation**: Each block cryptographically linked to predecessor
- **View Advancement**: View numbers must increase with block progression
- **Hash Integrity**: BLAKE3 verification for all block components

### Leader Selection and View Management

#### Deterministic Leader Rotation
```rust
fn determine_leader(view: i32, height: i32) -> Result<NodeId> {
    let validators = get_validators_at_height(height)?;
    let leader_index = (view as usize) % validators.len();
    Ok(validators[leader_index].node_id)
}
```

#### View Progression Mechanisms
- **Successful Commits**: View advances on successful block commitment
- **Timeout Detection**: Scheduled job detects view stagnation (30-second timeout)
- **Timeout Certificates**: Distributed timeout vote collection advances stalled views
- **Leader Validation**: Only designated leader can propose valid blocks for each view

## Validator Set Management

### Height-Based Validator Activation

#### Validator Registration Model
```sql
CREATE TABLE validators (
    effective_height INTEGER NOT NULL,   -- Height when validator becomes active
    node_id INTEGER NOT NULL,           -- Node identifier
    is_active BOOLEAN NOT NULL,         -- Active/inactive status
    
    PRIMARY KEY (effective_height, node_id)
);
```

#### Dynamic Membership Properties
- **Future Activation**: Validators registered for specific future heights
- **Deterministic Membership**: Validator set determined by consensus height, not wall-clock time
- **Smooth Transitions**: Validators-elect observe QCs before activation, allowing seamless participation in consensus once activated
- **Consensus-Driven Changes**: All validator set modifications require consensus agreement

#### Validator Lifecycle Management
1. **Registration**: New validators added through consensus transaction for future height
2. **Activation**: Automatic activation when consensus reaches specified height
3. **Participation**: Full consensus participation including vote collection and QC validation
4. **Deactivation**: Removal through consensus transaction with grace period

### Quorum and Threshold Management
- **Quorum Requirement**: 2/3+1 of active validators for all consensus decisions
- **Dynamic Quorum**: Quorum size adjusts automatically with validator set changes
- **Threshold Safety**: Byzantine fault tolerance maintained with up to 1/3 faulty validators
- **Participation Tracking**: Monitor validator response rates for health assessment

## Transaction Processing Framework

### Handler-Based Transaction System

#### Transaction Handler Interface
```rust
pub trait TransactionHandler: Send + Sync {
    fn get_name(&self) -> &'static str;
    fn handle(&self, tx_data: &[u8], execute: bool, app_state: Arc<AppState>) -> Result<(), TxError>;
    fn validate_user_permission(&self, user_id: i32, tx_data: &[u8]) -> Result<(), PermissionError>;
}
```

#### Current Transaction Types
- **File Operations**: File creation, deletion, and metadata management
- **User Management**: User registration and permission updates
- **Node Management**: Node addition and validator set modifications
- **System Configuration**: Network-wide policy and configuration changes

#### Planned Transaction Types
- **Shard Management**: Fragment placement policies and distribution rules
- **Metrics Recording**: Node performance and reliability metrics
- **Maintenance Tasks**: Scheduled system maintenance and cleanup operations
- **Network Policies**: Bandwidth allocation, storage quotas, and access control

### Transaction Execution Model

#### Two-Phase Validation and Execution
1. **Dry-Run Validation** (Propose Phase Pre-Vote): Handlers execute with `execute=false` to validate feasibility
2. **Actual Execution** (Following Lock QC Observation): Handlers execute with `execute=true` to apply state changes
3. **Atomic Operations**: All transactions in block succeed or fail together
4. **State Consistency**: Database transactions ensure consistent state updates

#### Transaction Validation Process
```rust
fn validate_and_execute_transactions(
    transactions: &Transactions, 
    execute: bool
) -> Result<(), ConsensusError> {
    for transaction in transactions.iter() {
        let handler = get_transaction_handler(&transaction.function_name)?;
        
        // Validate user permissions
        handler.validate_user_permission(transaction.user_id, &transaction.data)?;
        
        // Execute or validate transaction
        handler.handle(&transaction.data, execute, app_state)?;
    }
    Ok(())
}
```

## Network Coordination and Fault Tolerance

### Distributed Coordination Mechanisms

#### Quorum Certificate Management
- **QC Formation**: Collect 2/3+1 signatures for Propose and Lock phases
- **Parallel Collection**: Concurrent vote collection with early termination on quorum
- **QC Broadcasting**: Leaders distribute QCs to all validators and validators-elect
- **Batch Signature Verification**: Efficient multi-signature validation using ed25519_dalek

#### Timeout and Recovery Handling
- **Timeout Detection**: Periodic monitoring of view progression with configurable timeouts
- **Distributed Timeout Voting**: Validators coordinate timeout certificates for view advancement
- **Network Partition Recovery**: Catch-up mechanism for nodes behind current view
- **Graceful Degradation**: Partial network functionality during temporary partitions

### Catch-Up and Synchronization

#### Node Synchronization Protocol
1. **View Polling**: Detect when local node is behind network consensus
2. **Missing View Identification**: Determine required views for synchronization
3. **Parallel Data Fetching**: Retrieve QCs and blocks from multiple validators
4. **State Validation**: Cryptographically verify all synchronized data
5. **Progressive Catch-Up**: Handle large synchronization gaps efficiently

#### Catch-Up Optimization Strategies
- **Batch Fetching**: Request multiple views in single network operation
- **Round-Robin Source Selection**: Distribute load across available validators
- **Partial Failure Tolerance**: Continue catch-up despite individual validator failures
- **State Checkpoint Integration**: Leverage checkpoints for efficient large-gap synchronization

## Long-Term Operational Sustainability

### Height Saturation and Checkpoint Management

#### Integer Height Limitations
- **Current Implementation**: 32-bit signed integer heights (maximum ~2.1 billion blocks)
- **Saturation Timeline**: At 1 block/second = ~68 years, higher throughput reduces timeline
- **Performance Degradation**: Long chains increase synchronization time for new nodes
- **Storage Growth**: Consensus history storage grows indefinitely without management

#### Checkpoint and Genesis Reset Mechanism
```rust
pub struct NetworkCheckpoint {
    pub checkpoint_height: i32,
    pub checkpoint_hash: Blake3Hash,
    pub state_snapshot: DatabaseSnapshot,
    pub validator_set: Vec<ValidatorInfo>,
    pub consensus_parameters: ConsensusConfig,
}

impl NetworkCheckpoint {
    pub fn create_genesis_reset(&self) -> GenesisBlock {
        GenesisBlock {
            height: 0,  // Reset height to zero
            state_hash: self.state_snapshot.compute_hash(),
            initial_validators: self.validator_set.clone(),
            checkpoint_reference: self.checkpoint_hash,
        }
    }
}
```

#### Checkpoint Creation Process
1. **Checkpoint Proposal**: Consensus transaction to create network checkpoint at specific height
2. **State Snapshot**: Complete database state captured at checkpoint height
3. **Validator Agreement**: 2/3+1 validator approval for checkpoint creation
4. **Genesis Reset Preparation**: New genesis block created from checkpoint state
5. **Network Migration**: Coordinated transition to new genesis and height reset

#### Checkpoint Integration Requirements
- **Backward Compatibility**: Historical data access through checkpoint references
- **Time Machine Integration**: Historical browsing across checkpoint boundaries
- **Fragment Retention**: Ensure fragment availability spans checkpoint transitions
- **Client Synchronization**: Graceful client updates during genesis reset

### Scheduled Maintenance Framework

#### Maintenance Task Categories
- **Height Management**: Checkpoint creation and genesis reset coordination
- **Fragment Lifecycle**: Orphaned fragment cleanup and garbage collection
- **Key Rotation**: Cryptographic key updates and migration procedures
- **Performance Optimization**: Database maintenance, index rebuilding, cache optimization

#### Consensus-Coordinated Maintenance
- **Maintenance Proposals**: Maintenance tasks proposed and approved through consensus
- **Distributed Execution**: Coordinated execution across all validators
- **Progress Tracking**: Consensus-tracked maintenance task completion status
- **Rollback Capability**: Safe rollback mechanisms for failed maintenance operations

## Monitoring, Observability and Failure Handling

### Consensus Health Monitoring

#### Performance Metrics Collection
- **View Duration**: Time between view starts and successful block commits
- **Block Commit Rate**: Successful block commits per unit time
- **Validator Response Times**: Individual validator performance in consensus rounds
- **Network Synchronization Lag**: Node catch-up frequency and duration

#### Health Indicators
- **Consensus Participation Rate**: Percentage of validators actively participating
- **Fork Detection**: Monitor for potential safety violations or competing chains
- **Timeout Frequency**: Rate of timeout certificate generation indicating network issues
- **Transaction Throughput**: Rate of successful transaction processing

### Automatic Failure Detection and Recovery

#### Automatic Recovery Actions
- **View Advancement**: Automatic timeout certificate generation for stagnant views
- **Validator Health Checks**: Periodic connectivity and responsiveness verification
- **Catch-Up Triggering**: Automatic synchronization when falling behind consensus
- **Error Reporting**: Structured logging and alerting for consensus anomalies

#### Manual Intervention Capabilities
- **Emergency Validator Removal**: Consensus-based removal of consistently failing validators
- **Network Partition Resolution**: Manual coordination for severe network partitions
- **State Recovery**: Database rollback and recovery procedures for corruption scenarios
- **Genesis Reset Activation**: Emergency checkpoint creation and network reset procedures

## Race Condition Prevention and Fork Safety

### File Change Race Condition Handling

#### Concurrent File Operation Scenarios
- **Simultaneous Modifications**: Multiple users modifying same file simultaneously
- **Delete-Create Races**: File deletion racing with recreation or modification
- **Directory Structure Changes**: Parent directory modifications affecting child operations
- **Permission Changes**: Access control modifications racing with file operations

#### Consensus-Level Conflict Resolution
1. **Operation Ordering**: File operations ordered by consensus view and transaction position
2. **Conflict Detection**: Identify conflicting operations within same consensus round
3. **Precedence Rules**: Deterministic conflict resolution based on user ID and operation type
4. **Error Propagation**: Clear error messages for rejected conflicting operations

### Fork Prevention Architecture

#### Safety Mechanisms
- **Highest QC Tracking**: Prevent validators from voting for conflicting blocks
- **Parent Hash Validation**: Ensure proper chain linkage for all proposed blocks
- **View Monotonicity**: Enforce strictly increasing view numbers
- **Leader Validation**: Reject blocks from unauthorized leaders

#### Fork Detection and Recovery
- **Chain Validation**: Periodic verification of local chain against network consensus
- **Conflicting Block Detection**: Monitor for competing blocks at same height
- **Automatic Recovery**: Catch-up mechanism resolves minor fork scenarios
- **Manual Resolution**: Procedures for severe fork scenarios requiring operator intervention

## Integration with System Components

### File Storage System Integration
- **Metadata Consensus**: File system operations require consensus approval
- **Fragment Registration**: Fragment metadata recorded through consensus transactions
- **Storage Policy Distribution**: Consensus-managed policies for retention and redundancy
- **Historical State Coordination**: Time Machine functionality relies on consensus height tracking

### Authentication and Security Integration
- **User Registration**: User accounts managed through consensus transactions
- **Permission Management**: Access control changes coordinated via consensus
- **Key Rotation Coordination**: Cryptographic key updates scheduled through consensus
- **Audit Trail**: Consensus provides cryptographic audit trail for all system operations

### Network Communication Integration
- **Node Registration**: New nodes added through consensus validator transactions
- **Network Policy Distribution**: Bandwidth limits and connection policies via consensus
- **Health Metrics Reporting**: Node performance metrics reported through consensus transactions
- **Failure Coordination**: Network partition and failure handling coordinated via consensus

## Performance Considerations and Scalability

### Current Performance Characteristics
- **Throughput**: Optimized for small-to-medium networks (< 100 validators)
- **Latency**: 30-second timeout balances responsiveness with network tolerance
- **CPU Efficiency**: Ed25519 signatures provide excellent single-operation performance
- **Network Efficiency**: HTTP-based coordination with batch signature verification

### Scalability Enhancement Strategies

#### Hierarchical Consensus Architecture
- **Storage-Only Nodes**: Non-validator nodes participate in storage without consensus overhead
- **Regional Clustering**: Geographic grouping of validators for network efficiency
- **Consensus Delegation**: Lightweight consensus for storage nodes coordinated by validators
- **Load Distribution**: File operations distributed across storage nodes, consensus limited to coordination

#### Cryptographic Migration Path
- **BLS Signature Preparation**: Migration path to BLS aggregated signatures for large networks
- **Threshold Security**: Support for threshold signatures in validator rotation scenarios
- **Post-Quantum Readiness**: Framework for post-quantum cryptographic algorithm integration
- **Algorithm Agility**: Support for multiple signature algorithms during transition periods

## Implementation Priorities

### Phase 1: Feature Completeness [~]
- [ ] Implement checkpoint creation and genesis reset mechanisms
- [ ] Add shard management and metrics recording transaction types
- [ ] Create comprehensive consensus health monitoring system
- [ ] Build automatic failure detection and recovery capabilities

### Phase 2: Operational Maturity [ ]
- [ ] Develop scheduled maintenance framework with consensus coordination
- [ ] Implement advanced fork prevention and race condition handling
- [ ] Add comprehensive monitoring, alerting, and observability tools
- [ ] Create emergency intervention and manual recovery procedures

### Phase 3: Performance and Scalability [ ]
- [ ] Design hierarchical consensus architecture for storage-only nodes
- [ ] Implement BLS signature migration path for large network scaling
- [ ] Add advanced performance monitoring and optimization capabilities
- [ ] Create geographic distribution and regional clustering support

### Phase 4: Advanced Features [ ]
- [ ] Implement post-quantum cryptographic algorithm support
- [ ] Add advanced consensus analytics and predictive failure detection
- [ ] Create sophisticated maintenance scheduling and automation
- [ ] Build enterprise-grade disaster recovery and business continuity features