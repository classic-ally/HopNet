# RFC: Fragment Attestation System

## Overview

This RFC defines a dual-approach attestation system for verifying fragment availability in the HopNet distributed storage network. The system combines self-attestation for maintenance with retrieval-based reputation tracking for storage placement decisions.

## Attestation Approaches

### Self-Attestation (Internal Maintenance)

Nodes periodically verify their own fragment storage for data integrity and cleanup.

**Tier 1: Database Check**
- Queries local database to check fragment inventory consistency
- **Performance**: ~0.1ms per fragment (database lookup only)
- **Purpose**: Identify database/filesystem inconsistencies for repair

**Tier 2: Disk Verification**
- Verifies fragments actually exist on filesystem
- **Performance**: ~5-10ms per fragment (includes disk I/O)
- **Purpose**: Detect corrupted or missing files, update local state

**Security Model**: Self-reporting for maintenance, not proof of storage

### Retrieval-Based Reputation (Placement Decisions)

Track fragment request success/failure during normal operations to build node reputation scores.

**Flow**:
1. During normal downloads/repairs, record each fragment request outcome
2. Batch submit aggregated metrics to consensus periodically
3. Use reputation scores for future placement decisions

**Performance**: Zero additional network traffic (piggybacks on existing operations)
**Security**: Gaming-resistant since based on real user requests with actual consequences

## Data Structures

### AttestationReport
```rust
pub struct AttestationReport {
    pub fragment_hash: Blake3Hash,
    pub tester_id: i32,
    pub storer_id: i32,
    pub tier_used: AttestationTier,
    pub result: AttestationResult,
    pub timestamp: DateTime<Utc>,
    pub attestor_signature: Signature,
}

pub enum AttestationTier {
    DatabaseOnly,
    DiskChecksum,
    RemoteFetch,
}

pub enum AttestationResult {
    HasFile,
    NoFile,
    CorruptedFile,
}
```

## Implementation Notes

- All responses must be cryptographically signed by the testing node
- Batch requests should be supported for efficiency (up to 1000 fragments per request)
- Database checks can be implemented as simple vector comparisons against the `fragment_hashes.stored_locally` field
- Nodes should self-test for DatabaseOnly and DiskChecksum tests as it provides no additional safety to have another node sign off on an operation that trusts the tester
- Disk verification should use efficient batch file existence checks
- Content verification reuses existing fragment download endpoints
- Successful fragment recovery should be marked as passing attestation, avoiding immediate re-attestation of repaired fragments
- Attestation scheduling should use randomness to distribute load across nodes and time
- Fragment transfers between attestations may cause temporary inconsistencies; fallback to discovery algorithms handles this gracefully
- Node bootstrapping submits complete initial fragment inventory as differential from empty database state

## Database Design

### Self-Attestation Schema

```sql
CREATE TABLE fragment_inventory (
    fragment_hash           BLOB NOT NULL,
    node_id                 INTEGER NOT NULL,
    self_verified_height    INTEGER,  -- Last self-attestation check, once every so often we ensure this verification is actual disk check NOT only DB check.

    PRIMARY KEY (fragment_hash, node_id),
    FOREIGN KEY (node_id) REFERENCES nodes(node_id)
);
```

### Retrieval-Based Reputation Schema

```sql
-- Local staging table (per-node)
CREATE TABLE pending_fragment_requests (
    from_node INTEGER NOT NULL,
    to_node INTEGER NOT NULL,
    success BOOLEAN NOT NULL,
    recorded_at_height INTEGER NOT NULL,      -- When request actually occurred
    batch_upload_height INTEGER,              -- When submitted to consensus (NULL = pending)

    INDEX idx_pending (batch_upload_height, recorded_at_height),
    INDEX idx_timing (recorded_at_height, from_node, to_node)
);

-- Consensus-tracked reputation (aggregated from staging)
CREATE TABLE fragment_request_metrics (
    reporting_node INTEGER NOT NULL,    -- Node that reported these metrics
    from_node INTEGER NOT NULL,         -- Node that requested fragments
    to_node INTEGER NOT NULL,           -- Node that served fragments
    consensus_height INTEGER NOT NULL,   -- When metrics were submitted
    requests_sent INTEGER NOT NULL,
    requests_succeeded INTEGER NOT NULL,

    PRIMARY KEY (reporting_node, from_node, to_node, consensus_height)
);
```

## Fragment Discovery Integration

The `fragment_inventory` table enables a hybrid fragment location strategy that optimizes access performance while managing consensus overhead:

- **Primary path**: Query `fragment_inventory` for instant fragment location lookup (~0.1ms)
- **Fallback path**: Use existing discovery algorithms when fragments not found in inventory
- **Update strategy**: Batch fragment inventory changes to consensus hourly, with immediate updates for large changes
- **Performance guarantee**: No regression from current system due to discovery fallback

This allows 90%+ of fragment lookups to bypass network discovery entirely while maintaining system reliability.

## Future Work

This RFC establishes the basic attestation primitives. Future iterations will define:
- Strategies for selecting which tier to use for different scenarios
- Multi-node attestation protocols for Byzantine fault tolerance
- Consensus integration for network-wide attestation coordination
- Performance optimizations and caching strategies