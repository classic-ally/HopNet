# RFC: Fragment Attestation System

## Overview

This RFC defines a multi-tier attestation system for verifying fragment availability in the HopNet distributed storage network. The system provides three levels of verification with increasing cost and security guarantees.

## Attestation Tiers

### Tier 1: Database Check (trust-storer-fast)

The fastest attestation method that queries the storing node's local database to check if fragments are marked as stored locally.

**Performance**: ~0.1ms per fragment (database lookup only)
**Security**: Relies on storing node's honesty about database state

### Tier 2: Disk Verification (trust-storer-full)

Verifies that fragments actually exist on the storing node's filesystem, not just marked as locally stored in the database.

**Performance**: ~5-10ms per fragment (includes disk I/O)
**Security**: Verifies physical file existence, relies on storing node's honesty about file content

### Tier 3: Content Verification (trust-tester)

A separate node, the tester, downloads the fragment from the storing node and verifies the content matches the expected hash.

**Flow**:
1. Tester requests fragment download from storer via existing fragment API
2. Tester verifies downloaded content matches expected fragment hash
3. Tester signs attestation report of verification result

**Attestation Report**:
```json
{
  "fragment_hash": "blake3_hash",
  "content_verified": true,
  "storer_node": 123,
  "tester_node": 456,
  "verification_time": "2024-01-23T10:30:00Z",
  "tester_signature": "ed25519_signature"
}
```

**Performance**: ~50-200ms per fragment (network transfer + verification)
**Security**: Cryptographically verifies fragment content, limited by potential collusion between tester and storer

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

```sql
CREATE TABLE attestation_events (
    tester_id       INTEGER NOT NULL,    -- Who performed the check
    storer_id       INTEGER NOT NULL,    -- Who was being checked (same as tester for self-checks)
    consensus_height INTEGER NOT NULL,   -- When the check happened
    attestation_tier ENUM('database', 'disk', 'remote') NOT NULL,
    fragments_checked UINTEGER NOT NULL,  -- Total fragments examined

    PRIMARY KEY (tester_id, storer_id, consensus_height),
    INDEX idx_tester_history (tester_id, consensus_height),
    INDEX idx_storer_history (storer_id, consensus_height)
);

CREATE TABLE fragment_inventory (
    fragment_hash   BLOB NOT NULL,
    node_id        INTEGER NOT NULL,
    since_height   INTEGER NOT NULL,     -- Consensus height when storage started
    until_height   INTEGER,              -- Consensus height when storage ended (NULL = current)
    removal_reason  ENUM('missing', 'corrupted', 'node_offline') NULL, -- Why storage ended

    PRIMARY KEY (fragment_hash, node_id, since_height),
    INDEX idx_fragment_current (fragment_hash, until_height),  -- Current locations
    INDEX idx_node_inventory_at_height (node_id, since_height, until_height), -- Node state at time
    INDEX idx_removal_analysis (removal_reason, until_height)  -- Analyze removal patterns
);

CREATE TABLE consensus_blocks (
    height          INTEGER PRIMARY KEY,
    block_hash      BLOB NOT NULL,
    timestamp       TIMESTAMP NOT NULL,  -- When block was committed
    view_number     INTEGER NOT NULL
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