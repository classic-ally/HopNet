# RFC-002: File Storage System

## Overview

The HopNet File Storage System provides secure, distributed file storage with Reed-Solomon erasure coding, streaming capabilities, and time-based versioning. Built on privacy-first principles with multi-layer encryption, the system ensures data durability through intelligent redundancy while maintaining high performance for streaming use cases and historical data access.

## Design Philosophy

### Privacy-First Architecture
- **Multi-Layer Encryption**: Path, file, and chunk-level encryption with separate key hierarchies
- **Zero-Knowledge Storage**: Nodes cannot decrypt file content without explicit user access grants
- **Metadata Privacy**: File paths and directory structures encrypted with user-specific keys
- **Content Isolation**: Per-file encryption keys prevent cross-file data correlation

### Durability and Performance Balance
- **Conservative Redundancy**: 2:1 Reed-Solomon ratio (3x storage overhead) for strong fault tolerance
- **Streaming Optimization**: Memory-efficient processing with fragment size limits for network performance
- **Fast-Path Recovery**: Direct reconstruction when all original fragments available
- **Reed-Solomon Fallback**: Graceful degradation to erasure decoding when fragments missing

### Time-Based Data Management
- **Consensus Height Versioning**: Leverage blockchain-like height progression for historical state
- **Time Machine Functionality**: Browse and restore files from any previous consensus state
- **Retention-Based Cleanup**: Orphaned fragments preserved for historical access requirements
- **Operational Continuity**: Version management integrated with distributed consensus operations

## Fragment Generation and Processing

### Reed-Solomon Encoding Strategy

#### Redundancy Configuration
- **2:1 Ratio**: Generate 2 recovery fragments for every 1 original fragment
- **Dynamic Chunking**: Fragment count and size adapt to file size for optimal network performance
- **Minimum Chunk Count**: 10 original fragments minimum for efficient Reed-Solomon operations
- **Maximum Fragment Size**: 4MB limit for large files, ensuring consumer network compatibility
- **Even-Length Padding**: Fragment padding to meet Reed-Solomon algorithm requirements

#### Dynamic Chunking Algorithm
```rust
pub fn calculate_optimal_chunks(file_size: usize) -> (usize, usize) {
    if file_size == 0 {
        return (0, 0); // Empty files have no chunks
    }
    
    // Calculate minimum chunks needed to stay under fragment size limit
    let min_original_chunks = (file_size + MAX_FRAGMENT_SIZE - 1) / MAX_FRAGMENT_SIZE;
    
    // Ensure at least 10 original chunks for good Reed-Solomon efficiency
    let original_chunks = min_original_chunks.max(10);
    
    // Use 2:1 redundancy ratio (2 recovery for every 1 original)
    let recovery_chunks = original_chunks * 2;
    
    (original_chunks, recovery_chunks)
}
```

**Sizing Behavior:**
- **Small Files (≤40MB)**: Always use exactly 10 original + 20 recovery chunks (30 total)
  - Fragment size varies from very small (tiny files) up to 4MB (40MB files)
  - Optimizes Reed-Solomon efficiency over network considerations for manageable sizes
- **Large Files (>40MB)**: Use optimal chunking with ~4MB fragments
  - Chunk count: `ceiling(file_size / 4MB)` original chunks
  - Maintains consistent 4MB fragment sizes for predictable network performance
  - Example: 100MB file → 25 original + 50 recovery = 75 total fragments

#### Fragment Processing Pipeline
1. **Content Chunking**: Split file content into optimal-sized chunks with padding
2. **Chunk Encryption**: Encrypt each chunk with derived keys before Reed-Solomon processing
3. **Reed-Solomon Encoding**: Generate recovery fragments from encrypted original chunks
4. **Fragment Identification**: Assign UUID v7 identifiers for temporal ordering and nonce derivation
5. **Hash Generation**: Blake3 hash of encrypted content for integrity verification and deduplication

### Encryption Workflow

#### Key Hierarchy
- **Per-File Keys**: ChaCha20-Poly1305 keys generated uniquely per file upload
- **Chunk Keys**: Derived from per-file key + fragment UUID using Blake3 KDF
- **Wrapping Keys**: X25519 ECDH-derived keys for multi-user access control
- **Path Keys**: User-specific AES-SIV keys for filesystem metadata encryption

#### Chunk-Level Encryption
```rust
fn encrypt_chunk(file_key: &[u8], fragment_uuid: &UUID, chunk_data: &[u8]) -> EncryptedChunk {
    let chunk_key = derive_chunk_key(file_key, fragment_uuid);
    let nonce = derive_chunk_nonce(fragment_uuid);
    
    ChaCha20Poly1305::new(&chunk_key)
        .encrypt(&nonce, chunk_data)
        .expect("Encryption cannot fail with valid key/nonce")
}
```

## Local Storage Architecture

### Fragment Storage Organization

#### Directory Structure
- **Two-Level Nesting**: `fragments/ab/cd/abcdef123...` for filesystem efficiency
- **Hash-Based Naming**: Fragment filename derived from Blake3 hash of encrypted content
- **XDG Compliance**: Standard user data directory (`~/.local/share/hopnet/fragments`)
- **Platform Portability**: Consistent structure across Windows, macOS, and Linux

#### Fragment Metadata
```sql
CREATE TABLE fragment_hashes (
    data_block_id    UUID NOT NULL,
    fragment_index   INTEGER NOT NULL,
    fragment_id      UUID NOT NULL,
    fragment_hash    BLOB NOT NULL,
    chunk_type       ENUM('original', 'recovery') NOT NULL,
    stored_locally   BOOLEAN DEFAULT FALSE,
    stored_remotely  TEXT,  -- JSON array of node IDs storing this fragment
    
    PRIMARY KEY (data_block_id, fragment_index),
    FOREIGN KEY (data_block_id) REFERENCES data_blocks(id)
);
```

#### Storage Verification
- **Runtime Checks**: Verify fragment availability during file operations
- **Hash Validation**: Blake3 verification on fragment access
- **Missing Fragment Handling**: Graceful degradation when local fragments unavailable
- **Storage State Tracking**: Database flags for local and remote fragment availability

### File Metadata Management

#### Database Schema Design
```sql
CREATE TABLE data_blocks (
    id              UUID PRIMARY KEY,
    blake3_hash     BLOB NOT NULL,
    original_chunks INTEGER NOT NULL,
    recovery_chunks INTEGER NOT NULL,
    added_bytes     INTEGER NOT NULL,
    created_height  INTEGER NOT NULL,  -- Consensus height when created
    
    FOREIGN KEY (created_height) REFERENCES blocks(height)
);

CREATE TABLE inodes (
    owner_id        INTEGER NOT NULL,
    encrypted_path  BLOB NOT NULL,
    inode_type      ENUM('file', 'folder') NOT NULL,
    data_block_id   UUID,
    created_height  INTEGER NOT NULL,
    
    PRIMARY KEY (owner_id, encrypted_path),
    FOREIGN KEY (data_block_id) REFERENCES data_blocks(id)
);
```

#### Path Encryption System
- **AES-SIV Encryption**: Deterministic encryption for consistent encrypted paths
- **User-Specific Keys**: Each user's paths encrypted with individual derived keys
- **Directory Structure**: Hierarchical path encryption maintaining filesystem semantics
- **Privacy Protection**: Directory structures hidden from unauthorized users

## File Reconstruction and Streaming

### Reconstruction Algorithm Design

#### Fast-Path Reconstruction
```rust
fn reconstruct_file_fast_path(original_fragments: &[Fragment]) -> Result<Vec<u8>, ReconstructionError> {
    if original_fragments.len() < required_original_count {
        return Err(ReconstructionError::InsufficientFragments);
    }
    
    let mut file_data = Vec::new();
    for fragment in original_fragments.iter().sorted_by_key(|f| f.index) {
        let decrypted_chunk = decrypt_chunk(&fragment.data, &fragment.key)?;
        file_data.extend_from_slice(&decrypted_chunk);
    }
    
    Ok(remove_padding(file_data))
}
```

#### Reed-Solomon Recovery Path
```rust
fn reconstruct_file_reed_solomon(available_fragments: &[Fragment]) -> Result<Vec<u8>, ReconstructionError> {
    if available_fragments.len() < minimum_required_fragments {
        return Err(ReconstructionError::InsufficientFragments);
    }
    
    let reed_solomon = ReedSolomon::new(original_count, recovery_count)?;
    let reconstructed_data = reed_solomon.reconstruct(available_fragments)?;
    
    let mut file_data = Vec::new();
    for (index, chunk) in reconstructed_data.iter().enumerate() {
        if index < original_count {
            let decrypted_chunk = decrypt_chunk(chunk, &derive_chunk_key(index))?;
            file_data.extend_from_slice(&decrypted_chunk);
        }
    }
    
    Ok(remove_padding(file_data))
}
```

### Streaming Support

#### Requirements for Streaming Downloads
- **Progressive Reconstruction**: Begin file delivery as fragments become available
- **Memory Efficiency**: Process fragments without loading entire file into memory
- **Order Preservation**: Maintain chunk order for coherent file streaming
- **Error Recovery**: Handle missing fragments gracefully during streaming

#### Download Workflow Implementation
1. **Local Database Check**: Query local fragment database first (may be cached from previous downloads)
2. **Missing Fragment Identification**: Determine which fragments need retrieval from remote nodes
3. **Deterministic Placement Query**: Get preference-ordered list of 1/3 storage nodes for each missing fragment
4. **Sequential Fragment Retrieval**: Try nodes in preference order using HTTP GET /fragments/{hash} endpoints
5. **Broadcast Fallback**: If deterministic placement fails, query all nodes before Reed-Solomon reconstruction
6. **Progressive File Reconstruction**: Decrypt and stream file content as fragments become available
7. **Client Streaming**: Stream reconstructed file to client (this is where streaming happens, not fragment transfer)

#### Upload Workflow Implementation  
1. **Fragment Generation**: Create, encrypt, and hash fragments locally from uploaded file
2. **Local Fragment Storage**: Commit fragments to local database and filesystem
3. **Client Response**: Return HTTP 200 to client immediately after local storage commit
4. **Background Push Synchronization**: POST fragments to target nodes using preference-ordered node lists
5. **Safety Retention**: Don't delete local fragment copies until receiving 201 CREATED responses from target nodes
6. **Health Monitoring**: Periodic background jobs verify fragment integrity and pull missing fragments

## Distributed Storage Integration

### Fragment Distribution Contracts

#### Storage Interface for Shard Synchronization
```rust
pub trait DistributedFragmentStorage {
    // Fragment placement decisions (implemented by RFC-004)  
    async fn get_fragment_target_nodes(&self, fragment_hash: Blake3Hash) -> Vec<NodeId>; // Returns 1/3 of nodes, preference-ordered
    
    // Fragment transfer operations (implemented by RFC-003)
    async fn store_fragment_on_node(&self, fragment: &Fragment, target_node: NodeId) -> Result<(), StorageError>; // POST /fragments/{hash}
    async fn retrieve_fragment_from_node(&self, fragment_hash: Blake3Hash, source_node: NodeId) -> Result<Fragment, RetrievalError>; // GET /fragments/{hash}
    
    // Local fragment management
    async fn check_fragment_locally(&self, fragment_hash: Blake3Hash) -> bool; // Database check first
    async fn store_fragment_locally(&self, fragment: &Fragment) -> Result<(), StorageError>; // Local commit
    
    // Health monitoring
    async fn verify_fragment_health(&self, fragment_hash: Blake3Hash, node: NodeId) -> Result<bool, HealthError>; // GET /fragments/{hash}/health
}
```

#### Local Storage Responsibilities
- **Fragment Generation**: Create, encrypt, and hash fragments before distribution
- **Local Caching**: Maintain local copies of frequently accessed fragments
- **Integrity Verification**: Validate fragment hashes on storage and retrieval
- **Metadata Management**: Track fragment locations and availability in database

#### Integration Points with Consensus
- **Fragment Registration**: Register fragment metadata through consensus operations
- **Storage Policies**: Consensus-managed policies for retention, redundancy, and distribution
- **Node Health Integration**: Fragment placement influenced by consensus-tracked node metrics

### Fragment Lifecycle Management

#### Fragment Creation Workflow
1. **File Upload Processing**: Accept multipart uploads with streaming processing
2. **Fragment Generation**: Create encrypted fragments with Reed-Solomon encoding
3. **Local Storage**: Store fragments locally with hash-based organization
4. **Placement Determination**: Query shard synchronization system for optimal placement
5. **Remote Distribution**: Transfer fragments to selected nodes via shard sync protocols
6. **Metadata Recording**: Update database with fragment locations and consensus registration

#### Fragment Retrieval Workflow
1. **Fragment Location Discovery**: Check local availability, query remote locations if needed
2. **Parallel Retrieval**: Request missing fragments from multiple nodes simultaneously
3. **Integrity Verification**: Validate fragment hashes on retrieval
4. **Caching Decision**: Cache frequently accessed remote fragments locally
5. **Reconstruction**: Assemble file using fast-path or Reed-Solomon recovery

## Time-Based Versioning System

### Consensus Height Integration

#### Historical State Management
- **Version Identification**: Use consensus block height as primary versioning mechanism
- **State Reconstruction**: Query database state at any previous consensus height
- **Metadata Versioning**: Track when files/directories were created, modified, or deleted
- **Fragment Retention**: Preserve fragments for historical access requirements

#### Database Schema for Versioning
```sql
-- Add height tracking to existing tables
ALTER TABLE data_blocks ADD COLUMN created_height INTEGER NOT NULL;
ALTER TABLE data_blocks ADD COLUMN deleted_height INTEGER; -- NULL if not deleted

ALTER TABLE inodes ADD COLUMN created_height INTEGER NOT NULL;
ALTER TABLE inodes ADD COLUMN deleted_height INTEGER; -- NULL if not deleted

-- Index for efficient historical queries
CREATE INDEX idx_data_blocks_height ON data_blocks(created_height, deleted_height);
CREATE INDEX idx_inodes_height ON inodes(created_height, deleted_height);
```

#### Time Machine Functionality
```rust
pub struct HistoricalFileSystem {
    pub target_height: u64,
    pub current_height: u64,
}

impl HistoricalFileSystem {
    pub fn list_directory_at_height(&self, path: &str, height: u64) -> Result<Vec<DirectoryEntry>, FSError> {
        // Query inodes that existed at specified height
        let query = "
            SELECT * FROM inodes 
            WHERE created_height <= ? 
            AND (deleted_height IS NULL OR deleted_height > ?)
            AND encrypted_path LIKE ?
        ";
        // Implementation queries database state at historical height
    }
    
    pub fn restore_file_from_height(&self, path: &str, height: u64) -> Result<FileData, FSError> {
        // Reconstruct file as it existed at specified consensus height
        // May require historical fragment reconstruction
    }
}
```

### User Interface Integration

#### Timeline Navigation Interface (UI RFC Update Required)
- **Height Selector**: Consensus height input or date/time picker for historical browsing
- **Visual Timeline**: Graphical representation of file system changes over time
- **Diff Views**: Visual indicators showing changes between different time periods
- **Restoration Controls**: One-click restoration of files/directories from historical states

#### Historical State Indicators
- **Current State Marker**: Clear indication when viewing current vs historical state
- **Change Highlighting**: Visual indicators for files that have changed since selected time
- **Deleted Item Recovery**: Browse and recover files that have been deleted
- **Version Comparison**: Side-by-side comparison of file versions across time periods

## Storage Management and Operations

### Retention and Garbage Collection

#### Time-Based Retention Policies
```rust
pub struct RetentionPolicy {
    pub minimum_age_days: u32,        // Minimum age before fragment cleanup eligibility
    pub maximum_history_days: u32,    // Maximum history retention period
    pub consensus_height_buffer: u64, // Minimum consensus heights to preserve
}

impl RetentionPolicy {
    pub fn is_eligible_for_cleanup(&self, fragment: &Fragment, current_height: u64) -> bool {
        let min_height = current_height.saturating_sub(self.consensus_height_buffer);
        let deleted_before_buffer = fragment.deleted_height
            .map(|h| h < min_height)
            .unwrap_or(false);
        
        // Only cleanup fragments deleted before minimum height buffer
        deleted_before_buffer && self.meets_minimum_age_requirement(fragment)
    }
}
```

#### Garbage Collection Process
1. **Eligibility Assessment**: Identify fragments eligible for cleanup based on retention policy
2. **Historical Access Check**: Verify fragments not needed for active historical queries
3. **Cross-Node Coordination**: Ensure sufficient replicas remain before cleanup
4. **Gradual Cleanup**: Process cleanup in batches to avoid performance impact
5. **Audit Logging**: Record all cleanup operations for compliance and debugging

### Capacity Management

#### Storage Monitoring
- **Fragment Storage Usage**: Track local storage consumption by fragment type
- **User Quota Tracking**: Monitor per-user storage utilization against configured limits
- **Node Capacity Reporting**: Report available storage capacity to shard synchronization system
- **Historical Storage Growth**: Track storage growth patterns for capacity planning

#### Quota Enforcement
```rust
pub struct StorageQuota {
    pub user_id: i32,
    pub max_storage_bytes: u64,
    pub current_usage_bytes: u64,
    pub max_file_count: Option<u32>,
    pub current_file_count: u32,
}

impl StorageQuota {
    pub fn can_store_file(&self, file_size: u64) -> Result<(), QuotaError> {
        let projected_usage = self.current_usage_bytes + file_size;
        if projected_usage > self.max_storage_bytes {
            return Err(QuotaError::StorageExceeded);
        }
        
        if let Some(max_files) = self.max_file_count {
            if self.current_file_count >= max_files {
                return Err(QuotaError::FileCountExceeded);
            }
        }
        
        Ok(())
    }
}
```

### Preview and Thumbnail Generation

#### Secure Thumbnail Extraction
- **Server-Side Generation**: Generate thumbnails and previews during file upload processing
- **Encryption Integration**: Thumbnail data encrypted with same per-file key as content
- **Format Support**: Support for common image, video, and document formats
- **Size Optimization**: Multiple thumbnail sizes for different UI contexts

#### Preview Data Storage
```sql
CREATE TABLE file_previews (
    data_block_id   UUID NOT NULL,
    preview_type    ENUM('thumbnail', 'metadata', 'text_excerpt') NOT NULL,
    encrypted_data  BLOB NOT NULL,
    data_size       INTEGER NOT NULL,
    
    PRIMARY KEY (data_block_id, preview_type),
    FOREIGN KEY (data_block_id) REFERENCES data_blocks(id)
);
```

#### Preview Generation Pipeline
1. **Format Detection**: Identify file type during upload processing
2. **Preview Extraction**: Generate thumbnails/metadata while file is being processed
3. **Preview Encryption**: Encrypt preview data with same per-file key
4. **Database Storage**: Store encrypted preview data linked to file record
5. **Access Control**: Preview access follows same permissions as file access

## Performance Considerations and Future Enhancements

### Current Performance Characteristics

#### Reed-Solomon Performance Trade-offs
- **Fast Path Advantages**: Direct concatenation ~10x faster than Reed-Solomon reconstruction
- **Recovery Path Costs**: Reed-Solomon requires minimum fragment threshold, CPU-intensive
- **Network Implications**: Missing original fragments require multiple network round-trips
- **Memory Usage**: Reed-Solomon reconstruction temporarily requires more memory

#### Streaming Performance Limitations
- **Reed-Solomon Cliff**: Dramatic performance degradation when original fragments unavailable
- **Fragment Dependency**: Streaming blocked until sufficient fragments retrieved
- **Network Amplification**: Single missing original fragment requires multiple recovery fragments

### Future Performance Optimizations

#### Chunked Reed-Solomon Strategy
- **Concept**: Apply Reed-Solomon at chunk-level rather than file-level for streaming benefits
- **Trade-offs**: Reduced fault tolerance (chunk-level vs file-level redundancy) for improved streaming
- **Prerequisites**: High-quality node placement metrics and reliability scoring
- **Implementation**: Gradual migration based on file access patterns and node reliability

#### Advanced Caching Strategies
- **Predictive Caching**: Cache frequently accessed fragments based on user patterns
- **Geographic Caching**: Cache fragments closer to requesting users
- **Load-Based Caching**: Dynamic caching based on node capacity and network conditions
- **Collaborative Caching**: Coordinate caching decisions across network nodes

#### Storage Efficiency Improvements
- **Deduplication**: Detect and eliminate duplicate fragments across users/files
- **Compression Integration**: Transparent compression for fragments with high compression ratios
- **Tiered Storage**: Move rarely accessed fragments to slower, cheaper storage
- **Archive Integration**: Integration with external archive systems for long-term storage

## Implementation Priorities

### Phase 1: Distributed Storage Foundation [x] **COMPLETED**
- [x] **Accelerated fragment discovery system with deterministic placement matching**
- [x] **Work queue pattern for efficient concurrent fragment retrieval (fixes premature stopping)**
- [x] **Database/disk state consistency management with automatic correction**
- [x] **Ed25519 cryptographic authentication for all fragment transfer operations**
- [x] **Robust Reed-Solomon reconstruction handling missing/corrupted fragments**
- [x] Create fragment distribution contracts and integration points with RFC-004
- [x] Add remote fragment location tracking to database schema
- [x] Build basic distributed fragment storage and retrieval capabilities

**Key Implementation Details:**
- Fragment discovery uses 3-phase fallback: best candidate → deterministic placement candidates → network-wide gossip
- Worker threads use shared work queue to retry alternative fragments when individual fragments are unavailable
- Database consistency automatically maintained through fetch_and_verify_fragment failure handling
- All inter-node requests use dual Ed25519 signatures (node + user) with proper authentication middleware

### Phase 2: Time-Based Versioning [~]
- [ ] Add consensus height tracking to all file metadata tables
- [ ] Implement historical file system query capabilities
- [ ] Create Time Machine browsing and restoration APIs
- [ ] Update UI RFC to include historical browsing interface requirements

### Phase 3: Operational Features [ ]
- [ ] **Fragment storage maintenance system (scheduled job + manual route)**
  - [ ] Automated reconciliation between database state and disk storage
  - [ ] Detection and correction of corrupted/missing fragments
  - [ ] Orphaned fragment cleanup and database synchronization
  - [ ] Comprehensive reporting with detailed inconsistency analysis
- [ ] Implement time-based garbage collection with retention policies
- [ ] Add storage quota management and capacity monitoring
- [ ] Create preview and thumbnail generation during upload processing
- [ ] Build comprehensive storage analytics and reporting

### Phase 4: Advanced Performance [ ]
- [ ] Research and prototype chunked Reed-Solomon approach
- [ ] Implement advanced caching strategies and predictive fragment placement
- [ ] Add storage deduplication and compression optimization
- [ ] Create tiered storage and archive integration capabilities