# User Data Takeout

## Summary
Enable users to export all their files from HopNet in a standard, portable format. Users receive their complete file tree with fully reconstructed and decrypted files, preserving the original folder structure. The system uses consensus-based coordination to ensure network-wide consistency and proper cleanup coordination.

## Motivation
Users should have full control and portability of their data. This feature enables:
- Data portability between systems
- Personal backup creation
- Account migration
- Compliance with data protection regulations (GDPR right to data portability)

## Design

### Overview
The takeout system reconstructs user files from the distributed, encrypted storage backend and packages them into a downloadable archive with the original folder hierarchy intact.

### Takeout Initialization

#### 1. Authentication & Authorization
```
User Request → Verify JWT → Create Takeout Record → Begin File Materialization
```
- User must provide valid JWT authentication
- System creates takeout record with 24-hour validity window
- Rate limiting: One active takeout per user (checked at API level)
- Storage validation: Verify at least 2x user data size available (with safety factor)
  - Query node's most recent storage availability metrics
  - Account for ongoing storage participation during takeout

#### 2. Database Tracking (Consensus-Tracked)
```sql
-- Consensus-tracked table for network-wide takeout coordination
CREATE TABLE takeouts (
    id UUID PRIMARY KEY,                    -- UUIDv7 (contains creation timestamp)
    user_id INTEGER NOT NULL REFERENCES users(user_id),
    owner_node_id INTEGER NOT NULL,         -- Node that owns and processes this takeout
    status ENUM('pending', 'materializing', 'ready', 'expired', 'cancelled') NOT NULL DEFAULT 'pending',
    expires_at TIMESTAMP NOT NULL,
    consensus_height INTEGER NOT NULL      -- Height at takeout creation for point-in-time consistency
);

-- Temporary snapshot of user's files at takeout creation time (owner node only)
CREATE TEMPORARY TABLE takeout_inodes_{takeout_id} (
    id UUID NOT NULL,
    path VARCHAR NOT NULL,
    type ENUM('file', 'folder') NOT NULL,
    data_id UUID,                          -- No foreign key constraint (temporary table)
    materialization_status ENUM('pending', 'success', 'failed') DEFAULT 'pending',
    error_message VARCHAR
);
```

**Schema Notes:**
- UUIDv7 for takeout ID encodes creation timestamp, eliminating need for separate `created_at` column
- `TakeoutStatus` enum defined in `hopnet-common` for frontend/backend type sharing
- Temporary inode table created atomically with takeout record for consistency

**Consensus-Based Takeout Creation:**
Takeout creation uses consensus to ensure network-wide coordination:

```
API Route (Owner Node)
├── Validate user permissions and storage capacity  
├── Build TakeoutPayload with status='pending'
├── Submit to consensus middleware
└── Return success/failure to user

Consensus Processing (All Nodes)
├── Validation phase (execute=false)
│   ├── Check for existing active user takeouts
│   ├── Verify network-wide takeout conflicts  
│   └── Rollback validation transaction
├── Execution phase (execute=true) 
│   ├── Insert takeout record (all nodes)
│   ├── Owner node: Create temporary inode snapshot
│   ├── Other nodes: Record takeout for cleanup coordination
│   └── Commit transaction
```

This ensures:
- Network-wide takeout visibility for cleanup coordination
- Point-in-time consistency via consensus height boundaries
- Automatic node failure handling (owner change possible via cleanup)
- Prevention of conflicting takeouts across the network

#### 3. File Content Retrieval
For each file in the temporary inode snapshot:

```
Query Snapshot Table → Fetch Data Block → Gather Fragments → Retrieve from Distributed Network
```

- Track materialization status per file in the snapshot table
- Fragment retention: All nodes coordinate cleanup to respect active takeouts
  - Pre-flight check: `COUNT(*) FROM takeouts WHERE expires_at > CURRENT_TIMESTAMP`  
  - Consensus validation: Network-wide active takeout verification before deletion
  - Prevents data deletion during any active takeout across the network


### User Notification

- User needs to be notified that all files have been materialized to takeout node
- Need some timeout such that files which are unrecoverable don't stall progress forever
- Notify users of files that could not be collected for takeout

### Local Bundle Download
Upon user input, local node prepares archive and constructs internal decrypted content

#### 1. Folder Tree Reconstruction
```
Query User Inodes → Decrypt Paths → Build Directory Structure
```

**Database Query:**
```sql
SELECT id, path, type, data_id 
FROM inodes 
WHERE owner_id = :user_id
ORDER BY path
```

**Path Decryption:**
- Each path segment is encrypted with user's key
- Decrypt segments to reconstruct human-readable paths
- Build in-memory representation of folder hierarchy

#### 2. File Content Reconstruction

**Decrypt Per-File Key:**
- Use user's X25519 private key
- Perform ECDH with ephemeral public key
- Derive unwrapping key
- Decrypt the per-file encryption key

**Reassemble using erasure coding:**
```
let file_content = erasure_decode(chunks)?;
```


#### 3. Archive Generation
```
Create Archive → Incrementally Add Files → Stream or Save Locally
```

**Progressive Archive Creation:**
- Use tar streaming API to append files as they're decrypted
- Avoid loading entire archive into memory
- Set restrictive permissions (700) on temporary directory
- For remote users: Stream archive via chunked HTTP response

**Archive Structure:**
```
user-takeout-{timestamp}.tar.gz
└── files/
    ├── Documents/
    │   ├── report.pdf
    │   └── notes.txt
    ├── Photos/
    │   └── vacation/
    │       └── beach.jpg
    └── [user's complete folder structure]
```

### API Endpoints

**Core Operations:**
- `POST /takeout/initiate` - Start takeout process (checks rate limits, creates record)
- `GET /takeout/status` - Query progress and current status
- `GET /takeout/download` - Stream completed archive 
- `DELETE /takeout/cancel` - Cancel active takeout and cleanup

**Error Handling:**
- Handle unreachable fragments gracefully (skip and log)
- Track failed files separately with detailed error messages
- Timeout mechanism for stalled materialization

## Security Considerations

1. **Authentication**: Use existing JWT middleware for all takeout operations
2. **Temporary Storage**: Use takeout directories with restrictive UNIX permissions (700)
3. **Cleanup**: Automatically purge expired takeout data and temporary files
4. **Resource Limits**: Enforce storage availability checks before starting materialization

## Implementation Considerations

**Storage Efficiency:**
- Streaming archive creation to minimize memory usage
- Progressive file decryption and addition to archive
- Cleanup integration prevents fragment removal during active takeouts

**Error Recovery:**
- Point-in-time consistency using consensus height boundaries
- Graceful handling of network partitions during fragment retrieval
- Detailed error reporting for unreconstructable files

**Network Synchronization:**
- Takeout state synchronized to new joining nodes via setup system
- `SyncSetupObject` includes complete takeout records for network consistency
- Prevents consensus divergence and ensures cleanup coordination across all nodes

## Future Enhancements

- Incremental exports (changes since last takeout)
- Selective folder/file export
- Multiple format support (ZIP, 7z)
- Metadata export (sharing history, versions)
- WebSocket-based real-time progress updates