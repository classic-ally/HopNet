# HopNet System Overview

## Vision
**"A distributed filesystem designed for everyone, from enthusiast to enterprise"**

HopNet provides secure, reliable, performant file storage across networked devices using Byzantine fault-tolerant consensus and Reed-Solomon erasure coding. Users can create private networks spanning their devices, share with friends and family, or deploy enterprise-wide distributed storage solutions.

## Core Systems

### 1. Consensus System ([RFC-001](specs/consensus-system.md))
**Status**: Largely implemented, needs comprehensive test suite (further buildout of orchestrator) to ensure edge-case handling

Byzantine fault-tolerant consensus engine providing network coordination and state management.

- [x] HotStuff-2 style consensus with 2-phase voting (Propose → Lock)
- [x] Block height tracking and monotonic progression
- [x] Validator set management with height-based activation
- [x] Leader rotation and timeout handling
- [x] Quorum certificate generation and validation
- [x] Node catch-up mechanism for network synchronization
- [x] Performance metrics integration for node reliability (latency + throughput measurement complete)
- [x] **NEW**: Consensus locking and retry system with race condition prevention
- [x] **NEW**: Prepared block tracking (`prepared_block_hash`) for ongoing consensus operations
- [x] **NEW**: Automatic retry logic (up to 3 attempts) with view change detection
- [x] **NEW**: Timeout protection (5-second max wait) with 50ms polling intervals
- [x] **NEW**: Leadership change handling during consensus operations
- [~] **HOTFIX**: Database transaction retry logic for concurrent operations (write-write conflict handling)
- [ ] **ENHANCEMENT**: Atomic consensus lock operation to eliminate race conditions at database level
- [ ] Node health monitoring and automatic validator management

### 2. File Storage System ([RFC-002](specs/file-storage.md))
**Status**: Complete core functionality including chunked Reed-Solomon streaming reconstruction

Reed-Solomon encoded file storage with encryption, chunked encoding, and fragment management.

- [x] **NEW**: Chunked Reed-Solomon encoding (40MB chunks, 10 original + 20 recovery per chunk)
- [x] **NEW**: Progressive streaming reconstruction with 25x TTFB improvement for large files
- [x] **NEW**: Per-chunk fast-path and Reed-Solomon reconstruction
- [x] **NEW**: Chunk-aware database schema (chunk_number, local_index compound primary key)
- [x] ChaCha20-Poly1305 fragment encryption with per-file keys
- [x] Blake3 fragment hashing and integrity verification
- [x] Local fragment storage with 2-level directory structure
- [x] Event-driven fragment distribution after upload completion
- [x] Distributed fragment discovery with accelerated fallback pattern
- [x] Work queue pattern for efficient concurrent fragment retrieval
- [x] Database consistency management with automatic state correction
- [x] Cryptographic authentication for all fragment transfer operations
- [x] Consensus-based file deletion with user ownership validation and proper error handling
- [ ] Fragment lifecycle management and garbage collection
- [ ] Automated maintenance reconciliation (route + scheduled job)
- [ ] Storage capacity monitoring and quota management
- [ ] Secure thumbnail generation for encrypted files
- [ ] Preview data extraction while maintaining encryption

### 3. Node Communication System ([RFC-003](specs/node-communication.md))
**Status**: Iroh transport active for health checks and view sync, HTTP migration ongoing ([migration plan](refactors/iroh-migration.md))

HTTP-based inter-node communication with iroh (QUIC/TLS) transport migration in progress.

- [x] HTTP API with RESTful endpoints for consensus operations
- [x] Dual Ed25519 signature authentication (node + user signatures)
- [x] Node registry with IP addresses and public keys
- [x] Request routing (non-leaders forward to current leader)
- [x] Basic RTT latency measurement between nodes
- [x] Node discovery and network membership management
- [x] Fragment transfer protocols and endpoints (GET/POST /fragments/{hash}, health checks)
- [x] Bandwidth monitoring and quality-of-service metrics (latency + throughput measurement complete)
- [x] Iroh transport layer with peer validation and connection caching
- [x] Fragment health checks over iroh (Phase 2 complete)
- [~] Consensus messages over iroh (Phase 3)
- [ ] Fragment data transfer over iroh (Phase 4)
- [ ] Network topology awareness and geographic information

### 4. Shard Synchronization System ([RFC-004](specs/shard-synchronization.md))
**Status**: Complete modulo placement with chunked RS support, automated recovery pending

Intelligent fragment distribution system optimizing for performance, reliability, and even distribution.

- [x] Consensus height-based versioning for deterministic placement
- [x] **NEW**: Modulo placement algorithm (local_index % num_validators) with perfect balance
- [x] **NEW**: File-level node selection with metrics-based deterministic shuffle
- [x] **NEW**: Local-index-aware placement ensuring consistent chunk distribution
- [x] Metrics-based scoring for optimal node selection (40% availability, 30% throughput, 20% latency, 10% stability)
- [x] Event-driven distribution with consensus integration
- [x] Self-skip optimization (avoid sending fragments to local node)
- [x] Retry logic with exponential backoff and connection timeouts
- [x] Inter-node authentication for secure fragment transfer
- [x] Fragment discovery protocol for download reconstruction
- [x] Manual network rebalancing with atomic data block processing
- [x] Dynamic timeout calculation based on fragment count (1GB/30min transfer rate)
- [x] Direct node-to-node fragment transfer without intermediary
- [ ] Background orphan recovery with adaptive thresholds
- [ ] Node reliability scoring and roaming device detection
- [ ] Automated background rebalancing and fragment migration
- [ ] User proximity optimization for shared files

### 5. User Interface System ([RFC-005](specs/user-interface.md))
**Status**: Progressed to max for current system set

Cross-platform desktop application providing file management and network administration.

- [x] Tauri-based desktop application (Windows, macOS, Linux)
- [x] Svelte frontend with reactive file browser
- [x] QR code-based network joining workflow
- [x] File upload/download with progress tracking
- [x] Node management and network status monitoring
- [x] User authentication and session management
- [ ] File preview system with secure thumbnail generation
- [ ] Native OS integration (Apple FileProvider, Windows Cloud Files API)
- [ ] Advanced file operations (multi-select, context menus, drag-drop)
- [ ] Network health dashboard and metrics visualization
- [ ] Advanced file sharing controls and permissions
- [ ] Responsive mobile interface for thin client operations

### 6. Security & Authentication ([RFC-006](specs/security.md))
**Status**: Core Complete, Extensions Planned

End-to-end encryption and comprehensive authentication system.

- [x] Ed25519 cryptographic identity for nodes and users
- [x] X25519 key derivation for file access control
- [x] AES-SIV encrypted file paths and metadata
- [x] JWT-based user session management
- [x] Per-file encryption keys with user access control
- [x] Consensus operation authentication and validation
- [ ] Advanced permission models (read-only, time-limited access)
- [ ] Key rotation and recovery mechanisms
- [ ] Audit logging and security monitoring
- [ ] Geographic compliance and data sovereignty controls
- [ ] Thin client architecture for mobile/constrained devices

### 7. Maintenance & Operations System ([RFC-007](specs/maintenance-operations.md))
**Status**: Orphaned data cleanup and manual rebalancing complete, automated recovery pending

Automated background processes ensuring network health and storage efficiency.

- [x] **NEW**: Threshold-based orphaned data block cleanup with UUIDv7 age prioritization and consensus deletion
- [x] **NEW**: Two-transaction approach for DuckDB foreign key constraint handling 
- [x] **NEW**: Opportunistic local fragment deletion during consensus execution
- [x] **NEW**: Manual network rebalancing trigger with placement height consensus updates
- [x] **NEW**: Atomic data block rebalancing (only update placement_height after all fragments migrate)
- [x] **NEW**: RPC fragment fetch instructions with dual Ed25519 authentication
- [ ] Availability-aware cleanup prioritization (redundant vs historical)
- [ ] Automated background network rebalancing for node join/leave events
- [ ] Lost shard recovery with Reed-Solomon reconstruction
- [ ] Redundant copy cleanup for download/rebalancing artifacts
- [ ] Fragment health monitoring and remediation
- [ ] Consensus state management and archival
- [ ] Fragment filesystem cleanup for orphaned files
- [ ] Job coordination using node ID proximity to minimize duplicate work

### 8. User Data Takeout System ([RFC-010](specs/user-data-takeout.md))
**Status**: Complete implementation with responsive frontend

Consensus-coordinated user data export system enabling portable data extraction from the distributed network.

- [x] Consensus-tracked takeout lifecycle management with network-wide coordination
- [x] Rate limiting with one active takeout per user validation
- [x] Event-driven file materialization with real-time progress tracking
- [x] Streaming archive creation with tar.gz compression and encryption
- [x] Fragment retention coordination preventing cleanup during active takeouts
- [x] Automated maintenance jobs with batched consensus operations for expiration handling
- [x] Complete REST API endpoints (initiate, list, download, delete, can-create status)
- [x] Responsive frontend interface with selection-based bulk operations
- [x] Auto-refresh functionality with intelligent pausing during user actions
- [x] Account mode sidebar integration with proper UI state management
- [x] TypeScript type generation with DateTime serialization support
- [ ] **Future**: Incremental exports (changes since last takeout)
- [ ] **Future**: Selective folder/file export capabilities
- [ ] **Future**: Multiple archive format support (ZIP, 7z)
- [ ] **Future**: WebSocket-based real-time progress updates

### 9. Apple FileProvider Integration ([RFC-009](specs/apple-fileprovider.md))
**Status**: Phase 4b Complete ✅ + Comprehensive Testing Framework ✅ - Full Read/Write Support with Unified Change Tracking

Native macOS Finder and iOS Files app integration through Apple's FileProvider framework.

- [x] Swift-based FileProvider extension with native URLSession
- [x] HTTP API communication with scoped authentication via Keychain
- [x] Stable file identity using data_block_id for files, hex-encoded encrypted paths for folders
- [x] Read operations (enumerate, fetch metadata, download files)
- [x] Delete operations with recursive folder support
- [x] **NEW**: Unified modification log for comprehensive change tracking (all operations: create, modify, move, delete)
- [x] **NEW**: Efficient single-query incremental sync using LEFT JOIN pattern
- [x] **NEW**: Recursive folder modification dates showing most recent child activity
- [x] **NEW**: Parent folder inclusion in change queries for consistent FileProvider sync
- [x] **NEW**: Ancestor folder modification logging ensuring complete folder hierarchy change tracking
- [x] Process isolation maintaining consensus integrity
- [x] Fragment assembly and streaming for downloads
- [x] **Phase 2**: Write operations (createItem for files and folders)
- [x] **Phase 2**: Multipart upload integration with consensus via existing post_files endpoint
- [x] **Phase 2**: Folder identifier consistency fix using backend-generated encrypted identifiers
- [x] **Phase 2**: .allowsWriting capability for root container and folders
- [x] **Phase 3**: Enhanced metadata properties (creation dates, modification dates)
- [x] **Phase 3**: CustomDateTime deserialization supporting DuckDB native timestamps
- [x] **Phase 3**: Fallback logic using creation dates when modification dates are NULL
- [x] **Phase 3**: ISO 8601 timestamp formatting with Swift Date parsing
- [x] **Phase 4a**: Item modification (modifyItem for metadata-only rename/move operations)
- [x] **Phase 4a**: Folder identifier change handling via deletion_log tracking
- [x] **Phase 4a**: Trash detection with NSFeatureUnsupportedError for unsupported trashing
- [x] **Phase 4a**: Authentication header fix (Bearer token format)
- [x] **Phase 4a**: Consensus height boundary fix for deletion sync  
- [x] **Phase 4b**: Content modification (file content updates via modifyItem)
- [x] **Testing Framework**: Comprehensive FileProvider test suite with Swift executables and Rust orchestration
- [x] **Testing Framework**: Empty file support (handle both `file_size == Some(0)` and `file_size.is_none()`)
- [x] **Testing Framework**: Content verification for all file types with direct API download (bypasses system integration)
- [x] **Testing Framework**: Complete round-trip testing (create → enumerate → download → verify)
- [ ] **Next**: Explicit parent folder logging for complete change tracking (handle deletes/moves)
- [ ] Manual domain registration in HopNet settings
- [ ] Working sets support (recents, favorites, shared)
- [ ] Foundation for iOS thin client architecture
- [ ] Thumbnail generation and Quick Look integration

### 10. S3-Compatible API ([RFC-008](specs/s3-compatibility.md))
**Status**: Specification complete, implementation not started

S3-compatible API layer enabling standard S3 clients and SDKs to interact with HopNet.

- [ ] Virtual bucket layer mapping S3 buckets to encrypted paths
- [ ] Dual-mode credentials (secure proxy mode and portable standalone mode)
- [ ] AWS Signature v4 authentication
- [ ] Core S3 operations (ListBuckets, CreateBucket, GetObject, PutObject, etc.)
- [ ] Local proxy mode for secure key management
- [ ] Standalone mode for environments without local HopNet client
- [ ] Bucket-level access control and sharing
- [ ] Multipart upload support
- [ ] Pre-signed URLs for temporary access
- [ ] Integration with AWS CLI and standard S3 SDKs

## Current Focus

**Active Development**: FileProvider Phase 4b Implementation Ready
- Design finalized: Create new data blocks for modified content
- Add stable `inodes.id` (UUIDv7) for unified file/folder identification
- Reuse existing multipart upload pipeline for content updates
- Maintain last-write-wins semantics for concurrent modifications
- Leverage UUIDv7 timestamps for intrinsic creation/modification tracking

**Recently Completed**: FileProvider Testing Framework with Empty File Support ✅
- Implemented comprehensive FileProvider integration test suite with Swift test executables and Rust orchestration
- Fixed empty file handling in download route: properly handle both `file_size == Some(0)` and `file_size.is_none()`
- Resolved NSFileProviderError -1004 by using direct API download instead of FileProvider system integration
- Added content verification for all file types: empty, Unicode, JSON, large files (~50KB), and multiline content
- Established testing pattern: create → enumerate → download → verify content for complete round-trip validation
- All FileProvider file creation and content verification tests now passing

**Previously Completed**: Consensus System Locking and Retry Improvements ✅
- Implemented consensus state locking using `prepared_block_hash` to prevent race conditions
- Added comprehensive retry logic (up to 3 attempts) with view change detection and timeout handling
- Enhanced consensus middleware to wait for ongoing consensus before starting new operations
- Added leadership change detection and automatic forwarding to new leaders
- Improved error handling with 5-second timeout protection and proper cleanup mechanisms

**Previously Completed**: FileProvider Ancestor Logging Enhancement ✅
- Implemented comprehensive ancestor folder modification logging for complete hierarchy tracking
- Added get_all_ancestor_folders() helper using efficient single-query path matching
- Enhanced log_modification() to automatically log all ancestor folders for create/move/delete operations
- Ensures parent/grandparent folders show updated modification times when descendants change
- Root enumeration now automatically includes all affected ancestor folders without query changes

**Previously Completed**: FileProvider Phase 4a (Item Modification) ✅
- Implemented modifyItem() for metadata-only changes (rename/move operations)
- Added folder identifier change handling via deletion_log for FileProvider incremental sync
- Fixed authentication header format (Bearer token) for API communication
- Added trash detection with NSFeatureUnsupportedError for unsupported trashing operations
- Resolved consensus height boundary issue (>= instead of >) in deletion queries
- Successfully enabled folder rename and move operations with proper cleanup

**Infrastructure Complete**: Background Metrics Collection
- [x] Extended metrics table with consensus height and availability tracking  
- [x] Created reusable metrics collection infrastructure with timeout handling
- [x] Implemented consensus transaction batching for metrics submissions ("submit_metrics" handler)
- [x] Added manual metrics trigger API endpoint with consensus integration for debugging and testing
- [x] Fixed metrics retrieval API (GET /metrics) with proper timestamp handling for DuckDB compatibility
- [x] Implemented automated background metrics collection worker with randomized 10-minute intervals
- [x] Integrated throughput measurement using existing infrastructure
- [x] **COMPLETED**: Storage capacity metrics collection (storage_total_gb, storage_used_gb columns)
- [x] **COMPLETED**: Cross-platform storage metrics endpoint (/rpc/storage-server) with JWT+RPC dual authentication

**Primary**: Chunked Reed-Solomon with Modulo Placement (RFC-002 + RFC-004)
- ✅ **COMPLETED**: Chunked Reed-Solomon encoding with 40MB chunks for streaming optimization
  - 10 original + 20 recovery fragments per chunk (30 fragments total per chunk)
  - Progressive streaming reconstruction with 25x TTFB improvement for large files
  - Per-chunk fast-path and Reed-Solomon reconstruction
  - Chunk-aware database schema with (data_block_id, chunk_number, local_index) compound key
- ✅ **COMPLETED**: Modulo placement algorithm with file-level node selection
  - File-level: Metrics-based deterministic shuffle selects 30 nodes per file
  - Fragment-level: local_index % num_selected_nodes for primary placement
  - Metrics weighting: 40% availability, 30% throughput, 20% latency, 10% stability
  - Perfect balance: ±1 max imbalance across nodes for optimal failure tolerance
  - Local-index awareness: fragment[0] from all chunks goes to same node
- ✅ **COMPLETED**: Fragment discovery and cross-node retrieval workflows
  - Work queue pattern for efficient concurrent fragment retrieval
  - Database consistency management with automatic state correction
  - Ed25519 cryptographic authentication for all fragment requests
  - 3-phase fallback: best candidate → deterministic placement → network-wide gossip

**Secondary**: Upload/Download Workflow Integration
- ✅ **COMPLETED**: Fragment transfer integration with file download (missing fragment retrieval)
- Implement background push synchronization for uploaded files
- Add fragment health monitoring using /fragments/{hash}/health endpoint

### Reliability Goals
- **Single node failure**: No data loss, minimal performance impact
- **Regional outage**: Full data availability through geographic redundancy
- **Network partition**: Graceful degradation with majority consensus
- **Roaming devices**: No impact on network performance or availability

### Scalability Targets
- **Current scope**: Validator-sized networks (<100 nodes)
- **Near-term**: Support up to 1000 nodes with varying participation in consensus (e.g. some nodes storage only)
- **Long-term**: Enterprise deployments with geographic distribution

## Technology Stack

### Backend
- **Language**: Rust (performance, safety, concurrency)
- **Database**: DuckDB (embedded analytics, complex queries)
- **Consensus**: Custom HotStuff-based BFT implementation
- **Cryptography**: Ed25519, X25519, ChaCha20-Poly1305, Blake3
- **Networking**: HTTP with custom authentication

### Frontend
- **Framework**: Tauri (cross-platform desktop)
- **UI Library**: Svelte (reactive, lightweight)
- **Styling**: Modern CSS with dark theme
- **Build System**: Vite (fast development, optimized builds)

### Development
- **Version Control**: Git with conventional commits
- **Documentation**: Markdown
- **Testing**: Rust unit/integration tests, manual UI testing
- **Deployment**: Native binary distribution

## Development Roadmap

### Phase 1A: Infrastructure Completion (Critical Path)
**Goal**: Resolve blocking dependencies preventing distributed operations

1. **Complete RFC-003 fragment transfer protocols** ✅ - Critical blocker resolved
   - Fragment transfer HTTP endpoints implemented (GET/POST /fragments/{hash}, health checks)
   - Authentication integrated with dual signature system
   - Fragment size validation and automatic hash verification
   
2. **Implement background metrics collection infrastructure** - New critical blocker identified  
   - Extend metrics table with consensus height and availability boolean columns
   - Automated background metrics collection with randomized scheduling (every 10 minutes)
   - Consensus transaction batching to minimize network overhead
   - Manual trigger API endpoint for debugging and testing
   
3. **Enable RFC-002 distributed fragment storage** - Foundation for distributed network
   - Implement distributed fragment placement using metrics-based node reliability scoring
   - Add cross-node fragment discovery and retrieval (depends on RFC-003 ✅)
   - Complete storage capacity monitoring and basic quota management

### Phase 1B: Native OS Integration
**Goal**: Enable seamless native OS file access

1. **RFC-009 Apple FileProvider Integration** - Native macOS/iOS file access (PHASES 1-3 COMPLETE ✅)
   - ✅ Implemented Swift FileProvider extension with full read/delete operations
   - ✅ Added scoped HTTP API endpoints with Keychain authentication
   - ✅ Created stable file identity system using data_block_id
   - ✅ Added fragment assembly streaming for downloads
   - ✅ **Phase 2 (Complete)**: Implemented createItem for file/folder creation with multipart upload
   - ✅ **Phase 3 (Complete)**: Added enhanced metadata properties (creation dates, modification dates) with DuckDB timestamp support
   - ✅ **Phase 4a (Complete)**: Implemented modifyItem for metadata-only rename/move operations
   - **Phase 4b (Design Complete, Ready for Implementation)**: Content modification with new data blocks approach
   
### Phase 1C: Basic Distributed Operations ✅ **COMPLETED**
**Goal**: Enable core distributed filesystem functionality

1. **RFC-004 fragment placement and discovery** - Smart fragment distribution ✅
   - ✅ Implemented modulo placement for deterministic, balanced distribution
   - ✅ Added node reliability scoring with metrics-based selection
   - ✅ Chunked Reed-Solomon implementation with progressive streaming
   
2. **RFC-007 maintenance and operations** - Network health and efficiency
   - Implement threshold-based fragment cleanup with UUIDv7 age tracking
   - Add availability-aware cleanup prioritization
   - Build network rebalancing system for topology changes
   - Create redundant copy cleanup for storage optimization
   
3. **Complete UI features for distributed operations** - User-facing distributed functionality  
   - Advanced file operations (multi-select, drag-drop) with distributed backend
   - Network health dashboard showing distributed node status
   - File sharing controls leveraging distributed storage
   
4. **Node performance monitoring** - Foundation for reliability scoring
   - Implement comprehensive node metrics collection
   - Add automatic node health scoring for placement decisions
   - Build monitoring dashboard for network health

### Phase 2: Performance & Reliability
- Advanced node reliability scoring with predictive capabilities
- Automated rebalancing and fragment migration
- Performance optimization for streaming use cases
- NAT traversal implementation for simplified network setup

### Phase 3: Enterprise Features
- Geographic compliance framework with user-provided regions
- Advanced security features (audit logging, key rotation)
- Large network scaling optimizations
- Mobile thin client application

### Phase 4: Advanced Capabilities
- Machine learning-based placement optimization
- Integration with cloud storage providers
- Advanced geographic redundancy with regulatory compliance
- Developer APIs and third-party integrations

### Phase 5: Enterprise Integration APIs
**Goal**: Enable enterprise adoption through standard APIs

**FileProvider Next Steps (Phase 4b-5):**
- Phase 4b (Implementation Ready): Content modification with stable inode IDs and new data blocks 
- Phase 5: Enable working sets, thumbnails, and Quick Look integration

**S3 Compatibility Layer:**
- Implement core S3 operations with AWS Signature v4 authentication
- Build virtual bucket layer with encrypted path mappings
- Create dual-mode credential system (proxy and standalone)
- Add multipart upload and pre-signed URL support
- Integrate with existing file sharing and permission architecture
- Validate compatibility with AWS CLI and major S3 SDKs