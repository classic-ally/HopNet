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
- [ ] Node health monitoring and automatic validator management

### 2. File Storage System ([RFC-002](specs/file-storage.md))  
**Status**: Local node download and upload implemented, no update support, distribution pending

Reed-Solomon encoded file storage with encryption and fragment management.

- [x] Reed-Solomon encoding with 2:1 redundancy (N original + 2N recovery)
- [x] ChaCha20-Poly1305 fragment encryption with per-file keys
- [x] Blake3 fragment hashing and integrity verification
- [x] Local fragment storage with 2-level directory structure
- [x] File reconstruction from available fragments
- [x] Fast-path reconstruction when all original fragments available
- [ ] Distributed fragment placement across network nodes
- [ ] Fragment lifecycle management and garbage collection
- [ ] Storage capacity monitoring and quota management
- [ ] Secure thumbnail generation for encrypted files
- [ ] Preview data extraction while maintaining encryption

### 3. Node Communication System ([RFC-003](specs/node-communication.md))
**Status**: Fragment transfer endpoints complete, NAT traversal pending

HTTP-based inter-node communication with authentication and fragment transfer capabilities.

- [x] HTTP API with RESTful endpoints for consensus operations
- [x] Dual Ed25519 signature authentication (node + user signatures)
- [x] Node registry with IP addresses and public keys
- [x] Request routing (non-leaders forward to current leader)
- [x] Basic RTT latency measurement between nodes
- [x] Node discovery and network membership management
- [x] Fragment transfer protocols and endpoints (GET/POST /fragments/{hash}, health checks)
- [x] Bandwidth monitoring and quality-of-service metrics (latency + throughput measurement complete)
- [ ] Connection pooling and performance optimization
- [ ] Network topology awareness and geographic information
- [ ] NAT traversal implementation (deprioritized for defined IP infrastructure)

### 4. Shard Synchronization System ([RFC-004](specs/shard-synchronization.md))
**Status**: Ready for implementation (fragment transfer infrastructure complete)

Intelligent fragment distribution system optimizing for performance, reliability, and geographic redundancy.

- [~] Consensus height-based versioning for deterministic placement
- [~] Rendezvous hashing algorithm for fragment placement
- [~] Erasure-code aware placement (separate original vs recovery sets)
- [ ] Fragment discovery protocol with parallel queries
- [ ] Performance-optimized original fragment placement
- [ ] Geographic redundancy for recovery fragments
- [ ] Node reliability scoring and roaming device detection
- [ ] Background rebalancing and fragment migration
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

## Current Focus

**Phase 1A Critical Path**: Infrastructure completion to enable distributed operations

**Infrastructure Complete**: Background Metrics Collection
- [x] Extended metrics table with consensus height and availability tracking  
- [x] Created reusable metrics collection infrastructure with timeout handling
- [x] Implemented consensus transaction batching for metrics submissions ("submit_metrics" handler)
- [x] Added manual metrics trigger API endpoint with consensus integration for debugging and testing
- [x] Fixed metrics retrieval API (GET /metrics) with proper timestamp handling for DuckDB compatibility
- [x] Implemented automated background metrics collection worker with randomized 10-minute intervals
- [x] Integrated throughput measurement using existing infrastructure

**Primary**: RFC-004 Shard Synchronization Implementation  
- Implement deterministic fragment placement algorithm using collected node reliability metrics
- Integrate with completed fragment transfer endpoints (GET/POST /fragments/{hash})
- Build fragment discovery and cross-node retrieval workflows

**Secondary**: Upload/Download Workflow Integration
- Integrate fragment transfer with file download (missing fragment retrieval)
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

### Phase 1B: Basic Distributed Operations
**Goal**: Enable core distributed filesystem functionality

1. **RFC-004 fragment placement and discovery** - Smart fragment distribution
   - Implement rendezvous hashing for deterministic placement
   - Add node reliability scoring with basic metrics collection
   - Enable geographic distribution using RTT clustering + IP geolocation
   
2. **Complete UI features for distributed operations** - User-facing distributed functionality  
   - Advanced file operations (multi-select, drag-drop) with distributed backend
   - Network health dashboard showing distributed node status
   - File sharing controls leveraging distributed storage
   
3. **Node performance monitoring** - Foundation for reliability scoring
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