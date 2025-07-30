# RFC-003: Node Communication System

## Overview

The HopNet Node Communication System provides secure, authenticated inter-node communication for distributed consensus operations, file fragment transfer, and network coordination. The system prioritizes security and auditability through cryptographic authentication while maintaining performance suitable for small-to-medium networks.

## Design Philosophy

### Security-First Architecture
- **Cryptographic Authentication**: Ed25519-based dual signatures for request integrity and non-repudiation
- **Transport Security**: TLS encryption for all inter-node communication
- **Individual Accountability**: Maintain audit trails of all node actions for regulatory compliance
- **Defense in Depth**: Multiple layers of authentication and validation

### Performance Considerations
- **Ed25519 Over BLS**: Prioritize individual operation speed and accountability over aggregation benefits
- **Connection Efficiency**: Persistent connections and connection pooling for frequent operations
- **Minimal Overhead**: Efficient serialization and batch operations where possible

### Auditability Requirements
- **Individual Node Tracking**: All operations must be traceable to specific nodes
- **Signature Preservation**: Maintain cryptographic proof of node actions
- **Regulatory Compliance**: Support audit requirements and compliance frameworks

## Core Authentication Framework

### Dual Signature System
All inter-node RPC operations require dual Ed25519 signatures:

#### Node-Level Authentication
- **Node Identity**: Each node maintains an Ed25519 keypair for identity
- **Node Signature**: Signs request body with node private key
- **Node Registry**: Public keys stored in consensus-managed node registry

#### User-Level Authentication  
- **Current Implementation**: Single-user nodes using AppState keypairs as source of truth for all cryptographic operations
- **Implementation Detail**: User authentication via JWT for UI access, but inter-node operations use AppState user keys rather than JWT-derived keys
- **Multi-User Foundation**: User keypair infrastructure built but not yet activated pending user key login implementation (e.g., QR code transfer)
- **Future Evolution**: Multi-user support when user private key authentication replaces AppState-based approach
- **User Validation**: User public key verification against consensus user registry

#### Request Authentication Headers
```
X-Node-ID: {node_id}
X-User-ID: {user_id}
X-Node-Signature: {hex_encoded_ed25519_signature}
X-User-Signature: {hex_encoded_ed25519_signature}
```

### Transport Layer Security

#### TLS Implementation
- **Requirement**: All inter-node communication must use TLS encryption
- **Certificate Management**: TLS certificate distribution and validation strategy to be determined during implementation
- **Cipher Suites**: Modern TLS 1.3 cipher suites with forward secrecy
- **Certificate Validation**: Integration with node identity verification system

#### Security Benefits
- **Transport Encryption**: Protect data in transit between nodes
- **Forward Secrecy**: Ephemeral key exchange prevents retroactive decryption
- **Standard Implementation**: Leverage battle-tested TLS libraries
- **Performance**: Hardware-accelerated encryption on modern systems

## Network Discovery and Topology

### Node Discovery Mechanisms

#### Automatic Discovery
- **Local Networks**: Automatic discovery for same-subnet nodes
- **NAT Traversal**: Support for nodes behind NAT/firewall (implementation method TBD)
- **Service Discovery**: Integration with network service discovery protocols

#### Manual Configuration
- **Hardcoded Endpoints**: Support for manual IP/port configuration
- **VPN Compatibility**: Allow manual configuration for VPN-based deployments
- **Enterprise Networks**: Support for complex network topologies

#### Fallback Strategy in NAT Operation Mode
1. **Automatic Discovery**: Attempt automatic local network discovery
2. **Manual Configuration**: Fall back to user-configured endpoints
3. **Last Known Good**: Use previously successful connection information

### Network Topology Management

#### Node Registry
- **Consensus-Managed**: Node list maintained through consensus operations
- **Cryptographic Validation**: Public key verification for node identity claims
- **Health Tracking**: Node availability and performance metrics
- **Dynamic Updates**: Real-time updates to node availability

#### Connection Management
- **Connection Pooling**: Maintain persistent connections to frequently contacted nodes if deemed performance-enhancing
- **Health Monitoring**: Regular health checks and connection validation
- **Graceful Degradation**: Handle partial network connectivity

## Fragment Transfer Protocols

### Protocol Requirements

#### Bulk Transfer Support
- **Batch Operations**: Transfer multiple fragments in single operation enabling efficient fragment redistribution during network changes
- **Efficiency**: Ensure serialized content is efficiently represented to avoid overhead in large transfers
- **Progress Tracking**: Transfer progress indication for large operations

#### Streaming Protocol
- **Progressive Download**: Stream large files as fragments become available
- **Ordered Delivery**: Maintain fragment order for streaming reconstruction
- **Parallel Streams**: Concurrent fragment retrieval from multiple nodes

#### Transfer Validation
- **Fragment Integrity**: Blake3 hash verification for transferred fragments
- **Authentication**: All transfers authenticated with dual signature system
- **Retry Logic**: Automatic retry for failed transfers with exponential backoff
- **Deduplication**: Avoid redundant transfers of already-present fragments

### Integration with File Operations

#### User-Facing File Transfers
- **Resumable Downloads**: Support resumable file downloads for large files
- **Upload Progress**: Granular progress tracking for file uploads
- **Error Recovery**: Graceful handling of interrupted file operations

#### Fragment-Level Operations
- **Small Fragment Size**: Fragment transfers typically don't require resumability
- **Atomic Operations**: Fragment transfers complete atomically or fail
- **Efficient Protocols**: Optimized for small, frequent transfers

## API Architecture and Endpoints

### Endpoint Categories

#### Consensus Operations
- **Transaction Submission**: `/consensus/propose` for transaction forwarding
- **State Synchronization**: `/consensus/view/{view}` for catch-up operations
- **Quorum Certificates**: `/qc` for consensus certificate exchange
- **Timeout Certificates**: `/consensus/tc` for view progression

#### Fragment Management
- **Fragment Query**: Endpoints for fragment discovery and availability
- **Fragment Transfer**: Bulk and streaming fragment transfer endpoints
- **Fragment Validation**: Fragment integrity verification endpoints

#### Node Management
- **Node Registration**: `/nodes` for adding new nodes to network
- **Health Monitoring**: Node health and metrics collection endpoints
- **Network Topology**: Node relationship and connectivity information

#### User Operations
- **Authentication**: User session management and validation
- **File Operations**: User-initiated file upload/download/management
- **Permission Management**: User access control and sharing operations

#### System Operations
- **Maintenance Tasks**:  Start and monitor distributed maintenance tasks like orphaned fragment cleanup
- **Overall Statistics**: System-level metrics

### Request/Response Patterns

#### Standard Request Format
- **Authentication Headers**: Dual signature authentication
- **Request Body**: JSON-serialized operation data
- **Content Validation**: Request body signature verification

#### Error Handling
- **Standard Error Codes**: HTTP status codes with detailed error information
- **Retry Instructions**: Clear guidance on retryable vs permanent failures
- **Debugging Information**: Sufficient detail for troubleshooting without exposing sensitive data

#### Performance Optimization
- **Batch Operations**: Group related operations for efficiency
- **Compression**: Request/response compression for large payloads
- **Caching**: Appropriate caching headers for cacheable responses

## Security Requirements

### Authentication Security

#### Signature Validation
- **Body-Dependent Signatures**: Signatures cover request body to prevent tampering
- **Replay Attack Prevention**: Timestamp or nonce-based replay protection
- **Key Validation**: Public key verification against consensus registry
- **Signature Algorithm**: Ed25519 signature verification

#### Key Management
- **Node Key Security**: Secure storage and handling of node private keys
- **User Key Integration**: Integration with user key management system
- **Key Rotation**: Future support for periodic key rotation (to be detailed in consensus maintenance RFC)

### Network Security

#### Transport Protection
- **Mandatory TLS**: All communication encrypted with TLS
- **Certificate Validation**: Proper certificate chain validation
- **Cipher Suite Restrictions**: Only secure, modern cipher suites allowed
- **Perfect Forward Secrecy**: Ephemeral key exchange required

#### Access Control
- **Node Authorization**: Verify node is authorized for requested operations
- **User Permissions**: Validate user has permission for requested actions
- **Resource Limits**: Rate limiting and resource consumption controls
- **Network Isolation**: Support for network segmentation and access controls

## Performance and Scalability

### Current Scale Targets
- **Network Size**: Optimized for small-to-medium networks (<100 nodes)
- **Consensus Performance**: Ed25519 signature verification suitable for validator-scale networks
- **Connection Management**: Efficient connection pooling and reuse
- **Memory Usage**: Reasonable memory footprint for node operations

### Performance Monitoring
- **Latency Metrics**: Round-trip time measurement between nodes
- **Throughput Monitoring**: Transfer rate tracking for fragment operations
- **Error Rate Tracking**: Network error and retry statistics
- **Resource Utilization**: CPU, memory, and bandwidth usage monitoring

### Future Scalability Considerations
- **BLS Signatures**: Consider BLS signature aggregation for very large networks
- **Hierarchical Consensus**: Support for non-participating storage-only nodes
- **Regional Optimization**: Geographic optimization for distributed deployments

## Error Handling and Resilience

### Network Resilience

#### Connection Failures
- **Automatic Retry**: Exponential backoff retry for transient failures
- **Connection Pooling**: Maintain warm connections to active nodes
- **Failover Logic**: Automatic failover to alternative nodes when available
- **Graceful Degradation**: Partial functionality during network partitions

#### Consensus Resilience
- **Catch-Up Mechanisms**: Automatic synchronization for nodes behind current view
- **View Progression**: Timeout certificates for stalled consensus rounds
- **Leader Forwarding**: Automatic forwarding of transactions to current leader
- **State Validation**: Cryptographic verification of synchronized state

### Data Integrity

#### Transfer Validation
- **Hash Verification**: Blake3 hash validation for all transferred data
- **Signature Verification**: Cryptographic validation of all received data
- **Corruption Detection**: Automatic detection of corrupted transfers
- **Recovery Procedures**: Clear procedures for handling data corruption

#### Consensus Integrity
- **Signature Validation**: All consensus messages cryptographically verified
- **State Consistency**: Automatic detection of state inconsistencies
- **Fork Prevention**: Mechanisms to prevent and detect consensus forks
- **Audit Trail**: Complete audit trail of all consensus operations

## Future Considerations

### Authentication Evolution
- **Multi-User Support**: Full multi-user authentication with individual keys
- **Mobile Integration**: QR code-based key transfer for mobile clients
- **Zero-Knowledge Proofs**: Enhanced privacy through zero-knowledge authentication
- **Hardware Security**: Integration with hardware security modules

### Scalability Enhancements
- **BLS Signature Migration**: Migration path to BLS signatures for large networks
- **Hierarchical Networks**: Support for storage-only nodes and hierarchical consensus
- **Geographic Distribution**: Optimization for geographically distributed networks
- **CDN Integration**: Content delivery network integration for fragment distribution

### Security Hardening
- **Key Rotation**: Automated key rotation as part of consensus maintenance
- **Certificate Pinning**: TLS certificate pinning for enhanced security
- **Network Segmentation**: Advanced network isolation and access controls
- **Compliance Integration**: Enhanced audit logging and compliance reporting

## Implementation Priorities

### Phase 1A: Fragment Transfer Protocol (Critical Blocker) [ ]
- [ ] Research and select fragment transfer HTTP endpoint architecture (early implementation priority)
- [ ] Design bulk fragment transfer protocol for efficient multi-fragment operations
- [ ] Implement streaming fragment delivery for large file reconstruction
- [ ] Add fragment discovery and availability query endpoints
- [ ] Integrate fragment transfer validation with Blake3 hash verification
- [ ] Enable progress tracking for bulk transfer operations

### Phase 1B: Network Foundation [~]
- [ ] Complete TLS transport layer with certificate management strategy
- [ ] Enhance dual signature authentication system for production use
- [ ] Implement comprehensive performance metrics collection for node reliability scoring
- [ ] Add basic connection pooling and error handling

### Phase 2: Performance and Reliability [ ]
- [ ] Optimize request/response serialization and compression
- [ ] Add detailed error tracking and monitoring dashboard
- [ ] Implement advanced connection management and retry logic
- [ ] Add network topology awareness and RTT measurement

### Phase 3: Network Expansion [ ]
- [ ] Research and implement NAT traversal approach (deprioritized for defined IP infrastructure)
- [ ] Complete network discovery with automatic local subnet discovery
- [ ] Add enterprise security features (audit logging, compliance)
- [ ] Design consensus maintenance framework for key rotation

### Phase 4: Advanced Features [ ]
- [ ] Implement multi-user authentication when user key login available
- [ ] Add geographic distribution optimization
- [ ] Investigate BLS signature migration path for large-scale deployments
- [ ] Add CDN integration for fragment distribution