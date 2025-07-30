# RFC-006: Security & Authentication System

## Overview

The HopNet Security & Authentication System provides a comprehensive cryptographic framework for distributed file storage with granular access control. Built on modern cryptographic primitives and zero-knowledge principles, the system ensures that node operators cannot access user files without explicit authorization while maintaining strong authentication and audit capabilities for enterprise deployments.

## Design Philosophy

### Zero-Knowledge Architecture
- **Node Blindness**: Nodes cannot decrypt user files without explicit FileAccess entries
- **Granular Encryption**: Per-file encryption keys with individual user access control
- **Metadata Privacy**: Encrypted file paths and directory structures
- **Minimal Trust**: Users trust only their own private keys, not node operators

### Defense in Depth
- **Multi-Layer Encryption**: File → Chunk → Path encryption with different keys
- **Dual Authentication**: Separate node and user signatures for inter-node operations
- **Access Control**: Multiple validation layers for file and system operations
- **Cryptographic Verification**: All operations cryptographically verified and audible

### Enterprise Readiness
- **Audit Trail**: Complete cryptographic audit trail of all operations
- **Individual Accountability**: Every operation traceable to specific users and nodes
- **Compliance Foundation**: Architecture supporting regulatory compliance requirements
- **Scalable Authentication**: Stateless JWT-based sessions for horizontal scaling

## Cryptographic Architecture

### Core Primitives

#### Primary Algorithms
- **Ed25519**: Digital signatures for node/user authentication and consensus operations
- **X25519**: Elliptic Curve Diffie-Hellman for file encryption key agreement
- **ChaCha20-Poly1305**: Authenticated encryption for file data with streaming support
- **AES-256-SIV**: Deterministic authenticated encryption for file paths and metadata
- **Blake3**: Fast cryptographic hash for key derivation, integrity verification, and deterministic operations

#### Algorithm Selection Rationale
- **Ed25519**: Fast signature generation/verification, excellent security properties, wide library support
- **X25519**: Proven ECDH implementation, deterministic key derivation from Ed25519 keys
- **ChaCha20-Poly1305**: Superior performance on mobile hardware without HW AES, streaming capability for large files
- **AES-SIV**: Nonce-reuse-resistant authenticated encryption ideal for deterministic path encryption
- **Blake3**: Extreme performance, cryptographically secure, excellent for tree-based derivation

### Key Hierarchy and Derivation

#### Master Key Derivation
All cryptographic keys derive from user Ed25519 private keys using Blake3:

```
User Ed25519 Private Key (32 bytes)
├── X25519 Private Key ← Blake3("hopnet x25519_secret", ed25519_private_key)
├── Path SIV Key ← Blake3("hopnet file_path siv_key", ed25519_private_key) 
├── Path SIV Nonce ← Blake3("hopnet file_path siv_nonce", ed25519_private_key)
└── File Wrapping Context ← Blake3("hopnet key_wrap", ed25519_private_key)
```

#### Per-File Key Management
Each file receives unique encryption treatment:

1. **Per-File Key Generation**: Random 32-byte ChaCha20-Poly1305 key per file
2. **Key Wrapping**: File key wrapped using X25519 ECDH with ephemeral keypairs
3. **Access Control**: Individual FileAccess entries for each authorized user
4. **Chunk Encryption**: Per-chunk keys derived from file key and chunk UUID

#### Deterministic Operations
- **Chunk Nonces**: Blake3("hopnet chunk_nonce", chunk_uuid) ensures unique nonces
- **Path Encryption**: Deterministic SIV encryption enables consistent encrypted paths
- **Key Derivation**: Reproducible key generation for consistency across nodes

## Authentication Framework

### Current Single-User Model

#### Node-User Association
- **Development Implementation**: Building on user keypair infrastructure but using application state keypairs as source of truth
- **Simplified Authentication**: Node owner's Ed25519 key from AppState used for all operations rather than JWT user information
- **Multi-User Foundation**: User keypair infrastructure in place, allowing future multi-user support once user key login (e.g., QR code transfer) is implemented
- **Migration Path**: Current approach allows multi-user functionality to be enabled without architectural changes

#### User Authentication Methods
- **Current**: Username/password with Argon2 hashing and JWT sessions for UI access
- **Implementation Detail**: JWT provides UI authentication but AppState keypairs used for cryptographic operations
- **Future Evolution**: User private key-based authentication via QR code transfer will make JWT and AppState keys match
- **Session Management**: Stateless JWT tokens with 1-hour expiration for UI access

### Multi-User Authentication (Future)

#### User Private Key Authentication
- **Desktop**: Private key stored securely in application, used for automatic authentication
- **Server Deployment**: QR code-based challenge-response for mobile key transfer
- **Key Validation**: User public key becomes primary inter-node validation method

#### Authentication Transition Strategy
1. **Phase 1**: Current password system with single-user nodes
2. **Phase 2**: Optional private key authentication alongside passwords
3. **Phase 3**: Private key as primary authentication method
4. **Phase 4**: Multi-user support with individual key management

### Inter-Node Authentication

#### Dual Signature System
All RPC operations require both node and user signatures:

```
Request Headers:
X-Node-ID: {node_id}
X-User-ID: {user_id}
X-Node-Signature: hex(ed25519_sign(node_private_key, request_body))
X-User-Signature: hex(ed25519_sign(user_private_key, request_body))
```

#### Authentication Validation
1. **Signature Verification**: Both signatures validated against public keys in database
2. **Node Ownership**: Verify user owns the node making the request if it's a privileged operation
3. **Request Integrity**: Signatures cover request body preventing tampering
4. **Replay Protection**: Timestamps and nonces prevent replay attacks

## File Encryption and Access Control

### Multi-Layer File Security

#### Layer 1: Per-File Encryption
- **Unique Keys**: Each file encrypted with randomly generated ChaCha20-Poly1305 key
- **Key Generation**: Cryptographically secure random key generation per file upload
- **No Key Reuse**: Fresh keys eliminate cross-file cryptographic relationships

#### Layer 2: Access Control Key Wrapping
- **ECDH Key Agreement**: Ephemeral X25519 keypair generated per user access grant
- **Key Wrapping**: Per-file key encrypted using ECDH-derived wrapping key
- **Individual Access**: Each authorized user receives separately wrapped file key
- **Access Revocation**: Remove FileAccess entry to revoke user access

#### Layer 3: Chunk-Level Encryption
- **Chunk Keys**: Per-chunk keys derived from file key and chunk UUID using Blake3
- **Unique Nonces**: Deterministic but unique nonce per chunk
- **Streaming Support**: ChaCha20-Poly1305 enables efficient streaming decryption

#### Layer 4: Metadata Protection
- **Path Encryption**: File paths encrypted with user-specific AES-SIV keys
- **Deterministic Encryption**: Same path encrypts to same ciphertext for consistency
- **Metadata Privacy**: Directory structures hidden from unauthorized users

### Access Control Model

#### FileAccess Management
```sql
CREATE TABLE file_access_entries (
    data_block_id UUID NOT NULL,
    user_id INTEGER NOT NULL,
    ephemeral_pubkey BLOB NOT NULL,    -- X25519 public key for ECDH
    encrypted_file_key BLOB NOT NULL,  -- ChaCha20-Poly1305 encrypted file key
    PRIMARY KEY (data_block_id, user_id)
);
```

#### Access Control Enforcement
1. **File Access Check**: Verify FileAccess entry exists for user and file
2. **Key Recovery**: Perform ECDH with user's X25519 key to derive wrapping key
3. **File Key Decryption**: Decrypt per-file key using derived wrapping key
4. **Content Decryption**: Use per-file key to derive chunk keys and decrypt content

#### Sharing Model
- **Explicit Sharing**: Files shared by creating FileAccess entries for target users
- **No Global Access**: No master keys or global access mechanisms
- **Granular Control**: Per-file sharing decisions with individual key wrapping

## Enterprise Security Features

### Audit and Compliance

#### Audit Trail Requirements
- **Cryptographic Audit Trail**: All operations signed with Ed25519 keys
- **Individual Accountability**: Every operation traceable to specific user and node
- **Non-Repudiation**: Cryptographic signatures prevent operation denial
- **Complete Logging**: All consensus operations, file access, and administrative actions logged

#### Compliance Framework Considerations

**SOC 2 Compliance**:
- **Security**: Cryptographic access controls and authentication
- **Availability**: Distributed architecture with fault tolerance  
- **Processing Integrity**: Cryptographic verification of all operations
- **Confidentiality**: End-to-end encryption with granular access control
- **Privacy**: User data encrypted and access-controlled

**GDPR Compliance**:
- **Data Minimization**: Only necessary data stored, metadata encrypted
- **Right to Erasure**: File deletion capabilities with cryptographic verification
- **Data Portability**: Users control their private keys and can export data
- **Consent Management**: Explicit file sharing requires user action

**HIPAA Compliance**:
- **Access Controls**: Granular file-level access control
- **Audit Logs**: Comprehensive audit trail of all data access
- **Encryption**: Data encrypted at rest and in transit
- **Authentication**: Strong cryptographic authentication

### Role-Based Access Control (RBAC)

#### Access Control Framework
- **Current Model**: File-level access control with explicit sharing
- **Future Enhancement**: Role-based permissions for system administration
- **Enterprise Integration**: Support for organizational hierarchies and group permissions

#### Administrative Roles
- **Network Administrator**: Node management and network configuration
- **Security Administrator**: User management and access control policies
- **Audit Administrator**: Read-only access to audit logs and security events
- **User**: File operations within granted permissions

### Security Monitoring and Alerting

#### Security Event Detection
- **Failed Authentication Attempts**: Monitor and alert on authentication failures
- **Unusual Access Patterns**: Detect anomalous file access behavior
- **Consensus Anomalies**: Monitor consensus operations for security issues
- **Key Rotation Events**: Track and alert on cryptographic key lifecycle events

#### Monitoring Integration
- **Structured Logging**: JSON-formatted security logs for SIEM integration
- **Metrics Export**: Security metrics compatible with monitoring systems
- **Alert Framework**: Configurable alerting for security events

## Key Management and Rotation

### Key Lifecycle Management

#### Key Generation
- **Cryptographically Secure**: All keys generated using secure random number generators
- **Entropy Sources**: Platform-specific entropy sources for key generation
- **Key Strength**: All keys meet or exceed current cryptographic standards

#### Key Storage
- **Public Keys**: Stored in database for verification and consensus operations
- **Private Keys**: Stored securely on user devices, never transmitted to nodes
- **Application Keys**: JWT signing keys rotated on application restart

#### Key Rotation Framework

**Rotation Triggers**:
- **Scheduled Rotation**: Periodic rotation based on key age and usage
- **Compromise Detection**: Emergency rotation on suspected key compromise
- **Algorithm Migration**: Rotation during cryptographic algorithm upgrades
- **Compliance Requirements**: Rotation to meet regulatory compliance schedules

**Rotation Process**: Detailed in consensus maintenance RFC
- **Gradual Migration**: Support for old and new keys during transition periods
- **Consensus Coordination**: Key rotation coordinated through consensus operations
- **Backward Compatibility**: Ensure ongoing operations during rotation

### Hardware Security Integration (Future)

#### Hardware Security Module (HSM) Support
- **Enterprise Deployment**: Integration with HSMs for high-security environments
- **Key Protection**: Hardware-based private key protection
- **Compliance**: HSM support for regulatory compliance requirements

#### Secure Enclave Integration
- **Mobile Devices**: Integration with device secure enclaves for key storage
- **Desktop**: Support for TPM and secure enclave technologies
- **Cloud**: Integration with cloud HSM services for managed deployments

## Security Boundaries and Threat Model

### Trust Boundaries

#### Trusted Components
- **User Private Keys**: Users responsible for private key security
- **Application Code**: HopNet application trusted for correct cryptographic implementation
- **Consensus Network**: Majority of validators assumed honest for consensus operations

#### Untrusted Components
- **Node Operators**: Cannot access user files without explicit FileAccess entries
- **Network Communication**: All network traffic assumed monitored and potentially hostile
- **Storage Media**: All stored data encrypted, storage media assumed potentially compromised

### Threat Mitigation

#### Node Compromise Protection
- **Zero-Knowledge Design**: Compromised nodes cannot decrypt user files
- **Key Isolation**: Per-file keys prevent cross-file access from single key compromise
- **Access Control**: FileAccess entries limit blast radius of compromised access

#### Network Attack Protection
- **Cryptographic Authentication**: All operations cryptographically signed
- **Replay Attack Prevention**: Nonces and timestamps prevent message replay
- **Man-in-the-Middle Protection**: Ed25519 signatures detect message tampering

#### User Key Compromise
- **Limited Scope**: Compromised user key only affects that user's accessible files
- **Access Revocation**: FileAccess entries can be revoked to limit ongoing access
- **Key Rotation**: Emergency key rotation procedures for compromise response

## Future Security Enhancements

### Zero-Knowledge Proof Integration

#### Potential Applications
- **Access Control Verification**: Prove file access rights without revealing file content
- **Consensus Participation**: Prove validator eligibility without revealing identity
- **Search Capabilities**: Search encrypted content without decryption
- **Compliance Proofs**: Prove compliance without revealing sensitive data

#### Implementation Considerations
- **Performance Impact**: ZK proofs computationally expensive, requiring careful optimization
- **Complexity**: Significant implementation complexity for marginal privacy gains
- **Standardization**: Wait for ZK proof standardization and mature libraries

### Post-Quantum Cryptography

#### Migration Planning
- **Algorithm Monitoring**: Track NIST post-quantum cryptography standardization
- **Hybrid Approach**: Support both classical and post-quantum algorithms during transition
- **Performance Analysis**: Evaluate post-quantum algorithm performance impact

#### Implementation Strategy
- **Gradual Migration**: Phase in post-quantum algorithms alongside existing ones
- **Consensus Integration**: Post-quantum signatures for consensus operations
- **Key Agreement**: Post-quantum key agreement for file encryption

### Advanced Enterprise Features

#### Enterprise Identity Integration
- **SAML/LDAP Integration**: Integration with enterprise identity providers
- **OAuth2/OpenID Connect**: Modern authentication protocol support
- **Multi-Factor Authentication**: Enhanced authentication security
- **Single Sign-On**: Seamless integration with enterprise authentication systems

#### Advanced Audit Capabilities
- **Behavioral Analytics**: Machine learning-based anomaly detection
- **Forensic Analysis**: Detailed forensic capabilities for security investigations
- **Compliance Reporting**: Automated compliance report generation
- **Real-time Monitoring**: Real-time security event monitoring and alerting

## Implementation Priorities

### Phase 1: Enterprise Foundation [~]
- [ ] Implement comprehensive audit logging system
- [ ] Add role-based access control framework
- [ ] Create security event monitoring and alerting
- [ ] Develop key rotation trigger conditions and emergency procedures

### Phase 2: Authentication Evolution [ ]
- [ ] Design and implement user private key authentication system
- [ ] Create secure key transfer mechanisms (QR codes, secure channels)
- [ ] Build multi-user authentication support
- [ ] Integrate with consensus maintenance framework for key rotation

### Phase 3: Advanced Security [ ]
- [ ] Implement hardware security module integration
- [ ] Add advanced threat detection and response capabilities
- [ ] Create enterprise identity provider integration
- [ ] Develop post-quantum cryptography migration plan

### Phase 4: Future Enhancements [ ]
- [ ] Research and implement zero-knowledge proof applications
- [ ] Build advanced compliance and forensic capabilities
- [ ] Create behavioral analytics and anomaly detection
- [ ] Implement next-generation cryptographic protocols