# RFC-008: S3-Compatible API

## Abstract

This RFC specifies an S3-compatible API layer for HopNet that enables standard S3 clients and SDKs to interact with the distributed filesystem. The design provides two operational modes: a secure proxy mode where decryption is handled by a user-owned HopNet node (which contains the user keys necessary for the decryption), and a standalone mode for environments where running a local HopNet client is not feasible.

## Motivation

### Why S3 Compatibility?

1. **Ecosystem Integration**: Thousands of tools support S3 (backup software, CI/CD, data pipelines)
2. **Enterprise Adoption**: S3 is the de facto object storage standard
3. **Developer Experience**: Familiar APIs reduce learning curve
4. **Migration Path**: Easy transition from cloud storage to HopNet

### Design Goals

- Maintain HopNet's encryption-by-default security model
- Support both high-security and high-convenience use cases
- Enable bucket-level access control and sharing
- Preserve the native filesystem's flexibility

## Architecture

### Virtual Bucket Layer

S3 buckets are virtual mappings to encrypted paths in a user's HopNet namespace:

```
S3 Operation: PUT s3://my-bucket/file.txt
Maps to: User namespace /actual/encrypted/path/file.txt
```

Key properties:
- Bucket names are globally unique identifiers, not derived from folder names
- Any path in the user's namespace can be exposed as an S3 bucket
- Bucket creation requires explicit user action (not automatic from folders)
- Multiple buckets can map to the same or overlapping paths

### Database Schema

```sql
-- Virtual S3 bucket registry (consensus-synced)
CREATE TABLE s3_buckets (
    bucket_id        UINTEGER PRIMARY KEY,
    bucket_name      VARCHAR(63) UNIQUE NOT NULL,  -- S3-compliant name
    owner_id         INTEGER NOT NULL REFERENCES users(user_id),
    encrypted_path   BLOB NOT NULL,                -- AES-SIV encrypted actual path
    
    INDEX idx_bucket_name (bucket_name),
    INDEX idx_owner (owner_id)
);

-- Access control considerations need to be made following shared folder architecture.
```

## Dual-Mode Credentials

### Mode 1: Proxy Mode (Default, Secure)

- **Access Key Format**: `AKIAP{identifier}` (P = Proxy)
- **Secret Key**: Contains only authentication material
- **Operation**: Local proxy handles encryption/decryption
- **Security**: Private key never leaves user's device

```
[S3 Client] -> [Local Proxy :9000] -> [HopNet Nodes]
                    ↑
            Has user's private key
            Performs decryption locally
```

### Mode 2: Standalone Mode (Portable)

- **Access Key Format**: `AKIAS{identifier}` (S = Standalone)
- **Secret Key**: Contains authentication + decryption keys (sensitive!)
- **Operation**: Remote server handles encryption/decryption
- **Security**: Equivalent to sharing private key for S3-accessible paths

```
[S3 Client] -> [Remote HopNet S3 Endpoint]
                    ↑
            Uses embedded keys from secret
            Performs decryption on server
```

## Credential Generation

Both credential types are deterministically derived from the user's Ed25519 private key:

```rust
// Proxy mode - auth only
access_key = "AKIAP" + blake3_derive(private_key, "access-id")[0:16]
secret_key = blake3_derive(private_key, "auth-secret")

// Standalone mode - auth + decryption
access_key = "AKIAS" + blake3_derive(private_key, "access-id")[0:16]
secret_key = blake3_derive(private_key, "auth-secret") + "." +
             path_encryption_key
```

Note that the path encryption key will need to be generated for the corresponding S3 key type, and this is part of our overarching file and folder sharing architecture implementation.

## S3 API Mapping

### Phase 1: Core Operations

- [ ] ListBuckets
- [ ] CreateBucket
- [ ] DeleteBucket
- [ ] GetObject
- [ ] PutObject
- [ ] DeleteObject
- [ ] HeadObject

### Phase 2: Extended Features

- Multipart uploads with session management
- Pre-signed URLs for temporary access
- Object versioning (requires consensus integration)
- Bucket policies and ACLs

## Security Model

### Encryption Guarantees

1. **Path Privacy**: Bucket names don't reveal actual paths (AES-SIV encrypted)
2. **File Encryption**: All files remain ChaCha20-Poly1305 encrypted at rest
3. **Key Derivation**: S3 credentials derived from user's master key
4. **No Consensus Secrets**: Only public mappings in consensus, never keys

### Access Control

- **Bucket Level**: Per-key bucket permissioning
- **Operation Level**: Each S3 operation validates permissions
- **User Isolation**: S3 operations scoped to authenticated user's namespace

## Implementation Phases

### Phase 1: Basic S3 Compatibility (2 weeks)
- [ ] Core S3 operations (GET, PUT, DELETE)
- [ ] Bucket management APIs
- [ ] AWS Signature v4 authentication
- [ ] XML response formatting
- [ ] Local proxy implementation

### Phase 2: Credential Management (1 week)
- [ ] Dual-mode credential generation
- [ ] Access key mapping table
- [ ] CLI commands for credential management
- [ ] Security warnings and documentation

### Phase 3: Extended Features (2-3 weeks)
- [ ] Multipart upload support
- [ ] Pre-signed URLs
- [ ] Bucket sharing and permissions
- [ ] Object metadata and headers
- [ ] Range requests

### Phase 4: Production Readiness (1-2 weeks)
- [ ] Performance optimization
- [ ] Comprehensive testing
- [ ] S3 compliance validation
- [ ] Documentation and examples

## Usage Examples

### Creating a Bucket

```bash
# Create a bucket mapping to a HopNet path
hopnet-cli s3 create-bucket \
  --path /projects/data \
  --name "alice-project-data"
```

### Generating Credentials

```bash
# Secure proxy mode (default)
hopnet-cli s3 generate-credentials
# Output:
# Access Key: AKIAP1234567890ABCDEF
# Secret Key: abcdef123456...
# Endpoint: http://localhost:9000

# Standalone mode (with warnings)
hopnet-cli s3 generate-credentials --standalone
# ⚠️  WARNING: These credentials contain decryption keys!
# Access Key: AKIAS1234567890ABCDEF  
# Secret Key: auth.decrypt.path.combined...
# Endpoint: https://s3.hopnet.network
```

### Using with AWS CLI

```bash
# Configure AWS CLI
aws configure --profile hopnet
AWS Access Key ID: AKIAP1234567890ABCDEF
AWS Secret Access Key: abcdef123456...
Default region: us-east-1
Default output: json

# Use with endpoint override
aws s3 ls --profile hopnet --endpoint-url http://localhost:9000
aws s3 cp file.txt s3://alice-project-data/ --profile hopnet --endpoint-url http://localhost:9000
```

## Open Questions

1. **Bucket Naming**: Should we enforce S3's DNS-compliant naming rules or be more permissive?
2. **Region Handling**: Should we support virtual regions for geographic optimization?
3. **Versioning**: How deep should version support go given consensus implications?
4. **Public Buckets**: Should we support truly public buckets or require HopNet authentication?

## Security Considerations

1. **Credential Storage**: Users must understand standalone credentials are as sensitive as private keys
2. **Proxy Availability**: Local proxy must be running for proxy-mode credentials to work
3. **Audit Logging**: S3 operations should be logged for security monitoring
4. **Rate Limiting**: Prevent brute force attacks on S3 authentication

## Compatibility Notes

- Initial implementation targets basic S3 operations compatible with AWS CLI and major SDKs
- Full S3 API compatibility is not a goal (e.g., complex bucket policies, lambda triggers)
- Focus on storage operations, not AWS-specific features

## Conclusion

This S3-compatible API provides HopNet with enterprise-standard object storage interfaces while maintaining its security-first architecture. The dual-mode credential system balances security with practical deployment needs, enabling adoption across diverse environments from personal devices to cloud infrastructure.