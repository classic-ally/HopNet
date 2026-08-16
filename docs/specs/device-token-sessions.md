# RFC-012: Device Token Session Bootstrap

**Amended by**: RFC-022 (client compatibility — device-token requests
carry the client version; per-surface health probes, 2026-08-16)

## Abstract

This RFC specifies a mechanism for device tokens to carry wrapped user key material, enabling OS integration endpoints (FileProvider, DocumentProvider) to establish authenticated sessions without requiring a prior user login. By encrypting the user's key material with a key derived from the device secret at registration time, and storing the resulting ciphertext alongside the device token in the consensus-replicated `device_tokens` table, we couple the session lifecycle to the device token lifecycle. This eliminates the current dependency on ephemeral in-memory sessions for OS integration surfaces, and replaces the FileProvider's ad-hoc API key authentication with the existing device token infrastructure.

## Motivation

### The Session Gap

After the multi-user migration, all cryptographic operations (path encryption/decryption, transaction signing) require a `SessionEntry` containing the user's SIV key, SIV nonce, and Ed25519 private key. Sessions are created when a user logs in with their passphrase (Argon2id unwrap, 1 GiB memory) and expire after 1-24 hours.

OS integration surfaces (FileProvider, DocumentProvider) cannot trigger a user login. They run as background processes managed by the operating system and may be invoked at any time — Finder browsing, Spotlight indexing, Android content resolver queries. If the user's session has expired, all operations fail silently.

**Current workarounds:**
- FileProvider: Raw private key stored in macOS Keychain, loaded at startup into a permanent session (`expires_at: now + 876000 hours`). Owner-only, GUI+release builds only.
- DocumentProvider: Depends on the user having an active session from a recent web UI login. Breaks silently when sessions expire.
- `sign_out()` has a `#[cfg(feature = "gui")]` guard that refuses to remove the owner's session because FileProvider depends on it.

### The FileProvider Authentication Gap

FileProvider currently uses a static API key generated at startup, validated by `fileprovider_auth_middleware`. This key carries no user identity — all FileProvider routes call `app_state.get_user_id()` which returns the node owner's ID from a `OnceCell`. This is incompatible with the multi-user model where `get_session(user_id)` requires a per-user session, and where different users have different SIV keys for path encryption.

Meanwhile, DocumentProvider already uses `device_token_auth_middleware` with consensus-replicated device tokens. The infrastructure exists — FileProvider just doesn't use it.

### Device classes using this system

- **FileProvider / DocumentProvider** — the original consumers, via the drive projection's `AuthClass::DeviceToken` mounts (`/api/integrations/*`).
- **Photo-ingress daemon** (RFC-011 adapter) — registers as a device ("Photo Ingress") and authenticates against the photos thin-client surface, host-mounted at `/api/photos/client/*` under `device_token_auth_middleware`. The bootstrapped session is what signs its `photo_add` transactions and derives `uploaded_by`; because the token works against any node holding the consensus state, the daemon is not tied to a co-resident node, and revoking its device row revokes it mesh-wide.

### Design Goals

1. **Session independence**: Device tokens self-bootstrap sessions without requiring prior user login
2. **Lifecycle coupling**: Revoking a device token immediately revokes its ability to create sessions
3. **No weakened security**: The wrapped key path must not reduce the attack cost below the passphrase path
4. **Unified abstraction**: `get_session()` remains the single session interface for all callers
5. **Testable**: Orchestrator tests verify end-to-end decryptability via device token sessions

## Security Analysis

### Existing Passphrase Wrapping

The current system wraps the user's Ed25519 private key with:

```
passphrase (8 words × log2(7776) ≈ 103.4 bits entropy, system-generated)
    → Argon2id (m=1 GiB, t=2, p=1) → 32-byte wrapping key
    → ChaCha20-Poly1305(wrapping_key, privkey) → encrypted_privkey
```

An attacker with database access (`encrypted_privkey` + `key_salt`) must brute-force ~103 bits of passphrase entropy through a 1 GiB Argon2id barrier. This is the baseline we must not weaken.

### Device Token Wrapping

The proposed device token wrapping uses:

```
device_secret (32 random bytes = 256 bits entropy, machine-generated)
    → HKDF-Blake3(device_secret, context) → 32-byte wrapping key
    → ChaCha20-Poly1305(wrapping_key, privkey) → wrapped_user_key
```

An attacker with database access (`wrapped_user_key` + `api_key_hash`) must:
- Invert `blake3(device_secret)` to recover the device secret (256-bit preimage), OR
- Brute-force the 256-bit wrapping key directly

No key stretching (Argon2id) is needed because the device secret already has 256 bits of entropy — far beyond the ~103 bits of the passphrase. The device token path is strictly harder to attack from the ciphertext side.

### Threat Surface Comparison

| Vector | Passphrase Path | Device Token Path |
|--------|----------------|-------------------|
| **Ciphertext + DB access** | ~103 bits + Argon2id (1 GiB) | 256 bits + HKDF (fast, but irrelevant at 256 bits) |
| **Credential interception** | Passphrase sent once at login | Device secret sent on every request |
| **Credential storage** | User's memory only | Device keychain/keystore |
| **Revocation** | Change passphrase (re-wraps key) | Delete device token (removes ciphertext from all nodes) |
| **Transport security** | Pinned HTTPS or loopback (RFC-022) | Pinned HTTPS or loopback (RFC-022) |

The device secret is transmitted more frequently than the passphrase. Both travel over the node's TLS-only network surface ([pinned-https](pinned-https.md)) when the caller is remote (Hop Drive on the LAN), or over loopback plaintext when co-resident (FileProvider, photo-ingress, hopnet-mount). The device secret is stored on the device (macOS Keychain, Android Keystore), while the passphrase exists only in the user's memory. These are complementary threat surfaces — compromising one does not help with the other.

### Key Revocation Properties

When a device token is revoked via consensus:
1. The `device_tokens` row (including `wrapped_user_key`) is deleted from all nodes
2. The device still holds `{device_id}.{secret}`, but there is no ciphertext to unwrap
3. The blake3 hash is gone, so authentication fails before unwrapping is attempted
4. Any session bootstrapped from that token expires naturally (short TTL)

This is stronger than passphrase revocation, which requires the user to change their passphrase and for the old `encrypted_privkey` to be overwritten.

## Schema Changes

### `device_tokens` Table

The `wrapped_user_key` column is added directly to the table definition as a required (`NOT NULL`) column:

```sql
CREATE TABLE device_tokens (
    id                      TEXT PRIMARY KEY,
    user_id                 INTEGER NOT NULL,
    api_key_hash            BLOB NOT NULL,
    encrypted_device_name   TEXT NOT NULL,
    wrapped_user_key        BLOB NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);
CREATE INDEX idx_device_tokens_user_id ON device_tokens(user_id);
```

### Wrapped Blob Format

The `wrapped_user_key` blob contains only the Ed25519 private key. The SIV key and nonce are deterministically re-derived from the private key via `derive_siv_key_from_user` on unwrap, avoiding redundant storage.

```
HKDF-Blake3(device_secret, "hopnet-device-key-wrap-v1") → wrapping_key (32 bytes)

Plaintext (32 bytes):
    ed25519_private_key  (32 bytes)

ChaCha20-Poly1305(wrapping_key, random_nonce, plaintext) → nonce (12 bytes) || ciphertext (48 bytes)

Total wrapped_user_key: 60 bytes
```

The version string in the HKDF context (`v1`) enables future changes to the wrapping format without ambiguity.

## Registration Flow

### Device Registration

When a user registers a device via `POST /devices/register` (JWT-authenticated, active session required):

```
1. Generate device_id (UUIDv7)
2. Generate device_secret (32 random bytes)
3. Compute api_key_hash = blake3(hex(device_secret))
4. Encrypt device_name with user's SIV key
5. Derive wrapping_key = HKDF-Blake3(device_secret, "hopnet-device-key-wrap-v1")
6. Wrap private key:
     plaintext = session.user_keys.private_key
     wrapped_user_key = nonce || ChaCha20-Poly1305(wrapping_key, plaintext)
7. Build RegisterDevicePayload { id, user_id, api_key_hash, encrypted_device_name, wrapped_user_key }
8. Submit to consensus
9. Return { device_id, api_key: "{device_id}.{hex(device_secret)}" }
```

The wrapping happens on the registering node where the user has an active session. The `wrapped_user_key` blob is then replicated to all nodes via the consensus transaction.

## Authentication Flow

### Device Token Middleware

`device_token_auth_middleware` authenticates the device and bootstraps a session:

```
1. Parse Bearer token → (device_id, device_secret)
2. DB lookup: SELECT id, user_id, api_key_hash, wrapped_user_key FROM device_tokens WHERE id = ?
3. Verify: blake3(device_secret) == api_key_hash
4. Derive wrapping_key = HKDF-Blake3(device_secret, "hopnet-device-key-wrap-v1")
5. Unwrap: decrypt ChaCha20-Poly1305(wrapping_key, wrapped_user_key) → privkey
6. Re-derive: (siv_key, siv_nonce) = derive_siv_key_from_user(privkey, "file_path")
7. Check session store for user_id:
   - If session exists and not expired → skip (avoid write lock contention)
   - If session missing or expired → upsert SessionEntry with short TTL (5 minutes)
8. Insert user_id into request extensions (existing behavior)
9. Proceed to handler
```

The 5-minute TTL means the session is refreshed on active use but cleaned up quickly when the device stops making requests. This avoids accumulating stale sessions while ensuring `get_session()` always succeeds for authenticated device token requests.

## FileProvider Migration

### Phase 1: Switch to Device Token Auth

Replace `fileprovider_auth_middleware` with `device_token_auth_middleware` on the FileProvider route group. Update all FileProvider route handlers to extract `user_id` from `Extension<i32>` instead of calling `app_state.get_user_id()`.

**Before:**
```rust
pub async fn get_enumerate(
    State(app_state): State<AppState>,
    Query(query): Query<EnumerateQuery>,
) -> Result<Json<EnumerateResponse>, StatusCode> {
    let user_id = app_state.get_user_id()?;
    let session = app_state.get_session(user_id).await?;
```

**After:**
```rust
pub async fn get_enumerate(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
    Query(query): Query<EnumerateQuery>,
) -> Result<Json<EnumerateResponse>, StatusCode> {
    let session = app_state.get_session(user_id).await?;
```

All 7 FileProvider routes (`get_enumerate`, `get_changes`, `delete_item`, `download_file`, `get_item`, `create_item`, `modify_item`) receive this change.

### Phase 2: Swift Extension Registration Flow

Replace the static API key mechanism with device token registration:

1. **On first setup** (after user completes initial setup or login):
   - Main app calls `POST /devices/register` with device name (e.g., "MacBook Finder")
   - Stores returned `{device_id}.{secret}` in macOS Keychain (same keychain service, replacing the API key)
   - Drops `owner_privkey` and `owner_user_id` keychain entries (no longer needed)

2. **Swift extension init**:
   - Loads device token from Keychain (same `loadFromKeychain()` path, different value format)
   - Sends as `Authorization: Bearer {device_id}.{secret}` (same HTTP header format)
   - No code change needed in the extension's HTTP layer

3. **Device management UI**:
   - FileProvider device appears in the device list alongside Android devices
   - User can revoke FileProvider access from any node's web UI
   - Revocation propagates via consensus to all nodes

### Phase 3: Cleanup

Remove the following infrastructure made obsolete by this change:

- `fileprovider_auth_middleware` in `src/fileprovider/auth.rs`
- `generate_fileprovider_api_key()` in `src/auth.rs`
- `fileprovider_api_key` field from `AppState`
- `store_session_key` / `load_session_key` from `src/fileprovider/keychain.rs`
- The `#[cfg(feature = "gui")]` session protection guard in `sign_out()`
- The keychain auto-login block in `main.rs` (lines 199-218)
- The `owner_privkey` and `owner_user_id` keychain entries

The `FileProviderConfig` keychain entries (`api_key`, `base_url`) are retained but `api_key` now stores a device token instead of a static key.

## Orchestrator Test Specification

### Test: `device-token-session-bootstrap`

This test verifies that a device token can establish a session and perform file operations on a node where the user has never logged in. This is the critical property that distinguishes device token sessions from the current system.

```
Setup:
  - 3-node mesh, user created on node 0

Step 1: Register device on node 0 (JWT-authenticated)
  POST /devices/register { device_name: "test-device" }
  → Receive { device_id, api_key }
  Assert: api_key matches format {uuid}.{hex64}

Step 2: Poll until device token propagated to all nodes
  For each node: GET /integrations/documentprovider/enumerate
    with Authorization: Bearer {api_key}
  Poll until all nodes return 200 (token accepted)

Step 3: Verify session-less file operations
  On node 2 (user has never logged in here via passphrase):
    a. Upload file via device token:
       POST /integrations/documentprovider/upload
       with device token auth, multipart body with test content
    b. Poll until file appears on all nodes

Step 4: Verify file content decryptability
  On node 1 (different node from upload):
    a. Download file via device token:
       GET /integrations/documentprovider/download?id={file_id}
       with device token auth
    b. Assert: downloaded content matches uploaded content byte-for-byte

  This proves:
    - Node 1's session was bootstrapped from the device token's wrapped key
    - The SIV key correctly decrypted the file path for lookup
    - The file content was correctly decrypted and returned

Step 5: Revoke device token
  On node 0: DELETE /devices/{device_id} (JWT-authenticated)

Step 6: Poll until revocation propagated
  For each node: GET /integrations/documentprovider/enumerate
    with Authorization: Bearer {api_key}
  Poll until all nodes return 401 (token rejected)

Step 7: Verify session cleanup
  On node 2: GET /integrations/documentprovider/enumerate
    with device token → 401
  Confirms: revocation removes both the token and the ability to bootstrap sessions
```

### Test: `fileprovider-device-token-auth`

This test verifies the FileProvider-specific endpoints work with device token authentication after the migration. It extends the existing FileProvider integration test patterns but runs in the orchestrator with multi-node verification.

```
Setup:
  - 3-node mesh, user created, device registered, token propagated

Steps:
  1. Enumerate root via FileProvider API with device token
     GET /integrations/fileprovider/enumerate
     Assert: 200 with empty items list

  2. Create folder via FileProvider API with device token
     POST /integrations/fileprovider/create (multipart with folder_name)
     Assert: 201

  3. Create file with content via FileProvider API
     POST /integrations/fileprovider/create (multipart with file data)
     Assert: 201

  4. Download and verify file content on different node
     GET /integrations/fileprovider/download?identifier={id}
     Assert: content matches original

  5. Verify incremental sync via changes endpoint
     GET /integrations/fileprovider/changes?since_height=0
     Assert: created items appear in changes

  6. Revoke device token, verify all FileProvider endpoints return 401
```

## Implementation Phases

### Phase 1: Core Infrastructure [x]
- Add `wrapped_user_key` column to `device_tokens` schema
- Implement key wrapping in `post_register_device`
- Implement key unwrapping + session bootstrap in `device_token_auth_middleware`
- Update `RegisterDevicePayload` and consensus handler
- Unit tests for wrap/unwrap round-trip

### Phase 2: FileProvider Migration [x]
- Switch FileProvider routes to `device_token_auth_middleware`
- Update all 7 route handlers to use `Extension(user_id)`
- Update Swift extension to store/use device token from Keychain
- Add device registration to the main app's setup/login flow
- Update `register_fileprovider_domain` to register device token

### Phase 3: Orchestrator Tests [x]
- Implement `device-token-session-bootstrap` test
- Implement `fileprovider-device-token-auth` test
- Verify cross-node file decryptability via device token sessions

### Phase 4: Cleanup [x]
- Remove `fileprovider_auth_middleware` and related auth infrastructure
- Remove `generate_fileprovider_api_key` and `fileprovider_api_key` from `AppState`
- Remove API key generation, initialization, logging, and keychain config storage from `main.rs`
- Retain GUI auto-login keychain infrastructure (serves Tauri login-free experience independently)
- Update RFC-009 to reference this RFC for authentication
- Update system-overview.md progress indicators
