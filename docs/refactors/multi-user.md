# Multi-User Support

Enable multiple users to operate on a single HopNet node, with roaming user support across nodes and a path toward end-to-end encryption where user keys never reside on untrusted infrastructure.

## Current State

Nodes are bound 1:1 to a single user at setup time. The owner's Ed25519 private key is stored in `AppState` via `OnceCell` and used for all cryptographic operations (path SIV encryption, file key wrapping, transaction signing). JWT login gates API access but doesn't carry key material — the node already has the keys regardless of who logs in.

The **data layer** is already multi-user-ready:
- `file_access` table stores per-user wrapped file keys (ECDH + ChaCha20-Poly1305)
- Inodes are keyed by `(owner_id, path)` — per-user namespace isolation
- Consensus handlers verify `tx.user` matches the file owner
- `FileAccess::new_for_user()` and `decrypt_wrapped_file_key()` exist

What's missing is the application layer: the `OnceCell` binding, session-scoped key management, sharing endpoints, and the auth flow for roaming users.

## Design Decisions

### Node identity stays separate from user identity

Nodes retain their own Ed25519 keypair for transport (iroh), consensus participation, and inter-node authentication. User identity is orthogonal — a node can serve any user who authenticates to it. The `nodes.owner` field becomes the node's *administrator*, not the only user it can serve.

### GUI vs headless informs the auth model

HopNet uses Tauri — the same web UI serves both GUI installations (personal device, single-user) and headless installations (server, multi-user). These have different trust and UX requirements:

- **GUI mode**: The owner's keys load at startup. No login prompt needed — it's their device. This path doesn't change significantly.
- **Headless mode**: Multiple users sign in via the web UI. Each session needs its own key material. Login is required and keys must be provisioned per-session.

### Password-wrapped keys for roaming (Phase 1)

User private keys are wrapped with a key derived from the user's password (`KDF(password, salt)`) and stored encrypted in the `users` table, replicated via consensus. When a user logs in to any node:

1. Password is verified (Argon2 hash check, as today)
2. Wrapping key is derived from password
3. Encrypted private key is unwrapped
4. Key material is loaded into a session-scoped store (replacing the `OnceCell`)

This accepts a trust tradeoff: the node sees the password and holds the unwrapped key in memory for the session duration. A malicious node operator could capture it. This is acceptable for Phase 1 — users roam to nodes they broadly trust (their own headless server, a friend's mesh).

### Offline brute-force threat and mandatory passphrases

The `encrypted_privkey` blob is replicated to every node via consensus. Any node operator can extract it and attempt offline brute-force with zero rate-limiting or detection. The KDF parameters (Argon2id, 1 GiB memory, 3 iterations) constrain attackers to roughly 50-100 guesses/sec on serious GPU hardware — but weak user-chosen passwords still fall in minutes regardless of KDF strength.

The mitigation is mandatory server-generated passphrases (Phase 1e): 7 words from the EFF large wordlist (~90 bits entropy). At 100 guesses/sec, exhausting 90 bits takes ~400 billion years. This eliminates the weak-password attack class entirely. Users treat the passphrase as a recovery phrase — on GUI devices with auto-login (Phase 1d), they rarely type it. On headless/roaming nodes, it's the primary authentication method.

### Device-forwarded crypto for untrusted roaming (Phase 2, future)

The long-term architecture eliminates key residency on untrusted nodes entirely. When roaming, the headless node forwards crypto operations to the user's own device (Tauri desktop app, Android/iOS mobile app) over iroh transport:

- Node holds a session authorization token, not the user's key
- Path encryption, file key unwrapping, and transaction signing are forwarded as RPC requests to the user's device
- The user's device performs the crypto and returns results
- The node never sees the private key

iroh provides the transport (bidirectional streams, NAT traversal via DERP relay). The user's device acts as a remote crypto oracle — conceptually an HSM for their HopNet identity.

This coexists with Phase 1: users on their own trusted nodes use local keys directly; users roaming to untrusted nodes use device forwarding. The abstraction boundary is the same (session key source), just the backend changes.

### Dual signature system already separates node vs user operations

The consensus layer already distinguishes automated node operations from user-initiated mutations. `create_signed_transaction` (node key only) handles infrastructure: metrics, fragment cleanup, placement updates, node activation. `create_signed_user_transaction` (node + user key) handles user mutations: file upload/delete, device management, takeout creation. Automated consensus participation (timeout votes, view sync, QC/TC broadcast) uses only the node key. This means multi-user doesn't require reworking the consensus signing model — `create_signed_user_transaction` just needs to pull the user key from session context instead of AppState.

### FileProvider stays single-user; DocumentProvider needs migration

The macOS **FileProvider** authenticates via an ephemeral API key generated at startup and stored in macOS Keychain. It's inherently local and single-user — stays bound to the node owner's keys.

The Android **DocumentProvider** authenticates via consensus-replicated device tokens (`device_token_auth_middleware`), scoped to a `user_id`. This is already multi-device by design — a registered device can authenticate from anywhere. However, DocumentProvider routes currently pull SIV keys from AppState's global `OnceCell`, so they need the same session-scoped key migration as the web UI routes.

### File sharing via `file_access` entries

Sharing an encrypted file with another user means creating a `FileAccess` entry for them — wrapping the per-file key with their X25519 public key. The crypto infrastructure for this already exists. What's needed:

- API endpoints to create/revoke `FileAccess` entries
- User discovery (look up another user's X25519 public key)
- Consensus transaction type for sharing operations
- UI for managing shared access

---

## Phase 1: Session-Scoped Key Management
**Status:** [~] In Progress

Replace `OnceCell` user key binding with session-aware key management. Support password-wrapped key storage for roaming users. Uses a strangler pattern — the session key store is introduced alongside the existing `OnceCell`, call sites are migrated incrementally, and the `OnceCell` is removed last.

### Phase 1a: Foundation
**Status:** [x] Complete

1. [x] **Password-wrapped key storage**: Add `encrypted_privkey` and `key_salt` columns to `users` table. Wrap user private key with `KDF(password, salt)` at user creation time (genesis, join). Remove `user_privkey` from `this_node` schema (breaking change — not a migration, just a schema update since system isn't deployed).

**Validation:** Unit tests for wrapping round-trip. Existing orchestrator tests pass (OnceCell still works).

### Phase 1b: Session Key Store and Login
**Status:** [x] Complete

Introduce session key store alongside the existing `OnceCell`. New login flow populates the session store. Existing code continues to use `OnceCell`. Security fix: removed `password_hash` column and `verify_password()` — the 1 GiB Argon2id key unwrap is now the sole authentication mechanism (the weak default-parameter hash was a brute-force shortcut).

1. [x] **Session key store**: Introduce a session-scoped key store in `AppState` (keyed by user ID, populated at login). Key material is evicted from memory when the JWT expires. Default session is 1 hour (matching current JWT lifetime). A "Remember me for 24 hours" option extends both JWT expiry and in-memory key residence to 24 hours.
2. [x] **Login flow**: Unwrap private key (ChaCha20-Poly1305 decryption success = correct password) → derive SIV key/nonce → load into session store. Login runs in `spawn_blocking` (3-5s Argon2id). Frontend shows loading spinner and error messages.
3. [x] **JWT scope change**: The JWT `uid` claim now selects which session keys to use for crypto operations, not just API access gating. This elevates the JWT's security significance — a stolen JWT grants access to the user's decrypted key material in the session store for the session's remaining lifetime.
4. [x] **User creation endpoint**: Server generates all key material from `{username, password}`. Key wrapping runs in `spawn_blocking`. No RBAC gating yet — system isn't deployed, and this is needed for testing multi-user flows. Full governance gating deferred to Phase 2.

**Validation:** Login populates session store. New user creation works. Existing orchestrator tests still pass (OnceCell unchanged).

### Phase 1c: Call Site Migration
**Status:** [x] Complete

Migrated all user-facing `get_siv_key()` / `get_siv_nonce()` / `get_user_keys()` consumers from `OnceCell` to per-user session store lookups. `create_signed_user_transaction` made async. Sync DB functions (`materialize_folders`, `get_materialized_entries_for_archive`) accept SIV keys as parameters threaded from async callers.

1. [x] **`create_signed_user_transaction` → async**: Now resolves user private key from session store. All 10+ callers updated with `.await`.
2. [x] **`files/routes.rs`**: `get_files`, `get_file_fragments`, `post_files`, `delete_files`, `get_file_fragment_distribution` — all migrated.
3. [x] **`files/download.rs`**: `reconstruct_file_for_user` — user private key from session store.
4. [x] **`devices/routes.rs`**: `post_register_device`, `get_devices` — SIV encrypt/decrypt from session.
5. [x] **`documentprovider/routes.rs`**: All 4 route handlers — SIV keys from session via device token's `user_id`.
6. [x] **`nodes/routes.rs`**: `post_nodes` — user keys availability check + private key for JoinInfo from session.
7. [x] **`takeout/materialization.rs`**: `user_id` threaded as parameter (was `get_user_id()`), SIV keys from session.
8. [x] **`db/takeout.rs`**: `materialize_folders` and `get_materialized_entries_for_archive` accept SIV keys as parameters. `materialize_all_files` accepts `user_id` parameter.
9. [x] **FileProvider (`fileprovider/routes.rs`)**: `delete_item` and `modify_item` `.await` added for async `create_signed_user_transaction`. Remaining FileProvider routes stay on OnceCell (inherently single-user, Phase 1d).

**Validation:** `cargo check` clean. Orchestrator tests pass.

### Phase 1d: Cleanup and New Flows
**Status:** [x] Complete

Remove `OnceCell`, rework join flow, add GUI auto-login, add logout endpoint.

1. [x] **Remove `OnceCell`**: Removed `OnceCell<UserKeys>`, `OnceCell<Key<Aes256Siv>>`, `OnceCell<Nonce>` fields and their getters (`get_user_keys`, `get_siv_key`, `get_siv_nonce`, `initialize_siv_keys`) from `AppState`. Migrated 7 FileProvider route handlers (`get_enumerate`, `get_changes`, `delete_item`, `download_file`, `get_item`, `create_item`, `modify_item`) from OnceCell to session store lookups. Removed OnceCell writes from `post_setup` and `process_join_info`.
2. [x] **Join flow rework**: Removed `user_privkey` from `JoinInfo`. Coordinator no longer sends user private key to joining nodes. After join and catch-up, the user logs in via the web UI to unwrap their key from the consensus-replicated `encrypted_privkey`. Simplified `process_join_info` to only set `node_id`/`user_id` OnceCells and spawn catch-up.
3. [x] **GUI auto-login**: Owner's unwrapped private key stored in macOS Keychain (`com.hopnet.desktop.session`) on first login. At startup, loaded into session store with permanent expiry. Tauri IPC command `auto_login` (not HTTP — only callable from the Tauri webview) issues a JWT for the owner. Frontend detects Tauri environment via `window.__TAURI__` and calls `invoke('auto_login')`. Global `GUI_APP_STATE` bridges AppState from axum server to Tauri command handlers.
4. [x] **Logout endpoint**: `POST /logout` (authenticated) removes session from store. In GUI mode, owner's keychain-loaded session is protected from logout (FileProvider depends on it); non-owner sessions can still be cleared. Frontend `clearAuth()` calls `/logout` before clearing localStorage.
5. [!] **Password change**: Deferred — password-only change without key rotation provides no meaningful security in this architecture. The `encrypted_privkey` blob exists in consensus history on every node, so an attacker who brute-forces the old blob gets the unchanged private key. Password change should be paired with full key rotation (re-key all `file_access` entries + re-encrypt all inode paths), which is a larger feature.

**Validation:** `cargo check` clean. Orchestrator tests pass without OnceCell. GUI auto-login works. Join flow works without user key transfer.

### Phase 1e: Generated Passphrase Migration
**Status:** [ ] Not Started

Replace user-chosen passwords with mandatory server-generated passphrases. The `encrypted_privkey` blob is replicated to all nodes via consensus, making every node operator a potential offline attacker. Even with 1 GiB Argon2id, weak user-chosen passwords fall in minutes. Mandatory generation eliminates this class of vulnerability entirely.

1. [ ] **Passphrase generation endpoint**: Server generates 7-word passphrases from the EFF large wordlist (7776 words, ~90 bits entropy). Bundle the wordlist as a static asset in Rust. The `/setup` and user creation endpoints generate the passphrase, perform hashing and key wrapping server-side, and return the passphrase to the frontend for display.
2. [ ] **Setup flow UX**: Genesis setup presents the generated passphrase with a "write this down" confirmation page. After the user clicks "I've written it down", a second page asks them to enter 3 randomly-selected words by position ("Enter word #2", "Enter word #5", "Enter word #7"). Frontend validates locally against the passphrase received from the server. Setup only completes after successful verification.
3. [ ] **User creation UX**: Same write-down-and-verify flow for new users created via the admin interface. The creating admin sees the passphrase to transmit out-of-band to the new user.
4. [ ] **Login UX**: Login form accepts the passphrase as a space-separated string. Normalize whitespace and case on input. On GUI devices with auto-login (Phase 1d), users rarely type this — it functions as a recovery phrase.
5. [ ] **Remove password choice**: Remove free-text password input from setup and user creation. The key wrapping KDF uses the generated passphrase. (`password_hash` column and `verify_password()` were already removed in Phase 1b — the Argon2id key unwrap is the sole authentication mechanism.)

**Validation:** Setup generates and confirms passphrase. Login accepts passphrase. Orchestrator tests updated for generated passphrases.

### Phase 1f: Orchestrator Validation
**Status:** [ ] Not Started

Full multi-user integration tests.

- [ ] Create mesh, create second user, roaming user login on non-owner node
- [ ] File upload/download with correct per-user key isolation
- [ ] Verify cross-user file inaccessibility
- [ ] Divergence = 0

---

## Phase 2: File Sharing, User Creation, and RBAC
**Status:** [ ] Not Started

Enable file sharing across users, add the ability to create new users on a live mesh, and introduce role-based governance.

### Governance Model

HopNet is designed for cooperative storage — friends pooling their devices. This creates two distinct axes of authority:

- **Node-level ownership**: Already captured by `nodes.owner`. Each user administers their own node(s) — storage policies, device management, node configuration. This is "I own this hardware."
- **Network-level governance**: Decisions affecting the whole mesh — admitting new users, approving new nodes, network-wide policy. This is "who joins the co-op."

These are orthogonal. A user who contributes three nodes is admin of all three regardless of their network governance role.

For initial implementation, a `network_admin` role on the `users` table (genesis user by default) gates network-level decisions. Longer term, the cooperative model raises the question of whether network governance should require collective agreement — proposals approved by supermajority via the existing consensus mechanism rather than unilateral admin action. This needs deeper design discussion when we reach this phase.

### User Creation

Currently users only exist via genesis. A user creation flow is needed: generate keypair, wrap private key with initial password, store via consensus. The new user receives credentials out-of-band and changes their password on first login. Gated by network governance role.

### File Sharing

Sharing an encrypted file means creating a `FileAccess` entry for the target user — wrapping the per-file key with their X25519 public key. The crypto infrastructure exists (`FileAccess::new_for_user()`, `decrypt_wrapped_file_key()`). What's needed: sharing/revocation API endpoints, consensus transaction types for share operations, user discovery (username → X25519 public key), and UI.

---

## Phase 3: Device-Forwarded Crypto
**Status:** [ ] Not Started

Eliminate key residency on untrusted nodes by forwarding crypto operations to the user's own device over iroh.

- [ ] Define crypto oracle RPC protocol (path encrypt/decrypt, file key unwrap, transaction sign)
- [ ] Device registration: user's device advertises itself as a crypto oracle for their identity
- [ ] Session authorization: headless node holds a scoped token, forwards operations to device
- [ ] Latency mitigation: batch path encryption requests, short-lived SIV result caching
- [ ] Graceful degradation when device is unreachable
- [ ] Fallback to Phase 1 (password-wrapped keys) when no device is available
