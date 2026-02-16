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

The mitigation is mandatory server-generated passphrases (Phase 1e): 8 words from the EFF large wordlist (~103.4 bits classical entropy / ~51.7 bits post-Grover). At 100 guesses/sec, exhausting 103 bits takes ~3.2 × 10²³ years. This eliminates the weak-password attack class entirely. Users treat the passphrase as a recovery phrase — on GUI devices with auto-login (Phase 1d), they rarely type it. On headless/roaming nodes, it's the primary authentication method.

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
**Status:** [x] Complete

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
**Status:** [x] Complete

Replace user-chosen passwords with mandatory server-generated passphrases. The `encrypted_privkey` blob is replicated to all nodes via consensus, making every node operator a potential offline attacker. Even with 1 GiB Argon2id, weak user-chosen passwords fall in minutes. Mandatory generation eliminates this class of vulnerability entirely.

1. [x] **Passphrase generation module**: Server generates 8-word passphrases from the EFF large wordlist (7776 words, ~103.4 bits classical entropy / ~51.7 bits post-Grover). The wordlist is embedded as a `const` array in `src/passphrase.rs`. `normalize_passphrase()` lowercases and collapses whitespace for login tolerance. The `/setup` and user creation endpoints generate the passphrase, perform key wrapping server-side, and return the passphrase in a `PassphraseResponse` JSON body.
2. [x] **Setup flow UX**: Genesis setup presents the generated passphrase with a "write this down" confirmation page (`PassphraseDisplay`). After the user clicks "I've written it down", a verification page (`PassphraseVerify`) asks them to enter 3 randomly-selected words by position. Frontend validates locally against the passphrase received from the server. Page reloads only after successful verification.
3. [x] **User creation endpoint**: `POST /users` generates passphrase server-side and returns it in the response body. The creating admin sees the passphrase to transmit out-of-band to the new user.
4. [x] **Login UX**: Login form accepts the passphrase as a space-separated string in a visible text field. Input is normalized (case-insensitive, whitespace-tolerant). On GUI devices with auto-login (Phase 1d), users rarely type this — it functions as a recovery phrase.
5. [x] **Remove password choice**: Removed free-text password input from setup and user creation. `InitialSetupPayload` no longer contains `password`. `SignInData.password` renamed to `SignInData.passphrase`. `UserRequest` no longer contains `password`. Key wrapping uses the generated passphrase. `post_setup` now uses `spawn_blocking` for Argon2id (fixes latent bug where 3-5s Argon2id blocked tokio).

**Validation:** `cargo check` clean. Passphrase generation and normalization unit tests pass. Auth wrap/unwrap round-trip tests pass with generated passphrases. Orchestrator updated to parse and store passphrases from setup response.

### Phase 1f: Orchestrator Validation
**Status:** [x] Complete

Full multi-user integration test (`multi-user-isolation`). Exercises: user creation, roaming login, per-user file upload/download, cross-user isolation (SIV path encryption ensures 404), listing isolation, and zero divergence.

1. [x] `create_user` / `login_user` helpers (POST /users, POST /login)
2. [x] `try_download_file` non-panicking variant for cross-user 404 checks
3. [x] `fetch_state_snapshots` for divergence verification
4. [x] 15-step test flow registered as `multi-user-isolation`
5. [x] Passing on a live 3-node mesh (15/15 checks, 9.5s, zero divergence)

**Bug fix discovered during validation:** `get_files` in `src/db/files.rs` was missing an `owner_id` filter — when multiple users existed, listing files at `/` returned ALL users' inodes, then failed decrypting paths encrypted with other users' SIV keys (500). Fixed by adding `AND i.owner_id = ?` to the query and threading `user_id` from the route handler.

---

## Phase 2: File Sharing (Individual Files)
**Status:** [~] In Progress

Enable live collaborative file sharing between users. Individual files only — shared folders are deferred to Phase 3.

RBAC/governance is deferred. The `POST /users` endpoint is currently ungated; all users can create other users. This is acceptable for an undeployed cooperative mesh where all users are broadly trusted. Governance gating (`network_admin` role) will be added when needed, and is a low-cost retrofit (additive route guards, not structural changes).

### Design: Accept-and-Place Model

Sharing creates an **incoming share** that the recipient explicitly accepts and places in their filesystem:

1. **User A shares a file** → an `incoming_share` record is created with the current `data_block_id`, an encrypted display name (encrypted for B's eyes only), and a pre-computed `FileAccess` entry for User B (ECDH-wrapped per-file key using B's X25519 public key)
2. **User B sees the pending share** → notification in UI (bell icon or similar), decrypts display name with their X25519 private key
3. **User B accepts** → chooses where to place the file in their namespace → a new inode is created in B's namespace `(owner_id=B, path=SIV_B(chosen_path), data_id=current_data_block_id)`, encrypted with B's SIV key

Both users now have independent inodes pointing to the same `data_block`. Both have `file_access` entries (cryptographic access). Neither is "the owner" in a privileged sense — they are equal accessors to the shared content.

### Design: Schema

Three tables support the sharing system:

**`file_access`** (existing, unchanged) — cryptographic access layer. One row per `(data_block_id, user_id)`. Stores the ECDH-wrapped per-file key. Persists as long as the user's inode references the `data_block`.

**`incoming_shares`** (new) — pending share invitations not yet accepted by the recipient.

```sql
incoming_shares (
    id                       UUID PRIMARY KEY,       -- UUIDv7 (encodes creation timestamp)
    data_block_id            UUID NOT NULL,           -- current data_block (updated atomically on modify)
    sender_id                INTEGER NOT NULL,
    recipient_id             INTEGER NOT NULL,
    file_access              BLOB NOT NULL,           -- pre-computed FileAccess for recipient (updated on modify)
    display_ephemeral_pubkey BLOB NOT NULL,           -- X25519 ephemeral key for display_name decryption
    encrypted_display_name   BLOB NOT NULL,           -- filename encrypted for recipient only

    FOREIGN KEY (data_block_id) REFERENCES data_blocks(id),
    FOREIGN KEY (sender_id) REFERENCES users(user_id),
    FOREIGN KEY (recipient_id) REFERENCES users(user_id)
)
```

Key design decisions:
- **References `data_block_id` directly**: Both `shares` and `incoming_shares` use the same lookup pattern (`WHERE data_block_id = ?`). The `data_block_id` changes on every content modification, but the modify path already updates `incoming_shares` on every edit (to re-compute the `file_access` blob), so updating `data_block_id` in the same atomic operation costs nothing. This avoids JOIN-through-inodes indirection.
- **`file_access` blob + `data_block_id` updated atomically on modify**: When any sharer edits a file with pending incoming shares, the consensus handler updates both fields in the same transaction. The route handler (where the modifier's keys are in session) pre-computes the new `FileAccess` for each pending recipient.
- **`encrypted_display_name` uses a separate ephemeral key**: Independent from the `FileAccess` ECDH, so it doesn't need updating when the file key changes. Only the recipient can decrypt it. Other node operators see opaque ciphertext.
- **No `created_at` column**: The UUIDv7 `id` encodes the creation timestamp.

**`shares`** (new) — live-link membership layer. Tracks which users are in a live-link group for a given `data_block`. Only has rows when a file is actively shared between 2+ users. Unshared files have zero rows.

```sql
shares (
    data_block_id   UUID NOT NULL,
    user_id         INTEGER NOT NULL,
    PRIMARY KEY (data_block_id, user_id),
    FOREIGN KEY (data_block_id) REFERENCES data_blocks(id),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
)
```

**`file_access` vs `shares` — why two tables**: These serve different purposes with different lifetimes. `file_access` means "you can decrypt this data_block" and persists until the data_block is orphan-cleaned. `shares` means "your inode should track this data_block's evolution" and is removed on unshare. This separation is necessary for copy-on-write unsharing — removing a user's `shares` row stops future propagation without destroying their ability to decrypt the version they already have.

### Design: Live Link Semantics

Shared files maintain a live link — modifications by either user are visible to the other. The `shares` table is the authoritative record of live-link membership. When User A modifies a shared file:

1. New `data_block` is created (new content version)
2. Route handler checks `shares` for old `data_block_id` → finds live-linked participant list
3. Route handler checks `incoming_shares` for old `data_block_id` → finds pending recipients
4. Pre-computes new `FileAccess` entries for all (live-linked participants + pending recipients)
5. Consensus payload includes: standard modify (A's inode update) + share propagation + pending share updates
6. Handler: updates A's inode `data_id`; for each live-linked participant: updates their inode `data_id`, inserts `FileAccess`, moves `shares` rows to new `data_block_id`; for each pending recipient: updates `data_block_id` and `file_access` blob in `incoming_shares` atomically

Steps 2–4 happen in the **route handler** (not the consensus handler), because only the modifying user's node has the plaintext file key in session for ECDH wrapping. The consensus handler applies the pre-computed results.

If the file is not shared and has no pending incoming shares, the modification proceeds exactly as today — zero overhead for unshared files.

### Design: Deletion and Cleanup

**Deleting a file**: Removes the deleting user's inode, their `shares` row (if any), and any `incoming_shares` where `sender_id` matches the deleting user and `data_block_id` matches the deleted inode's data. The `data_block` and fragments persist as long as any other user's inode still references them (existing orphan-check in `delete_orphaned_data_blocks_consensus` protects them).

**Deleting a file with pending shares**: The `incoming_shares` rows from the deleting sender are cleaned up in the same consensus transaction. The pending share silently disappears from B's UI — the sender no longer has the file to share.

### Design: Copy-on-Write Unsharing

"Unsharing" severs the live link without duplicating data:

1. Remove the unsharing user's row from `shares` for this `data_block_id`
2. If only one user remains in `shares`, remove that row too (no longer shared)
3. The unsharing user's inode and `file_access` entry are **untouched** — they can still read the current version
4. Next modification by any remaining sharer creates a new `data_block` → `shares` consulted → unshared user is absent → their inode stays on the old version
5. The old `data_block` persists as long as the unshared user's inode references it (existing orphan-check protects it)

No instant fork. No data duplication. No re-encryption. Divergence happens lazily at the next write.

### Consensus Implications

**Current invariant**: All file-mutating handlers (`InsertFilesHandler`, `ModifyItemHandler`, `DeleteFilesHandler`) enforce `tx.user.id == payload.user_id`. A user can only modify their own inodes.

**Sharing requires a scoped exception** for one operation: live-link propagation. When A modifies a shared file's content, A's transaction must update other participants' inodes (`data_id` pointer).

**Integration surface.** Analysis of all file-mutating paths:

| Path | Shares awareness needed? | Reason |
|------|-------------------------|--------|
| `InsertFilesHandler` | No | New files are always unshared |
| `ModifyItemHandler` (rename/move) | No | Path changes are per-user; `data_id` unchanged |
| `ModifyItemHandler` (content update) | **Yes** | Propagate new `data_block` to live-linked users + update pending `incoming_shares` |
| `DeleteFilesHandler` | Yes | Remove `shares` row + clean up `incoming_shares` by `sender_id` + `data_block_id` |
| `UpdatePlacementHeightsHandler` | No | System operation, no user context |
| `DeleteOrphanedDataBlocksHandler` | No | Already checks for any inode reference |
| `SelfCheckFragmentsHandler` | No | Fragment inventory, not file-level |
| Takeout paths | No | Snapshots user's own inodes (shared files appear in both users' takeouts) |

`ModifyItemHandler` (content updates) carries the main complexity. `DeleteFilesHandler` gets cleanup queries. Everything else is unchanged.

The `ModifyItemPayload` is extended with optional propagation data:

```rust
pub struct SharePropagation {
    pub old_data_block_id: CustomUUID,
    pub participant_access: Vec<FileAccess>,         // for live-linked users (shares table)
    pub pending_share_updates: Vec<PendingShareUpdate>, // for incoming_shares
}

pub struct PendingShareUpdate {
    pub incoming_share_id: CustomUUID,
    pub updated_file_access: Vec<u8>,                // re-computed FileAccess blob
}
```

The handler's authorization for propagation: verify the submitter has a row in `shares` for `old_data_block_id`. The inode update is scoped to users present in `shares`; the `incoming_shares` update is scoped to rows matching `old_data_block_id`. Neither is a blanket "edit anyone's inodes" capability.

### API Design

All share endpoints use inode UUIDs (`FileItem.id`) rather than encrypted paths — the frontend already has these from file listings, and it avoids unnecessary SIV round-trips. The `data_block_id` is an internal detail that route handlers resolve from the inode; the frontend never sees it.

**File listing augmentation**: `GET /files` response extended with `shared_with_count`:

```typescript
export interface FileItem {
    id: string;                   // inode UUID (stable, used for share/unshare actions)
    path: string;                 // encrypted path
    inode_type: InodeType;
    file_size: string;
    creation_date: string;
    modification_date: string;
    shared_with_count: number;    // 0 = not shared, 1+ = number of OTHER users sharing
}
```

The count includes both accepted shares and pending invitations, computed server-side:

```sql
(SELECT COUNT(*) FROM shares s WHERE s.data_block_id = i.data_id AND s.user_id != ?)
+
(SELECT COUNT(*) FROM incoming_shares ist WHERE ist.data_block_id = i.data_id)
```

No self-exclusion needed on `incoming_shares` — the user can't be a pending recipient for a file they already have an inode for. Both subqueries use `data_block_id` (indexed as PK prefix on `shares`, should be indexed on `incoming_shares`). Computed in Rust for consistency across any future native clients. Unshared files with no rows in either table short-circuit to 0.

**Share a file** — `POST /shares`

```
Body: { "inode_id": "<uuid>", "recipient_username": "<username>" }
Response: 200 OK | 404 (file/user not found) | 409 (already shared)
```

Route handler: look up inode by id + authenticated user_id → get `data_block_id` → decrypt per-file key from sender's session → decrypt filename from SIV path server-side for display name → encrypt display name for recipient (separate ECDH) → create `FileAccess` for recipient → submit `ShareFileHandler` consensus transaction. The display name extraction happens server-side from the sender's SIV context — the frontend is not trusted for it.

Validation: self-share prevention (sender == recipient). Duplicate prevention: check no existing `incoming_shares` or `shares` row for this `(data_block_id, recipient_id)`. Note: post-fork re-sharing is allowed because the data_block_ids differ after copy-on-write divergence.

**List pending shares** — `GET /shares/incoming`

```
Response: [{
    "id": "<incoming_share_uuid>",      // UUIDv7, used for accept action
    "sender_username": "<name>",
    "display_name": "<decrypted_name>", // server decrypts from recipient's session keys
    "created_at": "<from_uuidv7>"
}]
```

Server decrypts `encrypted_display_name` using the recipient's X25519 private key from session store before returning. A lightweight `GET /shares/incoming/count` can serve badge notifications without the full listing.

**Accept a pending share** — `POST /shares/{id}/accept`

`{id}` is the `incoming_share` UUID from `GET /shares/incoming`.

```
Body: { "placement_path": "/Documents/shared-doc.pdf" }  // plaintext, server encrypts with SIV
Response: 200 OK | 404 (share not found/expired) | 409 (path conflict)
```

Server encrypts `placement_path` with the recipient's SIV key and submits `AcceptShareHandler` consensus transaction. The `data_block_id` comes from the `incoming_shares` row (kept current by the modify path).

**Get sharing details** — `GET /shares/file/{inode_id}`

```
Response: { "users": [
    { "username": "<name>", "user_id": N, "status": "accepted" },
    { "username": "<name>", "user_id": N, "status": "pending" }
]}
```

Detail view for "who has access" — fetched on demand when user opens share management for a specific file. Route handler: look up inode → get `data_block_id` → query both `shares` (status: accepted) and `incoming_shares` (status: pending) → resolve usernames.

**Unshare (self-removal only)** — `DELETE /shares/{inode_id}`

`{inode_id}` is the user's own inode UUID (`FileItem.id`).

```
Response: 200 OK | 404 (not shared)
```

Route handler: look up inode by id + authenticated user_id → get `data_block_id` → submit `UnshareHandler` consensus transaction → removes user's `shares` row. Copy-on-write: inode and `file_access` untouched, divergence happens at next modification.

**Self-removal only — no "remove others" action.** The `shares` table is a flat membership list with no ownership hierarchy. Removing another participant would be a unilateral governance decision affecting everyone in the share group (e.g., A removing C also stops B's future modifications from reaching C, even though B never chose to stop sharing with C).

To share with a subset, the flow is explicit:
1. A unshares (leaves the group)
2. A re-shares with the desired subset (new `incoming_share` invitations from A's current data_block)
3. Each recipient accepts or declines independently
4. The old share group continues without A — remaining members keep their live link

This ensures every participant makes their own choice. "Remove others" may be added later with governance (e.g., original inviter has removal rights, or consensus-based removal), but is out of scope for Phase 2.

**Cancel or decline a pending share** — `DELETE /shares/incoming/{id}`

`{id}` is the `incoming_share` UUID. Authorized for both `sender_id` (cancel an invitation you sent) and `recipient_id` (decline an invitation you received).

```
Response: 200 OK | 404 (share not found)
```

Removes the `incoming_share` record via consensus. The sender's file and `FileAccess` entries are untouched.

### Phase 2a: Share and Accept Flow
**Status:** [x] Complete

Schema, core consensus handlers, and API for the share → accept → download path. After this phase, A can share a file with B, B can accept and place it in their filesystem, and B can download the shared content. Live-link propagation is not yet implemented — if A modifies after B accepts, B stays on the version at acceptance time until Phase 2b.

- [x] `incoming_shares` table (schema above) — pending share invitations with encrypted display names
- [x] `shares` table (schema above) — live-link membership, only populated for actively shared files
- [x] Encrypted display name ECDH logic — separate ephemeral key for recipient-only decryption
- [x] `ShareFileHandler` consensus transaction — creates `FileAccess` entry for recipient + `incoming_share` record (with encrypted display name and pre-computed `FileAccess`)
- [x] `AcceptShareHandler` consensus transaction — creates inode in recipient's namespace (using `data_block_id` from `incoming_shares` row), inserts `shares` rows for both sender and recipient, removes `incoming_share`
- [x] Extend `GET /files` query with `shared_with_count` (subquery on `shares` + `incoming_shares`, excludes self, computed in Rust)
- [x] API: `POST /shares` (share file), `GET /shares/incoming` (pending shares), `GET /shares/incoming/count` (badge count), `POST /shares/{id}/accept` (accept + place), `DELETE /shares/incoming/{id}` (decline)
- [x] `GET /shares/file/{inode_id}` — sharing detail view (who has access, with accepted/pending status)
- [ ] `GET /users` endpoint for recipient discovery (username + display info, no key material exposed)
- [x] Validation: self-share prevention, duplicate prevention on `(data_block_id, recipient_id)` across both tables
- [x] `AcceptShareHandler` writes to `modification_log` for FileProvider consistency (new inode in recipient's namespace)

**Validation:** `multi-user-sharing` orchestrator test (29/29 checks, 12s, zero divergence):
- [x] Integration test: A uploads → A shares with B → B accepts → B downloads → content matches (all 3 nodes)
- [x] File listing shows `shared_with_count = 1` after sharing
- [x] Share details: both participants listed with correct status
- [x] Decline test: A shares with B → B declines → incoming_share removed, A's file unaffected (404 on download)
- [x] Duplicate prevention: sharing same file with same user twice returns 409 (preflight check)
- [x] Zero divergence across all share/accept/decline operations (16 tables)

### Phase 2b: Live-Link Propagation and Unshare
**Status:** [x] Complete

Layer live collaboration on top of the share/accept foundation. Modifications by any sharer propagate to all live-linked users and update pending incoming shares. Unshare severs the live link with copy-on-write semantics.

- [x] Extend `ModifyItemPayload` with `incoming_share_updates` — route handler populates from both `shares` and `incoming_shares` tables when applicable
- [x] `ModifyItemHandler` propagation logic — updates live-linked inodes' `data_id` + `FileAccess` entries + `shares` rows; updates `incoming_shares` `data_block_id` + `file_access` blobs atomically for pending recipients
- [x] `UnshareHandler` consensus transaction — removes user's `shares` row (copy-on-write: no data duplication)
- [x] `DeleteFilesHandler` cleanup — remove `shares` rows + `incoming_shares` where `sender_id` and `data_block_id` match the deleted file
- [x] API: `DELETE /shares/file/{inode_id}` (unshare self)

**Validation:** `multi-user-sharing-live-link` orchestrator test (51/51 checks, 24s, zero divergence):
- [x] Live link test: A modifies → B sees updated content (all 3 nodes)
- [x] Multi-sharer propagation: A shares with B and C → A modifies → both B and C see update
- [x] Pending share update: A shares with D (pending) → A modifies → D accepts → D gets latest version
- [x] Unshare test: B unshares → A modifies → B still has pre-fork version, C and D see update
- [x] Deletion isolation: A deletes → C still has file → data_block persists
- [x] Deletion with pending share: A deletes → pending incoming_share cleaned up (not tested separately — covered by deletion cleanup removing sender's outgoing shares)
- [x] Zero divergence across all sharing operations

**Bug fix discovered during validation:** `modify_item()` propagation SQL (`UPDATE inodes SET data_id = ? WHERE data_id = ? AND owner_id != ?`) was too broad — updated ALL inodes with matching `data_id`, including users who had unshared. After unshare, the user's inode would be updated to a new `data_block` they had no `file_access` for (403). Fixed by scoping: `AND owner_id IN (SELECT user_id FROM shares WHERE data_block_id = ?)`.

### Phase 2c: Frontend — Sharing UI
**Status:** [ ] Not Started

- [ ] Share button in file browser (context menu or selection toolbar), visible when file selected
- [ ] Recipient picker dialog (user list from `GET /users`)
- [ ] Incoming shares notification (bell icon with badge from `GET /shares/incoming/count`)
- [ ] Incoming shares panel with accept dialog (choose placement path)
- [ ] Share indicators on files in listing (`shared_with_count > 0` → icon overlay or badge)
- [ ] "Who has access" detail view with accepted/pending status (from `GET /shares/file/{inode_id}`)
- [ ] Unshare action (self-removal from share)
- [ ] Decline action for incoming shares

---

## Phase 3: Shared Folders
**Status:** [ ] Not Started

Shared folders require solving the SIV path encryption context problem — paths within a folder are encrypted with the owner's SIV key, making cross-user traversal impossible without key sharing or re-encryption.

### Problem

Inode paths are encrypted with per-user SIV keys derived from the user's Ed25519 private key. A shared folder's contents are encrypted with the owner's SIV key. The recipient can't enumerate, navigate, or resolve paths within the shared folder because they don't have the owner's SIV context.

### Possible Approaches (Needs Design)

- **Per-share SIV key**: Generate a new SIV key pair per shared folder. Re-encrypt all paths within under the share key. Both users get the share SIV key (wrapped for each). Requires a `shared_inodes` table or namespace separation. File adds/removes/renames require dual updates.
- **Mount-point model**: Shared folders live exclusively in a "Shared" UI section with their own SIV context. Clean separation but shared folders can't be placed into a user's own tree.
- **Inode-id redirection**: Map share boundaries in path traversal so `/my-stuff/shared-folder/nested/file` switches SIV context at the share boundary. Recursive nesting complicates resolution.

Each approach has significant implications for path resolution, query patterns, and the modification log. This needs its own design phase before implementation.

---

## Phase 4: Device-Forwarded Crypto
**Status:** [ ] Not Started

Eliminate key residency on untrusted nodes by forwarding crypto operations to the user's own device over iroh.

- [ ] Define crypto oracle RPC protocol (path encrypt/decrypt, file key unwrap, transaction sign)
- [ ] Device registration: user's device advertises itself as a crypto oracle for their identity
- [ ] Session authorization: headless node holds a scoped token, forwards operations to device
- [ ] Latency mitigation: batch path encryption requests, short-lived SIV result caching
- [ ] Graceful degradation when device is unreachable
- [ ] Fallback to Phase 1 (password-wrapped keys) when no device is available
