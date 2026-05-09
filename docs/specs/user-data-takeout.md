# User Data Portability

## Summary

HopNet users can freely move their data between meshes. **Takeout** exports a user's complete file tree as a portable, decrypted archive. **Import** ingests that archive into another mesh, reconstructing the folder hierarchy and file contents. Together they form a symmetric portability boundary: any user can leave a mesh with their data intact, and any user can enter a new mesh with data from elsewhere.

Both operations are consensus-coordinated to prevent conflicting state across nodes. Neither preserves mesh-specific context (sharing grants, timestamps, node identities, data block IDs) — portability applies to content (files, folder structure, paths), which is what moves meaningfully between meshes.

## Motivation

Users must have full control over their data. This is a hard requirement for:

- **Data portability between meshes** — leaving one mesh for another (distrust-motivated or convenience-motivated)
- **GDPR right to data portability** — regulatory compliance for users in applicable jurisdictions
- **Personal backup** — periodic export for offline retention
- **Disaster recovery** — restore files to a rebuilt mesh after schema changes, node loss, or operational failure
- **Development iteration** — nuke-and-reimport workflow for developers evolving schema without writing consensus-replayable migrations

Export alone addresses backup and GDPR compliance. Import is required for the remaining cases. The two are designed as complementary halves of one portability mechanism.

## Manifest Format

Every takeout archive contains a `manifest.json` file as its **first entry**. This placement is load-bearing: import streams the archive upload and reads the manifest before the rest of the archive arrives, enabling early quota rejection and version validation without buffering the full archive to disk.

Takeout writes the manifest. Import reads and verifies against it. Both sides must conform to this schema exactly; it is the sole contract between the two operations.

### Schema (version 1)

```json
{
  "version": 1,
  "takeout_id": "01HK7P3X8Z...",
  "created_at": "2026-04-24T12:34:56Z",
  "source_username": "allison",
  "total_files": 1234,
  "total_folders": 45,
  "total_bytes": 56789012,
  "folders": [
    { "path": "photos" },
    { "path": "photos/vacation-2024" }
  ],
  "files": [
    {
      "path": "photos/vacation-2024/IMG_0001.jpg",
      "size": 2345678,
      "source_data_block_id": "01HK7P...",
      "file_hash": "af3c...64-hex-chars..."
    }
  ]
}
```

### Field Specifications

| Field | Type | Required | Purpose |
|-------|------|----------|---------|
| `version` | integer | yes | Manifest schema version. Import rejects unknown versions. |
| `takeout_id` | string (UUIDv7) | yes | Source-mesh takeout identifier. Informational only; import does not use it. |
| `created_at` | string (ISO 8601 UTC) | yes | Archive generation time. Informational only. |
| `source_username` | string | yes | Display name of user who generated the takeout. Import shows this but does not match against target-mesh usernames. |
| `total_files` | integer | yes | Count of entries in `files[]`. Must match exactly. |
| `total_folders` | integer | yes | Count of entries in `folders[]`. Must match exactly. |
| `total_bytes` | integer | yes | Sum of all `files[].size`. Used for quota precheck against target mesh. Plaintext sizes only; does not include archive compression, encryption overhead, or RS expansion factor. Import applies the target-mesh RS expansion multiplier locally for quota math. |
| `folders[]` | array | yes | All folders, including empty ones. Enables empty-folder preservation and explicit parent creation order. |
| `folders[].path` | string | yes | Relative path from archive root. Forward-slash separated. No leading slash. UTF-8. |
| `files[]` | array | yes | All files (regular files only; symlinks, devices, and other special entries are not represented in v1). |
| `files[].path` | string | yes | As per `folders[].path`. |
| `files[].size` | integer | yes | File size in bytes (plaintext, decrypted). |
| `files[].source_data_block_id` | string (UUIDv7) | yes | Source-mesh `data_blocks.id`. Required by import for hash verification (see Privacy Invariants). |
| `files[].file_hash` | string (Blake3 hex, 64 chars) | yes | `blake3(plaintext_bytes ∥ source_data_block_id)`. Same formula as source-mesh `data_blocks.file_hash`. |

### Ordering

Entries in `folders[]` MUST be sorted by path depth ascending, then lexicographically. This guarantees import can create parents before children in a single forward pass.

Entries in `files[]` SHOULD be sorted lexicographically by path for deterministic archives, but import does not require it.

### Path Constraints

Paths SHOULD NOT exceed 1024 bytes (UTF-8 encoded). This is a guideline to stay within common filesystem limits (macOS 1024, Linux 4096, Windows 260 without long-path opt-in). Import does not enforce a hard cap; paths exceeding a target mesh's effective limit will fail naturally during file creation with `error_code = "path_too_long"`.

### Versioning

- Adding optional fields is non-breaking. Readers MUST ignore unknown fields.
- Removing, renaming, or changing the type or semantic of an existing field is breaking and requires incrementing `version`.
- Import MUST reject archives whose `version` exceeds the highest version it recognizes.
- Import MAY accept older versions if forward-compatible conversion is unambiguous; otherwise reject.

### Size Limits

Manifest size scales linearly with file count. At ~200 bytes per file entry, 1M files produces a ~200MB manifest. No hard limit is imposed in v1; users are expected to take manageable slices via the planned selective-export feature. Import MUST stream manifest parsing rather than loading it fully in memory. A hard cap may be introduced in later versions.

### Privacy Invariants

The salt on `file_hash` is not incidental. It exists to preserve HopNet's no-content-correlation property:

- `data_block_id` is UUIDv7: timestamp + random bits. It is **not** derived from content.
- `file_hash = blake3(plaintext ∥ data_block_id)` is therefore unique per data block even when two data blocks contain identical plaintext.
- Two meshes holding the same file contents yield different `file_hash` values, preventing cross-mesh content deduplication analysis.

**Manifest authors MUST NOT add plaintext-only hashes, content-derived identifiers, or any other field that would enable cross-user or cross-mesh content correlation.** Doing so breaks the privacy invariant for all users of the affected meshes.

Import verifies each file by recomputing `blake3(bytes_read_from_tar ∥ source_data_block_id)` and comparing to the manifest's `file_hash`. Mismatch indicates archive corruption and fails that file's import with `error_code = "hash_mismatch"`.

## Takeout (Export)

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

**Integrity Self-Verification:**
After reconstruction, the takeout node recomputes `blake3(file_content ∥ source_data_block_id)` and compares it against the stored `data_blocks.file_hash`. A mismatch indicates silent corruption somewhere in the fragment → RS-decode → decrypt pipeline. The file is marked failed in `takeout_inodes_{takeout_id}` with `error_message = "integrity check failed"` and excluded from the final archive. The per-file `file_hash` and `source_data_block_id` are retained for the manifest; other files proceed normally.

This check is not redundant with fragment-level Blake3 verification: fragment hashes protect against transport and storage corruption of individual fragments, while the reconstructed-file hash protects against RS-decode bugs, key mismatches, and decryption errors that yield structurally valid but semantically wrong output.

#### 3. Archive Generation

Archive creation is **two-phase**. Phase 1 materializes all files to an owner-node-local staging directory, collecting per-file metadata as each reconstruction succeeds (path, size, `source_data_block_id`, `file_hash`). Phase 2 streams the tar: `manifest.json` is written first using the collected metadata, then the `files/` tree is appended by reading back from staging.

The two phases are required because `manifest.json` must be the first tar entry (see Manifest Format), but manifest contents (`total_files`, `total_bytes`, per-file hashes) can only be finalized once all materialization completes. Streaming files directly into tar during materialization, as in the pre-manifest design, is no longer possible.

```
Phase 1: Materialize to Staging
  For each file in takeout_inodes:
    Reassemble from fragments → verify integrity → write to staging dir → record metadata

Phase 2: Emit Archive
  Open tar.gz output stream
  Write manifest.json (built from collected metadata)
  Write folders/ and files/ tree by reading back from staging dir
  Close stream
```

**Storage Overhead:**
Staging requires ~1x user data size on owner node disk. Combined with archive output, reserve ~2x during takeout. The storage precheck at initiation accounts for this.

**Progressive Archive Creation:**
- Phase 2 uses streaming tar writer; full archive is never held in memory
- Staging files are streamed through on a per-file basis
- Set restrictive permissions (700) on staging and archive directories
- For remote users: Phase 2 streams tar output as chunked HTTP response

**Archive Structure:**
```
user-takeout-{timestamp}.tar.gz
├── manifest.json                [first entry — see Manifest Format]
└── files/
    ├── Documents/
    │   ├── report.pdf
    │   └── notes.txt
    ├── Photos/
    │   └── vacation/
    │       └── beach.jpg
    └── [user's complete folder structure]
```

#### 4. Manifest Emission

At the boundary between Phase 1 and Phase 2, the owner node assembles `manifest.json` from data collected during materialization:

- `total_files` and `total_folders` reflect entries that successfully materialized (integrity-failed files are excluded from the archive and from these counts)
- `total_bytes` is the sum of successfully materialized `file_size` values
- `folders[]` is populated from the user's inode tree, sorted by depth ascending then lexicographically (see Manifest Format § Ordering)
- `files[]` is populated from `takeout_inodes_{takeout_id}` where `materialization_status = 'success'`, with `source_data_block_id` and `file_hash` carried directly from `data_blocks`

Manifest serialization is pretty-printed JSON with UTF-8 encoding. Pretty-printing costs negligible bytes after gzip and aids diagnostic inspection.

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

## Import (Ingest)

### Overview

Import ingests a takeout archive into the current mesh as the authenticated user's new data. Paths, folder structure, and file contents are preserved; mesh-specific context (timestamps, sharing grants, node identities, data block IDs) is not. The operation is consensus-coordinated so that concurrent writes from the same user are blocked network-wide while ingest is in progress.

### Scope in v1

Import is exposed from **setup terminal states only** — after genesis or join completes, the user sees an optional "Import existing data" step. The long-term Settings-pane entry point is intentionally deferred until login→import is designed with proper merge semantics.

| Entry point | v1 status | Reason |
|-------------|-----------|--------|
| Genesis → import | Supported | Fresh mesh, empty file tree, zero collision risk |
| Join → import | Supported | New user on existing mesh, fresh per-user namespace |
| Login → import | **Disallowed** | Merge semantics (skip/overwrite/rename-on-collision) unresolved |

### Import Initialization

#### 1. Upload & Streaming Read

- `POST /takeout/import` accepts `multipart/form-data` with a `tar.gz` body
- Owner node streams incoming bytes to a temp staging directory
- Tar reader consumes entries in order; the first entry MUST be `manifest.json`
- Manifest is parsed before the rest of the archive arrives, enabling early rejection
- Archive `version` is validated; unknown versions return `400 Bad Request`
- Quota check: `manifest.total_bytes × RS_EXPANSION (3)` must fit within `sum(validator_storage_available) - safety_margin`. Failure returns `507 Insufficient Storage` and discards the upload without touching consensus

#### 2. Consensus Lifecycle

Import mirrors the takeout lifecycle pattern: a consensus-replicated row for network-wide visibility, and a per-node-local temp table for byte-level progress.

```sql
-- Consensus-tracked, replicated to all nodes.
-- created_at derives from the UUIDv7 id. No consensus_height column —
-- import doesn't snapshot any target-mesh state, so the field would be unused metadata.
CREATE TABLE imports (
    id UUID PRIMARY KEY,                    -- UUIDv7 (contains creation timestamp)
    user_id INTEGER NOT NULL REFERENCES users(user_id),
    owner_node_id INTEGER NOT NULL,         -- Node processing this import
    status ENUM('pending', 'extracting', 'importing', 'completed', 'failed', 'cancelled') NOT NULL DEFAULT 'pending'
);

-- Local only on owner node; per-file progress and reporting
CREATE TEMPORARY TABLE import_paths_{import_id} (
    path VARCHAR NOT NULL,
    type ENUM('file', 'folder') NOT NULL,
    size_bytes BIGINT,
    source_data_block_id UUID,              -- For hash verification; discarded after import
    status ENUM('pending', 'imported', 'skipped', 'failed') DEFAULT 'pending',
    error_code VARCHAR,                     -- See Error Classification
    error_message VARCHAR,
    processed_at TIMESTAMP
);
```

Consensus transaction types:

- `create_import` — validates no active import for user, inserts pending row. Submitted only after quota passes.
- `complete_import` / `fail_import` — terminal state transitions.

Per-file progress is **not** consensus-tracked. 10k files would produce 10k rounds of consensus churn for zero network-wide benefit. Owner-node-local state suffices; on restart the job can resume from the local table.

#### 3. Write Gating

While a row exists in `imports` with status in `{pending, extracting, importing}` for a given `user_id`, all write-path routes return `409 Conflict`:

- `POST /files` (file upload, via UI or FileProvider)
- `POST /folders`
- `DELETE /files`, `DELETE /folders`
- FileProvider `modifyItem`, `createItem`, `deleteItem` endpoints
- Any share-grant modification routes

Reads are unaffected. Users can browse the target mesh while import is in progress and see files populate as they land.

Gating is performed at the route layer via a shared middleware check against the consensus-replicated `imports` table; therefore it holds across all nodes regardless of which node receives the write attempt.

#### 4. Extraction & Verification

Once the manifest passes quota, the remaining tar entries are extracted to staging:

- Each file is read from the tar stream and written to staging
- Upon write completion, the owner node computes `blake3(staging_bytes ∥ source_data_block_id)`
- The computed value is compared to the manifest's `file_hash`
- Mismatch marks the file's `import_paths_{id}` row as failed with `error_code = "hash_mismatch"` and the staging file is deleted; the import continues with other files

#### 5. Per-File Creation

Import reuses the same backend logic as `post_files` to avoid code drift between normal uploads and imports. A shared in-process helper `create_file_with_fragments(user_id, path, bytes) -> Result<DataBlockId>` is extracted from the existing post_files route; both the HTTP handler and the import job call it.

- Folders are created first, walking `import_paths_{id}` where `type = 'folder'` ordered by path depth
- Files are created next, walking where `type = 'file'`
- Concurrency: `N = 4` parallel file creations per batch (tunable). Fragment distribution remains `tokio::spawn`'d inside the shared helper; import-side concurrency bounds the spawn rate transitively
- Per-file consensus cost is unchanged from a normal upload. Batch-import consensus transaction types are deferred (see Future Enhancements)
- On per-file failure: classify the error, retain the error message, update the row, continue

#### 6. Progress & Reporting

- `GET /takeout/import/{id}` — current status and aggregate counts (total, imported, failed, pending)
- `GET /takeout/import/{id}/report` — paginated per-file failure list grouped by `error_code`
- Polling interval: client default 1 Hz; server imposes no hard rate limit but supports HTTP 304 semantics via ETag if progress has not advanced

### Error Classification

| `error_code` | Meaning | Retryable |
|--------------|---------|-----------|
| `invalid_path` | Path contains null bytes, `..` escape, empty segments, or control characters | No |
| `path_too_long` | Path exceeds target filesystem or HopNet limit | No |
| `already_exists` | Path collision (reserved for future login→import) | No |
| `hash_mismatch` | `blake3(bytes ∥ source_data_block_id) ≠ manifest.file_hash` | No |
| `read_error` | Tar entry read or staging write failed | Maybe |
| `consensus_rejected` | `create_file_with_fragments` consensus txn returned error | Yes |
| `distribution_failed` | File committed locally, but RS fragment distribution errored | Yes (background retry) |
| `quota_exceeded` | Network storage exhausted mid-import despite precheck | No |
| `unknown` | Catchall | Maybe |

Retryable failures are retained for a future "retry failed" UI action (Phase 5+). Non-retryable failures are final for this import; the user must regenerate a clean takeout or manually re-upload.

### API Endpoints

- `POST /takeout/import` — upload archive, start import (multipart, `archive` field; 507 on quota, 409 on active import)
- `GET /takeout/import` — singleton current-import row for the authenticated user (`ImportRecord`); replicated network-wide so non-owner nodes return same shape
- `GET /takeout/import/status` — aggregate per-status counts (`ImportPathCounts`); owner-only (404 on non-owner)
- `GET /takeout/import/paths` — per-path debug list (`ImportPathRow[]`); owner-only; powers terminal failure-by-error-code breakdown
- `PUT /users/me/onboarding` — flip `users.onboarding_flags` bitfield; `{set: OnboardingFlag[], clear: OnboardingFlag[]}` body; replicated via consensus
- `GET /users/me` — returns `SelfUserInfo` (includes `onboarding_flags: u32` for frontend onboarding gate)
- `GET /takeout/import/{id}/report` — per-file failure report (Phase 5)
- `DELETE /takeout/import/{id}` — cancel active import; does not roll back already-created files in v1 (Phase 5)

## Implementation Phases

Each phase is scoped to a single atomically testable increment. Sub-phases within a phase are independently mergeable and can ship in separate PRs. Every phase specifies its acceptance criterion so "done" is unambiguous.

### Phase 1: Takeout Core [x] COMPLETED

Foundational takeout system with consensus-coordinated lifecycle, materialization, and archive delivery.

- [x] Consensus-tracked takeout lifecycle (`takeouts` table, `create_takeout` / `update_takeout_status` handlers)
- [x] Rate limiting: one active takeout per user
- [x] Storage validation: node-local capacity check (3x user data size) before materialization
- [x] Point-in-time consistency via consensus height boundaries
- [x] Event-driven file materialization with real-time progress tracking
- [x] Streaming `tar.gz` archive creation (per-file append, no full-archive memory buffering)
- [x] Fragment retention coordination across nodes to prevent deletion during active takeouts
- [x] Automated maintenance job for expiration handling with batched consensus operations
- [x] REST API endpoints: `/takeout/initiate`, `/takeout/status`, `/takeout/download`, `/takeout/cancel`, `/takeout/can-create`
- [x] Frontend takeout pane with selection-based bulk operations
- [x] Auto-refresh with intelligent pausing during user interaction
- [x] Account-mode sidebar integration and UI state management
- [x] Typeshare-generated TypeScript types with DateTime serialization support

**Acceptance:** Existing orchestrator integration tests pass; users can initiate, monitor, download, and delete takeouts end-to-end across multi-node meshes.

### Phase 2: Takeout Portability Contract [ ]

Takeout-side changes required to produce archives that import can consume. Upgrades Phase 1 archive format to v1 of the portability contract.

#### 2.1: Integrity Self-Verification [x]
- [x] In `materialize_single_file`, hash plaintext incrementally during streaming write, then after `file.sync_all()` compute `blake3(plaintext ∥ source_data_block_id)` and compare to stored `data_blocks.file_hash`
- [x] On mismatch, mark `takeout_inodes_{takeout_id}` row `materialization_status = 'failed'` with `error_message = "integrity check failed"` and delete the corrupted staging file
- [x] Failed files are excluded from the archive; takeout continues with remaining files (archive assembly already filters by `materialization_status = Success`)
- [x] `get_pending_files_batch` extended to JOIN `data_blocks` and surface `file_hash` to the materialization worker

**Acceptance:** New `takeout-happy-path` orchestrator scenario (`orchestrator/tests/takeout.rs`) uploads 3 files, initiates takeout, waits for Ready, downloads archive, decompresses, and verifies byte-exact match for each file. Verified on a fresh 3-node mesh: 10/10 checks pass, all nodes consistent post-test. Formula drift is self-policing because takeout and `post_files` (`src/files/routes.rs:239`) share the same salted-hash formula — any divergence would fail every takeout integrity check.

#### 2.2: Manifest Emission [x]
- [x] `build_takeout_manifest` collects metadata directly from `takeout_inodes_{id}` JOIN `data_blocks` after materialization completes (no per-file accumulation needed; status filter excludes failed files)
- [x] `TakeoutManifest::to_archive_bytes` is the canonical wire format — single source of truth for pretty-printed JSON encoding
- [x] `create_archive` emits `manifest.json` as the first tar entry via `tar::Builder::append_data` before iterating content entries
- [x] Content entries are wrapped under `files/` prefix (`ARCHIVE_FILES_PREFIX`) so manifest and content are cleanly separated
- [x] `MANIFEST_VERSION = 1` constant introduced; `version` field embedded in every emitted manifest

**Acceptance:** Extended `takeout-happy-path` orchestrator scenario deserializes the archive into the production `TakeoutManifest` type and asserts: first tar entry is `manifest.json`; `version == 1` (literal pin, not constant — circular checks aren't real verification); `takeout_id`, `total_files`, `total_folders`, and `total_bytes` match reality; per-file manifest size matches uploaded size; per-file `file_hash` reconstructed test-side as `blake3(content ∥ source_data_block_id_bytes)` matches manifest exactly; content lives under `files/<path>` prefix. Verified on a fresh 3-node mesh: 21/21 checks pass, all nodes consistent across all 16 consensus tables post-test.

### Phase 2.3: Bounded Concurrency and Database Contention Guarantees [ ]

Current materialization spawns `BATCH_SIZE = 10` concurrent tokio tasks (`src/db/takeout.rs::materialize_all_files`) while the r2d2 pool is configured with `max_size = 8` (`src/main.rs:47,65`). Two tasks in every batch block on `pool.get()` until earlier tasks release, so the intended 10-way parallelism is actually 8-way with forced serialization. Worse, during any batch the pool is fully saturated, which means any concurrent HTTP request — status polling, file reads, FileProvider enumeration — must wait for a takeout batch to complete before acquiring a connection. Under dogfood load this surfaces as apparent UI freezes and sporadic request timeouts whose root cause is entirely internal.

The rigid batch-wait-batch-wait pattern also gates the next batch on the slowest file in the current batch. A single large file stalls nine fast ones.

This phase replaces the pattern with an idiomatic Tokio streaming pipeline using `FuturesUnordered` / `buffer_unordered`, sizes concurrency as a function of pool capacity with explicit reservation for other routes, and audits the rest of the codebase for the same shape.

#### 2.3.1: Streaming Materialization Pipeline [x]
- [x] Replaced the batched `materialize_all_files` loop with a `tokio::task::JoinSet` streaming pipeline that primes up to `N` reconstruction futures and replenishes as each one completes
- [x] `N` derives from `db_worker_concurrency_budget()` (see 2.3.2)
- [x] **Inverted the per-task DB pattern**: workers no longer acquire conns for status writes. The coordinator holds a reserved conn (mirroring `consensus_queue::batch_processor` at `src/consensus/queue.rs:240`) and applies status updates serially as worker results arrive. Eliminates the entire pool-acquisition path for status writes; only reconstruction-internal acquires remain.
- [x] Per-file commit per status update for crash durability
- [x] `BATCH_SIZE` constant and `get_pending_files_batch` paginator removed; `list_pending_files` returns the full set in one query (acceptable in our reserved-conn architecture, with a soft warning above 50k files)

**Acceptance:** `takeout-happy-path` orchestrator scenario file count bumped from 3 to 30, exercising the streaming pipeline beyond a single batch boundary. Verified end-to-end on a fresh 3-node mesh.

#### 2.3.2: Pool Capacity Reservation [x]
- [x] `DB_POOL_MAX_SIZE` (32) lives in `src/db/mod.rs` as the single source of truth; both `Pool::builder()` sites in `src/main.rs` reference it
- [x] `db_worker_concurrency_budget()` derives the worker concurrency cap as `pool − reserved (2) − route headroom (8)`. Workers and the materialization pipeline call this rather than hardcoding numbers; bumping `DB_POOL_MAX_SIZE` automatically scales worker capacity without re-tuning every site
- [x] Reservation accounting documents the two known long-held conns: `consensus_queue::batch_processor` and the takeout coordinator

**Acceptance:** Single-source pool size; no remaining hardcoded `BATCH_SIZE = 10` in the takeout pipeline.

#### 2.3.3: Audit Other Batch-Spawn Sites [ ]
- [ ] Grep for `tokio::spawn` inside loops and `.collect::<Vec<_>>()` patterns across `src/`; identify any site that launches N tasks where N is not bounded relative to pool size
- [ ] Known candidates to check: orphan cleanup (`src/files/jobs.rs`), fragment distribution (`src/files/distribution.rs`), fragment health checks, FileProvider enumeration workers
- [ ] Retrofit any hit with the streaming pipeline pattern from 2.3.1 or document why it's benign (e.g. non-DB work, explicit semaphore already in place)

**Acceptance:** Inventory of spawn-loop sites is captured in commit body; each is either refactored or justified. No remaining site spawns more concurrent DB-using tasks than `DB_POOL_CONCURRENCY_BUDGET`.

### Phase 2.4: Crash-Resume of In-Progress Takeouts [ ]

If `execute_takeout_materialization` exits non-cleanly (process kill, OOM, host reboot) while a takeout is in `Materializing` status, the takeout never reaches `Ready` and never produces an archive. Per-file status rows committed before the crash are preserved on disk, but no automation picks up where it left off — the user has to wait for `expires_at` cleanup and retry from scratch. This phase adds startup-time resume of partial takeouts and a stalled-takeout reaper for the case where the owner node never returns.

#### 2.4.1: Owner-Node Startup Resume [ ]
- [ ] On owner-node startup, scan `takeouts` for rows where `owner_node_id = self AND status = 'materializing'`
- [ ] For each, re-enter `execute_takeout_materialization` starting at the `materialize_all_files` phase; existing per-file `Success` rows are skipped, `Pending` rows resume reconstruction
- [ ] Emit a tracing event distinguishing fresh vs resumed materialization

**Acceptance:** Orchestrator scenario kills the owner node mid-batch during a 30-file takeout, restarts the node, and observes the takeout reach `Ready` with all 30 files present in the archive without re-doing files that completed before the kill.

#### 2.4.2: Stalled-Takeout Detection and Reaping [ ]

The hard part of this phase is defining "stalled" — wall-clock elapsed time is a poor signal because legitimately large takeouts can legitimately run for hours. Time-based reaping risks killing healthy work.

Better signals to design around:

- **Owner unreachable**: peer nodes detect the owner has not participated in consensus / RPC / heartbeats for an extended period. Existing node health infrastructure (metrics, gossip) can drive this.
- **Progress stalled**: per-file completion rate drops to zero unexpectedly. Requires emitting progress heartbeats (e.g. periodic `update_takeout_progress` consensus txns or local last-progress timestamps surfaced via a status route) — neither exists today.
- **Explicit liveness**: owner publishes a periodic "still working" signal; absence triggers reaping.

Implementation should pick one or combine, not bake in a fixed time threshold. Threshold values, if any, should be tunable.

- [ ] Define a "stalled" predicate based on the signals above and document it in the spec before implementing
- [ ] If owner is confirmed unreachable / stalled, mark the takeout `Cancelled` via consensus so user can retry without waiting for `expires_at`

**Acceptance:** Manual: kill owner node permanently; takeout transitions to `Cancelled` once the predicate fires; user can initiate a new takeout without waiting for the 24h expiration window. A separate test confirms a long-running but healthy takeout (large file count, slow reconstruction) is **not** prematurely reaped.

### Phase 3: Import Backend MVP [ ]

Backend infrastructure for ingesting takeout archives. Each sub-phase is independently shippable behind a feature flag; the endpoint is only exposed to users after 3.7 lands.

#### 3.1: Imports Table + Consensus Handlers + Status Routes [x]
- [x] `imports` table added directly to `src/db/shared.rs` (no migration framework yet — system not deployed). 4-state lifecycle: Pending → Importing → Completed/Failed. Cancelled deferred to Phase 5 when DELETE endpoint lands.
- [x] `ImportStatus` and `ImportRecord` in `hopnet-common/src/db.rs` with `#[typeshare]`; `ToSql`/`FromSql` impls in `common/src/db_impl.rs`
- [x] `ImportPayload` and `ImportStatusPayload` in `src/db/imports.rs`; consensus handlers `CreateImportHandler` and `UpdateImportStatusHandler` in `src/takeout/handlers.rs` (single update handler covers all status transitions, mirrors takeout)
- [x] Eligibility check `is_import_eligible(conn, user_id)`: blocks if user has any blocking import (`status IN Pending/Importing/Completed`) OR any existing inodes. Inode check enforces v1 "empty user tree" precondition since merge semantics for login→import are not yet designed
- [x] `POST /takeout/import` stub: route-level `is_import_eligible` fast-reject (429), then consensus `create_import` txn (handler re-checks for network-wide enforcement)
- [x] `GET /takeout/import` returns `Option<ImportRecord>` singleton (no `/{id}` route — users have at most one current import)
- [x] Auth tests in `src/consensus/tests/authorization.rs`: `test_dual_authorized_create_import`, `test_dual_user_unauthorized_create_import`, `test_dual_node_unauthorized_create_import`

**Acceptance:** New orchestrator scenario `import-create-active-conflict` runs against a fresh 3-node mesh: POST creates a Pending row, all nodes report the same record via GET (consensus propagation), and concurrent POSTs (both same-node and cross-node, same user) are rejected with 429. Auth unit tests pass (`cargo test --lib consensus::tests::authorization` covers 14 cases including the 3 new import ones). Orchestrator harness now supports auto mesh management — `orchestrator test <name>` (no `--mesh-id`) creates a fresh mesh, runs the test, deletes the mesh on pass and leaves it up on fail.

#### 3.2: Shared File/Folder Creation Helpers [x]
- [x] Extract `create_file_with_fragments(user_id, path, bytes) -> Result<DataBlockId>` from `post_files` as an in-process helper (lives in `src/files/helpers.rs` alongside the underlying `assemble_file_inode` / `prepend_missing_parents` / `build_upload_attestation` / `submit_inodes` primitives the multipart route was decomposed into)
- [x] Extract `create_folder(user_id, path) -> Result<()>` analogously from the folder-creation route
- [x] Rewire existing HTTP routes to call the helpers; behavior unchanged (verified — `post-files-consensus-shape`, `mixed-files-and-folders-one-request` plus 9 broader regressions including `file-upload-consistency`, `multi-size-file-consistency`, `multi-user-isolation`, `multi-user-sharing`, `documentprovider-write-consistency`, `fragment-distribution`, `consensus-queue-burst`, `takeout-happy-path`, `import-create-active-conflict` all passed post-refactor)

**Acceptance:** Full existing orchestrator test suite passes unchanged; `post_files` and folder-creation routes produce byte-identical consensus history before and after the refactor. Phase 3.2 baseline scenarios `post-files-consensus-shape` and `mixed-files-and-folders-one-request` (added in commit A pre-refactor) must continue passing post-refactor.

#### 3.3: Upload Endpoint + Manifest Read + Quota Check [x]
- [x] `POST /takeout/import` route: accepts multipart tar.gz, streams to owner-node staging directory under `{fragments_dir}/imports/{import_id_simple}/upload.tar.gz`
- [x] Streaming tar reader consumes first entry, validates `manifest.json` schema and `version` (sync `tar::Archive<flate2::read::GzDecoder>` invoked via `tokio::task::spawn_blocking`; reader lives at `src/takeout/archive.rs::read_manifest_from_archive`)
- [x] Quota check: `manifest.total_bytes × 3 + STORAGE_SAFETY_MARGIN_BYTES ≤ sum(validator_storage_available)`. Aggregated across the active validator set via `db::imports::get_total_validator_storage_available`. Bootstrap fallback (owner filesystem × validator count) handles fresh meshes whose ~10 min metrics cron hasn't fired yet.
- [x] On manifest-parse or quota failure, return `400` or `507`, delete staging, do NOT submit `create_import`. Each error variant of `ImportUploadError` (in `src/takeout/import.rs`) maps to its specific status code.
- [x] On success, submit `create_import` consensus txn and stop (no extraction yet)
- [x] **Session "pinning" via owned clone**: handler captures the user's `SessionEntry` from `state.get_session(user_id).await` once at the start of `process_upload`. The clone lives in the handler's stack frame through every `?` exit and final return; RAII drops it on any path. No global pin map, no release lifecycle, no leak risk. 3.4/3.5 (still in the same handler scope) read from the local clone. 3.7 owner-restart resume requires re-login (the in-process clone doesn't survive a process boundary) — deferred.

**Acceptance:** Curl-level test: valid manifest + ample quota → `imports` row created, `201` returned. Over-quota archive → `507` returned, no consensus row, staging cleaned. Missing manifest → `400`, no consensus row. Session-pin test (logout-mid-import) deferred to Phase 3.4/3.5 since it requires extraction to consume the cloned session. Phase 3.3 acceptance covers the upload pipeline shape via the orchestrator scenarios `import-upload-happy-path`, `import-upload-version-rejected`, `import-upload-missing-manifest`, `import-upload-quota-exceeded`.

#### 3.4: Extraction + Per-File Hash Verification [x]
- [x] Extend the upload handler to consume remaining tar entries into staging after `create_import` — bg `tokio::spawn`'d after 201 returns; cloned `SessionEntry` moves into the task. `run_extraction` in `src/takeout/import.rs` owns the workflow; tar walk runs inside `tokio::task::spawn_blocking` since `tar` + `flate2` are sync.
- [x] Populate `import_paths_{id}` from manifest entries with `status = 'pending'` — `src/db/import_paths.rs` provides `create_import_paths_table`, `insert_path_pending`, `mark_path_failed`, `list_paths`. Persistent `CREATE TABLE IF NOT EXISTS` (not `TEMPORARY`) so the table survives owner restart for the Phase 3.7 resume sweep.
- [x] For each extracted file: compute `blake3(bytes ∥ source_data_block_id)`, compare to manifest `file_hash` — formula matches `src/takeout/materialization.rs:111` (verified by an orchestrator scenario synthesizing manifests with the takeout-side hash).
- [x] Mark mismatched rows `failed` with `error_code = "hash_mismatch"`; delete the corrupted staging file — also catches `wrong_prefix` (entry not under `files/`) and `not_in_manifest` (file present in tar but absent from manifest).
- [x] Bg task flips `imports.status` Pending → Importing on entry; terminal flips deferred to 3.5/3.7. The `ImportStatus` enum stays 4-variant (no `Extracting`); both extraction and creation block writes (3.6 gating list) and look identical to clients.
- [x] Debug route `GET /takeout/import/paths` returns the per-import path table for the current import on the owner node only (404 from any non-owner). Phase 3.7's aggregate status route supersedes.

**Acceptance:** Two new orchestrator scenarios pass: `import-extraction-happy-path` (5-entry archive, all hashes correct → 5 Pending rows on owner, status flips to Importing, no errors, non-owner /paths returns 404) and `import-extraction-hash-mismatch` (one file's tar bytes diverge from manifest hash → that row Failed/`hash_mismatch`, peer files + folders remain Pending, status remains Importing). Real cross-mesh round-trip is Phase 5 acceptance — synthetic archive bytes are sufficient for the 3.4 hash-formula contract since the test helper `compute_archive_file_hash` shares the formula with the takeout side.

#### 3.5: Per-File Creation Pipeline [x]
- [x] Walk `import_paths_{id}` where `type = 'folder'` ordered by path depth (slash count), invoke `create_folder` helper. `run_creation_phase` in `src/takeout/import.rs` chains directly off `run_extraction`'s tail; depth-ascending so parent commits land before child reads.
- [x] Walk `import_paths_{id}` where `type = 'file'`, invoke `create_file_with_fragments` helper. Each file's already-extracted plaintext bytes streamed from `{staging}/files/<user-path>` into the helper.
- [x] Sequential execution only in 3.5; parallelism added in Phase 5. `submit_inodes` awaits commit-ack so backpressure is implicit; Phase 5 will swap in JoinSet-based parallel-N=4 with mpsc channel-coordinator fan-in.
- [x] Update each row with `status` and `error_code` as appropriate. `mark_path_imported` (added in `src/db/import_paths.rs` alongside `mark_path_failed`) clears stale error metadata; per-call failures use `error_code = "create_folder"` / `"create_file"` with status code in the message.
- [x] Submit `complete_import` txn when all rows are terminal. Reuses 3.4's `submit_status_update` helper for the Importing → Completed flip; staging dir removed via `tokio::fs::remove_dir_all` after the terminal commit. `Failed` terminal reserved for catastrophic infra errors (DB pool dead) — those leave the import at `Importing` for the 3.7 owner-restart sweep.

**Acceptance:** Two new orchestrator scenarios pass: `import-creation-happy-path` (5-entry archive, all hashes correct → status reaches Completed on every node, all 5 path rows Imported, every file queryable + byte-exact via `GET /files/<path>` on every node) and `import-creation-mixed-failure` (one file's tar bytes diverge from manifest hash → that row stays Failed/`hash_mismatch`, peer rows Imported, status reaches Completed, surviving files queryable, corrupted file not queryable). Path-encoding bug found during testing and fixed: `encrypt_path` collapses single-segment paths to `/` (root placeholder); `run_creation_phase` now prepends `/` to all relative paths from `import_paths_{id}` before calling the helpers.

#### 3.6: Write Gating Middleware [x]
- [x] Per-user import gate `import_gate` middleware in `src/takeout/import_gate.rs`. Reads `user_id` from request extensions (auth/device-token middleware populates upstream); calls `imports::has_active_import` (`Pending | Importing` rows for the user); returns 409 if active. Distinct from `is_import_eligible` — eligibility blocks Completed too (v1 forbids re-import) but gating allows writes once Completed.
- [x] Applied to write-path routes via per-router read/write `Router::merge` split: `files::routes::router` (POST/PATCH/DELETE on `/`), `shares::routes::router` (POST `/`, DELETE `/incoming/{id}`, POST `/{id}/accept`, DELETE `/file/{inode_id}`), FileProvider create/modify/delete (split inline in `main.rs`), DocumentProvider writes (split inline in `documentprovider::routes::router`). Reads bypass the gate entirely — gate attachment is explicit, no in-middleware method peek.
- [x] 409 Conflict on hit; passes through otherwise. Auth context populated by `auth_middleware` or `device_token_auth_middleware` upstream of the gate layer.

**Acceptance:** Orchestrator scenario `import-write-gate` passes: cross-node POST /files mid-import returns 409, succeeds after Completed terminal flip propagates to peer. Sanity-confirmed via `import-creation-happy-path`, `import-create-active-conflict`, `file-upload-consistency`: gate doesn't false-409 either the import endpoint itself or normal users with no imports.

#### 3.7: Status Endpoint + Owner-Node Resume [x]
- [x] `GET /takeout/import/status` returns aggregate `ImportPathCounts { total, pending, imported, skipped, failed }` for the user's current import on the owner node (404 from any non-owner — table is owner-local). Singleton model retained from 3.1 — no `/{id}` history route. `count_paths_by_status` helper in `src/db/import_paths.rs` runs a single `SELECT status, COUNT(*) ... GROUP BY status` over the per-import table.
- [x] Owner-startup scan + lazy resume via auth-event hooks. `takeout::jobs::scan_at_startup` reads `imports WHERE status IN (Pending, Importing) AND owner_node_id = self`, filters to status == Importing AND `import_paths_{id}` table seeded with at least one Pending row, stashes `(user_id, import_id)` into `TakeoutRuntime::resume_registry`. `maybe_resume_for_user` is fired from three session-establishment paths: macOS keychain auto-load (`main.rs`), `auth::sign_in` (POST /login), and the device-token middleware (`devices/auth.rs`). The hook drains the registry and spawns `run_creation_phase` which is idempotent — only walks rows with `status = Pending` so already-`Imported` rows skip naturally.
- [x] Mid-extraction recovery is **not** v1 — scan filters out imports stuck at status `Pending` (extraction never reached the Importing flip). Those require user re-upload. Phase 6 if pain emerges.
- [x] Per-module `TakeoutRuntime` introduced on AppState (single Arc field) instead of growing AppState's flat list. Holds `resume_registry` + `barriers` (test-only). Test barriers refactored: shared `crate::barriers::Barriers` primitive + inventory-registered subsystem accessors → URL `/test/barriers/{subsystem}/{name}/...`.

**Acceptance:** Three orchestrator scenarios pass: `import-status-counts` (mixed-failure import → counts.imported=4, counts.failed=1, counts.total=5; non-owner returns 404), `import-resume-after-restart` (hold the takeout-side `before_import_creation_walk` barrier on owner pre-upload → upload → status flips to Importing → `docker stop/start` owner → re-login via stored passphrase → resume hook drives creation walk to Completed; all 5 path rows Imported; files queryable byte-exact on every node), and existing `import-create-active-conflict` continues to pass after the URL change for consensus barriers (`/test/barrier/{name}/...` → `/test/barriers/consensus/{name}/...`).

### Phase 4: Import Frontend MVP [x]

Lifted scope from "single ImportPane" to a generalized **WelcomeModal onboarding flow** with the import drag-drop as one step. Reasoning: import is a one-shot lifecycle event for most users; a permanent sidebar entry burns visual real estate. Surfacing it through onboarding (and a banner that re-opens onboarding while steps remain) exposes the affordance only when actually needed. The architecture is pluggable so future onboarding steps (profile, backup, multi-device) drop in without rewiring.

**Backend prep [x]** — replicated `users.onboarding_flags` bitfield as the cross-device source of truth:
- [x] DDL change: `users.onboarding_flags INTEGER NOT NULL DEFAULT 0` (single source of truth in `src/db/shared.rs`; fresh-network only, no migration tx)
- [x] `OnboardingFlags` newtype in `common/src/users.rs` with `ToSql`/`FromSql` impls in `common/src/db_impl.rs`; `OnboardingFlag` typeshare enum is the closed set exposed to frontend (`ImportOffered`, `ImportCompleted`)
- [x] `SelfUserInfo` typeshare struct returned only from `GET /users/me`; list endpoint keeps `PublicUserInfo` (no peer leak of personal flags)
- [x] `update_user_onboarding` consensus tx (handler + payload + `update_user_onboarding_tx` in `src/db/users.rs`); applied as `flags = (flags | set_flags) & ~clear_flags` (idempotent, additive when `clear_flags = NONE`)
- [x] `PUT /users/me/onboarding` route accepts `{set: OnboardingFlag[], clear: OnboardingFlag[]}` (typed enum prevents stringly-typed boundary)
- [x] Auto-set `IMPORT_OFFERED | IMPORT_COMPLETED` on terminal `Completed` from `run_creation_phase` via shared `submit_onboarding_update` helper in `src/users/helpers.rs`
- [x] Roundtrip handler tests: `test_onboarding_payload_roundtrip`, `test_onboarding_flag_iter_collect` (113 lib tests pass)

**Frontend [x]** — onboarding architecture in `frontend/src/lib/panes/onboarding/`:
- [x] `WelcomeModal.svelte` orchestrator: checklist view ↔ step-page view; auto-opens post-login when any onboarding bit unset; reopens via banner; `Mark all as done` footer button with two-stage inline confirmation; `All set` view when fully complete
- [x] `OnboardingChecklistItem.svelte` — thin wrapper over `Button` `card` variant (new), maps step status (`todo` / `active` / `done`) to trailing icon + colour; reuses Button hover/focus/disabled chrome
- [x] `ImportStep.svelte` — page content embedding `ImportDropZone` / `ImportProgressCard` / `ImportSummaryCard` based on `importStatusStore` state; `Mark as done` button on idle dismisses without uploading
- [x] `steps.ts` registry — pluggable step list, derives status from `(user, importState)`; `computeIncompleteSteps` is the truth source for banner + auto-open
- [x] `Banner.svelte` primitive (info / warning / success); `Modal` primitive gained an optional `onBack` prop (top-left, mirrors close button) so multi-step modals route the back affordance through chrome rather than per-step footer
- [x] `Button.svelte` `card` variant — full-width two-row button (text + subtitle + trailing icon/cta); reused by checklist items
- [x] `importStatusStore` — 1 Hz subscriber-counted polling store; pulls `GET /takeout/import` (always) and `GET /takeout/import/status` (owner only — 404 on non-owner mapped to `null` counts); auto-stops on terminal flip; manual `refresh()` on upload submit
- [x] `writesGatedStore` derived store; write-affordance gating wired in `BrowsePane` (Upload, New Folder, Delete, Share), `IncomingSharesList` (Accept, Decline), `ShareDetailsModal` (Leave Share); affordances disable + swap tooltip while import active. Backend `import_gate` middleware remains the security boundary; this is UX polish only.
- [x] `Interface.svelte` integration: subscribes to user store + import store; banner at top of main content (variant flips: import-active → import-progress info; otherwise onboarding-incomplete prompt → opens modal); `WelcomeModal` mount conditional on `showWelcomeModal`
- [x] Quota (HTTP 507) and active-import (HTTP 409) surfaces in `ImportStep` upload handler with explicit messages
- [x] No SetupSM changes — genesis flow ends with reload as before; post-reload Interface mount fires the WelcomeModal auto-open via `onboarding_flags`. Same path for join flow: server-replicated bits mean cross-device suppression works for free.
- [x] Stories for every new component: Banner (3 variants), Modal (with back button), Button (card variants × 4), ImportDropZone (4 states), ImportProgressCard (4 states), ImportSummaryCard (4 states), OnboardingChecklistItem (3 statuses), WelcomeModal (5 views including step pages)
- [x] typeshare regen + `pnpm check` clean (no new errors, baseline preserved)

**Acceptance:** Manual E2E pending — from fresh install, complete genesis setup, observe WelcomeModal auto-open with `Import existing data` checklist item; clicking opens ImportStep page with drag-drop; drop archive, banner appears at top of Interface, write affordances disable; counts tick to terminal; summary card shows imported/failed; reload — modal does not reappear. Cross-device: log in on second device same user, modal does not appear (`IMPORT_OFFERED` replicated). 507 quota path: oversize archive shows inline error.

### Phase 5: Import Polish [ ]
- [ ] Parallel file creation (default 4 concurrent, configurable via env var)
- [ ] `GET /takeout/import/{id}/report` endpoint with pagination, grouped by `error_code`
- [ ] Frontend failure list UI driven by the report endpoint
- [ ] `DELETE /takeout/import/{id}` cancellation endpoint (marks status `cancelled`, no rollback)
- [ ] Manifest streaming parser to handle multi-MB manifests without full-in-memory load

**Acceptance:** Orchestrator round-trip test: generate takeout on mesh A (50+ files), nuke, import on mesh B, diff decrypted bytes byte-for-byte. Parallel pipeline completes faster than sequential baseline; no new failure modes introduced.

### Phase 6: Deferred [ ]

Out of scope for v1 ship. Revisit when dogfood surfaces concrete need.

- [ ] Timestamp preservation (requires `post_files` API extension to accept explicit `created_at` / `modified_at`)
- [ ] Selective import (subfolder picker UI + manifest subset handling)
- [ ] Login→import with collision handling (skip / overwrite / rename or `Imported/<date>/` namespacing)
- [ ] Retry-failed-files action in UI leveraging `error_code.retryable` flag
- [ ] Sharing state preservation (requires source→target `data_block_id` mapping layer)
- [ ] Batch-import consensus transaction type (bulk inode/data_block creation) for large-archive throughput
- [ ] Incremental import (apply only changes since last import)
- [ ] WebSocket or SSE real-time progress streaming in place of polling

### Onboarding follow-ups

Phase 4 introduced the `users.onboarding_flags` bitfield + `WelcomeModal` checklist architecture. New onboarding steps drop in by adding a bit to the `OnboardingFlag` enum (`common/src/users.rs::OnboardingFlag::bit`) and a step entry to `frontend/src/lib/panes/onboarding/steps.ts`. Tracked separately from import:

- [ ] **Profile setup** step — `ProfileSet` flag; auto-set in `update_user_profile` handler when first/last name first written; step component links into existing `AccountsPane` profile editor
- [ ] **Recovery passphrase verification** step — `BackupVerified` flag; set in SetupSM `PassphraseVerify` (creator path) and via post-login passphrase-display modal (joiners). Requires nailing down the joiner's passphrase derivation/storage first.
- [ ] **Multi-device pairing** step — `MultiDevicePrompted` flag; nudges genesis owner to add a second device for resilience.
- [ ] **Settings re-onboarding entry** — clear individual onboarding bits via UI to re-trigger steps (backend already supports clear-array on `PUT /users/me/onboarding`).

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

### Takeout
- Incremental exports (changes since last takeout)
- Selective folder/file export
- Multiple format support (ZIP, 7z)
- Metadata export (sharing history, versions)
- WebSocket-based real-time progress updates
- Per-file SHA-256 or Blake3-unsalted entries (requires extending the privacy-invariant policy; do not add without a separate threat-model review)