# RFC-011: Photos Module

## Summary

A self-contained photo management system that leverages HopNet's distributed storage (`data_blocks`, Reed-Solomon fragments, inter-node replication) and encryption primitives without coupling to the filesystem's inode layer. Photos are a distinct domain — not files in a folder hierarchy — with their own metadata model, operation history, shared library coordination, and lifecycle management.

The consensus layer stores only opaque encrypted data — no plaintext photo metadata. All queryable metadata lives in a client-side sidecar database, populated by decrypting consensus-tracked encrypted blobs. This preserves HopNet's zero-trust architecture while enabling rich query patterns (timeline views, map views, camera filters, face grouping) entirely on the client.

## Motivation

HopNet's distributed, encrypted, per-file-key-sharing architecture is well suited to a photo management experience that competes with Apple Photos, Google Photos, and Immich. The key differentiators:

- **End-to-end encryption**: Unlike Immich (plaintext server storage) or Google Photos (provider has access), HopNet encrypts per-photo with per-user key wrapping. Shared library members each hold their own decryption keys. The consensus database exposes zero plaintext metadata — not even dates or dimensions.
- **No central server**: Photos are replicated across the user's own devices via Reed-Solomon fragments. No single point of failure, no cloud dependency.
- **True multi-user shared libraries**: Any member can add or remove photos. There is no "owner" — the shared library is a peer relationship, enabled by the existing file-level sharing and key distribution infrastructure.
- **Clean separation from filesystem**: Photos don't pollute the file browser with thousands of images. Screenshots and documents stay in the filesystem; photos live in the photos module. Users who want filesystem access to photos can export explicitly.

## Design Philosophy

### Separate Domain, Shared Storage

The photos module maintains its own tables for photo identity, metadata, albums, and history. It references `data_blocks` for actual byte storage and `file_access` for per-user encryption keys, but does **not** create inodes. This means:

- Photos don't appear in the filesystem or FileProvider unless explicitly exported
- The module can be compiled out entirely for deployments that don't need it
- No path management, folder hierarchy, or naming collision concerns

### Zero-Trust Metadata: Encrypted Consensus, Decrypted Sidecar

The consensus database stores photo metadata as an opaque encrypted blob. No plaintext queryable columns exist on any consensus-tracked table — a compromised node learns nothing about photo content, dates, locations, or camera information beyond what `data_blocks` already exposes (file size and upload timestamp via UUIDv7).

Rich query patterns are supported through a **client-side sidecar database**: a local, non-replicated store that the client populates by decrypting the consensus-tracked metadata blobs. Timeline sorting, date filtering, map views, camera grouping, and face search all query against this sidecar. The sidecar is ephemeral — it can be rebuilt at any time from the encrypted consensus state.

Metadata is encrypted per-photo using the same ECDH + ChaCha20-Poly1305 pattern as `file_access`. Each photo has its own ephemeral key pair for metadata encryption, and the metadata key is wrapped per-user — enabling independent sharing of individual photos to any context (personal library, shared library, shared album) without re-encrypting the metadata blob.

**Initial hydration** (building the sidecar from scratch) requires one X25519 ECDH operation per photo to unwrap the metadata key, then one ChaCha20-Poly1305 decrypt per metadata blob. ECDH performance should be validated during implementation, but at ~50-80μs per operation on modern hardware, a 50k photo library should hydrate in a few seconds. After initial hydration, incremental updates process only new consensus transactions.

### Operation Log, Not Version Chain

Photo history is tracked as an append-only operation log where each entry describes what happened (content edit, metadata change, album modification, deletion) and carries enough information to reverse it. Content edits record prior and new `data_block_id`s explicitly, keeping the data_blocks table as the single source of truth for replicated bytes. Metadata-only operations carry a small diff payload.

### Modular Orphan Cleanup

The existing orphan data block cleanup checks only for inode references. The photos module must register itself as an additional reference provider so that data blocks referenced by photos or retained by operation history are not prematurely purged.

## Schema

### Consensus-Tracked Tables

These tables are replicated across all nodes via consensus. They contain **no plaintext photo metadata**.

#### Photos

```sql
CREATE TABLE photos (
    id               TEXT PRIMARY KEY,     -- UUIDv7 (creation timestamp encoded)
    library_id       TEXT,                 -- NULL = personal library
    uploaded_by      INTEGER NOT NULL,     -- user who added this photo
    encrypted_metadata       BLOB NOT NULL,-- ChaCha20-Poly1305 encrypted metadata
    metadata_nonce           BLOB NOT NULL,-- 12-byte nonce for metadata decryption

    -- Soft delete: NULL = active, set = tombstoned, 30-day retention window.
    -- Periodic cleanup hard-deletes the row and cascades to photo_resources
    -- once retention expires.
    deleted_at       TEXT,                 -- ISO 8601, NULL when active
    deleted_by       INTEGER,              -- user who deleted (FK users)

    FOREIGN KEY (uploaded_by) REFERENCES users(user_id),
    FOREIGN KEY (deleted_by) REFERENCES users(user_id),
    FOREIGN KEY (library_id) REFERENCES shared_libraries(id)
);

CREATE INDEX idx_photos_library ON photos(library_id);
CREATE INDEX idx_photos_deleted ON photos(deleted_at) WHERE deleted_at IS NOT NULL;
```

The `encrypted_metadata` blob contains all photo metadata: date taken, dimensions, orientation, media type, duration, camera make/model, GPS coordinates, EXIF data, **and cross-asset grouping** (`group_id`, `group_type`, `group_index`, `is_group_pick`). None of this is queryable at the consensus level. The photo ID (UUIDv7) encodes upload timestamp, which is the only temporal signal visible to nodes.

Bytes for a photo (original, edited variant, paired Live Photo video, thumbnails, etc.) live in `photo_resources` (below). A photo has at minimum one resource (the original). The `photos` row carries identity and tombstone state only.

##### Group Types

| Value | Name | Description |
|-------|------|-------------|
| 0 | `burst` | PhotoKit burst frames sharing a `burstIdentifier` |
| 1 | `stack` | Stacked / Live Text grouping |
| 2 | `panorama_frames` | Source frames of a panorama |
| 3 | `hdr_bracket` | Bracketed exposures of an HDR composite |

A photo not part of any group has `group_id = NULL` (inside the encrypted blob). Group membership is NOT observable at the consensus level — `group_id`, `group_type`, `group_index`, and `is_group_pick` are all inside `encrypted_metadata` (amended from the original plaintext design: no consensus query needs group awareness — deletion expands to a batch tx constructed client-side from sidecar queries, and burst rollup is a sidecar-only query at `idx_sidecar_group` — so the plaintext columns leaked structural correlation and photography habits via `group_type` without any offsetting consensus use). The original "future work may encrypt group_id" hedge is done.

#### Photo Metadata Access

```sql
-- Per-user metadata decryption keys, following the file_access pattern.
-- Each photo's metadata has a symmetric key; this table wraps that key
-- per-user via ECDH so any user with access can decrypt the metadata blob.
CREATE TABLE photo_metadata_access (
    photo_id               TEXT NOT NULL,
    user_id                INTEGER NOT NULL,
    ephemeral_pubkey       BLOB NOT NULL,     -- 32-byte X25519 ephemeral pubkey
    encrypted_metadata_key BLOB NOT NULL,     -- 48 bytes (32 key + 16 auth tag)

    PRIMARY KEY (photo_id, user_id),
    FOREIGN KEY (photo_id) REFERENCES photos(id),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);
```

This mirrors the `file_access` table pattern. When a photo is shared to a new context (shared library, shared album, individual share), the per-photo metadata key is wrapped with each new recipient's pubkey and a row is inserted. The metadata blob itself does not change.

#### Photo Resources

A single photo can have multiple byte streams associated with it: the original capture, an edited variant, the paired Live Photo video, RAW sensor data, thumbnails, and so on. PhotoKit exposes these as `PHAssetResource` entries on a single `PHAsset`. The `photo_resources` table maps a photo to its constituent data blocks:

```sql
CREATE TABLE photo_resources (
    photo_id         TEXT NOT NULL,
    resource_type    INTEGER NOT NULL,     -- see Resource Types below
    data_block_id    TEXT NOT NULL,         -- FK to data_blocks

    PRIMARY KEY (photo_id, resource_type),
    FOREIGN KEY (photo_id) REFERENCES photos(id),
    FOREIGN KEY (data_block_id) REFERENCES data_blocks(id)
);

CREATE INDEX idx_photo_resources_data_block ON photo_resources(data_block_id);
```

##### Resource Types

| Value | Name | Description |
|-------|------|-------------|
| 0 | `original` | Unmodified capture as taken (HEIC, JPEG, ProRAW, MOV for video) |
| 1 | `edited` | User-edited current version (post-crop/filter/adjustment) |
| 2 | `paired_video` | Live Photo motion track (MOV alongside HEIC still) |
| 3 | `adjustment_data` | PhotoKit `PHAdjustmentData` blob for reversible edit reconstruction |
| 4 | `raw_alternate` | RAW sensor data paired with a JPEG original (PhotoKit `alternateRepresentation`) |
| 5 | `thumbnail_small` | ~256px gallery thumbnail |
| 6 | `thumbnail_medium` | ~1024px detail preview |
| 7 | `edited_paired_video` | Edited render of a Live Photo's motion track (accompanies `edited`; PhotoKit `fullSizePairedVideo`) |

The "primary display" resource for gallery view is `edited` if present, otherwise `original`. Clients enforce this at query time against the sidecar.

All resources except thumbnails are required to be supplied by the client at upload time when they exist on the source asset — the daemon ingesting from PhotoKit must enumerate `PHAsset.assetResources` and submit every applicable resource. Thumbnails are generated client-side from whichever resource is the primary display. Each resource is encrypted with its own per-data-block key and replicated via the standard fragment distribution path. The server never sees raw image data.

#### Shared Libraries

```sql
-- Shared library definition
CREATE TABLE shared_libraries (
    id               TEXT PRIMARY KEY,     -- UUIDv7
    encrypted_name   BLOB NOT NULL,        -- ChaCha20-Poly1305 encrypted library name
    name_ephemeral_pubkey BLOB NOT NULL    -- X25519 ephemeral pubkey for name decryption
);

-- Library membership (N-way, no owner)
CREATE TABLE shared_library_members (
    library_id       TEXT NOT NULL,
    user_id          INTEGER NOT NULL,

    PRIMARY KEY (library_id, user_id),
    FOREIGN KEY (library_id) REFERENCES shared_libraries(id),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);
```

There is no owner column. All members have equal standing — any member can add photos, remove photos, or invite new members. The `uploaded_by` field on `photos` records provenance for activity feeds but confers no special permissions.

#### Albums

```sql
CREATE TABLE photo_albums (
    id               TEXT PRIMARY KEY,     -- UUIDv7
    library_id       TEXT,                 -- NULL = personal album
    encrypted_name   BLOB NOT NULL,
    name_ephemeral_pubkey BLOB NOT NULL,
    created_by       INTEGER NOT NULL,

    FOREIGN KEY (library_id) REFERENCES shared_libraries(id),
    FOREIGN KEY (created_by) REFERENCES users(user_id)
);

CREATE TABLE photo_album_entries (
    album_id         TEXT NOT NULL,
    photo_id         TEXT NOT NULL,
    sort_order       INTEGER,              -- user-defined ordering within album

    PRIMARY KEY (album_id, photo_id),
    FOREIGN KEY (album_id) REFERENCES photo_albums(id),
    FOREIGN KEY (photo_id) REFERENCES photos(id)
);
```

Albums are lightweight groupings. A photo can belong to multiple albums. Albums can be personal (visible only to the user) or shared (visible to all library members, or shared independently with non-members). Deleting a photo from an album does not delete the photo itself.

When an album is shared with a non-library-member, the per-photo metadata keys for all photos in the album are wrapped for the recipient (rows added to `photo_metadata_access`), along with `file_access` entries for the photo content and thumbnails.

#### Favorites

```sql
-- Per-user favorites
CREATE TABLE photo_favorites (
    photo_id         TEXT NOT NULL,
    user_id          INTEGER NOT NULL,

    PRIMARY KEY (photo_id, user_id),
    FOREIGN KEY (photo_id) REFERENCES photos(id),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);
```

#### Operation Log

```sql
-- Append-only history of all photo operations.
-- Enables undo, audit trail, and retention-aware cleanup.
CREATE TABLE photo_operations (
    id                    TEXT PRIMARY KEY,  -- UUIDv7 (encodes timestamp)
    library_id            TEXT,              -- denormalized filter, NOT FK
    photo_id              TEXT NOT NULL,
    operation_type        INTEGER NOT NULL,  -- see Operation Types below
    resource_type         INTEGER,           -- which resource (content ops only); NULL otherwise
    prior_data_block_id   TEXT,              -- soft pointer (NOT FK) — previous data_block (content ops only)
    new_data_block_id     TEXT,              -- soft pointer (NOT FK) — new data_block (content ops only)
    operation_data        BLOB,              -- payload for non-content ops (encrypted metadata diff, album_id, etc.)
    performed_by          INTEGER NOT NULL,

    FOREIGN KEY (photo_id) REFERENCES photos(id),
    FOREIGN KEY (performed_by) REFERENCES users(user_id)
);

-- `prior_data_block_id` and `new_data_block_id` are deliberately NOT
-- FK-constrained: operation rows are retained indefinitely for audit
-- (see Retention), but the blobs they reference become collectable after
-- the edit-history window. The PhotosReferenceProvider enforces the
-- window via UUIDv7 timestamp filtering — a hard FK would raise
-- SQLITE_CONSTRAINT on every orphan-cleanup pass once the first edit
-- ages out, and that cleanup runs inside a consensus tx, so the failure
-- would replay on every validator, forever. Soft-pointer-policed-by-
-- provider is the design.

CREATE INDEX idx_photo_ops_photo ON photo_operations(photo_id);
CREATE INDEX idx_photo_ops_prior_data ON photo_operations(prior_data_block_id) WHERE prior_data_block_id IS NOT NULL;
CREATE INDEX idx_photo_ops_new_data ON photo_operations(new_data_block_id) WHERE new_data_block_id IS NOT NULL;
CREATE INDEX idx_photo_ops_library ON photo_operations(library_id);
```

##### Operation Types

| Value | Name | resource_type | prior_data_block_id | new_data_block_id | operation_data |
|-------|------|---------------|---------------------|---------------------|----------------|
| 0 | `add` | NULL | NULL | NULL | NULL (resources captured in `photo_resources`) |
| 1 | `content_edit` | edited resource type | old data_block_id for that resource | new data_block_id | NULL |
| 2 | `delete` | NULL | NULL | NULL | NULL (tombstone recorded via `photos.deleted_at`) |
| 3 | `metadata_edit` | NULL | NULL | NULL | encrypted diff of changed fields |
| 4 | `album_add` | NULL | NULL | NULL | album_id |
| 5 | `album_remove` | NULL | NULL | NULL | album_id |
| 6 | `favorite` | NULL | NULL | NULL | NULL |
| 7 | `unfavorite` | NULL | NULL | NULL | NULL |
| 8 | `restore` | NULL | NULL | NULL | NULL (clears `photos.deleted_at`) |

**Undo semantics:**

- **Content edit**: Revert by pointing the affected `photo_resources` row back to `prior_data_block_id`. The old data block is still alive because the operation log references it for the duration of edit history retention.
- **Delete**: Restore by clearing `photos.deleted_at` and `photos.deleted_by`. All `photo_resources` rows are retained intact during the 30-day window, so no resource re-linking is required.
- **Metadata edit**: Apply the inverse of `operation_data` to `photos.encrypted_metadata`.
- **Album/favorite changes**: Reverse the relation change (insert ↔ delete in the junction table).

A content edit on a Live Photo emits **two** operation entries (one for the `edited` still, one for `edited_paired_video`) if both renders change. Editing only the still emits one entry. The `original` and `paired_video` resources are never touched by edits — edited renders are separate resources.

### Client-Side Sidecar Database

The sidecar is a local, non-replicated SQLite database maintained by each client. It holds the decrypted photo metadata that enables all query patterns. The sidecar is ephemeral — it can be deleted and rebuilt from consensus state at any time.

Because it contains plaintext dates, locations, and camera metadata, the host creates the sidecar with mode `0600` on Unix. Sign-out drops the in-memory recipient key and stops synchronization but preserves the file and cursor for an incremental resume; the UI reports this paused-on-disk state explicitly. Choosing **Remove** deletes the file, while **Re-sync** deletes and rebuilds it from consensus.

#### Sidecar Schema

```sql
-- Decrypted photo metadata index. All query patterns run against this table.
CREATE TABLE photo_index (
    photo_id         TEXT PRIMARY KEY,
    library_id       TEXT,

    -- Temporal
    date_taken       TEXT,                 -- ISO 8601
    upload_date      TEXT,                 -- derived from photo UUIDv7

    -- Media properties
    media_type       INTEGER NOT NULL,     -- 0=image, 1=video, 2=live_photo
    width            INTEGER,
    height           INTEGER,
    orientation      INTEGER,              -- EXIF orientation (1-8)
    duration_ms      INTEGER,              -- video/live photo duration

    -- Camera
    camera_make      TEXT,
    camera_model     TEXT,

    -- Location (if user opted to store GPS)
    latitude         REAL,
    longitude        REAL,

    -- Grouping (mirrored from consensus photos table)
    group_id         TEXT,
    group_type       INTEGER,
    group_index      INTEGER,
    is_group_pick    INTEGER NOT NULL DEFAULT 0,

    -- Soft-delete state (mirrored from consensus)
    deleted_at       TEXT,                 -- NULL = active
    deleted_by       INTEGER,
    expires_at       TEXT,                 -- 30 days after deleted_at; NULL when active

    -- Sync tracking
    synced_at_height INTEGER NOT NULL      -- consensus height when last processed
);

CREATE INDEX idx_sidecar_date ON photo_index(date_taken);
CREATE INDEX idx_sidecar_library ON photo_index(library_id);
CREATE INDEX idx_sidecar_media ON photo_index(media_type);
CREATE INDEX idx_sidecar_location ON photo_index(latitude, longitude);
CREATE INDEX idx_sidecar_camera ON photo_index(camera_make, camera_model);
CREATE INDEX idx_sidecar_group ON photo_index(group_id) WHERE group_id IS NOT NULL;
CREATE INDEX idx_sidecar_active ON photo_index(deleted_at) WHERE deleted_at IS NULL;
CREATE INDEX idx_sidecar_recently_deleted ON photo_index(expires_at) WHERE deleted_at IS NOT NULL;

-- Local cache of which resources each photo has and their data_block_ids.
-- Populated from the consensus photo_resources table. Used to find the
-- primary display resource for gallery rendering and to drive byte fetches.
CREATE TABLE photo_resources_cache (
    photo_id         TEXT NOT NULL,
    resource_type    INTEGER NOT NULL,
    data_block_id    TEXT NOT NULL,
    PRIMARY KEY (photo_id, resource_type)
);

CREATE INDEX idx_resources_cache_data_block ON photo_resources_cache(data_block_id);
```

Active gallery queries filter `WHERE deleted_at IS NULL`. The Recently Deleted view filters `WHERE deleted_at IS NOT NULL AND expires_at > now`. Burst frame rollups query `WHERE is_group_pick = 1` to show one frame per burst, with expansion fetching all rows for a given `group_id`.

The sidecar schema can evolve freely — it's local-only, not consensus-tracked, and can be rebuilt at any time. New columns can be added for face tags, scene labels, or any future metadata without schema migrations on the consensus side.

#### Sidecar Sync Model

**Initial hydration** (new device or cache rebuild):
1. Fetch all `photos` rows (including soft-deleted) and `photo_metadata_access` entries for the current user
2. For each photo: ECDH with the ephemeral pubkey to unwrap the per-photo metadata key, then ChaCha20-Poly1305 decrypt the metadata blob
3. Insert decrypted fields into `photo_index`, copying `deleted_at` / `deleted_by` and computing `expires_at = deleted_at + 30d` when soft-deleted
4. Fetch all `photo_resources` rows for those photos and populate `photo_resources_cache`
5. Record the current consensus height as `synced_at_height`

**Incremental sync** (ongoing):
1. On new consensus block, check for photo-related transactions since last synced height
2. For new/modified photos: decrypt metadata, upsert into `photo_index`
3. For new/modified resources: upsert into `photo_resources_cache`
4. For soft-deleted photos: set `deleted_at` / `expires_at` on the existing `photo_index` row (no row move)
5. For restored photos: clear `deleted_at` / `expires_at`
6. For hard-deleted photos (30-day retention expired): drop from `photo_index` and `photo_resources_cache`
7. Update `synced_at_height`

The incremental path processes only changes since the last sync, so ongoing cost is proportional to new activity, not library size.

#### Thumbnail Caching

Thumbnails are encrypted data blocks fetched and decrypted on demand as the user scrolls. Decrypted thumbnails are cached locally (in-memory LRU and/or on-disk cache) since they're immutable — the same `data_block_id` always produces the same bytes. The client prefetches thumbnails ahead of the scroll position to maintain smooth rendering.

### Deletion Lifecycle

Deletion is soft. The `photos` row and all `photo_resources` rows stay in place; only the tombstone columns flip. This avoids snapshotting multi-resource state into a separate table for the recovery window.

When a photo is deleted (by any shared library member, or the user for personal photos):

1. A `delete` operation is logged in `photo_operations` (operation_type=2, no data_block fields)
2. `photos.deleted_at` is set to the current timestamp and `photos.deleted_by` is set to the actor
3. `photo_album_entries`, `photo_favorites`, and any per-user state are deleted (photo disappears from active views immediately)
4. `photo_resources`, `photo_metadata_access`, and `file_access` entries are **retained** for the 30-day recovery window

A periodic cleanup job scans for expired tombstones:

```sql
SELECT id FROM photos
WHERE deleted_at IS NOT NULL
  AND datetime(deleted_at, '+30 days') < datetime('now')
```

For each expired photo, the job:
1. Deletes `photo_resources` rows (data blocks become orphan-cleanup candidates)
2. Deletes `photo_metadata_access` and `file_access` rows for those data blocks
3. Deletes the `photos` row itself
4. Optionally compacts the `photo_operations` history for the photo (delete + add entries; content-edit entries may be retained or pruned per edit history retention policy)

The 30-day window is enforced by the `photos` row's existence (with `deleted_at` set) keeping all its `photo_resources` rows alive, which in turn keep their `data_block`s pinned via the `DataBlockReferenceProvider` mechanism described below.

Clients flip `deleted_at` / `expires_at` on the sidecar `photo_index` row during incremental sync and hard-delete the row only after the consensus row is hard-deleted.

**Restore**: any operation_type=8 (`restore`) entry within the 30-day window clears `deleted_at` / `deleted_by`. All resources and access entries are still present, so restore is atomic and free of data movement.

## Integration with Core Storage

### Data Block Reference Provider

The existing orphan cleanup in `src/db/fragments.rs` checks whether a `data_block_id` has any referencing inodes:

```rust
let has_inodes: bool = db_tx.query_row(
    "SELECT COUNT(*) > 0 FROM inodes WHERE data_id = ?",
    rusqlite::params![data_block_id],
    |row| row.get(0)
)?;
```

The photos module adds additional reference checks. The orphan cleanup query must be expanded to also verify that a data block is not referenced by:

1. **`photo_resources.data_block_id`** — any resource (original, edited, paired_video, thumbnails, etc.) of any photo, active or soft-deleted within the retention window
2. **`photo_operations.prior_data_block_id`** — historical content retained for edit-history undo (within retention window)
3. **`photo_operations.new_data_block_id`** — symmetric: covers in-flight content edits and recently-superseded edits within retention

The cleanest integration approach is a `DataBlockReferenceProvider` trait:

```rust
pub trait DataBlockReferenceProvider: Send + Sync {
    fn references_data_block(
        &self,
        db_tx: &rusqlite::Transaction,
        data_block_id: &str,
    ) -> Result<bool, DatabaseError>;
}

inventory::collect!(&'static dyn DataBlockReferenceProvider);
```

The filesystem module registers a provider that checks `inodes` and `shares`. The photos module, if compiled in, registers a provider that checks `photo_resources` and non-expired `photo_operations`. Orphan cleanup iterates all registered providers and only proceeds when none claim the data block.

For the photos module, the reference check is:

```rust
pub struct PhotosReferenceProvider;

impl DataBlockReferenceProvider for PhotosReferenceProvider {
    fn references_data_block(
        &self,
        db_tx: &rusqlite::Transaction,
        data_block_id: &str,
    ) -> Result<bool, DatabaseError> {
        // Any resource (original, edited, paired_video, thumbnails, etc.)
        // of any photo, active or soft-deleted within retention.
        // photo_resources rows are only hard-deleted after the parent photo's
        // 30-day tombstone expires, so this check naturally covers both
        // active photos and recently-deleted ones.
        let in_resources: bool = db_tx.query_row(
            "SELECT COUNT(*) > 0 FROM photo_resources WHERE data_block_id = ?",
            rusqlite::params![data_block_id],
            |row| row.get(0),
        )?;
        if in_resources { return Ok(true); }

        // Edit-history retention: prior versions referenced by content_edit
        // operations within the 30-day window. UUIDv7 encodes timestamp,
        // so we can filter by ID range.
        let retention_cutoff = Uuid::now_v7_minus_days(30);
        let in_history: bool = db_tx.query_row(
            "SELECT COUNT(*) > 0 FROM photo_operations
             WHERE (prior_data_block_id = ? OR new_data_block_id = ?)
               AND id > ?",
            rusqlite::params![data_block_id, data_block_id, retention_cutoff.to_string()],
            |row| row.get(0),
        )?;

        Ok(in_history)
    }
}

inventory::submit! {
    &PhotosReferenceProvider as &dyn DataBlockReferenceProvider
}
```

### Consensus Transaction Types

The photos module registers its own transaction handlers via the existing `inventory` pattern. No modifications to core consensus code are required.

| Transaction | Handler | Description |
|-------------|---------|-------------|
| `photo_add` | `PhotoAddHandler` | Batch: create photo rows + photo_resources rows (per-asset variants and thumbnails) + metadata_access, file_access for library members. Single upload is batch of one. |
| `photo_delete` | `PhotoDeleteHandler` | Log delete operation, set `photos.deleted_at` / `deleted_by`, drop album/favorite rows. Resources and access entries retained for the recovery window. |
| `photo_edit_content` | `PhotoEditContentHandler` | Log content edit for a specific `resource_type`, update `photo_resources.data_block_id` for that row, distribute new file_access to library members |
| `photo_edit_metadata` | `PhotoEditMetadataHandler` | Log metadata diff, update encrypted_metadata blob on photos row |
| `photo_restore` | `PhotoRestoreHandler` | Restore soft-deleted photo within retention window (clear `deleted_at` / `deleted_by`) |
| `photo_undo` | `PhotoUndoHandler` | Revert most recent operation on a photo (content edit on a specific resource, or metadata edit) |
| `create_shared_library` | `CreateSharedLibraryHandler` | Create library + initial membership |
| `join_shared_library` | `JoinSharedLibraryHandler` | Add member, create file_access + photo_metadata_access for all existing library photos |
| `leave_shared_library` | `LeaveSharedLibraryHandler` | Remove member, clean up their file_access and photo_metadata_access entries |
| `album_create` | `AlbumCreateHandler` | Create album |
| `album_add_photo` | `AlbumAddPhotoHandler` | Add photo to album, log operation |
| `album_remove_photo` | `AlbumRemovePhotoHandler` | Remove photo from album, log operation |

**Batch payloads**: The `photo_add` transaction accepts a `Vec<PhotoAddEntry>`, where each entry contains the photo metadata, thumbnail references, and per-member key wrappings. Single-photo upload submits a batch of one. This scales along both axes — multiple photos per transaction, and multiple transactions per consensus block.

**Concurrent edit policy**: Last writer wins via consensus ordering. The `photo_edit_content` handler logs `prior_data_block_id` as the **current** value at execution time (looked up from `photo_resources` for the targeted `resource_type`), not the value claimed in the payload. This ensures the operation chain is contiguous even when edits race: if A's edit lands first (X → A_version) and B's lands second, B's log entry records (prior=A_version, new=B_version) regardless of what B's payload claimed. All versions are reachable by walking the operation log, and any superseded edit can be manually restored.

### Shared Library: Add Photo Flow

When a member adds a photo to a shared library:

```
Client-side:
  1. Extract metadata from raw image (EXIF, dimensions, etc.)
  2. Enumerate source asset resources (original, edited, paired_video,
     adjustment_data, raw_alternate as applicable from PhotoKit)
  3. Generate thumbnails (small + medium) from the primary display resource
  4. Encrypt each resource's bytes → its own data_block
  5. Encrypt metadata blob with per-photo metadata key
  6. Wrap metadata key for each library member (ECDH per member)
  7. Wrap each resource's file key for each library member (file_access pattern)
  8. Upload all data_blocks + submit photo_add consensus transaction

photo_add handler (all nodes):
  1. Insert into photos (id, library_id, encrypted_metadata, group_*, ...)
  2. Insert one photo_resources row per supplied resource
  3. For each shared_library_member:
     a. Insert file_access entries for every resource's data_block
     b. Insert photo_metadata_access entry
  4. Log photo_operations entry (type=add)
```

This is analogous to the existing `AcceptShareHandler` flow but automated for all library members — no pending state, no explicit acceptance.

### Shared Library: Content Edit Flow

When a member edits a photo (e.g. external editor export, crop, filter):

```
Client-side:
  1. Decide which resource_type is being edited (typically `edited`;
     creating it if the photo had only `original` before)
  2. Generate new thumbnails reflecting the edit
  3. Encrypt new resource bytes → new data_block
  4. Encrypt new thumbnails → new data_blocks
  5. Wrap new file keys for each library member
  6. Upload + submit photo_edit_content transaction (one tx per resource_type
     being edited; Live Photo still + paired_video edits = two txs)

photo_edit_content handler (all nodes):
  1. Look up current data_block_id for (photo_id, resource_type) in photo_resources
  2. Log photo_operations (type=content_edit, resource_type=R,
     prior=current, new=payload.new_id)
  3. Upsert photo_resources(photo_id, resource_type=R) → new data_block_id
  4. Upsert photo_resources rows for the thumbnail resource_types to new data_blocks
  5. For each shared_library_member: insert file_access for each new data_block
  6. Update shares entries pointing to the superseded data_block
```

The old data block is retained because the operation log entry references its `prior_data_block_id`. Any member can undo by submitting `photo_undo`, which swaps the `photo_resources` row back.

### Shared Library: Join Flow

When a new member joins an existing library:

```
Client-side (initiated by existing member):
  1. For each photo in library:
     a. For each resource of the photo: wrap file key for new member
     b. Wrap metadata key for new member
  2. Submit join_shared_library transaction with all wrapped keys

join_shared_library handler (all nodes):
  1. Insert into shared_library_members
  2. For each photo in library:
     a. Insert file_access entries for every photo_resources row
     b. Insert photo_metadata_access entry
```

This may be a large transaction for libraries with many photos. A batched approach (process N photos per consensus round) may be necessary for libraries exceeding a few thousand photos. This is a detail for implementation phase.

## Retention Policy

| Data | Retention | Mechanism |
|------|-----------|-----------|
| Active photo resources (original, edited, paired_video, thumbnails, …) | Indefinite | Referenced by `photo_resources.data_block_id` for the active `photos` row |
| Edit history (prior versions) | 30 days (edit history window) | Referenced by `photo_operations.prior_data_block_id`; window enforced by UUIDv7 ID range filter in reference provider |
| Soft-deleted photo resources | 30 days | `photos.deleted_at + 30d` keeps the row alive, which keeps `photo_resources` rows alive, which pins data_blocks |
| Operation log entries (non-delete) | Indefinite | Small rows; negligible storage cost |
| Operation log entries (delete) | Indefinite (small row, no data_block reference) | Optionally pruned after the soft-delete window if desired |
| Sidecar photo_index / photo_resources_cache | Ephemeral | Rebuilt from consensus state; can be purged at any time |

The periodic cleanup job scans for expired tombstones:

```sql
-- Find photos whose soft-delete window has elapsed
SELECT id FROM photos
WHERE deleted_at IS NOT NULL
  AND datetime(deleted_at, '+30 days') < datetime('now')
```

For each expired photo, the job:
1. Deletes all `photo_resources` rows for that photo (drops references; data blocks become candidates for orphan cleanup)
2. Deletes all `photo_metadata_access` and `file_access` rows for that photo and its data blocks
3. Deletes the `photos` row itself
4. The standard orphan cleanup job handles the data blocks on its next pass (if no other references exist)

Edit-history retention (separate from soft-delete retention) is enforced entirely in the `DataBlockReferenceProvider` by filtering `photo_operations` rows by UUIDv7 timestamp. No periodic deletion of operation log rows is required; the rows themselves are small and can be retained indefinitely for audit purposes.

## Client Architecture

### photos-core: Shared Client Library

Photo clients span two fundamentally different contexts:

- **Node clients** (desktop Tauri, web UI): run on a device that participates in consensus directly. They can submit transactions to the local consensus engine.
- **Thin clients** (mobile, future integrations): do not participate in consensus. They formulate transactions locally and forward them over HTTP to a node in the network, which submits to consensus on their behalf. This is the same pattern used by the Apple FileProvider extension.

Both contexts need the same client-side logic: crypto operations, metadata extraction, thumbnail generation, sidecar management, and transaction payload construction. The difference is only in how a formulated transaction reaches consensus.

This shared logic lives in a standalone Rust crate (`crates/photos-core/`) with no dependency on Tauri, Axum, or any server/node framework:

```
crates/photos-core/
  lib.rs              ← public API
  crypto.rs           ← ECDH key wrapping, metadata encrypt/decrypt, file key management
  metadata.rs         ← EXIF extraction, metadata blob construction/parsing
  thumbnails.rs       ← thumbnail generation (image crate)
  sidecar.rs          ← sidecar SQLite: schema, hydration, incremental sync, queries
  payloads.rs         ← consensus transaction payload construction
  dispatch.rs         ← PhotoDispatch trait definition
```

### Dispatch Trait

The dispatch trait abstracts the boundary between client logic and transaction submission:

```rust
#[async_trait]
pub trait PhotoDispatch {
    /// Upload encrypted data block bytes to the network
    async fn upload_data_block(&self, ...) -> Result<DataBlockId>;

    /// Submit a fully-formed photo transaction for consensus
    async fn submit_transaction(&self, payload: Vec<u8>, tx_type: &str) -> Result<()>;

    /// Fetch library members' public keys (needed for key wrapping)
    async fn fetch_library_members(&self, library_id: &str) -> Result<Vec<MemberInfo>>;

    /// Fetch encrypted photo rows for sidecar hydration/sync
    async fn fetch_photos_since(&self, height: u64) -> Result<Vec<EncryptedPhotoRow>>;

    /// Fetch encrypted thumbnail data block
    async fn fetch_thumbnail(&self, data_block_id: &str) -> Result<Vec<u8>>;
}
```

**Node client dispatch** (`src/photos/dispatch_local.rs`): calls directly into the local consensus submission pipeline and reads from the local database. Transaction submission is a function call, not an HTTP round-trip.

**Thin client dispatch** (in the mobile app crate): makes HTTP requests to a remote node's photo API endpoints. The node receives the pre-formulated transaction payload and relays it to consensus. The thin client holds the user's keys and does all crypto locally — the node never sees unencrypted metadata or unwrapped keys.

### Upload Flow by Client Type

Both client types perform the same preparation via `photos-core`:

```
photos-core::prepare_upload(raw_image, user_keys, library_members):
  1. Extract EXIF metadata
  2. Generate thumbnails (small + medium)
  3. Encrypt photo bytes → data_block
  4. Encrypt thumbnails → data_blocks
  5. Encrypt metadata blob with per-photo metadata key
  6. Wrap metadata key for each library member
  7. Wrap file keys for each library member
  8. Construct PhotoAddPayload
  → returns (encrypted_data_blocks, transaction_payload)
```

The dispatch diverges only at submission:

- **Node client**: `dispatch_local.upload_data_block()` writes fragments to local storage and triggers distribution. `dispatch_local.submit_transaction()` feeds the payload directly into consensus.
- **Thin client**: `dispatch_remote.upload_data_block()` POSTs encrypted bytes to `POST /photos/upload`. `dispatch_remote.submit_transaction()` POSTs the payload to `POST /photos/submit`, where the node validates the request and submits to consensus on behalf of the thin client.

### Consumer Bindings

- **Desktop (Tauri)**: `photos-core` is a Rust dependency called via Tauri commands. The Svelte frontend invokes Tauri commands, which call `photos-core` functions with the local dispatch implementation.
- **Mobile (native iOS/Android)**: `photos-core` is exposed via UniFFI (or swift-bridge for iOS). The native UI (SwiftUI/Jetpack Compose) calls into the Rust library for all crypto, sidecar, and payload work, then the thin-client dispatch implementation handles HTTP communication with the network.
- **Web UI**: The node's HTTP API serves the web frontend. The node can perform `photos-core` operations on behalf of the web user (the node already escrows the user's keys in the current architecture). Future work may move crypto to WASM in the browser to eliminate server-side key access.

## Server Module Boundary

The server-side photos module (`src/photos/`) handles consensus processing and the HTTP API. It is feature-gated behind `#[cfg(feature = "photos")]` and can be compiled out entirely for deployments that don't need photo support.

```
src/photos/
  mod.rs                  ← module root, re-exports
  handlers.rs             ← consensus transaction handlers (registered via inventory)
  routes.rs               ← HTTP API: upload, submit, sync, library management
  db.rs                   ← consensus database queries and schema migration
  dispatch_local.rs       ← impl PhotoDispatch for node-local consensus submission
  reference_provider.rs   ← DataBlockReferenceProvider implementation
```

The server module depends on:
- `src/db/shared.rs` — data_blocks, file_access table access
- `src/files/functions.rs` — encryption, data block creation, fragment generation
- `src/handlers.rs` — `TransactionHandler` trait
- `src/shares/` — shares table access for live-link coordination
- `crates/photos-core/` — payload types, crypto primitives (shared with clients)

The server module does **not** depend on:
- `src/db/files.rs` — inode queries
- `src/fileprovider/` — FileProvider integration
- Any filesystem path logic

If the module is not compiled, no photos tables are created, no handlers are registered, no routes are mounted, and orphan cleanup works exactly as it does today.

## Implementation Phases

### Phase 1: photos-core Crate and Schema [~]
- [~] Extract `hopnet-photos-core` with crypto, metadata, payload, dispatch trait, and optional sidecar; thumbnail generation remains deferred
- [x] Consensus-tracked photo tables (photos, photo_metadata_access, photo_resources, photo_operations, shared_libraries, shared_library_members, photo_albums, photo_album_entries, photo_favorites) — `hopnet-photos` crate, RFC-016 projection registry
- [x] `photo_add` / `photo_delete` / `photo_restore` consensus handlers — `PhotoAddHandler` (batch, per-entry `uploaded_by` authz, rejected non-NULL `library_id` until Phase 3), `PhotoDeleteHandler` (per-entry ownership check, `deleted_at` derived from `operation_id.extract_timestamp()` — no clocks in handlers), `PhotoRestoreHandler`. 6 handler tests covering authz, tombstone, restore-on-active rejection, nonexistent-library rejection, and validate-vs-apply transaction separation.
- [x] `DataBlockReferenceProvider` integration with orphan cleanup — `PhotosReferenceProvider` with UUIDv7-timestamp-filtered edit-history retention; 10 tests covering both surfaces, the retention boundary, the over-exclusion leak direction, and Rust↔SQL implementation agreement
- [x] `committed_blob_ids` distribution hook — `photo_add` arm extracts blob ids from resources for the storage engine's distribution kick
- [x] Periodic cleanup job for expired soft-deleted photos — `photo_cleanup_expired` consensus handler (node-signed, wall-clock predicate host-side in scan query, deterministic `datetime(deleted_at, '+30 days') < datetime(scan_cutoff)` check in handler); `run_photo_tombstone_cleanup` scan job batching 50 IDs per tx via `TxGateway::submit_batch`; daily randomized apalis cron registered via `photos_host::spawn_tombstone_cleanup_worker`. 4 handler tests + 3 DB tests covering hard-delete, within-window skip, missing-photo idempotency, user-signed rejection, and active-photo skip.
- [x] `dispatch_local` implementation for node clients — signs user transactions through the local consensus queue and reads the local encrypted sync feed
- [x] Source-independent asset model in `hopnet-photos-core::asset` — namespaced source identities, typed resource kinds, resource descriptors, and validation; publisher, byte transport, and ingress adapters remain deferred
- [~] Basic HTTP API — transaction submission, gallery/detail queries, recently-deleted view, and per-user sidecar lifecycle are mounted; content upload/fetch routes remain deferred
- [x] Metadata sync endpoint — user-scoped encrypted photo state + `photo_resources` rows with monotonic high-water marks
- [ ] ECDH per-photo performance validation

### Phase 2: Sidecar and History [x]
- [x] Sidecar database schema and sync logic in `photos-core` (no framework dependency)
- [x] Initial hydration flow (full library decrypt)
- [x] Incremental sync (process new consensus transactions)
- [x] Operation log: content edits with undo
- [x] Metadata edit operations with undo
- [x] Deletion with 30-day retention and restore
- [x] Periodic cleanup job for expired deletions

### Phase 3: Shared Libraries [ ]
- Library creation and membership management
- Auto-share on add (file_access + photo_metadata_access distribution to all members)
- Member join flow with bulk key wrapping
- Member leave with cleanup
- Library member pubkey endpoint (for thin client key wrapping)

### Phase 4: Albums and Organization [ ]
- Album CRUD with consensus
- Album photo membership
- Shared albums with non-library-members (per-photo metadata key wrapping)
- Per-user favorites

### Phase 5: Desktop Frontend [~]
- *Deferred — separate RFC for gallery UI, timeline view, shared library management*
- Tauri command bindings to `photos-core`
- [~] Svelte metadata gallery, sidecar opt-in/resume/remove flow, pagination, and photo detail modal; thumbnail/content rendering remains deferred

### Phase 6: Mobile Clients [ ]
- *Deferred — UniFFI bindings for `photos-core`, thin client dispatch implementation*
- *Native UI (SwiftUI / Jetpack Compose) consuming shared Rust core*

### Phase 7: FileProvider Export [ ]
- *Deferred — optional filesystem projection of photos for external tool access*

### Phase 8: Advanced Features [ ]
- *Deferred — face detection, scene classification, smart albums, geo-tagging*
