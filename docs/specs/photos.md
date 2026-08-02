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

    -- Cross-device asset identity: lowercase-hex keyed HMAC of the source
    -- library's stable asset id (PHCloudIdentifier for the macOS ingress).
    -- NULL = local-only or non-PhotoKit asset (no dedupe).
    cloud_fingerprint TEXT,

    FOREIGN KEY (uploaded_by) REFERENCES users(user_id),
    FOREIGN KEY (deleted_by) REFERENCES users(user_id),
    FOREIGN KEY (library_id) REFERENCES shared_libraries(id)
);

CREATE INDEX idx_photos_library ON photos(library_id);
CREATE INDEX idx_photos_deleted ON photos(deleted_at) WHERE deleted_at IS NOT NULL;

-- Dedupe uniqueness is a partial-index PAIR because SQLite treats NULLs as
-- distinct in UNIQUE indexes: a composite UNIQUE(library_id, cloud_fingerprint)
-- would never constrain personal (NULL-library) rows.
CREATE UNIQUE INDEX idx_photos_fp_personal ON photos(cloud_fingerprint)
    WHERE library_id IS NULL AND cloud_fingerprint IS NOT NULL;
CREATE UNIQUE INDEX idx_photos_fp_shared ON photos(library_id, cloud_fingerprint)
    WHERE library_id IS NOT NULL AND cloud_fingerprint IS NOT NULL;
```

##### Cloud Fingerprint (cross-device identity)

Two devices ingesting the same source library (e.g. two Macs on one iCloud
account) each mint their own photo ids, so without a shared identity key
every asset would duplicate mesh-side. The fingerprint is that key:

- **Computed node-side, keyed per-user**: `blake3::keyed_hash(k, cloud_id)`
  where `k = blake3 derive_key("hopnet photos cloud fingerprint v1", user
  Ed25519 privkey)` (context string frozen — changing it orphans every
  committed fingerprint). The thin client obtains fingerprints from
  `POST /api/photos/client/resolve`; it holds no user key material itself.
  Keyed per RFC-014's confirmation-oracle rule: replicated state carries no
  unkeyed function of the identifier, so validators (who hold no user keys)
  cannot test whether a given cloud id is present.
- **Opaque to validators**: handlers cannot re-derive it; enforcement is
  solely the partial UNIQUE pair. A device can submit garbage fingerprints —
  the blast radius is the user's own dedupe scope.
- **Claim-on-conflict**: the resolve probe is the cooperative path (a hit
  returns the committed photo id and the client adopts instead of
  publishing); the UNIQUE index is the race backstop — the losing insert
  fails deterministically at the proposer preflight, and the loser adopts on
  its next resolve.
- **Tombstones hold their fingerprint** until the 30-day hard delete frees
  the index entry — a resolve on a mesh-deleted photo still returns its id
  (the client adopts a tombstoned photo rather than colliding with it).
  "Undelete by republish" is deliberately not a thing.
- **Shared-library scoping**: a resolve carrying `library_id` uses the
  **library-scoped key** instead — `blake3 derive_key("hopnet photos cloud
  fingerprint library v1", library_key)` (context frozen, pinned by test
  vector; the fn lives in photos-core beside the library-key wrap). Every
  member derives the same key from their own `shared_library_keys` wrap
  (the route unwraps it with the device-bootstrapped session), so the same
  PHCloudIdentifier fingerprints identically no matter which member's
  daemon publishes — `idx_photos_fp_shared` then dedupes across members,
  and the shared lookup deliberately has NO owner filter (any member's
  committed photo answers; membership is checked at the route, 403
  `library_not_member`, with `library_key_pending` distinguishing
  convergence lag). Two members' daemons racing one asset: the loser's
  `photo_add` fails deterministically on the index and adopts the winner
  on its next resolve.

##### Ingress Responsibility (explicit publish designation)

```sql
CREATE TABLE photo_ingress_responsibility (
    user_id       INTEGER NOT NULL,
    library_id    TEXT,                  -- NULL = personal scope
    device_id     TEXT NOT NULL,
    operation_id  TEXT NOT NULL,         -- UUIDv7, audit/ordering

    PRIMARY KEY (user_id, library_id),   -- shared uniqueness + snapshot ordering
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (device_id) REFERENCES device_tokens(id),
    FOREIGN KEY (library_id) REFERENCES shared_libraries(id)
);
-- NULLs are distinct even in the composite PK (SQLite rowid-table
-- quirk), so the personal singleton needs the partial index:
CREATE UNIQUE INDEX idx_ingress_resp_personal
    ON photo_ingress_responsibility(user_id) WHERE library_id IS NULL;
```

One device per (user, scope) may publish ingress mutations — the personal
partition (`library_id` NULL) and each shared library are independent
scopes. Each member of a shared library claims independently for their own
devices; cross-member dedup of the actual photos is the fingerprint's job,
not responsibility's. Claims are **explicit and JWT-only**:
`POST /api/photos/ingress/claim {"device_id", "library_id"?}` builds and
submits the `photo_ingress_claim` tx server-side (curl/GUI friendly); a
shared-scope claim requires applied membership (handler-enforced,
deterministic), and `library_remove_member` dissolves the removed member's
scope claim so a kicked member's daemon stops passing the gate.
`GET /api/photos/ingress/responsibility` returns the personal holder plus
per-library holders (shape additive on v1). The thin-client transaction
route enforces `DEVICE_TX_FUNCTIONS` (which excludes the claim tx — a
daemon can never designate itself) and 403s per scope with
`ingress_not_responsible:{other|unclaimed}`: the route decodes the
payload's touched scopes (photo_add carries them per entry; photo-targeting
txs resolve them from the committed rows) and requires the authed device to
hold EVERY one — holding the personal claim never admits shared-library
writes or vice versa. Claim and transfer are one upsert — the dead-Mac
case is any logged-in session re-claiming to a new device, after which the
new daemon's first pass adopts the whole published archive by fingerprint
instead of re-uploading it. Enforcement is admission-time (device identity
is an HTTP-layer concept); the fingerprint UNIQUE pair is the correctness
backstop for any admission race. Adoption and the resolve/committed probes
are reads and deliberately ungated.

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

**Access-row existence is the read grant only for PERSONAL photos.** For shared-library photos, reads additionally require a `shared_library_members` row (Phase 3, "Design B"): the convergence worker pre-stages wraps for pending invitees *before* they accept, and a kicked member's wraps are deleted lazily — in both cases the wrap row alone must not read. The gate lives in `query_changes` (both statements, boundary batch included) and the resource byte path (`lookup_resource_block_authz`, wrap ∧ membership, 403 on non-membership).

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

All resources are supplied by the ingesting client at upload time when they exist on the source asset — the daemon ingesting from PhotoKit enumerates `PHAsset.assetResources` and submits every applicable resource, and **generates the thumbnail resources itself** (~256px/~1024px JPEG renditions via `PHImageManager` at ingest; video assets get poster frames). Thumbnails are what keep the gallery renderable for formats browsers cannot decode (HEIC, HEVC). Each resource is encrypted with its own per-data-block key and replicated via the standard fragment distribution path.

**Deferred: non-PhotoKit ingest paths must supply thumbnails too.** When manual/import upload of arbitrary image files lands (e.g. a user importing a HEIC without Apple Photos involved), that path needs its own rendition generation — either import-time generation in the ingesting client, or a node-side fallback renderer (a port of the interim viewer's `render.rs` decode cache). Recorded here so the HEIC-blank-cell gap doesn't silently recur on a new ingest surface.

#### Shared Libraries

```sql
-- Shared library definition. The name is encrypted under the LIBRARY
-- key (below), not a single-recipient seal — every member can render it.
CREATE TABLE shared_libraries (
    id               TEXT PRIMARY KEY,     -- UUIDv7
    encrypted_name   BLOB NOT NULL,        -- ChaCha20-Poly1305 under the library key
    name_nonce       BLOB NOT NULL         -- 12-byte nonce
);

-- Library membership (N-way, no owner). Membership is also the READ
-- GATE for shared photos (see Photo Metadata Access).
CREATE TABLE shared_library_members (
    library_id       TEXT NOT NULL,
    user_id          INTEGER NOT NULL,

    PRIMARY KEY (library_id, user_id),
    FOREIGN KEY (library_id) REFERENCES shared_libraries(id),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

-- Per-member wrapped LIBRARY key (X25519 ECDH, LIBRARY_KEY_WRAP_DOMAIN,
-- wrap id = library id). Decrypts the library name today; the designed
-- seam for the future library-scoped cloud-fingerprint key.
CREATE TABLE shared_library_keys (
    library_id       TEXT NOT NULL,
    user_id          INTEGER NOT NULL,
    ephemeral_pubkey BLOB NOT NULL,
    wrapped_key      BLOB NOT NULL,
    PRIMARY KEY (library_id, user_id),
    FOREIGN KEY (library_id) REFERENCES shared_libraries(id),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

-- Pending membership (consent pattern, mirroring drive's incoming_shares).
-- Carries the invitee's library-key wrap, minted AT invite time, so
-- accept needs no inviter online and the invite listing shows the name.
CREATE TABLE shared_library_invites (
    library_id       TEXT NOT NULL,
    user_id          INTEGER NOT NULL,     -- invitee
    invited_by       INTEGER NOT NULL,
    operation_id     TEXT NOT NULL,
    ephemeral_pubkey BLOB NOT NULL,
    wrapped_key      BLOB NOT NULL,
    PRIMARY KEY (library_id, user_id),
    FOREIGN KEY (library_id) REFERENCES shared_libraries(id),
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (invited_by) REFERENCES users(user_id)
);

-- Per-user VIEW-change signal: "your visibility into this library
-- changed at height h". Written by accept/grant handlers, consumed by
-- the sidecar sync worker to trigger a targeted library backfill.
-- Deliberately NOT photo_changes: a membership or grant change alters
-- the user's view, not the photo — bumping the global feed would make
-- every member re-sync the whole library on each join.
CREATE TABLE photo_view_changes (
    user_id           INTEGER NOT NULL,
    library_id        TEXT NOT NULL,
    changed_at_height INTEGER NOT NULL,
    PRIMARY KEY (user_id, library_id),
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (library_id) REFERENCES shared_libraries(id)
);
```

There is no owner column. All members have equal standing — any member can add photos, remove photos, invite new members, or remove members (self-removal is leave; removing another is kick). The `uploaded_by` field on `photos` records provenance for activity feeds but confers no special permissions. A library whose last member leaves is stranded (rows unreadable by anyone) — allowed, documented; GC is future work.

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

The node's content route serves `ETag: "{data_block_id}"` with `Cache-Control: private, immutable, max-age=31536000` and answers `If-None-Match` with 304. Because a content edit swaps the blob under the same `(photo_id, resource_type)` URL, clients must key caches by `data_block_id` (carried in the gallery/detail payloads' `resources` field); the ETag is the revalidation fallback.

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
| `photo_ingress_claim` | `PhotoIngressClaimHandler` | Claim/transfer ingress responsibility (upsert; device ownership validated against consensus-replicated `device_tokens`). JWT route only — excluded from `DEVICE_TX_FUNCTIONS`. |
| `photo_undo` | `PhotoUndoHandler` | Revert most recent operation on a photo (content edit on a specific resource, or metadata edit) |
| `create_shared_library` | `CreateSharedLibraryHandler` | Create library + creator membership + creator's library-key wrap |
| `library_invite` | `LibraryInviteHandler` | Member-only; parks the invitee's library-key wrap on the invite row (accept works with the inviter offline) |
| `library_invite_accept` | `LibraryInviteAcceptHandler` | Invitee-signed consent: membership + wrap promotion + view-change signal. Re-delivery is a deterministic NotFound |
| `library_invite_decline` | `LibraryInviteDeclineHandler` | Invitee refuses, or any member retracts |
| `library_remove_member` | `LibraryRemoveMemberHandler` | Leave/kick (one equal-standing op): deletes membership, key wrap, pending invite, view signal. Access rows revoked lazily by convergence |
| `library_access_grant` | `LibraryAccessGrantHandler` | Convergence batch: OR-IGNORE metadata + blob wraps for ONE member-or-invitee target; validates every photo is live and in-library, every block backs one; recipient pubkey resolved from consensus state, never the wire; signals the target's view change |
| `library_access_revoke` | `LibraryAccessRevokeHandler` | Convergence batch: delete a departed user's wraps. Target must be NEITHER member nor invitee — the inversion prevents a stealth kick bypassing `library_remove_member` |
| `album_create` | `AlbumCreateHandler` | Create album |
| `album_add_photo` | `AlbumAddPhotoHandler` | Add photo to album, log operation |
| `album_remove_photo` | `AlbumRemovePhotoHandler` | Remove photo from album, log operation |

**Batch payloads**: The `photo_add` transaction accepts a `Vec<PhotoAddEntry>`, where each entry contains the photo metadata, thumbnail references, and per-member key wrappings. Single-photo upload submits a batch of one. This scales along both axes — multiple photos per transaction, and multiple transactions per consensus block.

**Concurrent edit policy**: Last writer wins via consensus ordering. The `photo_edit_content` handler logs `prior_data_block_id` as the **current** value at execution time (looked up from `photo_resources` for the targeted `resource_type`), not the value claimed in the payload. This ensures the operation chain is contiguous even when edits race: if A's edit lands first (X → A_version) and B's lands second, B's log entry records (prior=A_version, new=B_version) regardless of what B's payload claimed. All versions are reachable by walking the operation log, and any superseded edit can be manually restored.

**Edit payload validation** (both edit handlers): the handler is the trust
boundary — every node applies it, and the publishing client's own checks are
advisory by comparison. Enforced there, not only client-side:

- *Shape* (`photo_edit_content`, `InvalidPayload`): an entry must upsert or
  remove something; no kind may repeat within either list or appear in both
  (the upsert loop runs first, so a kind in both would register a blob and
  write its row only to delete it, stranding a data block reachable solely
  from the operation log); and `remove_resources` may not name the original.
  `photo_add` treats "every asset has an original" as an invariant, and an
  edit must not be able to retire it — the operation log records only the
  first removed kind's prior blob, so undo could not restore it either.
- *Re-key coverage* (both, `ConflictError`): when an entry supplies a
  non-empty `metadata_access`, it must carry a wrap for every user who
  currently holds one for that photo **and is still entitled to one**
  (members ∪ pending invitees, or the uploader for a personal photo).
  Upserting only the supplied rows leaves anyone omitted holding a wrap of
  the *superseded* key: their metadata stops decrypting, silently, at read
  time, and the convergence worker cannot repair it — `missing_metadata_grants`
  looks for an absent row, not a stale one. Coverage is measured against
  current *holders* rather than membership so that a member with no wrap yet
  (the worker's own job) and a holder who has left the library (the revoke
  sweep's) neither block an edit. Every production writer of
  `photo_metadata_access` mints rows only for members, invitees, or the
  uploader, so that set is currently complete — **album sharing to a
  non-library-member (above) must extend it**, or a re-key would strand
  exactly those recipients. An empty `metadata_access` remains legal:
  it declares a re-encrypt under the existing key, which the node cannot
  verify either way since it never sees the key.

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

### Shared Library: Invite, Join, and the Convergence Worker

Membership changes are consent-based (mirroring the drive share accept
pattern) and access distribution is a **background convergence loop**, not
a one-shot bulk transaction. The original design — one
`join_shared_library` tx carrying every wrap — could not scale past a few
thousand photos and made the join race (photos added between wrap
computation and commit) unfixable handler-side, since validators hold no
member key and cannot wrap.

**The convergence worker** runs per signed-in user beside the sidecar sync
worker (same session-key lifetime — no session, no unwrapping). Each 30 s
tick (or an immediate poke from the invite route), per library the user
belongs to:

```
assertion set  = members ∪ pending invitees
For each target in the assertion set (≠ self):
  delta = live library photos/blocks the target has no wrap for
  unwrap own wraps → rewrap for target → library_access_grant (≤500/batch)
For each user holding wraps who is NEITHER member nor invitee:
  library_access_revoke for their rows
Loop until a pass emits nothing (cap 200 rounds/tick).
```

Grant handlers are OR IGNORE (first committed wrap wins), so any member's
worker can cover any delta and racing workers are harmless. A rejected tx
means state moved (kick/leave race) — the next tick re-derives; nothing
retries a stale batch. The worker's loop iterates a `ConvergeLane` enum
with a single `Access` variant: the future **`Keys` rotation lane**
(re-encrypt blocks under fresh keys after a kick, so remembered keys stop
decrypting new fetches) slots in without changing the tick or tx surface —
designed, deliberately not built.

**Invite → accept flow:**

```
Invite (any member): rewrap the LIBRARY key for the invitee, submit
  library_invite (wrap parked on the invite row), poke convergence —
  access pre-staging begins immediately. Pre-staged rows are inert:
  reads gate on membership.
Accept (invitee-signed): insert membership + promote the library-key
  wrap + delete invite + write the invitee's photo_view_changes signal.
  The invitee instantly sees everything pre-staged — even if the
  inviter has been offline since the invite.
```

**Client-side rematerialization**: the sidecar sync worker's
membership-diff pre-phase reconciles against memberships +
`photo_view_changes` (never the photo_changes cursor — the photos didn't
change): a new library or moved view signal triggers a paged,
cursor-independent backfill of photos-with-my-wraps; a departed library
purges its local rows (the client half of kick — the mesh read gate
closed the instant the membership row died). Publish-time fan-out
(`fetch_library_members`) returns members ∪ invitees, so new adds/edits
cover pending invitees directly; the worker is backfill, not race repair.

**Revocation semantics (v1)**: kick = instant API revocation (membership
row deletion closes the read gate) + lazy wrap deletion by convergence.
This is deliberately NOT cryptographic revocation — a departed member who
dumped DB state beforehand retains keys for the bytes they already had
access to; the accepted trade until the `Keys` rotation lane lands.
Plaintext already downloaded is theirs forever under any scheme.

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
    /// Encrypt and store one resource's bytes as a data block
    async fn upload_data_block(
        &self,
        blob_id: BlobId,
        source: Box<dyn AsyncRead + Unpin + Send>,
        file_size: usize,
        per_blob_key: chacha20poly1305::Key,
    ) -> Result<UploadedDataBlock>;

    /// Submit a fully-formed photo transaction for consensus
    async fn submit_transaction(&self, tx_type: &str, payload: Vec<u8>) -> Result<()>;

    /// Resolve publish recipients. The dispatch derives the acting user
    /// (`LibraryMembership::uploaded_by`) from its own authenticated state —
    /// callers never supply an identity. None = personal library.
    async fn fetch_library_members(
        &self,
        library_id: Option<CustomUUID>,
    ) -> Result<LibraryMembership>;

    /// Fetch encrypted photo rows for sidecar hydration/sync
    async fn fetch_photos_since(&self, height: u64) -> Result<SyncBatch>;
}
```

Content fetch (`fetch_data_block`) remains deferred to the thin-client
dispatch commit; node clients fetch decrypted resource bytes via
`GET /api/photos/{id}/resource/{type}` (blob_access-gated, Range-capable,
ETag/immutable-cached).

**Node client dispatch** (`src/photos/dispatch_local.rs`): calls directly into the local consensus submission pipeline and reads from the local database. Transaction submission is a function call, not an HTTP round-trip.

**Thin client dispatch** (`crates/ingress-publisher::HttpDispatch` today; the mobile app crate later): makes HTTP requests to a node's `/api/photos/client/*` routes, authenticated with an RFC-012 device token. The node receives the pre-formulated transaction payload and relays it to consensus. Metadata crypto happens client-side — the node never sees unencrypted metadata. Content bytes stream as plaintext with the client-minted per-blob key (see the encryption boundary note below): under device tokens the node can already unwrap the user's keys, so client-side content encryption would buy nothing while forcing the encrypt/RS pipeline to exist twice.

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
- **Thin client**: `HttpDispatch` targets the shipped `/api/photos/client/*` surface (device-token auth class, host-mounted in `src/main.rs`):
  - `GET  /api/photos/client/membership?library_id=` → `LibraryMembership` (`uploaded_by` derived from the authenticated device's user, never the caller)
  - `POST /api/photos/client/data-block/{blob_id}` — raw streamed body of exactly `X-Hopnet-File-Size` plaintext bytes plus the client-minted per-blob key in `X-Hopnet-Blob-Key` (64 hex chars); returns `UploadedDataBlock`. The declared length is enforced inline server-side (a truncated body 422s mid-put, never a short-but-committed blob)
  - `POST /api/photos/client/transaction` — same contract as the JWT route; the node signs via the device token's bootstrapped session and blocks until the consensus decision (120s)
  - `GET  /api/photos/client/committed/{photo_id}` — confirm probe for the publisher idempotency contract: 200 iff the photo is committed AND owned by the authenticated user, 404 otherwise (404 ⇒ retrying the same photo_id is safe)

**Encryption boundary (deliberate):** the thin client streams *plaintext* content plus the client-minted per-blob key; `api::put` on the node encrypts and RS-encodes exactly as it does for node clients. Client-side content encryption was evaluated and rejected for this phase: it would require splitting `api::put` into a client-reusable encoder plus a fragment-upload surface, triple upload bandwidth (RS 10+20 over ciphertext), and gain nothing while device tokens carry the wrapped user key. The route contract keeps room for a fragment-level variant if the key model tightens.

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
- [x] Source-independent asset model in `hopnet-photos-core::asset` — namespaced source identities, typed resource kinds, resource descriptors, and validation
- [x] Photo publisher — `hopnet-photos-core::publisher::publish_photo_add`: exact-length streaming upload via the dispatch (`ExactLen` adapter, no staging copy), per-recipient blob/metadata key wrapping, `PartialPublish` reconciliation contract; `PhotoDispatch` upload pipe (`upload_data_block`, `fetch_library_members`) implemented on the node-local `Submitter` with `uploaded_by` derived from the authenticated dispatch state, never the caller. 17 publisher tests. Thin-client byte-transport routes shipped (`/api/photos/client/*`, device-token auth — membership, streaming data-block upload with inline declared-length enforcement, transaction relay, committed-state confirm probe); the macOS ingress publisher adapter shipped as its Rust slice (`crates/ingress-publisher`: sidecar→PhotoAsset mapping, `HttpDispatch`, confirm-then-retry `NodePublisher`; publish queue + park-on-unreachable tick in the ingress daemon loop). Swift/FFI wiring of the daemon remains for the Mac phase
- [~] Basic HTTP API — transaction submission, gallery/detail queries, recently-deleted view, and per-user sidecar lifecycle are mounted; keyset browse page `GET /photos/page` (base64url `sort_ms:photo_id` cursors, bidirectional, media-type filter pushdown) and month histogram `GET /photos/histogram` shipped; content fetch route `GET /photos/{id}/resource/{type}` shipped; manual multipart ingest `POST /photos` shipped (asset descriptor + per-kind resource parts, buffered — streaming multi-resource deferred with video; fresh UUIDv7 per request, SourceIdentity dropped at publish) (blob_access-gated, soft-delete-agnostic, Range support, ETag `"{data_block_id}"` + private/immutable caching with 304 revalidation; gallery/detail payloads carry per-photo `resources` lists from the sidecar cache); thin-client content upload routes shipped under `/api/photos/client/*` (device-token auth class)
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

### Phase 3: Shared Libraries [~]
- [x] Library creation and membership lifecycle (2026-08-01): `create_shared_library` + invite/accept/decline/remove consensus txs (JWT-only — excluded from `DEVICE_TX_FUNCTIONS`), per-member wrapped library key (`shared_library_keys`; name encrypted under it), consent-pattern invites with invite-parked wraps, JWT routes `/api/photos/libraries{,/{id}/invite,accept,decline,leave}`
- [x] Auto-share on add: `photo_add`/`photo_edit_content` fan out to members ∪ pending invitees (`read_library_membership` union); handler authz widened from uploader-only to member (equal standing) across delete/restore/edit/undo/favorite
- [x] Access convergence worker (replaces the one-shot bulk join): per-user session-lifetime loop deriving grant/revoke deltas, `library_access_grant`/`_revoke` batches, rotation-lane seam (`ConvergeLane`)
- [x] Membership-gated reads (Design B): shared photos require membership at `query_changes` (both statements) and the resource byte path; pre-staged invitee wraps inert until accept; kick = instant API revocation
- [x] Sidecar rematerialization: membership-diff pre-phase, `photo_view_changes`-triggered paged backfill, purge on leave/kick (`sidecar_libraries` state)
- [x] Ingress daemon shared publish (2026-08-02 — **closes the iCloud shared-library cutover gate**): `mesh_library_id` binding on the ingress `libraries` table (`library set-mesh-id`, scope-bound-only invariant), has-publish-target claim predicate, scope-partitioned publish pass (per-scope resolve/responsibility/parking/attempt-burn; unreachable still parks whole pass), per-(user, library) ingress responsibility with membership-checked claims and kick dissolution, library-scoped fingerprint key on `POST /api/photos/client/resolve` (derived from the library key — cross-member dedup, `photos-ingress-shared` scenario proves adopt-not-reupload e2e)
- [x] Ingress tombstone + restore propagation (2026-08-02): iCloud deletes and Recently-Deleted restores now reach the mesh. `photos.tombstone_published_at` (plus its own retry ledger) records what consensus has been told; disagreeing with `deleted_at` IS the queue, and the marker is deliberately resettable so a delete → restore → delete cycle converges. Rides the same scope-partitioned pass under the same holder gate, after publishing (a photo added and deleted between passes must reach consensus before it is tombstoned there); adopted photos propagate under `consensus_photo_id`. Hard delete holds a published tombstone past its cutoff until the mesh knows, so an offline daemon cannot strand a photo in HopNet forever. `photos-ingress-tombstone` proves both directions e2e with zero divergence
- [x] Ingress edit + metadata propagation (2026-08-02): iCloud edits, reverts and metadata refreshes now reach the mesh. Value-keyed markers — `photo_resources.published_content_hash` for the bytes, `photos.published_asset_modified_at` for the metadata — each disagreeing with its live counterpart forming half the queue; per-photo edit ledger, since one transaction carries every diverged resource. A revert travels as a removal (no upsert expresses an absence) and the local row is retired to a marker until it propagates. **Both edit envelopes amended (pre-release, dev meshes wiped)** to carry `metadata_access`: they previously carried a ciphertext with no way to ship the wraps of its key, which only a writer holding a member private key could satisfy — the ingress daemon holds none, and replacing the ciphertext without new wraps makes a photo's metadata undecryptable for every member, silently. `photo_edit_content` also gained `remove_resources`, and its entry may now be removal-only. Publish and adoption stamp the markers in the same transaction; spool eviction spares bytes an edit still owes the mesh; the scope pass runs publish → restore → edit → delete because both edit handlers reject a tombstoned photo. `photos-ingress-edit` proves all three shapes e2e with zero divergence
- [ ] `Keys` rotation lane (cryptographic revocation after kick) — designed into the convergence contract, not built
- [ ] Empty-library GC (last leaver strands the library row)

### Phase 4: Albums and Organization [ ]
- Album CRUD with consensus
- Album photo membership
- Shared albums with non-library-members (per-photo metadata key wrapping)
- Per-user favorites

### Phase 5: Desktop Frontend [~]
- *Deferred — separate RFC for gallery UI, timeline view, shared library management*
- Tauri command bindings to `photos-core`
- [x] Svelte photo gallery — ingress viewer components folded into the main frontend (windowed keyset browse grid with day headers and hover preview, month histogram rail with jump-to-month, media filters, lightbox with info panel and authenticated downloads); content rendered through a module-scope blob cache (authenticated fetch → object URL keyed by data_block_id, LRU-evicted, flushed on logout); sidecar opt-in/resume/remove flow retained; recently-deleted view rendered through the same grid. Favorites filter and shared-library dropdown deferred (Phases 3-4); lightbox video is full-buffer (no Range streaming through object URLs)

### Phase 6: Mobile Clients [ ]
- *Deferred — UniFFI bindings for `photos-core`, thin client dispatch implementation*
- *Native UI (SwiftUI / Jetpack Compose) consuming shared Rust core*

### Phase 7: FileProvider Export [ ]
- *Deferred — optional filesystem projection of photos for external tool access*

### Phase 8: Advanced Features [ ]
- *Deferred — face detection, scene classification, smart albums, geo-tagging*
