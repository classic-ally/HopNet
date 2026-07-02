-- state.db initial schema.
-- DDL from docs/specs/apple-photos-ingress.md §Local State Schema — do not
-- edit shapes here without updating the spec.

CREATE TABLE libraries (
    library_id           TEXT PRIMARY KEY,   -- 'personal', 'shared_household'; also the on-disk path component
    display_name         TEXT NOT NULL,      -- UI string for the CLI
    blob_root            TEXT NOT NULL,      -- absolute path on the ingesting device
    sidecar_root_remote  TEXT,               -- backup root on the storage side; NULL = no remote sidecar backup
    scope_binding        TEXT UNIQUE,        -- PhotoKit scope binding; NULL for personal
    retention_days       INTEGER NOT NULL DEFAULT 30,
    created_at           TEXT NOT NULL       -- ISO 8601
);

CREATE TABLE photos (
    photo_id          TEXT PRIMARY KEY,      -- UUIDv7, minted at first discovery
    library_id        TEXT,                  -- NULL = unmapped scope, ingest blocked
    cloud_id          TEXT UNIQUE,           -- PHCloudIdentifier; NULL for local-only assets
    local_id          TEXT,                  -- PHAsset.localIdentifier; device-scoped convenience handle

    -- Cross-asset grouping (RFC-011-compatible, copied verbatim at migration)
    group_id          TEXT,
    group_type        INTEGER,               -- 0=burst, 1=stack, 2=panorama_frames, 3=hdr_bracket
    group_index       INTEGER,
    is_group_pick     INTEGER NOT NULL DEFAULT 0,

    -- Pipeline state (ingress-only, dropped at migration)
    discovered_at     TEXT NOT NULL,
    asset_modified_at TEXT,
    materialized_at   TEXT,                  -- NULL = not all resources written yet
    sidecar_replicated_at TEXT,              -- NULL = local sidecar newer than remote copy

    -- Tombstone (RFC-011-compatible; deleted_by deliberately absent)
    deleted_at        TEXT,

    FOREIGN KEY (library_id) REFERENCES libraries(library_id)
);

CREATE INDEX idx_photos_library ON photos(library_id);
CREATE INDEX idx_photos_pending ON photos(materialized_at) WHERE materialized_at IS NULL;
CREATE INDEX idx_photos_deleted ON photos(deleted_at) WHERE deleted_at IS NOT NULL;
CREATE INDEX idx_photos_group ON photos(group_id) WHERE group_id IS NOT NULL;

CREATE TABLE photo_resources (
    photo_id         TEXT NOT NULL,
    resource_type    INTEGER NOT NULL,       -- RFC-011 values: 0,1,2,3,4,7
    content_hash     TEXT,                   -- BLAKE3 hex; NULL until fetched+hashed
    ext              TEXT,
    size_bytes       INTEGER,

    -- Per-resource pipeline state (ingress-only)
    written_at       TEXT,                   -- NULL = not yet durably on the storage root
    retry_count      INTEGER NOT NULL DEFAULT 0,
    next_retry_at    TEXT,
    last_error       TEXT,

    PRIMARY KEY (photo_id, resource_type),
    FOREIGN KEY (photo_id) REFERENCES photos(photo_id)
);

CREATE INDEX idx_photo_resources_hash ON photo_resources(content_hash) WHERE content_hash IS NOT NULL;

CREATE TABLE blobs (
    library_id       TEXT NOT NULL,
    content_hash     TEXT NOT NULL,
    ext              TEXT NOT NULL,
    size_bytes       INTEGER NOT NULL,
    ref_count        INTEGER NOT NULL,
    written_at       TEXT NOT NULL,

    PRIMARY KEY (library_id, content_hash),
    FOREIGN KEY (library_id) REFERENCES libraries(library_id)
);

CREATE TABLE ingest_log (
    id           INTEGER PRIMARY KEY,        -- rowid, monotonic
    at           TEXT NOT NULL,
    event_type   TEXT NOT NULL,
    photo_id     TEXT,                       -- NULL for non-photo-scoped events
    detail       TEXT                        -- JSON, event-specific payload
);

CREATE INDEX idx_ingest_log_photo ON ingest_log(photo_id) WHERE photo_id IS NOT NULL;
