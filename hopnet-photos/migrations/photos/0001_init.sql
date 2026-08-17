-- photos baseline — ordinal 0001 (RFC-020 S1).
-- Generated once from `initialize` by the baseliner tool, then
-- frozen: replay of this chain is the only installer. Never edit
-- a released step — append a new one.

CREATE TABLE shared_libraries (
            id                       TEXT PRIMARY KEY,    -- UUIDv7
            encrypted_name           BLOB NOT NULL,        -- ChaCha20-Poly1305 under library key
            name_nonce               BLOB NOT NULL         -- 12-byte nonce
        );

CREATE TABLE shared_library_members (
            library_id               TEXT NOT NULL,
            user_id                  INTEGER NOT NULL,

            PRIMARY KEY (library_id, user_id),
            FOREIGN KEY (library_id) REFERENCES shared_libraries(id),
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        );

CREATE TABLE shared_library_keys (
            library_id               TEXT NOT NULL,
            user_id                  INTEGER NOT NULL,
            ephemeral_pubkey         BLOB NOT NULL,        -- 32-byte X25519
            wrapped_key              BLOB NOT NULL,        -- 48 bytes (32 key + 16 tag)

            PRIMARY KEY (library_id, user_id),
            FOREIGN KEY (library_id) REFERENCES shared_libraries(id),
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        );

CREATE TABLE shared_library_invites (
            library_id               TEXT NOT NULL,
            user_id                  INTEGER NOT NULL,     -- invitee
            invited_by               INTEGER NOT NULL,
            operation_id             TEXT NOT NULL,        -- UUIDv7, audit/ordering
            ephemeral_pubkey         BLOB NOT NULL,        -- invitee's library-key wrap
            wrapped_key              BLOB NOT NULL,

            PRIMARY KEY (library_id, user_id),
            FOREIGN KEY (library_id) REFERENCES shared_libraries(id),
            FOREIGN KEY (user_id) REFERENCES users(user_id),
            FOREIGN KEY (invited_by) REFERENCES users(user_id)
        );

CREATE TABLE photos (
            id                       TEXT PRIMARY KEY,     -- UUIDv7 (upload timestamp)
            library_id               TEXT,                 -- NULL = personal library
            uploaded_by              INTEGER NOT NULL,
            encrypted_metadata       BLOB NOT NULL,
            metadata_nonce            BLOB NOT NULL,        -- 12-byte ChaCha20 nonce

            -- Soft delete: NULL = active; 30-day retention window before
            -- periodic cleanup hard-deletes the row + cascades.
            deleted_at               TEXT,                 -- ISO 8601, NULL when active
            deleted_by               INTEGER,

            -- Cross-device asset identity: lowercase-hex keyed HMAC of the
            -- source library's stable asset id (PHCloudIdentifier), keyed
            -- per-user (RFC-014: no unkeyed function of plaintext in
            -- replicated state). NULL = local-only asset, no dedupe.
            -- Opaque to validators; enforced by the partial UNIQUE pair.
            cloud_fingerprint        TEXT,

            FOREIGN KEY (uploaded_by) REFERENCES users(user_id),
            FOREIGN KEY (deleted_by) REFERENCES users(user_id),
            FOREIGN KEY (library_id) REFERENCES shared_libraries(id)
        );

CREATE INDEX idx_photos_library ON photos(library_id);

CREATE INDEX idx_photos_deleted ON photos(deleted_at) WHERE deleted_at IS NOT NULL;

CREATE UNIQUE INDEX idx_photos_fp_personal ON photos(cloud_fingerprint)
            WHERE library_id IS NULL AND cloud_fingerprint IS NOT NULL;

CREATE UNIQUE INDEX idx_photos_fp_shared ON photos(library_id, cloud_fingerprint)
            WHERE library_id IS NOT NULL AND cloud_fingerprint IS NOT NULL;

CREATE TABLE photo_metadata_access (
            photo_id                 TEXT NOT NULL,
            user_id                  INTEGER NOT NULL,
            ephemeral_pubkey         BLOB NOT NULL,         -- 32-byte X25519
            encrypted_metadata_key   BLOB NOT NULL,         -- 48 bytes (32 key + 16 tag)

            PRIMARY KEY (photo_id, user_id),
            FOREIGN KEY (photo_id) REFERENCES photos(id),
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        );

CREATE TABLE photo_resources (
            photo_id                 TEXT NOT NULL,
            resource_type            INTEGER NOT NULL,     -- 0=original,1=edited,2=paired_video,
                                                          -- 3=adjustment_data,4=raw_alternate,7=edited_paired_video
            data_block_id            TEXT NOT NULL,

            PRIMARY KEY (photo_id, resource_type),
            FOREIGN KEY (photo_id) REFERENCES photos(id),
            FOREIGN KEY (data_block_id) REFERENCES data_blocks(id)
        );

CREATE INDEX idx_photo_resources_data_block ON photo_resources(data_block_id);

CREATE TABLE photo_operations (
            id                       TEXT PRIMARY KEY,    -- UUIDv7 (encodes timestamp)
            library_id               TEXT,                -- denormalized filter, NOT FK
            photo_id                 TEXT NOT NULL,
            operation_type           INTEGER NOT NULL,    -- 0=add,1=content_edit,2=delete,
                                                          -- 3=metadata_edit,4=album_add,5=album_remove,
                                                          -- 6=favorite,7=unfavorite,8=restore
            resource_type            INTEGER,             -- which resource (content ops only)
            prior_data_block_id      TEXT,                -- soft pointer — see crate doc
            new_data_block_id        TEXT,                -- soft pointer — see crate doc
            operation_data           BLOB,                -- payload for non-content ops
            performed_by             INTEGER NOT NULL,

            FOREIGN KEY (photo_id) REFERENCES photos(id),
            FOREIGN KEY (performed_by) REFERENCES users(user_id)
        );

CREATE INDEX idx_photo_ops_photo ON photo_operations(photo_id);

CREATE INDEX idx_photo_ops_prior_data ON photo_operations(prior_data_block_id)
            WHERE prior_data_block_id IS NOT NULL;

CREATE INDEX idx_photo_ops_new_data ON photo_operations(new_data_block_id)
            WHERE new_data_block_id IS NOT NULL;

CREATE INDEX idx_photo_ops_library ON photo_operations(library_id);

CREATE TABLE photo_changes (
            photo_id             TEXT PRIMARY KEY,
            changed_at_height    INTEGER NOT NULL
        );

CREATE INDEX idx_photo_changes_height ON photo_changes(changed_at_height);

CREATE TABLE photo_view_changes (
            user_id              INTEGER NOT NULL,
            library_id           TEXT NOT NULL,
            changed_at_height    INTEGER NOT NULL,

            PRIMARY KEY (user_id, library_id),
            FOREIGN KEY (user_id) REFERENCES users(user_id),
            FOREIGN KEY (library_id) REFERENCES shared_libraries(id)
        );

CREATE TABLE photo_albums (
            id                       TEXT PRIMARY KEY,    -- UUIDv7
            library_id               TEXT,                -- NULL = personal album
            encrypted_name           BLOB NOT NULL,
            name_ephemeral_pubkey    BLOB NOT NULL,
            created_by               INTEGER NOT NULL,

            FOREIGN KEY (library_id) REFERENCES shared_libraries(id),
            FOREIGN KEY (created_by) REFERENCES users(user_id)
        );

CREATE TABLE photo_album_entries (
            album_id                 TEXT NOT NULL,
            photo_id                 TEXT NOT NULL,
            sort_order               INTEGER,              -- user-defined ordering

            PRIMARY KEY (album_id, photo_id),
            FOREIGN KEY (album_id) REFERENCES photo_albums(id),
            FOREIGN KEY (photo_id) REFERENCES photos(id)
        );

CREATE TABLE photo_favorites (
            photo_id                 TEXT NOT NULL,
            user_id                  INTEGER NOT NULL,

            PRIMARY KEY (photo_id, user_id),
            FOREIGN KEY (photo_id) REFERENCES photos(id),
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        );

CREATE TABLE photo_ingress_responsibility (
            user_id                  INTEGER NOT NULL,
            library_id               TEXT,                 -- NULL = personal scope
            device_id                TEXT NOT NULL,
            operation_id             TEXT NOT NULL,        -- UUIDv7, audit/ordering

            PRIMARY KEY (user_id, library_id),
            FOREIGN KEY (user_id) REFERENCES users(user_id),
            FOREIGN KEY (device_id) REFERENCES device_tokens(id),
            FOREIGN KEY (library_id) REFERENCES shared_libraries(id)
        );

CREATE UNIQUE INDEX idx_ingress_resp_personal
            ON photo_ingress_responsibility(user_id)
            WHERE library_id IS NULL;

