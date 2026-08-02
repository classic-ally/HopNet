//! Photos-owned schema unit (RFC-011 Phase 1).
//!
//! The photos projection's tables install and uninstall as ONE unit. The
//! consensus DB stores only opaque encrypted blobs (zero-trust metadata:
//! photos.md:7,30-32) — no plaintext photo metadata is queryable at the
//! consensus level, including cross-asset grouping (group_id, group_type,
//! group_index, is_group_pick all live inside `encrypted_metadata`). All
//! rich query patterns live in a client-side sidecar (photos.md:270-336),
//! which is NOT installed here.
//!
//! Install order matters: photos FKs point at the host's `users` and the
//! substrate's `data_blocks`. Within this unit, `shared_libraries` is the
//! sole parent table — `photos`, `shared_library_members`,
//! `shared_library_keys`, `shared_library_invites`, `photo_view_changes`,
//! and `photo_albums` FK it; everything else FKs `photos` or `users`.

/// The tables this projection owns, in dependency order (parents first).
/// Exposed for divergence tooling and the host's boot tripwire.
pub const TABLES: &[&str] = &[
    "shared_libraries",
    "shared_library_members",
    "shared_library_keys",
    "shared_library_invites",
    "photos",
    "photo_metadata_access",
    "photo_resources",
    "photo_operations",
    "photo_albums",
    "photo_album_entries",
    "photo_favorites",
    "photo_changes",
    "photo_view_changes",
    "photo_ingress_responsibility",
];

/// This projection's snapshot section (RFC-019 S1) — declared next to the
/// DDL so the schema and the exported set cannot drift.
///
/// Every table here exports. All fourteen are written only by consensus
/// handlers inside the decide transaction, so each is a deterministic
/// function of applied blocks and carries across an epoch boundary
/// verbatim. That includes the two change-feeds: `photo_changes` and
/// `photo_view_changes` are derived, but derived IDENTICALLY on every
/// node (upsert-to-latest-height, keyed by photo / by (user, library)),
/// which is what exportability turns on — not whether a row is primary.
///
/// Contrast `hopnet-drive`'s `modification_log`, which is node-local: it
/// is an append-only local index the FileProvider prunes on its own
/// schedule, so two honest nodes legitimately hold different rows.
/// Nothing here has that property, so `node_local_tables` stays empty
/// and the trait default is left in place.
///
/// Table order matches `TABLES` (dependency order) and is load-bearing:
/// the section hash rolls up per table in declaration order.
pub const SNAPSHOT_SECTION: hopnet_common::SectionSpec = hopnet_common::SectionSpec {
    name: "photos",
    format_version: 1,
    tables: &[
        hopnet_common::TableSpec::exported("shared_libraries"),
        hopnet_common::TableSpec::exported("shared_library_members"),
        hopnet_common::TableSpec::exported("shared_library_keys"),
        hopnet_common::TableSpec::exported("shared_library_invites"),
        hopnet_common::TableSpec::exported("photos"),
        hopnet_common::TableSpec::exported("photo_metadata_access"),
        hopnet_common::TableSpec::exported("photo_resources"),
        hopnet_common::TableSpec::exported("photo_operations"),
        hopnet_common::TableSpec::exported("photo_albums"),
        hopnet_common::TableSpec::exported("photo_album_entries"),
        hopnet_common::TableSpec::exported("photo_favorites"),
        hopnet_common::TableSpec::exported("photo_changes"),
        hopnet_common::TableSpec::exported("photo_view_changes"),
        hopnet_common::TableSpec::exported("photo_ingress_responsibility"),
    ],
};

pub mod libraries;
pub mod photos;

/// Edit-history retention window (days). Operation-log rows older than
/// this stop pinning their `prior`/`new_data_block_id` blobs, allowing
/// orphan cleanup to collect superseded edits. The window is enforced
/// in BOTH the per-row reference check (Rust, `CustomUUID::retention_cutoff`)
/// AND the bulk-candidate subquery (SQL, `uuid_extract_timestamp`). The
/// two implementations must agree to within clock skew or a boundary
/// block fails the cleanup batch (fails closed, self-heals next pass).
pub const EDIT_HISTORY_RETENTION_DAYS: i64 = 30;

/// Install the photos projection's tables. Requires `users` (host) and
/// `data_blocks` (hopnet-storage) to exist already — the host chains
/// host DDL → consensus → storage → drive → photos → takeout.
///
/// `photo_operations.prior_data_block_id` and `new_data_block_id` are
/// deliberately NOT FK-constrained: operation rows are retained
/// indefinitely for audit (photos.md:581), but the blobs they reference
/// become collectable after the edit-history window. A hard FK would
/// raise SQLITE_CONSTRAINT on every orphan-cleanup pass once the first
/// edit ages out — and that cleanup runs inside a consensus tx, so the
/// failure would replay on every validator, forever. Soft-pointer-
/// policed-by-provider is the design (photos.md:397-475).
pub fn install_schema(conn: &rusqlite::Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch(
        "
        -- Shared libraries: multi-user key-distribution membership
        -- (photos.md:155-176). There is no owner column — all members
        -- have equal standing. The personal library is NULL on `photos`
        -- (photos.md:57), not a sentinel row here. The name is encrypted
        -- under the LIBRARY key (per-member wraps in shared_library_keys),
        -- not a single-recipient ECDH seal — every member can render it.
        CREATE TABLE shared_libraries (
            id                       TEXT PRIMARY KEY,    -- UUIDv7
            encrypted_name           BLOB NOT NULL,        -- ChaCha20-Poly1305 under library key
            name_nonce               BLOB NOT NULL         -- 12-byte nonce
        );

        -- Library membership (N-way, no owner). Membership is the READ
        -- GATE for shared photos: access-row existence alone is not
        -- sufficient (pre-staged invitee wraps are inert without a row
        -- here).
        CREATE TABLE shared_library_members (
            library_id               TEXT NOT NULL,
            user_id                  INTEGER NOT NULL,

            PRIMARY KEY (library_id, user_id),
            FOREIGN KEY (library_id) REFERENCES shared_libraries(id),
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        );

        -- Per-member wrapped library key (X25519 ECDH wrap, LIBRARY_KEY_
        -- WRAP_DOMAIN, wrap id = library id bytes). Decrypts the library
        -- name; the designed seam for the future library-scoped cloud
        -- fingerprint key.
        CREATE TABLE shared_library_keys (
            library_id               TEXT NOT NULL,
            user_id                  INTEGER NOT NULL,
            ephemeral_pubkey         BLOB NOT NULL,        -- 32-byte X25519
            wrapped_key              BLOB NOT NULL,        -- 48 bytes (32 key + 16 tag)

            PRIMARY KEY (library_id, user_id),
            FOREIGN KEY (library_id) REFERENCES shared_libraries(id),
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        );

        -- Pending membership: consent pattern mirroring drive's
        -- incoming_shares. The row carries the invitee's library-key wrap,
        -- minted AT invite time, so accept needs no inviter online and the
        -- library name renders in the invite listing. Access-row
        -- pre-staging (the convergence worker) targets invitees too;
        -- membership-gated reads keep everything invisible until accept.
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

        -- Photos: identity, tombstone. The encrypted_metadata blob carries
        -- date/dimensions/EXIF/camera/GPS AND cross-asset grouping
        -- (group_id, group_type, group_index, is_group_pick) — none
        -- queryable at the consensus level (photos.md:7,30-32,85).
        -- Grouping was folded into the encrypted blob (originally
        -- photos.md:62-98 plaintext, amended): no consensus query needs
        -- group awareness (deletion expands to a batch tx client-side;
        -- burst rollup is a sidecar query at photos.md:338), and the
        -- plaintext columns leaked structural correlation + photography
        -- habits (group_type) without any offsetting consensus use.
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

        -- Dedupe uniqueness must be split: SQLite treats NULLs as distinct
        -- in UNIQUE indexes, so a composite UNIQUE(library_id,
        -- cloud_fingerprint) would never constrain personal (NULL-library)
        -- rows. Fingerprints are per-user-keyed HMACs, so a global index
        -- over personal rows is collision-safe across users.
        CREATE UNIQUE INDEX idx_photos_fp_personal ON photos(cloud_fingerprint)
            WHERE library_id IS NULL AND cloud_fingerprint IS NOT NULL;
        CREATE UNIQUE INDEX idx_photos_fp_shared ON photos(library_id, cloud_fingerprint)
            WHERE library_id IS NOT NULL AND cloud_fingerprint IS NOT NULL;

        -- Per-user metadata decryption keys (photos.md:100-116). Mirrors
        -- the storage substrate's `blob_access` pattern: each photo's
        -- metadata has its own symmetric key, wrapped per-user via ECDH.
        CREATE TABLE photo_metadata_access (
            photo_id                 TEXT NOT NULL,
            user_id                  INTEGER NOT NULL,
            ephemeral_pubkey         BLOB NOT NULL,         -- 32-byte X25519
            encrypted_metadata_key   BLOB NOT NULL,         -- 48 bytes (32 key + 16 tag)

            PRIMARY KEY (photo_id, user_id),
            FOREIGN KEY (photo_id) REFERENCES photos(id),
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        );

        -- A photo's byte streams (original, edited, paired_video, etc.).
        -- (photos.md:120-153). data_block_id FKs the storage substrate.
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

        -- Append-only operation log (photos.md:222-268). Enables undo,
        -- audit trail, and retention-aware cleanup.
        --
        -- prior/new_data_block_id are SOFT pointers: operation rows are
        -- retained indefinitely while the blobs they reference become
        -- collectable after EDIT_HISTORY_RETENTION_DAYS. The
        -- PhotosReferenceProvider enforces the window via UUIDv7 timestamp
        -- filtering (photos.md:459-466). No FK — see install_schema doc.
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

        -- Incremental sync feed: upserted by every handler (add, delete,
        -- restore, edit, cleanup) so clients can poll for changes by
        -- consensus height. NO FK — the feed row must survive hard-delete
        -- so offline clients learn of the tombstone expiry.
        CREATE TABLE photo_changes (
            photo_id             TEXT PRIMARY KEY,
            changed_at_height    INTEGER NOT NULL
        );
        CREATE INDEX idx_photo_changes_height ON photo_changes(changed_at_height);

        -- Per-user VIEW-change signal: 'your visibility into this library
        -- changed at height h' — written by membership/grant/revoke
        -- handlers, consumed by the sidecar sync worker to trigger a
        -- targeted library backfill or purge. Deliberately separate from
        -- photo_changes, which records only changes to the photo itself;
        -- a grant does not edit the photo. Upserted to the latest height.
        CREATE TABLE photo_view_changes (
            user_id              INTEGER NOT NULL,
            library_id           TEXT NOT NULL,
            changed_at_height    INTEGER NOT NULL,

            PRIMARY KEY (user_id, library_id),
            FOREIGN KEY (user_id) REFERENCES users(user_id),
            FOREIGN KEY (library_id) REFERENCES shared_libraries(id)
        );

        -- Albums: lightweight groupings (photos.md:178-205). A photo can
        -- belong to multiple albums. Personal or shared.
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

        -- Per-user favorites (photos.md:208-218).
        CREATE TABLE photo_favorites (
            photo_id                 TEXT NOT NULL,
            user_id                  INTEGER NOT NULL,

            PRIMARY KEY (photo_id, user_id),
            FOREIGN KEY (photo_id) REFERENCES photos(id),
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        );

        -- Ingress responsibility, per (user, scope): the single device
        -- allowed to publish ingress mutations for a user within a scope —
        -- NULL library_id is the personal partition, non-NULL a shared
        -- library the user is a member of. Each member claims
        -- independently for their own devices; cross-member dedup within a
        -- shared library is the fingerprint pair's job, not
        -- responsibility's. Claimed and transferred ONLY via the JWT claim
        -- route (photo_ingress_claim) — daemons never auto-claim. Enforced
        -- at thin-client route admission, not in handlers: the UNIQUE
        -- fingerprint pair above is the correctness backstop for any
        -- admission race. device_tokens is consensus-replicated, so the FK
        -- and the handler's ownership check are deterministic on every
        -- validator.
        -- The composite PK owns shared-scope uniqueness AND gives the
        -- debug state snapshot its deterministic ORDER BY (snapshot
        -- hashing requires a declared PK). SQLite treats NULLs as
        -- distinct even in a PRIMARY KEY (rowid-table quirk), so the PK
        -- cannot constrain the personal (NULL-library) row — the partial
        -- index below does, mirroring idx_photos_fp_personal.
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
        ",
    )
}

/// Drop the photos projection's tables (reverse dependency order:
/// children first, since `PRAGMA foreign_keys = ON` is set on every
/// pooled connection). Nothing in the host or other projections FKs
/// INTO photos tables, so this is a clean unit.
pub fn uninstall_schema(conn: &rusqlite::Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch(
        "
        DROP TABLE IF EXISTS photo_ingress_responsibility;
        DROP TABLE IF EXISTS photo_view_changes;
        DROP TABLE IF EXISTS photo_favorites;
        DROP TABLE IF EXISTS photo_changes;
        DROP TABLE IF EXISTS photo_album_entries;
        DROP TABLE IF EXISTS photo_albums;
        DROP TABLE IF EXISTS photo_operations;
        DROP TABLE IF EXISTS photo_resources;
        DROP TABLE IF EXISTS photo_metadata_access;
        DROP TABLE IF EXISTS photos;
        DROP TABLE IF EXISTS shared_library_invites;
        DROP TABLE IF EXISTS shared_library_keys;
        DROP TABLE IF EXISTS shared_library_members;
        DROP TABLE IF EXISTS shared_libraries;
        ",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Should: install cleanly on top of host-fixture + storage tables,
    /// enforce the FK surface (photo → user/shared_library/data_block),
    /// and uninstall as a clean unit leaving host + storage tables intact.
    /// Should not: leave any photos table behind after uninstall.
    /// Impact: the schema unit IS the projection's modularity contract —
    /// a hidden dependency breaks add/remove of the photos projection.
    #[test]
    fn install_fk_integrity_uninstall_round_trip() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "PRAGMA foreign_keys = ON;
             CREATE TABLE users (user_id INTEGER PRIMARY KEY, username TEXT);
             CREATE TABLE consensus_meta (key TEXT PRIMARY KEY, value BLOB);
             CREATE TABLE nodes (node_id INTEGER PRIMARY KEY);
             CREATE TABLE device_tokens (
                 id TEXT PRIMARY KEY,
                 user_id INTEGER NOT NULL,
                 api_key_hash BLOB NOT NULL,
                 encrypted_device_name TEXT NOT NULL,
                 wrapped_user_key BLOB NOT NULL,
                 FOREIGN KEY (user_id) REFERENCES users(user_id)
             );",
        )
        .unwrap();
        hopnet_storage::store::install_schema(&conn).unwrap();
        install_schema(&conn).unwrap();

        conn.execute("INSERT INTO users (user_id, username) VALUES (1, 'a')", [])
            .unwrap();
        conn.execute(
            "INSERT INTO data_blocks (id, file_hash, fragment_count, added_bytes, file_size)
             VALUES ('blob1', x'00', 1, 0, 10)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO shared_libraries (id, encrypted_name, name_nonce)
             VALUES ('lib1', x'00', x'00')",
            [],
        )
        .unwrap();

        // FK surface holds: valid photo insert passes.
        conn.execute(
            "INSERT INTO photos
               (id, library_id, uploaded_by, encrypted_metadata, metadata_nonce)
             VALUES ('ph1', 'lib1', 1, x'00', x'00')",
            [],
        )
        .unwrap();
        // Personal library (NULL library_id) is valid.
        conn.execute(
            "INSERT INTO photos
               (id, library_id, uploaded_by, encrypted_metadata, metadata_nonce)
             VALUES ('ph2', NULL, 1, x'00', x'00')",
            [],
        )
        .unwrap();

        // Dangling references are rejected.
        assert!(
            conn.execute(
                "INSERT INTO photos
                   (id, library_id, uploaded_by, encrypted_metadata, metadata_nonce)
                 VALUES ('ph3', 'lib1', 99, x'00', x'00')",
                [],
            )
            .is_err(),
            "unknown uploaded_by must be rejected"
        );
        assert!(
            conn.execute(
                "INSERT INTO photos
                   (id, library_id, uploaded_by, encrypted_metadata, metadata_nonce)
                 VALUES ('ph4', 'nope', 1, x'00', x'00')",
                [],
            )
            .is_err(),
            "unknown library_id must be rejected"
        );
        assert!(
            conn.execute(
                "INSERT INTO photo_resources (photo_id, resource_type, data_block_id)
                 VALUES ('ph1', 0, 'nope')",
                [],
            )
            .is_err(),
            "dangling data_block_id must be rejected"
        );

        // Valid resource + operation log entry.
        conn.execute(
            "INSERT INTO photo_resources (photo_id, resource_type, data_block_id)
             VALUES ('ph1', 0, 'blob1')",
            [],
        )
        .unwrap();
        // Soft pointers: prior/new_data_block_id can reference blocks that
        // don't exist (the operation log outlives the blobs it points at).
        conn.execute(
            "INSERT INTO photo_operations
               (id, library_id, photo_id, operation_type,
                prior_data_block_id, new_data_block_id, performed_by)
             VALUES ('op1', 'lib1', 'ph1', 1, 'old_blob_gone', 'blob1', 1)",
            [],
        )
        .unwrap();

        // Membership-lifecycle FK surface: key wraps, invites, and view
        // signals all hang off shared_libraries + users; dangling parents
        // are rejected.
        conn.execute(
            "INSERT INTO shared_library_keys (library_id, user_id, ephemeral_pubkey, wrapped_key)
             VALUES ('lib1', 1, x'00', x'00')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO shared_library_invites
               (library_id, user_id, invited_by, operation_id, ephemeral_pubkey, wrapped_key)
             VALUES ('lib1', 1, 1, 'op-inv', x'00', x'00')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO photo_view_changes (user_id, library_id, changed_at_height)
             VALUES (1, 'lib1', 7)",
            [],
        )
        .unwrap();
        assert!(
            conn.execute(
                "INSERT INTO shared_library_keys (library_id, user_id, ephemeral_pubkey, wrapped_key)
                 VALUES ('nope', 1, x'00', x'00')",
                [],
            )
            .is_err(),
            "key wrap for an unknown library must be rejected"
        );
        assert!(
            conn.execute(
                "INSERT INTO shared_library_invites
                   (library_id, user_id, invited_by, operation_id, ephemeral_pubkey, wrapped_key)
                 VALUES ('lib1', 99, 1, 'op-x', x'00', x'00')",
                [],
            )
            .is_err(),
            "invite for an unknown user must be rejected"
        );
        assert!(
            conn.execute(
                "INSERT INTO photo_view_changes (user_id, library_id, changed_at_height)
                 VALUES (1, 'nope', 7)",
                [],
            )
            .is_err(),
            "view signal for an unknown library must be rejected"
        );

        // Responsibility FK surface: valid claim for an existing owned
        // device passes; a dangling device_id is rejected.
        conn.execute(
            "INSERT INTO device_tokens VALUES ('dev1', 1, x'00', 'enc', x'00')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO photo_ingress_responsibility (user_id, device_id, operation_id)
             VALUES (1, 'dev1', 'op1')",
            [],
        )
        .unwrap();
        assert!(
            conn.execute(
                "INSERT INTO photo_ingress_responsibility (user_id, device_id, operation_id)
                 VALUES (99, 'nope', 'op2')",
                [],
            )
            .is_err(),
            "dangling device_id must be rejected"
        );

        // Uninstall drops exactly the photos unit. Must clear children first
        // (foreign_keys = ON).
        conn.execute("DELETE FROM photo_ingress_responsibility", [])
            .unwrap();
        conn.execute("DELETE FROM photo_view_changes", []).unwrap();
        conn.execute("DELETE FROM photo_operations", []).unwrap();
        conn.execute("DELETE FROM photo_resources", []).unwrap();
        conn.execute("DELETE FROM photo_metadata_access", [])
            .unwrap();
        conn.execute("DELETE FROM photo_favorites", []).unwrap();
        conn.execute("DELETE FROM photo_album_entries", []).unwrap();
        conn.execute("DELETE FROM photos", []).unwrap();
        conn.execute("DELETE FROM shared_library_invites", [])
            .unwrap();
        conn.execute("DELETE FROM shared_library_keys", []).unwrap();
        conn.execute("DELETE FROM shared_library_members", [])
            .unwrap();
        conn.execute("DELETE FROM shared_libraries", []).unwrap();
        uninstall_schema(&conn).unwrap();

        for table in TABLES {
            let exists: bool = conn
                .query_row(
                    "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name=?",
                    [table],
                    |r| r.get(0),
                )
                .unwrap();
            assert!(!exists, "{table} must be gone after uninstall");
        }
        // Host + storage tables intact
        for table in ["users", "data_blocks", "fragment_hashes"] {
            let exists: bool = conn
                .query_row(
                    "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name=?",
                    [table],
                    |r| r.get(0),
                )
                .unwrap();
            assert!(exists, "{table} must survive photos uninstall");
        }
    }

    // Impact: SQLite treats NULLs as distinct in UNIQUE indexes, so dedupe
    // correctness rests entirely on the partial-index pair having exactly
    // the right predicates — this pins them at the SQL level.
    // Should: reject a duplicate fingerprint within the personal scope and
    // within one shared library.
    // Should not: constrain NULL fingerprints, or collide a personal-scope
    // fingerprint with the same value in a shared library.
    #[test]
    fn fingerprint_partial_unique_index_semantics() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "PRAGMA foreign_keys = OFF;", // isolate index semantics from FK setup
        )
        .unwrap();
        install_schema(&conn).unwrap();

        let insert = |id: &str, lib: Option<&str>, fp: Option<&str>| {
            conn.execute(
                "INSERT INTO photos (id, library_id, uploaded_by, encrypted_metadata, metadata_nonce, cloud_fingerprint)
                 VALUES (?1, ?2, 1, x'00', x'00', ?3)",
                rusqlite::params![id, lib, fp],
            )
        };

        insert("p1", None, Some("fp_a")).unwrap();
        assert!(
            insert("p2", None, Some("fp_a")).is_err(),
            "duplicate personal-scope fingerprint must be rejected"
        );
        insert("p3", Some("lib1"), Some("fp_a"))
            .expect("same fingerprint under a shared library is a different scope");
        assert!(
            insert("p4", Some("lib1"), Some("fp_a")).is_err(),
            "duplicate fingerprint within one shared library must be rejected"
        );
        insert("p5", Some("lib2"), Some("fp_a"))
            .expect("same fingerprint under a different shared library is fine");
        insert("p6", None, None).unwrap();
        insert("p7", None, None).expect("NULL fingerprints are unconstrained (local-only assets)");
    }

    // Impact: NULLs are distinct even in the composite PRIMARY KEY, so
    // personal-scope uniqueness rests entirely on the partial index — a
    // wrong predicate would let a user hold two holders for one scope,
    // and the claim upsert's conflict targets would silently insert
    // instead of transferring.
    // Should: allow one personal row per user plus one row per shared
    // library, including the same library across different users.
    // Should not: allow a second personal row or a second row for the
    // same (user, library).
    #[test]
    fn responsibility_partial_unique_index_semantics() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = OFF;").unwrap();
        install_schema(&conn).unwrap();

        let insert = |user: i32, lib: Option<&str>, dev: &str| {
            conn.execute(
                "INSERT INTO photo_ingress_responsibility (user_id, library_id, device_id, operation_id)
                 VALUES (?1, ?2, ?3, 'op')",
                rusqlite::params![user, lib, dev],
            )
        };

        insert(1, None, "d1").unwrap();
        assert!(
            insert(1, None, "d2").is_err(),
            "second personal row for one user must be rejected"
        );
        insert(1, Some("lib1"), "d1").expect("personal and shared scopes coexist");
        insert(1, Some("lib2"), "d2").expect("distinct libraries are distinct scopes");
        assert!(
            insert(1, Some("lib1"), "d2").is_err(),
            "second row for one (user, library) must be rejected"
        );
        insert(2, Some("lib1"), "d9").expect("two members hold the same library independently");
    }
}
