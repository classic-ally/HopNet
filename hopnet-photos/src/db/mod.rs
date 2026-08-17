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

/// This module's schema chain (RFC-020): replay is the only installer.
/// Head ordinal == SNAPSHOT_SECTION.format_version, pinned by host
/// registry tests.
pub static CHAIN: hopnet_common::Chain = hopnet_common::Chain {
    module: "photos",
    steps: &[hopnet_common::Step::sql(
        1,
        "init",
        include_str!("../../migrations/photos/0001_init.sql"),
    )],
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
        hopnet_storage::store::CHAIN.install(&conn).unwrap();
        CHAIN.install(&conn).unwrap();

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
        CHAIN.install(&conn).unwrap();

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
        CHAIN.install(&conn).unwrap();

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

    // Impact: this index is load-bearing for CONSENSUS, not just for
    // business uniqueness, and that is easy to miss when reading it as
    // "redundant with the PRIMARY KEY".
    //
    // `photo_ingress_responsibility` is exported in this projection's
    // RFC-019 snapshot section, and the canonical serializer orders rows by
    // the declared primary key — here `(library_id, user_id)`, sorted
    // lexicographically. SQLite treats NULLs as distinct even inside a
    // PRIMARY KEY, so without the partial index a user could hold two
    // personal-scope rows, both with `library_id IS NULL`, and the ORDER BY
    // would no longer be a TOTAL order. Two honest replicas could then
    // serialize the same state in different row orders and compute
    // different section hashes.
    //
    // That is a divergence at the seal, where every validator recomputes
    // the artifact hash to vote on `regenesis_commit` — so the failure mode
    // is a boundary no quorum can agree on, not a query returning rows in a
    // surprising order.
    // Should: keep a UNIQUE index that makes the NULL-library case unique
    //   per user, so the exported order stays total.
    #[test]
    fn personal_scope_index_keeps_the_exported_row_order_total() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = OFF;").unwrap();
        CHAIN.install(&conn).unwrap();

        let sql: String = conn
            .query_row(
                "SELECT sql FROM sqlite_master
                 WHERE type = 'index' AND tbl_name = 'photo_ingress_responsibility'
                   AND sql LIKE '%UNIQUE%'",
                [],
                |row| row.get(0),
            )
            .expect(
                "photo_ingress_responsibility needs a UNIQUE index over the NULL-library case: \
                 the composite PK admits duplicate NULLs, which would make the snapshot's \
                 canonical ORDER BY non-total and the certified hash node-dependent",
            );
        assert!(
            sql.contains("user_id") && sql.contains("library_id IS NULL"),
            "the index must be the one that uniquifies personal scope, got: {sql}"
        );

        // And the property it buys, stated directly.
        conn.execute(
            "INSERT INTO photo_ingress_responsibility (user_id, library_id, device_id, operation_id)
             VALUES (1, NULL, 'd1', 'op')",
            [],
        )
        .unwrap();
        assert!(
            conn.execute(
                "INSERT INTO photo_ingress_responsibility (user_id, library_id, device_id, operation_id)
                 VALUES (1, NULL, 'd2', 'op')",
                [],
            )
            .is_err(),
            "two NULL-library rows for one user would make the exported order ambiguous"
        );
    }
}
