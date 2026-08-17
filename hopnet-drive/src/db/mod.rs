//! Drive-owned schema unit (RFC-015 schema seam).
//!
//! The drive's tables install and uninstall as ONE unit, so the projection
//! is genuinely add/removable. Install order matters: drive FKs point at
//! the host's `users` and the substrate's `data_blocks` — the host chains
//! host DDL → consensus → storage → drive.

pub mod documentprovider;
pub mod fileprovider;
pub mod files;
pub mod mount;
pub mod shares;
pub mod users;

/// The tables this projection owns, in dependency order (parents first).
/// Exposed for divergence tooling and the host's boot tripwire.
pub const TABLES: &[&str] = &["inodes", "modification_log", "incoming_shares", "shares"];

/// This projection's section of the canonical state snapshot (RFC-019 S1).
pub const SNAPSHOT_SECTION: hopnet_common::SectionSpec = hopnet_common::SectionSpec {
    name: "drive",
    format_version: 1,
    tables: &[
        hopnet_common::TableSpec::exported("inodes"),
        hopnet_common::TableSpec::exported("incoming_shares"),
        hopnet_common::TableSpec::exported("shares"),
    ],
};

/// Node-local tables — outside the snapshot universe entirely.
pub const NODE_LOCAL_TABLES: &[&str] = &["modification_log"];

/// This module's schema chain (RFC-020): replay is the only installer.
/// Head ordinal == SNAPSHOT_SECTION.format_version, pinned by host
/// registry tests.
pub static CHAIN: hopnet_common::Chain = hopnet_common::Chain {
    module: "drive",
    steps: &[hopnet_common::Step::sql(
        1,
        "init",
        include_str!("../../migrations/drive/0001_init.sql"),
    )],
};

/// Current decided consensus height — the projection layer's canonical
/// reader (RFC-017 Stage 3; this crate's verbatim SQL copy died with it,
/// same 0-pre-genesis / RecallError semantics).
pub(crate) use hopnet_projection::current_height;

/// Drop the drive's tables (reverse dependency order). Nothing in the host
/// or other projections FKs INTO drive tables, so this is a clean unit.
pub fn uninstall_schema(conn: &rusqlite::Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch(
        "
        DROP TABLE IF EXISTS shares;
        DROP TABLE IF EXISTS incoming_shares;
        DROP TABLE IF EXISTS modification_log;
        DROP TABLE IF EXISTS inodes;
        ",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Should: install cleanly on top of host-fixture + storage tables,
    /// enforce the FK surface (inode → user/data_block), and uninstall as
    /// a clean unit leaving host + storage tables intact.
    /// Should not: leave any drive table behind after uninstall.
    /// Impact: the schema unit IS the projection's modularity contract —
    /// a hidden dependency breaks add/remove of the drive.
    #[test]
    fn install_fk_integrity_uninstall_round_trip() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "PRAGMA foreign_keys = ON;
             CREATE TABLE users (user_id INTEGER PRIMARY KEY, username TEXT);
             CREATE TABLE nodes (node_id INTEGER PRIMARY KEY);",
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

        // FK surface holds: valid insert passes, dangling refs fail.
        conn.execute(
            "INSERT INTO inodes (id, owner_id, path, type, data_id)
             VALUES ('i1', 1, '/a', 0, 'blob1')",
            [],
        )
        .unwrap();
        assert!(
            conn.execute(
                "INSERT INTO inodes (id, owner_id, path, type, data_id)
                 VALUES ('i2', 99, '/b', 0, NULL)",
                [],
            )
            .is_err(),
            "unknown owner must be rejected"
        );
        assert!(
            conn.execute(
                "INSERT INTO inodes (id, owner_id, path, type, data_id)
                 VALUES ('i3', 1, '/c', 0, 'nope')",
                [],
            )
            .is_err(),
            "dangling blob reference must be rejected"
        );

        // Uninstall drops exactly the drive unit.
        conn.execute("DELETE FROM inodes", []).unwrap();
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
            assert!(exists, "{table} must survive drive uninstall");
        }
    }
}
