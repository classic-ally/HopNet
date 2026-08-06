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

/// Current decided consensus height — the projection layer's canonical
/// reader (RFC-017 Stage 3; this crate's verbatim SQL copy died with it,
/// same 0-pre-genesis / RecallError semantics).
pub(crate) use hopnet_projection::current_height;

/// Install the drive's tables. Requires `users` (host) and `data_blocks`
/// (hopnet-storage) to exist already.
pub fn install_schema(conn: &rusqlite::Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch(
        "
        CREATE TABLE inodes (
            -- stable identifier for FileProvider (UUIDv7 encodes creation time)
            id              TEXT UNIQUE NOT NULL,
            -- owner of this reference
            owner_id        INTEGER REFERENCES users(user_id) NOT NULL,
            -- denormalized deterministically encrypted string
            -- enables fast folder listing queries without need for recursive parent_id
            path            TEXT NOT NULL,
            -- type of the inode
            type            INTEGER NOT NULL CHECK(type IN (0, 1)),  -- 0=file, 1=folder
            -- FK to the content block
            data_id         TEXT REFERENCES data_blocks(id),

            PRIMARY KEY     (owner_id, path)
        );

        -- 1. The MOST IMPORTANT index for listing folder contents.
        -- Don't need text_pattern_ops due to ART index
        CREATE INDEX idx_inodes_path ON inodes (path);

        -- 2. An index to quickly find all inodes belonging to a specific user.
        CREATE INDEX idx_inodes_owner ON inodes (owner_id);

        -- 3. Index for FileProvider lookups by stable ID
        CREATE INDEX idx_inodes_id ON inodes (id);

        -- NOTE: modification_log is NOT consensus tracked - it's used for local FileProvider state delta computation
        -- This table tracks all file/folder modifications to support incremental sync in FileProvider
        -- It provides a unified change tracking mechanism for all file system operations
        CREATE TABLE modification_log (
            inode_id           TEXT NOT NULL,     -- Stable inode identifier
            owner_id           INTEGER NOT NULL,
            old_parent_id      TEXT,              -- Parent folder BEFORE modification (NULL for new items)
            modified_at_height INTEGER NOT NULL,

            PRIMARY KEY (inode_id, modified_at_height),
            FOREIGN KEY (owner_id) REFERENCES users(user_id)
        );

        -- Index for efficient queries: what was modified for user X since height Y?
        CREATE INDEX idx_modification_log_height ON modification_log (owner_id, modified_at_height);

        -- Sharing: pending share invitations
        CREATE TABLE incoming_shares (
            id                       TEXT PRIMARY KEY,
            data_block_id            TEXT NOT NULL,
            sender_id                INTEGER NOT NULL,
            recipient_id             INTEGER NOT NULL,
            file_access              BLOB NOT NULL,
            display_ephemeral_pubkey BLOB NOT NULL,
            encrypted_display_name   BLOB NOT NULL,
            FOREIGN KEY (data_block_id) REFERENCES data_blocks(id),
            FOREIGN KEY (sender_id) REFERENCES users(user_id),
            FOREIGN KEY (recipient_id) REFERENCES users(user_id)
        );
        CREATE INDEX idx_incoming_shares_recipient ON incoming_shares(recipient_id);
        CREATE INDEX idx_incoming_shares_data_block ON incoming_shares(data_block_id);

        -- Sharing: live-link membership
        CREATE TABLE shares (
            data_block_id   TEXT NOT NULL,
            user_id         INTEGER NOT NULL,
            PRIMARY KEY (data_block_id, user_id),
            FOREIGN KEY (data_block_id) REFERENCES data_blocks(id),
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        );
        ",
    )
}

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
