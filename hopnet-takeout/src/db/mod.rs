//! Takeout-owned schema unit + DB surface (RFC-015 Stage D5b).
//!
//! The static `takeouts` / `imports` tables install and uninstall as ONE
//! unit (moved out of the host DDL); the per-takeout / per-import WORK
//! tables (`takeout_entries_{id}`, `import_paths_{id}`) are created and
//! dropped by the pipelines at runtime and carry a `projection` column so
//! one takeout/import spans every registered projection.

pub mod entries;
pub mod import_paths;
pub mod imports;
pub mod takeout;

/// The static tables this service owns. Work tables are per-id and excluded.
pub const TABLES: &[&str] = &["takeouts", "imports"];

/// Current decided consensus height — the projection layer's canonical
/// reader (RFC-017 Stage 3; this crate's verbatim SQL copy died with it,
/// same 0-pre-genesis / RecallError semantics).
pub(crate) use hopnet_projection::current_height;

/// Install the takeout/import tables. Requires the host's `users` table to
/// exist already (both FK it).
pub fn install_schema(conn: &rusqlite::Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch(
        "
        -- User data takeout tracking (consensus-tracked for network-wide coordination)
        CREATE TABLE takeouts (
            id TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(user_id),
            owner_node_id INTEGER NOT NULL,         -- Node that owns and processes this takeout
            status INTEGER NOT NULL DEFAULT 0 CHECK(status IN (0, 1, 2, 3, 4)),  -- 0=pending, 1=materializing, 2=ready, 3=expired, 4=cancelled
            expires_at TEXT NOT NULL,
            consensus_height INTEGER NOT NULL
        );

        -- Index for efficient lookups of active takeouts and cleanup
        CREATE INDEX idx_takeouts_user_status ON takeouts (user_id, status);
        CREATE INDEX idx_takeouts_expires ON takeouts (expires_at);
        CREATE INDEX idx_takeouts_owner_node ON takeouts (owner_node_id);

        -- User data import tracking (consensus-tracked for network-wide coordination)
        -- status: 0=pending, 1=importing, 2=completed, 3=failed
        -- created_at is derived from UUIDv7 id via CustomUUID::extract_timestamp()
        CREATE TABLE imports (
            id TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(user_id),
            owner_node_id INTEGER NOT NULL,
            status INTEGER NOT NULL DEFAULT 0 CHECK(status IN (0, 1, 2, 3))
        );

        CREATE INDEX idx_imports_user_status ON imports (user_id, status);
        CREATE INDEX idx_imports_owner_node ON imports (owner_node_id);
        ",
    )
}

/// Drop the takeout/import tables. Work tables are per-id and owned by
/// their pipelines; this drops only the static unit.
pub fn uninstall_schema(conn: &rusqlite::Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch(
        "
        DROP TABLE IF EXISTS imports;
        DROP TABLE IF EXISTS takeouts;
        ",
    )
}
