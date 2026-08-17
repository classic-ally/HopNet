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

/// This service's section of the canonical state snapshot (RFC-019 S1).
/// Per-id work tables are runtime-created and node-local by construction;
/// they never exist in a fresh schema and are outside the universe.
pub const SNAPSHOT_SECTION: hopnet_common::SectionSpec = hopnet_common::SectionSpec {
    name: "takeout",
    format_version: 1,
    tables: &[
        hopnet_common::TableSpec::exported("takeouts"),
        hopnet_common::TableSpec::exported("imports"),
    ],
};

/// Node-local tables — none static; see SNAPSHOT_SECTION note.
pub const NODE_LOCAL_TABLES: &[&str] = &[];

/// This module's schema chain (RFC-020): replay is the only installer.
/// Head ordinal == SNAPSHOT_SECTION.format_version, pinned by host
/// registry tests.
pub static CHAIN: hopnet_common::Chain = hopnet_common::Chain {
    module: "takeout",
    steps: &[hopnet_common::Step::sql(
        1,
        "init",
        include_str!("../../migrations/takeout/0001_init.sql"),
    )],
};

/// Current decided consensus height — the projection layer's canonical
/// reader (RFC-017 Stage 3; this crate's verbatim SQL copy died with it,
/// same 0-pre-genesis / RecallError semantics).
pub(crate) use hopnet_projection::current_height;

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
