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

/// Name prefixes of the per-id work tables (`entries::table_name`,
/// `import_paths::table_name`). Each is followed by the id's 32-hex
/// `simple()` form.
pub const WORK_TABLE_PREFIXES: &[&str] = &["takeout_entries_", "import_paths_"];

/// Whether `name` is a runtime work table rather than schema. Strict on
/// purpose: prefix plus exactly 32 lowercase hex, so the boot fingerprint
/// (which skips these) still refuses any other stray table.
pub fn is_work_table(name: &str) -> bool {
    WORK_TABLE_PREFIXES.iter().any(|prefix| {
        name.strip_prefix(prefix).is_some_and(|id| {
            id.len() == 32 && id.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
        })
    })
}

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

/// What a boot sweep did, by table name.
#[derive(Debug, Default, PartialEq)]
pub struct SweepReport {
    pub dropped: Vec<String>,
    pub kept: Vec<String>,
}

/// Drop work tables nothing will ever clean up.
///
/// Takeout cleanup rides `ctx.work.schedule("takeout.cleanup")`, which is
/// fire-and-forget: a process that dies between the Expired/Cancelled apply
/// and the spawned drop — or a node that rejoins via the epoch splice and
/// never re-runs the handler — strands the table forever. So at boot:
///
/// - `takeout_entries_{id}`: dropped when the takeout row is missing or
///   terminal (Expired/Cancelled). A Ready takeout keeps it until expiry.
/// - `import_paths_{id}`: dropped only when the import row is missing.
///   Terminal imports stay readable through the import status/paths routes,
///   which need the table.
pub fn sweep_orphaned_work_tables(
    conn: &rusqlite::Connection,
) -> Result<SweepReport, hopnet_projection::DatabaseError> {
    use hopnet_common::TakeoutStatus;
    use hopnet_projection::DatabaseError;
    use rusqlite::OptionalExtension;

    let recall = |e: rusqlite::Error| DatabaseError::classified(&e, DatabaseError::RecallError);

    let work_tables: Vec<String> = conn
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
        .and_then(|mut stmt| {
            stmt.query_map([], |r| r.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()
        })
        .map_err(recall)?
        .into_iter()
        .filter(|name| is_work_table(name))
        .collect();

    let mut report = SweepReport::default();
    for table in work_tables {
        // is_work_table guarantees `<prefix><32 hex>`; ids are stored hyphenated.
        let (orphaned, hex) = if let Some(hex) = table.strip_prefix("takeout_entries_") {
            let status: Option<TakeoutStatus> = conn
                .query_row(
                    "SELECT status FROM takeouts WHERE replace(id, '-', '') = ?1",
                    [hex],
                    |r| r.get(0),
                )
                .optional()
                .map_err(recall)?;
            let orphaned = matches!(
                status,
                None | Some(TakeoutStatus::Expired | TakeoutStatus::Cancelled)
            );
            (orphaned, hex)
        } else {
            let hex = &table["import_paths_".len()..];
            let exists = conn
                .query_row(
                    "SELECT 1 FROM imports WHERE replace(id, '-', '') = ?1",
                    [hex],
                    |_| Ok(()),
                )
                .optional()
                .map_err(recall)?
                .is_some();
            (!exists, hex)
        };

        if orphaned {
            // Name is validated hex, so formatting it into SQL is safe.
            conn.execute(&format!("DROP TABLE IF EXISTS {table}"), [])
                .map_err(|e| DatabaseError::classified(&e, DatabaseError::ProcessingError))?;
            tracing::info!("Dropped orphaned work table {table} (job {hex})");
            report.dropped.push(table);
        } else {
            tracing::debug!("Keeping work table {table}: job {hex} still live");
            report.kept.push(table);
        }
    }
    Ok(report)
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

#[cfg(test)]
mod tests {
    use super::*;
    use hopnet_common::CustomUUID;

    fn takeout_db() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        // Host-owned table the takeout rows reference.
        conn.execute_batch(
            "CREATE TABLE users (user_id INTEGER PRIMARY KEY);
             INSERT INTO users (user_id) VALUES (0);",
        )
        .unwrap();
        hopnet_common::chain::replay(&conn, &CHAIN, CHAIN.head()).unwrap();
        conn
    }

    fn add_takeout(conn: &rusqlite::Connection, status: i32) -> CustomUUID {
        let id = CustomUUID::new(None);
        conn.execute(
            "INSERT INTO takeouts (id, user_id, owner_node_id, status, expires_at, consensus_height)
             VALUES (?1, 0, 1, ?2, '2026-08-26T03:02:16+00:00', 1)",
            rusqlite::params![id.to_string(), status],
        )
        .unwrap();
        entries::create_entries_table(conn, &id).unwrap();
        id
    }

    fn add_import(conn: &rusqlite::Connection, status: i32) -> CustomUUID {
        let id = CustomUUID::new(None);
        conn.execute(
            "INSERT INTO imports (id, user_id, owner_node_id, status) VALUES (?1, 0, 1, ?2)",
            rusqlite::params![id.to_string(), status],
        )
        .unwrap();
        import_paths::create_import_paths_table(conn, &id).unwrap();
        id
    }

    fn sorted(mut names: Vec<String>) -> Vec<String> {
        names.sort();
        names
    }

    // Should: recognise both work-table families by prefix plus the id's 32-hex form.
    // Should not: treat schema tables, near-misses, or uppercase/short ids as work tables.
    #[test]
    fn work_table_names_are_matched_strictly() {
        let id = CustomUUID::new(None);
        assert!(is_work_table(&entries::table_name(&id)));
        assert!(is_work_table(&import_paths::table_name(&id)));

        for name in [
            "takeouts",
            "imports",
            "stray",
            "takeout_entries_",
            "takeout_entries_notahex",
            "takeout_entries_01A036DE57DE7763BE60263B3093B507",
            "takeout_entries_01a036de57de7763be60263b3093b5",
            "takeout_entries_01a036de57de7763be60263b3093b507x",
        ] {
            assert!(!is_work_table(name), "{name} matched");
        }
    }

    // Impact: cleanup is scheduled fire-and-forget on the Expired/Cancelled
    // apply, so a restart in between strands the table on the owner forever.
    // Should: drop entries tables whose takeout is expired, cancelled, or gone.
    // Should not: drop the entries table of a takeout that is still in progress or ready.
    #[test]
    fn sweep_drops_only_finished_or_unknown_takeouts() {
        let conn = takeout_db();
        let materializing = add_takeout(&conn, 1);
        let ready = add_takeout(&conn, 2);
        let expired = add_takeout(&conn, 3);
        let cancelled = add_takeout(&conn, 4);
        let gone = CustomUUID::new(None);
        entries::create_entries_table(&conn, &gone).unwrap();

        let report = sweep_orphaned_work_tables(&conn).unwrap();

        let names =
            |ids: &[&CustomUUID]| sorted(ids.iter().map(|id| entries::table_name(id)).collect());
        assert_eq!(
            sorted(report.dropped),
            names(&[&expired, &cancelled, &gone])
        );
        assert_eq!(sorted(report.kept), names(&[&materializing, &ready]));

        let remaining: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE name LIKE 'takeout_entries_%'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(remaining, 2);
    }

    // Should: drop an import work table whose import row no longer exists.
    // Should not: drop the work table of a finished import, which the status and paths routes still read.
    #[test]
    fn sweep_keeps_import_tables_while_their_import_exists() {
        let conn = takeout_db();
        let completed = add_import(&conn, 2);
        let gone = CustomUUID::new(None);
        import_paths::create_import_paths_table(&conn, &gone).unwrap();

        let report = sweep_orphaned_work_tables(&conn).unwrap();

        assert_eq!(report.dropped, vec![import_paths::table_name(&gone)]);
        assert_eq!(report.kept, vec![import_paths::table_name(&completed)]);
    }
}
