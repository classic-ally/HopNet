//! Committed regenesis boundary state (RFC-019 S5): readers/writers for
//! the `regenesis_state` singleton. Normal has ONE canonical encoding —
//! the row is ABSENT (fresh meshes converge on absence, and abort
//! deletes) — so the divergence hash never sees two spellings of the
//! same phase. Only consensus handlers write here; everything else
//! (admission gate, engine, advisory) reads.

use rusqlite::OptionalExtension;

use crate::db::DatabaseError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RegenesisPhase {
    #[default]
    Normal,
    Moratorium,
    Sealed,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct RegenesisState {
    pub phase: RegenesisPhase,
    pub target_version_code: Option<u32>,
    pub snapshot_hash: Option<Vec<u8>>,
    pub seal_height: Option<u64>,
}

pub fn read_regenesis_state(conn: &rusqlite::Connection) -> Result<RegenesisState, DatabaseError> {
    let row = conn
        .query_row(
            "SELECT phase, target_version_code, snapshot_hash, seal_height
             FROM regenesis_state WHERE internal_id = 1",
            [],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, u32>(1)?,
                    row.get::<_, Option<Vec<u8>>>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                ))
            },
        )
        .optional()
        .map_err(|_| DatabaseError::RecallError)?;
    let Some((phase, target, hash, seal)) = row else {
        return Ok(RegenesisState::default());
    };
    let phase = match phase {
        1 => RegenesisPhase::Moratorium,
        2 => RegenesisPhase::Sealed,
        // Handlers never write anything else; a stray value is corruption,
        // not a phase.
        _ => return Err(DatabaseError::RecallError),
    };
    Ok(RegenesisState {
        phase,
        target_version_code: Some(target),
        snapshot_hash: hash,
        seal_height: seal.map(hopnet_common::height::height_from_db),
    })
}

/// `regenesis_start` applies: enter the moratorium. Plain INSERT — the
/// phase rule guarantees no row exists, so a constraint violation here
/// is a loud logic bug, never something to paper over.
pub fn set_moratorium_tx(
    db_tx: &rusqlite::Transaction,
    target_version_code: u32,
) -> Result<(), DatabaseError> {
    db_tx
        .execute(
            "INSERT INTO regenesis_state
                 (internal_id, phase, target_version_code, snapshot_hash, seal_height)
             VALUES (1, 1, ?, NULL, NULL)",
            rusqlite::params![target_version_code],
        )
        .map_err(|_| DatabaseError::ProcessingError)?;
    Ok(())
}

/// `regenesis_commit` applies: seal the epoch at terminal height H. The
/// WHERE clause enforces the window — sealing requires an open
/// moratorium row, so 0 updated rows is a phase violation, not a shrug.
pub fn set_sealed_tx(
    db_tx: &rusqlite::Transaction,
    snapshot_hash: &[u8],
    seal_height: u64,
) -> Result<(), DatabaseError> {
    let n = db_tx
        .execute(
            "UPDATE regenesis_state SET phase = 2, snapshot_hash = ?, seal_height = ?
             WHERE internal_id = 1 AND phase = 1",
            rusqlite::params![
                snapshot_hash,
                hopnet_common::height::height_to_db(seal_height)
            ],
        )
        .map_err(|_| DatabaseError::ProcessingError)?;
    if n == 0 {
        return Err(DatabaseError::ProcessingError);
    }
    Ok(())
}

/// `regenesis_abort` applies: back to normal — the row disappears (the
/// single canonical Normal encoding).
pub fn clear_to_normal_tx(db_tx: &rusqlite::Transaction) -> Result<(), DatabaseError> {
    let n = db_tx
        .execute("DELETE FROM regenesis_state WHERE internal_id = 1", [])
        .map_err(|_| DatabaseError::ProcessingError)?;
    if n == 0 {
        return Err(DatabaseError::ProcessingError);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh_conn() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE regenesis_state (
                internal_id         INTEGER PRIMARY KEY CHECK (internal_id = 1),
                phase               INTEGER NOT NULL,
                target_version_code INTEGER NOT NULL,
                snapshot_hash       BLOB,
                seal_height         INTEGER
            );",
        )
        .unwrap();
        conn
    }

    // Should: report the normal phase on a fresh database with no row.
    // Impact: absence IS the canonical Normal encoding — every fresh
    // mesh node converges on it without any seeding write.
    #[test]
    fn absent_row_reads_as_normal() {
        let conn = fresh_conn();
        assert_eq!(
            read_regenesis_state(&conn).unwrap(),
            RegenesisState::default()
        );
    }

    // Should: round-trip moratorium entry and the abort back to normal.
    // Should not: allow clearing when no boundary is in progress.
    #[test]
    fn moratorium_round_trip() {
        let mut conn = fresh_conn();

        let tx = conn.transaction().unwrap();
        set_moratorium_tx(&tx, 20260801).unwrap();
        let state = read_regenesis_state(&tx).unwrap();
        assert_eq!(state.phase, RegenesisPhase::Moratorium);
        assert_eq!(state.target_version_code, Some(20260801));
        tx.commit().unwrap();

        let tx = conn.transaction().unwrap();
        clear_to_normal_tx(&tx).unwrap();
        assert_eq!(
            read_regenesis_state(&tx).unwrap(),
            RegenesisState::default()
        );
        assert!(clear_to_normal_tx(&tx).is_err());
    }
}
