//! Validator membership rows (RFC-CONSENSUS-002). Crate-owned table;
//! `get_validators` JOINs the HOST's `nodes` table (node_id, name, owner,
//! pubkey) — a documented interface contract, not a foreign key (the crate
//! installs standalone; see the schema comment in `store.rs`).
//!
//! All functions take `&rusqlite::Connection`; both `r2d2::PooledConnection`
//! and `rusqlite::Transaction` deref-coerce to it (the `store::meta_get`
//! pattern). Heights are `i32` to match the host's height plumbing.

use rusqlite::{params, Connection, OptionalExtension};

use crate::store::StoreError;
use crate::types::PubKey;

/// Departure class of a deactivation row (RFC-CONSENSUS-001 "Departure
/// classes"): the kind is an attribute of the shared deactivation write,
/// recorded in the deterministic execute phase.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum DepartureKind {
    Voluntary,
    VotedOut,
}

impl DepartureKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            DepartureKind::Voluntary => "voluntary",
            DepartureKind::VotedOut => "voted_out",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "voluntary" => Some(DepartureKind::Voluntary),
            "voted_out" => Some(DepartureKind::VotedOut),
            _ => None,
        }
    }
}

/// A seated validator joined to its registration row in the host's `nodes`
/// table. `pubkey` uses the crate's `PubKey`, which reads the same
/// bincode-encoded blobs the host writes.
#[derive(Debug, Clone)]
pub struct ValidatorEntry {
    pub node_id: i32,
    pub name: String,
    pub owner: i32,
    pub pubkey: PubKey,
}

const LATEST_ACTIVE_CTE: &str = "
    WITH latest_effective AS (
        SELECT
            node_id,
            MAX(effective_height) AS max_eff
        FROM validators
        WHERE effective_height <= ?
        GROUP BY node_id
    ),
    active_validators AS (
        SELECT
            v.node_id,
            v.is_active
        FROM validators v
        JOIN latest_effective le
            ON v.node_id = le.node_id
            AND v.effective_height = le.max_eff
        WHERE v.is_active = true
    )";

/// The active validator set at `height` (latest effective row per node
/// wins), joined to the host's `nodes` table for keys and identity.
pub fn get_validators(conn: &Connection, height: i32) -> Result<Vec<ValidatorEntry>, StoreError> {
    let sql = format!(
        "{LATEST_ACTIVE_CTE}
        SELECT n.node_id, n.name, n.owner, n.pubkey
        FROM active_validators av
        JOIN nodes n ON av.node_id = n.node_id"
    );
    let mut stmt = conn.prepare_cached(&sql)?;
    let rows = stmt.query_map([height], |row| {
        Ok(ValidatorEntry {
            node_id: row.get(0)?,
            name: row.get(1)?,
            owner: row.get(2)?,
            pubkey: row.get(3)?,
        })
    })?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

/// |active validator set| at `height`. Deliberately no `nodes` JOIN —
/// guard math must not depend on the interface table.
pub fn count_active_validators(conn: &Connection, height: i32) -> Result<u64, StoreError> {
    let sql = format!("{LATEST_ACTIVE_CTE} SELECT COUNT(*) FROM active_validators");
    let mut stmt = conn.prepare_cached(&sql)?;
    let count: i64 = stmt.query_row([height], |row| row.get(0))?;
    Ok(count as u64)
}

/// Whether `node_id` is active at `height` (most recent row at or before
/// the height; absent = never activated = inactive).
pub fn is_node_active(conn: &Connection, node_id: i32, height: i32) -> Result<bool, StoreError> {
    let is_active: Option<bool> = conn
        .query_row(
            "SELECT is_active FROM validators
             WHERE node_id = ? AND effective_height <= ?
             ORDER BY effective_height DESC
             LIMIT 1",
            params![node_id, height],
            |row| row.get(0),
        )
        .optional()?;
    Ok(is_active.unwrap_or(false))
}

/// Activate a validator at `effective_height`. If the node already has a
/// future activation (after the current decided height), that row is moved
/// instead — the hot-swap path. Must run inside the caller's transaction
/// (multi-statement).
pub fn activate_validator(
    conn: &Connection,
    node_id: i32,
    effective_height: i32,
) -> Result<(), StoreError> {
    let current_height = crate::store::last_decided_height(conn)?
        .map_or(0i32, |h| i32::try_from(h.as_db()).unwrap_or(i32::MAX));

    let existing_future_activation: Option<i32> = conn
        .query_row(
            "SELECT effective_height FROM validators
             WHERE node_id = ? AND effective_height > ? AND is_active = true
             ORDER BY effective_height ASC
             LIMIT 1",
            params![node_id, current_height],
            |row| row.get(0),
        )
        .optional()?;

    if let Some(old_height) = existing_future_activation {
        conn.execute(
            "UPDATE validators SET effective_height = ?
             WHERE node_id = ? AND effective_height = ?",
            params![effective_height, node_id, old_height],
        )?;
        tracing::info!(
            "Updated activation for node {node_id} from height {old_height} to height {effective_height}"
        );
    } else {
        conn.execute(
            "INSERT INTO validators (effective_height, node_id, is_active) VALUES (?, ?, true)",
            params![effective_height, node_id],
        )?;
        tracing::info!("Scheduled activation for node {node_id} at height {effective_height}");
    }

    Ok(())
}

/// Insert a deactivation row — the shared execute path of LEAVE and
/// VOTE-OUT; the kind is the only difference. A PK collision on
/// (effective_height, node_id) surfaces as an error and fails the apply
/// identically mesh-wide (deterministic; unreachable through the committed
/// path, which serializes membership transitions one per block).
pub fn deactivate_validator(
    conn: &Connection,
    node_id: i32,
    effective_height: i32,
    kind: DepartureKind,
) -> Result<(), StoreError> {
    conn.execute(
        "INSERT INTO validators (effective_height, node_id, is_active, departure_kind)
         VALUES (?, ?, false, ?)",
        params![effective_height, node_id, kind.as_str()],
    )?;
    tracing::info!(
        "Deactivated validator {node_id} at height {effective_height} ({})",
        kind.as_str()
    );
    Ok(())
}

/// Start height of the current seat: the latest activation row at or
/// before `height`. Meaningful only for members of the valset at that
/// height (the proven-quorum ceiling's pre-boot arm reads it).
pub fn activation_height(
    conn: &Connection,
    node_id: i32,
    height: i32,
) -> Result<Option<i32>, StoreError> {
    let h: Option<i32> = conn
        .query_row(
            "SELECT effective_height FROM validators
             WHERE node_id = ? AND effective_height <= ? AND is_active = true
             ORDER BY effective_height DESC
             LIMIT 1",
            params![node_id, height],
            |row| row.get(0),
        )
        .optional()?;
    Ok(h)
}

/// lastDeparture(node) as of `height`: the latest deactivation row at or
/// before the height, `None` if the node never departed (genesis members
/// and never-departed nodes need no sentinel).
pub fn last_departure(
    conn: &Connection,
    node_id: i32,
    height: i32,
) -> Result<Option<DepartureKind>, StoreError> {
    let kind: Option<String> = conn
        .query_row(
            "SELECT departure_kind FROM validators
             WHERE node_id = ? AND effective_height <= ? AND is_active = false
             ORDER BY effective_height DESC
             LIMIT 1",
            params![node_id, height],
            |row| row.get(0),
        )
        .optional()?;
    match kind {
        None => Ok(None),
        Some(s) => match DepartureKind::parse(&s) {
            Some(k) => Ok(Some(k)),
            None => Err(StoreError::Apply(format!("unknown departure_kind '{s}'"))),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;

    fn test_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        crate::store::install_schema(&conn).unwrap();
        // Interface shim: minimal host nodes table for the JOIN.
        conn.execute_batch(
            "CREATE TABLE nodes (node_id INTEGER PRIMARY KEY, name TEXT NOT NULL,
                                 owner INTEGER NOT NULL, pubkey BLOB NOT NULL);",
        )
        .unwrap();
        conn
    }

    fn add_node(conn: &Connection, node_id: i32) {
        let key = SigningKey::from_bytes(&[node_id as u8; 32]);
        let pubkey = PubKey(key.verifying_key());
        conn.execute(
            "INSERT INTO nodes (node_id, name, owner, pubkey) VALUES (?, ?, 1, ?)",
            params![node_id, format!("node-{node_id}"), &pubkey],
        )
        .unwrap();
    }

    // Should: record departure kinds through a full lifecycle — leave,
    // readmit, vote-out — with latest-wins and height-scoped reads.
    // Should not: erase departure history on readmission.
    // Impact: the S_min exemption (voluntary leavers) reads this fact in
    // the readmission validate phase; wrong history mis-gates seatings.
    #[test]
    fn departure_kind_lifecycle() {
        let conn = test_conn();
        add_node(&conn, 1);

        activate_validator(&conn, 1, 10).unwrap();
        assert!(is_node_active(&conn, 1, 15).unwrap());
        assert_eq!(last_departure(&conn, 1, 15).unwrap(), None);

        deactivate_validator(&conn, 1, 30, DepartureKind::Voluntary).unwrap();
        assert!(!is_node_active(&conn, 1, 35).unwrap());
        assert_eq!(
            last_departure(&conn, 1, 35).unwrap(),
            Some(DepartureKind::Voluntary)
        );

        activate_validator(&conn, 1, 50).unwrap();
        assert!(is_node_active(&conn, 1, 60).unwrap());
        // Readmission does not erase history.
        assert_eq!(
            last_departure(&conn, 1, 60).unwrap(),
            Some(DepartureKind::Voluntary)
        );

        deactivate_validator(&conn, 1, 70, DepartureKind::VotedOut).unwrap();
        // Latest wins…
        assert_eq!(
            last_departure(&conn, 1, 80).unwrap(),
            Some(DepartureKind::VotedOut)
        );
        // …and reads are height-scoped.
        assert_eq!(
            last_departure(&conn, 1, 40).unwrap(),
            Some(DepartureKind::Voluntary)
        );
    }

    // Should: reject rows that violate the departure-kind vocabulary.
    // Should not: allow an active row with a kind, a deactivation without
    // one, or an unknown kind string.
    // Impact: the CHECK is the cheapest determinism guard on the kind —
    // divergent kind writes would split state hashes mesh-wide.
    #[test]
    fn check_constraint_rejections() {
        let conn = test_conn();
        let insert = |h: i32, active: i32, kind: Option<&str>| {
            conn.execute(
                "INSERT INTO validators (effective_height, node_id, is_active, departure_kind)
                 VALUES (?, 1, ?, ?)",
                params![h, active, kind],
            )
        };
        assert!(insert(10, 1, Some("voluntary")).is_err());
        assert!(insert(11, 0, None).is_err());
        assert!(insert(12, 0, Some("bogus")).is_err());
        assert!(insert(13, 1, None).is_ok());
        assert!(insert(14, 0, Some("voted_out")).is_ok());
    }

    // Should: the crate schema install produce the departure_kind column
    // and the per-node index.
    // Impact: a host-side CREATE TABLE shadowing the crate DDL (IF NOT
    // EXISTS) would silently lose the column — this is the tripwire.
    #[test]
    fn column_and_index_exist_post_install() {
        let conn = test_conn();
        conn.prepare("SELECT departure_kind FROM validators LIMIT 0")
            .unwrap();
        let idx: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'index' AND name = 'idx_validator_node'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(idx, 1);
    }

    // Should: report the active set per height with latest-effective-wins
    // semantics across activation, deactivation, and reactivation.
    // Impact: this is the engine's validator_set(height) source — the
    // exact CTE the host used before the descent.
    #[test]
    fn get_validators_with_deactivation() {
        let conn = test_conn();
        add_node(&conn, 1);
        add_node(&conn, 2);

        activate_validator(&conn, 1, 10).unwrap();
        activate_validator(&conn, 2, 10).unwrap();
        deactivate_validator(&conn, 1, 30, DepartureKind::Voluntary).unwrap();

        let at_20: Vec<i32> = get_validators(&conn, 20)
            .unwrap()
            .into_iter()
            .map(|v| v.node_id)
            .collect();
        assert_eq!(at_20, vec![1, 2]);

        let at_35: Vec<i32> = get_validators(&conn, 35)
            .unwrap()
            .into_iter()
            .map(|v| v.node_id)
            .collect();
        assert_eq!(at_35, vec![2]);
    }

    // Should: a deactivated node reappear after a later activation row.
    // Impact: the leave -> rejoin round trip rests on this shape.
    #[test]
    fn get_validators_reactivation() {
        let conn = test_conn();
        add_node(&conn, 1);

        activate_validator(&conn, 1, 10).unwrap();
        deactivate_validator(&conn, 1, 30, DepartureKind::Voluntary).unwrap();
        assert!(get_validators(&conn, 40).unwrap().is_empty());

        // Simulate the mesh having decided past height 30 so the new
        // activation takes the INSERT branch, then verify reactivation
        // (stored as INTEGER, matching the engine's set_last_decided).
        conn.execute(
            "INSERT OR REPLACE INTO consensus_meta (key, value) VALUES ('last_decided_height', ?)",
            params![45i64],
        )
        .unwrap();
        activate_validator(&conn, 1, 50).unwrap();
        let at_60: Vec<i32> = get_validators(&conn, 60)
            .unwrap()
            .into_iter()
            .map(|v| v.node_id)
            .collect();
        assert_eq!(at_60, vec![1]);
    }

    // Should: activation_height report the CURRENT seat's start —
    // latest-wins across reactivation, height-scoped, None when never
    // activated.
    // Impact: the proven-quorum ceiling's pre-boot arm.
    #[test]
    fn activation_height_semantics() {
        let conn = test_conn();
        add_node(&conn, 1);
        assert_eq!(activation_height(&conn, 1, 100).unwrap(), None);

        activate_validator(&conn, 1, 10).unwrap();
        assert_eq!(activation_height(&conn, 1, 100).unwrap(), Some(10));

        deactivate_validator(&conn, 1, 30, DepartureKind::Voluntary).unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO consensus_meta (key, value) VALUES ('last_decided_height', ?)",
            params![45i64],
        )
        .unwrap();
        activate_validator(&conn, 1, 50).unwrap();
        assert_eq!(activation_height(&conn, 1, 100).unwrap(), Some(50));
        assert_eq!(activation_height(&conn, 1, 40).unwrap(), Some(10));
    }

    // Should: count_active_validators agree with get_validators().len()
    // across the lifecycle (count has no nodes JOIN — guard math must not
    // depend on the interface table).
    #[test]
    fn count_agrees_with_len() {
        let conn = test_conn();
        for id in 1..=3 {
            add_node(&conn, id);
            activate_validator(&conn, id, 10).unwrap();
        }
        deactivate_validator(&conn, 2, 30, DepartureKind::VotedOut).unwrap();
        for h in [15, 35] {
            assert_eq!(
                count_active_validators(&conn, h).unwrap(),
                get_validators(&conn, h).unwrap().len() as u64
            );
        }
    }
}
