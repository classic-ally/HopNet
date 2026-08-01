//! Pins: substrate-owned, projection-meaning (RFC-STORAGE-001 Copy
//! classes). A projection pins a blob it needs kept on THIS node; the
//! substrate stores the pin without knowing why. Node-local table, never
//! replicated. Eviction never removes pinned copies — no pressure
//! override; freeing pinned space is the owning projection's own
//! (user-facing, manual) unpin flow.

use crate::types::BlobId;
use std::collections::HashSet;

/// Pin a blob on this node for `owner` (an opaque projection tag, e.g.
/// "drive" or "photos:albums"). Idempotent per (blob, owner).
pub fn pin(conn: &rusqlite::Connection, blob_id: &BlobId, owner: &str) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO hopnet_storage_pins (blob_id, owner, pinned_at)
         VALUES (?1, ?2, datetime('now'))",
        rusqlite::params![blob_id.to_string(), owner],
    )?;
    Ok(())
}

/// Remove `owner`'s pin. The blob stays pinned while ANY owner holds one.
pub fn unpin(conn: &rusqlite::Connection, blob_id: &BlobId, owner: &str) -> rusqlite::Result<()> {
    conn.execute(
        "DELETE FROM hopnet_storage_pins WHERE blob_id = ?1 AND owner = ?2",
        rusqlite::params![blob_id.to_string(), owner],
    )?;
    Ok(())
}

/// Every pinned blob id on this node (any owner) — the eviction planner's
/// keep-set.
pub fn pinned_blob_ids(conn: &rusqlite::Connection) -> rusqlite::Result<HashSet<String>> {
    let mut stmt = conn.prepare("SELECT DISTINCT blob_id FROM hopnet_storage_pins")?;
    let ids = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<_, _>>()?;
    Ok(ids)
}

/// Owners pinning one blob (projection introspection / debugging).
pub fn pins_for_blob(
    conn: &rusqlite::Connection,
    blob_id: &BlobId,
) -> rusqlite::Result<Vec<String>> {
    let mut stmt =
        conn.prepare("SELECT owner FROM hopnet_storage_pins WHERE blob_id = ?1 ORDER BY owner")?;
    let owners = stmt
        .query_map([blob_id.to_string()], |row| row.get(0))?
        .collect::<Result<_, _>>()?;
    Ok(owners)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn conn_with_pins() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE hopnet_storage_pins (
                blob_id TEXT NOT NULL, owner TEXT NOT NULL, pinned_at TEXT NOT NULL,
                PRIMARY KEY (blob_id, owner));",
        )
        .unwrap();
        conn
    }

    // Should: keep a blob in the pinned set while ANY owner pins it, and
    // release it only when the last owner unpins.
    // Should not: let one projection's unpin drop another projection's
    // pin.
    // Impact: pins are the only thing standing between a projection's
    // must-keep data and pressure eviction.
    #[test]
    fn pin_lifecycle_multi_owner() {
        let conn = conn_with_pins();
        let blob = BlobId::from_str("01890a5d-ac96-774b-b9aa-9f8b24f0c9a1").unwrap();

        pin(&conn, &blob, "drive").unwrap();
        pin(&conn, &blob, "photos").unwrap();
        pin(&conn, &blob, "drive").unwrap(); // idempotent
        assert_eq!(
            pins_for_blob(&conn, &blob).unwrap(),
            vec!["drive", "photos"]
        );

        unpin(&conn, &blob, "drive").unwrap();
        assert!(pinned_blob_ids(&conn).unwrap().contains(&blob.to_string()));

        unpin(&conn, &blob, "photos").unwrap();
        assert!(pinned_blob_ids(&conn).unwrap().is_empty());
    }
}
