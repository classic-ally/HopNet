use super::*;
use crate::consensus::types::*;
use rusqlite::TransactionBehavior;

pub fn get_validators_with_conn(
    db_lock: &r2d2::PooledConnection<SqliteConnectionManager>,
    height: i32,
) -> Result<Vec<Node>, DatabaseError> {
    let mut stmt = db_lock
        .prepare(
            "
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
        )
        SELECT n.node_id, n.name, n.owner, n.pubkey
        FROM active_validators av
        JOIN nodes n ON av.node_id = n.node_id;
        ",
        )
        .map_err(|_| DatabaseError::RecallError)?;

    let results = stmt.query_map([height], |row| {
        Ok(Node {
            node_id: row.get(0)?,
            name: row.get(1)?,
            owner: row.get(2)?,
            pubkey: row.get(3)?,
        })
    });

    match results {
        Ok(rows) => {
            let nodes: Vec<Node> = rows.collect::<Result<_, _>>().map_err(|e| {
                tracing::debug!("Error collecting validator rows: {:?}", e);
                DatabaseError::ProcessingError
            })?;
            Ok(nodes)
        }
        Err(e) => {
            tracing::error!("Failed to query validators: {:?}", e);
            Err(DatabaseError::RecordError)
        }
    }
}

pub fn get_validators(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    height: i32,
) -> Result<Vec<Node>, DatabaseError> {
    match db_connection {
        Ok(db_lock) => get_validators_with_conn(&db_lock, height),
        Err(_) => Err(DatabaseError::LockError),
    }
}

pub fn insert_tx_nonces_tx(
    db_tx: &rusqlite::Transaction,
    nonces: &[hopnet_common::CustomUUID],
) -> Result<(), DatabaseError> {
    if nonces.is_empty() {
        return Ok(());
    }
    // Build a single INSERT with multiple VALUES rows for O(1) round-trips.
    // ON CONFLICT DO NOTHING prevents duplicate nonces from crashing the consensus commit.
    let placeholders: Vec<&str> = vec!["(?)"; nonces.len()];
    let sql = format!(
        "INSERT INTO committed_tx_nonces (nonce) VALUES {} ON CONFLICT DO NOTHING",
        placeholders.join(", ")
    );
    let params: Vec<&dyn rusqlite::ToSql> =
        nonces.iter().map(|n| n as &dyn rusqlite::ToSql).collect();
    db_tx.execute(&sql, params.as_slice()).map_err(|e| {
        tracing::error!(
            "Failed to insert {} transaction nonces: {:?}",
            nonces.len(),
            e
        );
        DatabaseError::InsertError
    })?;
    Ok(())
}

/// Check which nonces from the given batch are already committed.
/// Returns the set of nonce strings that exist in committed_tx_nonces.
pub fn check_committed_nonces(
    conn: &r2d2::PooledConnection<SqliteConnectionManager>,
    nonces: &[hopnet_common::CustomUUID],
) -> Result<std::collections::HashSet<String>, DatabaseError> {
    let mut committed = std::collections::HashSet::new();
    if nonces.is_empty() {
        return Ok(committed);
    }
    // Single query with IN (...) for O(1) round-trips.
    let placeholders: Vec<&str> = vec!["?"; nonces.len()];
    let sql = format!(
        "SELECT nonce FROM committed_tx_nonces WHERE nonce IN ({})",
        placeholders.join(", ")
    );
    let params: Vec<&dyn rusqlite::ToSql> =
        nonces.iter().map(|n| n as &dyn rusqlite::ToSql).collect();
    let mut stmt = conn.prepare(&sql).map_err(|_| DatabaseError::RecallError)?;
    let mut rows = stmt
        .query(params.as_slice())
        .map_err(|_| DatabaseError::RecallError)?;
    while let Some(row) = rows.next().map_err(|_| DatabaseError::RecallError)? {
        let nonce_str: String = row.get(0).map_err(|_| DatabaseError::RecallError)?;
        committed.insert(nonce_str);
    }
    Ok(committed)
}

/// Delete nonces older than the cutoff UUID (UUIDv7 ordering = chronological).
/// Called inside a consensus transaction for deterministic cleanup across all nodes.
pub fn cleanup_old_nonces(
    db_tx: &rusqlite::Transaction,
    cutoff: &hopnet_common::CustomUUID,
) -> Result<usize, DatabaseError> {
    let deleted = db_tx
        .execute(
            "DELETE FROM committed_tx_nonces WHERE nonce < ?",
            params![cutoff],
        )
        .map_err(|_| DatabaseError::InsertError)?;
    Ok(deleted)
}

/// Get QC by hash within an existing transaction (core implementation)
pub fn get_node_pubkey(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    node_id: i32,
) -> Result<PubKey, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            let mut stmt = db_lock
                .prepare("SELECT pubkey FROM nodes WHERE node_id = ?")
                .map_err(|_| DatabaseError::RecallError)?;

            let node_pubkey: PubKey = stmt
                .query_row([node_id], |row| row.get(0))
                .map_err(|_| DatabaseError::RecallError)?;

            Ok(node_pubkey)
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}

pub fn get_all_node_pubkeys(
    db_lock: &r2d2::PooledConnection<SqliteConnectionManager>,
) -> Result<std::collections::HashMap<i32, PubKey>, DatabaseError> {
    let mut stmt = db_lock
        .prepare("SELECT node_id, pubkey FROM nodes")
        .map_err(|_| DatabaseError::RecallError)?;

    let rows = stmt
        .query_map([], |row| {
            let node_id: i32 = row.get(0)?;
            let pubkey: PubKey = row.get(1)?;
            Ok((node_id, pubkey))
        })
        .map_err(|_| DatabaseError::RecallError)?;

    let mut map = std::collections::HashMap::new();
    for row in rows {
        let (node_id, pubkey) = row.map_err(|_| DatabaseError::RecallError)?;
        map.insert(node_id, pubkey);
    }

    Ok(map)
}

pub fn get_all_user_pubkeys(
    db_lock: &r2d2::PooledConnection<SqliteConnectionManager>,
) -> Result<std::collections::HashMap<i32, PubKey>, DatabaseError> {
    let mut stmt = db_lock
        .prepare("SELECT user_id, pubkey FROM users")
        .map_err(|_| DatabaseError::RecallError)?;

    let rows = stmt
        .query_map([], |row| {
            let user_id: i32 = row.get(0)?;
            let pubkey: PubKey = row.get(1)?;
            Ok((user_id, pubkey))
        })
        .map_err(|_| DatabaseError::RecallError)?;

    let mut map = std::collections::HashMap::new();
    for row in rows {
        let (user_id, pubkey) = row.map_err(|_| DatabaseError::RecallError)?;
        map.insert(user_id, pubkey);
    }

    Ok(map)
}

pub struct StartupState {
    pub node_id: i32,
    pub user_id: i32,
    pub node_privkey: PrivKey,
}

/// Load all necessary state from database for node restart
pub fn get_startup_state(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
) -> Result<StartupState, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            // First, get node_id and privkey from this_node table
            let mut stmt = db_lock
                .prepare("SELECT node_id, privkey FROM this_node")
                .map_err(|_| DatabaseError::RecallError)?;

            let (node_id, node_privkey) = stmt
                .query_row([], |row| {
                    let node_id: i32 = row.get(0)?;
                    let node_privkey: PrivKey = row.get(1)?;
                    Ok((node_id, node_privkey))
                })
                .map_err(|_| DatabaseError::RecallError)?;

            // Now get user_id from nodes table
            let mut stmt = db_lock
                .prepare("SELECT owner FROM nodes WHERE node_id = ?")
                .map_err(|_| DatabaseError::RecallError)?;

            let user_id: i32 = stmt
                .query_row([node_id], |row| row.get(0))
                .map_err(|_| DatabaseError::RecallError)?;

            Ok(StartupState {
                node_id,
                user_id,
                node_privkey,
            })
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}


pub fn get_current_consensus_height(conn: &rusqlite::Connection) -> Result<i32, DatabaseError> {
    // RFC-017 Stage 3: delegates to the projection layer's canonical reader
    // (hopnet-consensus's SQL underneath). Host entry point kept — the
    // snapshotter captures this function by label.
    hopnet_projection::current_height(conn)
}

/// Check if a node is active at a given height
/// Returns true if the node has an active validator entry effective at or before the given height
pub fn is_node_active(
    tx: &rusqlite::Transaction,
    node_id: i32,
    height: i32,
) -> Result<bool, DatabaseError> {
    use rusqlite::OptionalExtension;

    // Get the most recent validator record at or before this height
    let is_active: Option<bool> = tx
        .query_row(
            "SELECT is_active FROM validators
         WHERE node_id = ? AND effective_height <= ?
         ORDER BY effective_height DESC
         LIMIT 1",
            params![node_id, height],
            |row| row.get(0),
        )
        .optional()
        .map_err(|_| DatabaseError::RecallError)?;

    // If no record found, node is not active
    Ok(is_active.unwrap_or(false))
}

/// Activate a validator at a specific effective height
/// If the node already has a future activation (after current height), it will be updated
/// This enables hot-swap operations where validator-elect activation can be moved forward
pub fn activate_validator(
    tx: &rusqlite::Transaction,
    node_id: i32,
    effective_height: i32,
) -> Result<(), DatabaseError> {
    use rusqlite::OptionalExtension;

    let current_height = get_current_consensus_height(tx)?;

    // Check if node already has the NEXT future activation (earliest after current height)
    let existing_future_activation: Option<i32> = tx
        .query_row(
            "SELECT effective_height FROM validators
         WHERE node_id = ? AND effective_height > ? AND is_active = true
         ORDER BY effective_height ASC
         LIMIT 1",
            params![node_id, current_height],
            |row| row.get(0),
        )
        .optional()
        .map_err(|_| DatabaseError::RecallError)?;

    if let Some(old_height) = existing_future_activation {
        // UPDATE the existing next activation to new height
        tx.execute(
            "UPDATE validators SET effective_height = ?
             WHERE node_id = ? AND effective_height = ?",
            params![effective_height, node_id, old_height],
        )
        .map_err(|_| DatabaseError::InsertError)?;

        tracing::info!(
            "Updated activation for node {} from height {} to height {}",
            node_id,
            old_height,
            effective_height
        );
    } else {
        // INSERT new activation record
        tx.execute(
            "INSERT INTO validators (effective_height, node_id, is_active) VALUES (?, ?, ?)",
            params![effective_height, node_id, true],
        )
        .map_err(|_| DatabaseError::InsertError)?;

        tracing::info!(
            "Scheduled activation for node {} at height {}",
            node_id,
            effective_height
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::SqliteConnectionManager;
    use ed25519_dalek::SigningKey;
    use rand::rand_core::UnwrapErr;
    use rand::rngs::SysRng;

    fn setup_test_db() -> r2d2::Pool<SqliteConnectionManager> {
        let manager = SqliteConnectionManager::memory();
        let pool = r2d2::Pool::builder()
            .max_size(1)
            .connection_customizer(Box::new(crate::db::shared::SqliteInitializer))
            .build(manager)
            .unwrap();

        crate::db::shared::initialize(pool.get().unwrap()).unwrap();
        pool
    }

    fn generate_test_pubkey() -> crate::db::PubKey {
        let signing_key = SigningKey::generate(&mut UnwrapErr(SysRng));
        crate::db::PubKey(signing_key.verifying_key())
    }

    #[test]
    fn test_get_validators_empty() {
        let pool = setup_test_db();

        // Query at height 0 with no validators should return empty list
        let validators = get_validators(pool.get(), 0).unwrap();
        assert_eq!(validators.len(), 0);
    }

    #[test]
    fn test_get_validators_basic_activation() {
        let pool = setup_test_db();
        let conn = pool.get().unwrap();

        let user_pubkey = generate_test_pubkey();
        let node1_pubkey = generate_test_pubkey();
        let node2_pubkey = generate_test_pubkey();

        // Insert test user
        conn.execute(
            "INSERT INTO users (user_id, username, pubkey, x25519_pubkey, encrypted_privkey, key_salt)
             VALUES (?, ?, ?, ?, ?, ?)",
            params![1, "test", &user_pubkey, &vec![0u8; 32], &vec![0u8; 44], &vec![0u8; 16]]
        ).unwrap();

        // Insert test nodes
        conn.execute(
            "INSERT INTO nodes (node_id, name, owner, pubkey)
             VALUES (?, ?, ?, ?)",
            params![1, "node1", 1, &node1_pubkey],
        )
        .unwrap();

        conn.execute(
            "INSERT INTO nodes (node_id, name, owner, pubkey)
             VALUES (?, ?, ?, ?)",
            params![2, "node2", 1, &node2_pubkey],
        )
        .unwrap();

        // Activate validators at different heights
        conn.execute(
            "INSERT INTO validators (effective_height, node_id, is_active) VALUES (10, 1, true)",
            [],
        )
        .unwrap();

        conn.execute(
            "INSERT INTO validators (effective_height, node_id, is_active) VALUES (20, 2, true)",
            [],
        )
        .unwrap();

        drop(conn);

        // At height 5: no validators active yet
        let validators = get_validators(pool.get(), 5).unwrap();
        assert_eq!(validators.len(), 0);

        // At height 15: only node 1 active
        let validators = get_validators(pool.get(), 15).unwrap();
        assert_eq!(validators.len(), 1);
        assert_eq!(validators[0].node_id, 1);
        // At height 25: both nodes active
        let validators = get_validators(pool.get(), 25).unwrap();
        assert_eq!(validators.len(), 2);
        // Results should be ordered by node_id
        assert_eq!(validators[0].node_id, 1);
        assert_eq!(validators[1].node_id, 2);
    }

    #[test]
    fn test_get_validators_with_deactivation() {
        let pool = setup_test_db();
        let conn = pool.get().unwrap();

        let user_pubkey = generate_test_pubkey();
        let node_pubkey = generate_test_pubkey();

        // Insert test user and node
        conn.execute(
            "INSERT INTO users (user_id, username, pubkey, x25519_pubkey, encrypted_privkey, key_salt)
             VALUES (?, ?, ?, ?, ?, ?)",
            params![1, "test", &user_pubkey, &vec![0u8; 32], &vec![0u8; 44], &vec![0u8; 16]]
        ).unwrap();

        conn.execute(
            "INSERT INTO nodes (node_id, name, owner, pubkey)
             VALUES (?, ?, ?, ?)",
            params![1, "node1", 1, &node_pubkey],
        )
        .unwrap();

        // Activate at height 10, deactivate at height 30
        conn.execute(
            "INSERT INTO validators (effective_height, node_id, is_active) VALUES (10, 1, true)",
            [],
        )
        .unwrap();

        conn.execute(
            "INSERT INTO validators (effective_height, node_id, is_active) VALUES (30, 1, false)",
            [],
        )
        .unwrap();

        drop(conn);

        // At height 20: active
        let validators = get_validators(pool.get(), 20).unwrap();
        assert_eq!(validators.len(), 1);
        assert_eq!(validators[0].node_id, 1);

        // At height 35: deactivated
        let validators = get_validators(pool.get(), 35).unwrap();
        assert_eq!(validators.len(), 0);
    }

    #[test]
    fn test_get_validators_reactivation() {
        let pool = setup_test_db();
        let conn = pool.get().unwrap();

        let user_pubkey = generate_test_pubkey();
        let node_pubkey = generate_test_pubkey();

        // Insert test user and node
        conn.execute(
            "INSERT INTO users (user_id, username, pubkey, x25519_pubkey, encrypted_privkey, key_salt)
             VALUES (?, ?, ?, ?, ?, ?)",
            params![1, "test", &user_pubkey, &vec![0u8; 32], &vec![0u8; 44], &vec![0u8; 16]]
        ).unwrap();

        conn.execute(
            "INSERT INTO nodes (node_id, name, owner, pubkey)
             VALUES (?, ?, ?, ?)",
            params![1, "node1", 1, &node_pubkey],
        )
        .unwrap();

        // Activate, deactivate, reactivate
        conn.execute(
            "INSERT INTO validators (effective_height, node_id, is_active) VALUES (10, 1, true)",
            [],
        )
        .unwrap();

        conn.execute(
            "INSERT INTO validators (effective_height, node_id, is_active) VALUES (30, 1, false)",
            [],
        )
        .unwrap();

        conn.execute(
            "INSERT INTO validators (effective_height, node_id, is_active) VALUES (50, 1, true)",
            [],
        )
        .unwrap();

        drop(conn);

        // At height 20: active (first activation)
        let validators = get_validators(pool.get(), 20).unwrap();
        assert_eq!(validators.len(), 1);

        // At height 40: inactive (deactivated)
        let validators = get_validators(pool.get(), 40).unwrap();
        assert_eq!(validators.len(), 0);

        // At height 60: active again (reactivated)
        let validators = get_validators(pool.get(), 60).unwrap();
        assert_eq!(validators.len(), 1);
    }

    #[test]
    fn test_get_validators_multiple_nodes() {
        let pool = setup_test_db();
        let conn = pool.get().unwrap();

        let user_pubkey = generate_test_pubkey();

        // Insert test user
        conn.execute(
            "INSERT INTO users (user_id, username, pubkey, x25519_pubkey, encrypted_privkey, key_salt)
             VALUES (?, ?, ?, ?, ?, ?)",
            params![1, "test", &user_pubkey, &vec![0u8; 32], &vec![0u8; 44], &vec![0u8; 16]]
        ).unwrap();

        // Insert 5 test nodes
        for i in 1..=5 {
            let node_pubkey = generate_test_pubkey();
            conn.execute(
                "INSERT INTO nodes (node_id, name, owner, pubkey)
                 VALUES (?, ?, ?, ?)",
                params![i, format!("node{}", i), 1, &node_pubkey],
            )
            .unwrap();

            // All activate at height 10
            conn.execute(
                "INSERT INTO validators (effective_height, node_id, is_active) VALUES (10, ?, true)",
                params![i]
            ).unwrap();
        }

        // Deactivate nodes 2 and 4 at height 30
        conn.execute(
            "INSERT INTO validators (effective_height, node_id, is_active) VALUES (30, 2, false)",
            [],
        )
        .unwrap();

        conn.execute(
            "INSERT INTO validators (effective_height, node_id, is_active) VALUES (30, 4, false)",
            [],
        )
        .unwrap();

        drop(conn);

        // At height 20: all 5 nodes active
        let validators = get_validators(pool.get(), 20).unwrap();
        assert_eq!(validators.len(), 5);

        // At height 40: only nodes 1, 3, 5 active (2 and 4 deactivated)
        let validators = get_validators(pool.get(), 40).unwrap();
        assert_eq!(validators.len(), 3);
        assert_eq!(validators[0].node_id, 1);
        assert_eq!(validators[1].node_id, 3);
        assert_eq!(validators[2].node_id, 5);
    }

    #[test]
    fn test_get_validators_query_past_height() {
        let pool = setup_test_db();
        let conn = pool.get().unwrap();

        let user_pubkey = generate_test_pubkey();
        let node_pubkey = generate_test_pubkey();

        // Insert test user and node
        conn.execute(
            "INSERT INTO users (user_id, username, pubkey, x25519_pubkey, encrypted_privkey, key_salt)
             VALUES (?, ?, ?, ?, ?, ?)",
            params![1, "test", &user_pubkey, &vec![0u8; 32], &vec![0u8; 44], &vec![0u8; 16]]
        ).unwrap();

        conn.execute(
            "INSERT INTO nodes (node_id, name, owner, pubkey)
             VALUES (?, ?, ?, ?)",
            params![1, "node1", 1, &node_pubkey],
        )
        .unwrap();

        // Activate at height 10
        conn.execute(
            "INSERT INTO validators (effective_height, node_id, is_active) VALUES (10, 1, true)",
            [],
        )
        .unwrap();

        drop(conn);

        // Query at height 100 (far in future): should still return node 1 as active
        let validators = get_validators(pool.get(), 100).unwrap();
        assert_eq!(validators.len(), 1);
        assert_eq!(validators[0].node_id, 1);
    }
}
