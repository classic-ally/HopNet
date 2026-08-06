//! node_staged_version handler behavior (RFC-019 S3).

use super::*;
use crate::consensus::dispatch::process_transaction;
use crate::consensus::types::Transaction;
use crate::db::DatabaseError;
use crate::upgrade::NodeStagedVersion;

fn payload(report: &NodeStagedVersion) -> Vec<u8> {
    bincode::serde::encode_to_vec(report, bincode::config::standard()).unwrap()
}

/// Register the mock node (and its owning user) in the DB so the
/// execute-path UPDATE has a row to hit — FKs are ON.
fn register_node(node: &MockNode) {
    let conn = node.app_state.db_pool.get().unwrap();
    conn.execute(
        "INSERT INTO users (user_id, username, pubkey, x25519_pubkey, encrypted_privkey, key_salt)
         VALUES (1, 'u', ?, ?, ?, ?)",
        rusqlite::params![
            &node.verifying_key,
            &vec![0u8; 32],
            &vec![0u8; 44],
            &vec![0u8; 16]
        ],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO nodes (node_id, name, owner, pubkey) VALUES (?, 'n', 1, ?)",
        rusqlite::params![node.node_id, &node.verifying_key],
    )
    .unwrap();
}

fn attestation(node_id: i32, running: u32, staged: Option<u32>, height: u64) -> NodeStagedVersion {
    NodeStagedVersion {
        node_id,
        running_code: running,
        staged_code: staged,
        attested_height: height,
    }
}

// Should: accept a node's own version attestation — automated, node-
// signed, no user signature.
#[test]
fn node_staged_version_authorized() {
    let node = MockNode::new(3);
    register_node(&node);
    let tx = Transaction::new(
        "node_staged_version".to_string(),
        payload(&attestation(3, 20260800, None, 5)),
        node.node_id,
        &node.signing_key,
    )
    .unwrap();

    let mut conn = node.app_state.db_pool.get().unwrap();
    let db_tx = conn.transaction().unwrap();
    process_transaction(&tx, &node.app_state, false, 0, &db_tx).unwrap();
    let _ = db_tx.rollback();
}

// Should not: let a node attest a version claim for another node.
// Impact: staged version is a node's objective claim about ITSELF
// (RFC-019); impersonation would fabricate the S5 regenesis_start
// precondition.
#[test]
fn node_staged_version_unauthorized() {
    let node = MockNode::new(3);
    register_node(&node);
    let tx = Transaction::new(
        "node_staged_version".to_string(),
        payload(&attestation(4, 20260800, None, 5)),
        node.node_id,
        &node.signing_key,
    )
    .unwrap();

    let mut conn = node.app_state.db_pool.get().unwrap();
    let db_tx = conn.transaction().unwrap();
    let err = process_transaction(&tx, &node.app_state, false, 0, &db_tx).unwrap_err();
    assert!(matches!(err, DatabaseError::AuthorizationError));
    let _ = db_tx.rollback();
}

// Should: overwrite the submitter's whole version claim on every
// attestation — attesting twice converges, and a staged version the
// deployment can no longer reach is erased by the next attestation.
// Impact: logical idempotence lives in the column semantics (nonce dedup
// is per-tx only), and stale staged claims must self-clean when upstream
// moves on.
#[test]
fn node_staged_version_overwrites_and_self_cleans() {
    let node = MockNode::new(3);
    register_node(&node);
    let mut conn = node.app_state.db_pool.get().unwrap();

    let submit = |conn: &mut r2d2::PooledConnection<crate::db::SqliteConnectionManager>,
                  report: &NodeStagedVersion| {
        let tx = Transaction::new(
            "node_staged_version".to_string(),
            payload(report),
            3,
            &node.signing_key,
        )
        .unwrap();
        let db_tx = conn.transaction().unwrap();
        let result = process_transaction(&tx, &node.app_state, true, 0, &db_tx);
        db_tx.commit().unwrap();
        result
    };

    submit(&mut conn, &attestation(3, 20260800, Some(20260801), 5)).unwrap();
    // Node upgraded to 2026.8.1; nothing staged anymore.
    submit(&mut conn, &attestation(3, 20260801, None, 9)).unwrap();

    let (running, staged, height): (Option<u32>, Option<u32>, Option<i64>) = conn
        .query_row(
            "SELECT running_version_code, staged_version_code, version_attested_height
             FROM nodes WHERE node_id = 3",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(running, Some(20260801));
    assert_eq!(staged, None, "stale staged claim must be erased");
    assert_eq!(height, Some(9));

    // Converges: same attestation again leaves identical columns.
    submit(&mut conn, &attestation(3, 20260801, None, 9)).unwrap();
    let again: Option<u32> = conn
        .query_row(
            "SELECT running_version_code FROM nodes WHERE node_id = 3",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(again, Some(20260801));
}

// Should not: write anything for malformed claims — invalid CalVer codes,
// or a staged version repeating running (running counts as trivially
// staged; a repeat is a malformed claim, not information).
#[test]
fn node_staged_version_rejects_malformed_claims() {
    let node = MockNode::new(3);
    register_node(&node);
    let mut conn = node.app_state.db_pool.get().unwrap();

    for report in [
        attestation(3, 20261300, None, 5),           // month 13
        attestation(3, 20260800, Some(20260800), 5), // staged repeats running
    ] {
        let tx = Transaction::new(
            "node_staged_version".to_string(),
            payload(&report),
            3,
            &node.signing_key,
        )
        .unwrap();
        let db_tx = conn.transaction().unwrap();
        let err = process_transaction(&tx, &node.app_state, true, 0, &db_tx).unwrap_err();
        assert!(matches!(err, DatabaseError::InvalidPayload));
        db_tx.commit().unwrap();
    }

    let running: Option<u32> = conn
        .query_row(
            "SELECT running_version_code FROM nodes WHERE node_id = 3",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(running, None);
}
