//! Regenesis boundary handler behavior (RFC-019 S5): the start/abort
//! phase machine over the regenesis_state singleton.

use super::*;
use crate::consensus::dispatch::process_transaction;
use crate::consensus::types::Transaction;
use crate::db::DatabaseError;
use crate::db::regenesis::{
    RegenesisPhase, RegenesisState, read_regenesis_state, set_moratorium_tx,
};
use crate::regenesis::{RegenesisAbort, RegenesisStart};

fn start_payload(target: u32) -> Vec<u8> {
    bincode::serde::encode_to_vec(
        RegenesisStart {
            target_version_code: target,
        },
        bincode::config::standard(),
    )
    .unwrap()
}

fn abort_payload() -> Vec<u8> {
    bincode::serde::encode_to_vec(RegenesisAbort {}, bincode::config::standard()).unwrap()
}

/// Register the mock node (and its owning user) — FKs are ON.
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

/// Seat the node as a validator and give it a committed running version.
fn seat_with_version(node: &MockNode, node_id: i32, running: u32) {
    let conn = node.app_state.db_pool.get().unwrap();
    conn.execute(
        "UPDATE nodes SET running_version_code = ? WHERE node_id = ?",
        rusqlite::params![running, node_id],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO validators (effective_height, node_id, is_active, departure_kind)
         VALUES (0, ?, 1, NULL)",
        rusqlite::params![node_id],
    )
    .unwrap();
}

/// Register a second, keyless validator row in the same database (only
/// its committed table state matters for the precondition).
fn register_extra_seated(node: &MockNode, node_id: i32, running: Option<u32>) {
    // A distinct VALID encoded pubkey — get_validators decodes the blob.
    let extra_key = MockUser::new(node_id).verifying_key;
    let conn = node.app_state.db_pool.get().unwrap();
    conn.execute(
        "INSERT INTO nodes (node_id, name, owner, pubkey, running_version_code)
         VALUES (?, 'extra', 1, ?, ?)",
        rusqlite::params![node_id, &extra_key, running],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO validators (effective_height, node_id, is_active, departure_kind)
         VALUES (0, ?, 1, NULL)",
        rusqlite::params![node_id],
    )
    .unwrap();
}

/// Validate-then-apply through the real dispatch path; commits on Ok.
fn apply(node: &MockNode, function: &str, payload: Vec<u8>) -> Result<(), DatabaseError> {
    let tx = Transaction::new(
        function.to_string(),
        payload,
        node.node_id,
        &node.signing_key,
    )
    .unwrap();
    let mut conn = node.app_state.db_pool.get().unwrap();
    let db_tx = conn.transaction().unwrap();
    let result = process_transaction(&tx, &node.app_state, true, &db_tx);
    match result {
        Ok(_) => {
            db_tx.commit().unwrap();
            Ok(())
        }
        Err(e) => {
            let _ = db_tx.rollback();
            Err(e)
        }
    }
}

fn committed_state(node: &MockNode) -> RegenesisState {
    let conn = node.app_state.db_pool.get().unwrap();
    read_regenesis_state(&conn).unwrap()
}

// Should: enter the moratorium when a seated validator starts a
// same-version regenesis and every seated validator runs the target.
// Should not: let unseated registered nodes gate the precondition.
#[test]
fn start_enters_moratorium_when_all_seated_run_target() {
    let node = MockNode::new(3);
    register_node(&node);
    seat_with_version(&node, 3, 20260800);
    // A registered but UNSEATED node with no attestation must not block.
    {
        let conn = node.app_state.db_pool.get().unwrap();
        conn.execute(
            "INSERT INTO nodes (node_id, name, owner, pubkey) VALUES (9, 'standby', 1, X'09')",
            [],
        )
        .unwrap();
    }

    apply(&node, "regenesis_start", start_payload(20260800)).unwrap();

    let state = committed_state(&node);
    assert_eq!(state.phase, RegenesisPhase::Moratorium);
    assert_eq!(state.target_version_code, Some(20260800));
}

// Should not: accept a boundary start from an unseated node.
// Impact: start/abort are authorized as the membership-ops class (OQ2
// v1) — only a seated validator's node signature can freeze the mesh.
#[test]
fn start_requires_seated_submitter() {
    let node = MockNode::new(3);
    register_node(&node);

    let err = apply(&node, "regenesis_start", start_payload(20260800)).unwrap_err();
    assert!(matches!(err, DatabaseError::AuthorizationError));
    assert_eq!(committed_state(&node).phase, RegenesisPhase::Normal);
}

// Should not: start a regenesis some seated validator visibly cannot
// complete — the deterministic precondition over committed attestations.
#[test]
fn start_refused_when_a_seated_validator_lacks_target() {
    let node = MockNode::new(3);
    register_node(&node);
    seat_with_version(&node, 3, 20260800);
    register_extra_seated(&node, 4, Some(20260700)); // behind, nothing staged

    let err = apply(&node, "regenesis_start", start_payload(20260800)).unwrap_err();
    assert!(matches!(err, DatabaseError::ProcessingError), "got {err:?}");
    assert_eq!(committed_state(&node).phase, RegenesisPhase::Normal);

    // A never-attested seated validator blocks too.
    let conn = node.app_state.db_pool.get().unwrap();
    conn.execute(
        "UPDATE nodes SET running_version_code = NULL WHERE node_id = 4",
        [],
    )
    .unwrap();
    drop(conn);
    let err = apply(&node, "regenesis_start", start_payload(20260800)).unwrap_err();
    assert!(matches!(err, DatabaseError::ProcessingError));
}

// Should not: decide a second start once the moratorium holds.
#[test]
fn start_refused_during_moratorium() {
    let node = MockNode::new(3);
    register_node(&node);
    seat_with_version(&node, 3, 20260800);

    apply(&node, "regenesis_start", start_payload(20260800)).unwrap();
    let err = apply(&node, "regenesis_start", start_payload(20260800)).unwrap_err();
    assert!(matches!(err, DatabaseError::ProcessingError));
    assert_eq!(committed_state(&node).phase, RegenesisPhase::Moratorium);
}

// Should: abort back to normal from the moratorium, and only from there.
// Impact: the abort window is exactly (start decided, commit decided) —
// the model's abortRoundTripTest, at the handler layer.
#[test]
fn abort_round_trip_and_window() {
    let node = MockNode::new(3);
    register_node(&node);
    seat_with_version(&node, 3, 20260800);

    // Nothing to abort in normal.
    let err = apply(&node, "regenesis_abort", abort_payload()).unwrap_err();
    assert!(matches!(err, DatabaseError::ProcessingError));

    apply(&node, "regenesis_start", start_payload(20260800)).unwrap();
    apply(&node, "regenesis_abort", abort_payload()).unwrap();
    assert_eq!(committed_state(&node), RegenesisState::default());

    // The window closed with the abort itself.
    let err = apply(&node, "regenesis_abort", abort_payload()).unwrap_err();
    assert!(matches!(err, DatabaseError::ProcessingError));

    // And a fresh start is possible again — abort wedges nothing.
    apply(&node, "regenesis_start", start_payload(20260800)).unwrap();
    assert_eq!(committed_state(&node).phase, RegenesisPhase::Moratorium);
}

// Should not: let an unseated node abort someone else's boundary.
#[test]
fn abort_requires_seated_submitter() {
    let node = MockNode::new(3);
    register_node(&node);
    // Enter moratorium directly at the db layer; the submitter stays
    // unseated.
    {
        let mut conn = node.app_state.db_pool.get().unwrap();
        let db_tx = conn.transaction().unwrap();
        set_moratorium_tx(&db_tx, 20260800).unwrap();
        db_tx.commit().unwrap();
    }

    let err = apply(&node, "regenesis_abort", abort_payload()).unwrap_err();
    assert!(matches!(err, DatabaseError::AuthorizationError));
    assert_eq!(committed_state(&node).phase, RegenesisPhase::Moratorium);
}

// Should not: accept malformed boundary payloads (invalid CalVer target,
// undecodable bytes).
#[test]
fn start_rejects_malformed_payloads() {
    let node = MockNode::new(3);
    register_node(&node);
    seat_with_version(&node, 3, 20260800);

    let err = apply(&node, "regenesis_start", start_payload(20261300)).unwrap_err(); // month 13
    assert!(matches!(err, DatabaseError::InvalidPayload));

    let err = apply(&node, "regenesis_start", vec![]).unwrap_err();
    assert!(matches!(err, DatabaseError::InvalidPayload));

    assert_eq!(committed_state(&node).phase, RegenesisPhase::Normal);
}
