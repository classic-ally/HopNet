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
    let result = process_transaction(&tx, &node.app_state, true, 0, &db_tx);
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

// Should: refuse NEW submissions at the queue chokepoint once the
// moratorium holds — the same gate every client route and internal cron
// funnels through — with the retryable Moratorium error, not Rejected.
// Impact: the freeze must be real at the submission layer, or our own
// crons keep refilling the pool and the drain never terminates.
#[test]
fn queue_refuses_new_submissions_during_moratorium() {
    use crate::consensus::queue::ConsensusSubmitError;

    let node = MockNode::new(3);
    register_node(&node);
    {
        let mut conn = node.app_state.db_pool.get().unwrap();
        let db_tx = conn.transaction().unwrap();
        set_moratorium_tx(&db_tx, 20260800).unwrap();
        db_tx.commit().unwrap();
    }

    let tx = Transaction::new(
        "node_staged_version".to_string(),
        vec![],
        node.node_id,
        &node.signing_key,
    )
    .unwrap();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let err = rt
        .block_on(node.app_state.consensus_queue.submit(tx))
        .unwrap_err();
    assert!(
        matches!(
            err,
            ConsensusSubmitError::Moratorium {
                phase: "moratorium",
                target_version_code: Some(20260800),
            }
        ),
        "got {err:?}"
    );
}

fn commit_payload(snapshot_hash: [u8; 32], seal_height: u64, target_version_code: u32) -> Vec<u8> {
    bincode::serde::encode_to_vec(
        crate::regenesis::RegenesisCommit {
            snapshot_hash,
            seal_height,
            target_version_code,
        },
        bincode::config::standard(),
    )
    .unwrap()
}

// Should not: seal outside the moratorium — the commit's one
// deterministic phase rule at the handler layer.
#[test]
fn commit_refused_in_normal() {
    let node = MockNode::new(3);
    register_node(&node);
    seat_with_version(&node, 3, 20260800);

    let err = apply(
        &node,
        "regenesis_commit",
        commit_payload([7u8; 32], 5, 20260800),
    )
    .unwrap_err();
    assert!(matches!(err, DatabaseError::ProcessingError));
    assert_eq!(committed_state(&node).phase, RegenesisPhase::Normal);
}

// Should: seal from the moratorium, recording the certified hash and the
// terminal height; afterwards the window is closed forward-only — no
// second commit, no abort, no new start.
#[test]
fn commit_seals_and_closes_the_window() {
    let node = MockNode::new(3);
    register_node(&node);
    seat_with_version(&node, 3, 20260800);

    apply(&node, "regenesis_start", start_payload(20260800)).unwrap();
    apply(
        &node,
        "regenesis_commit",
        commit_payload([7u8; 32], 9, 20260800),
    )
    .unwrap();

    let state = committed_state(&node);
    assert_eq!(state.phase, RegenesisPhase::Sealed);
    assert_eq!(state.snapshot_hash, Some(vec![7u8; 32]));
    assert_eq!(state.seal_height, Some(9));
    assert_eq!(state.target_version_code, Some(20260800));

    // RFC-020 S6: [7u8; 32] matches no honest recompute, so applying
    // this commit IS the diverged-but-outvoted case — the dissent
    // marker must have been written inside the same decide.
    {
        let conn = node.app_state.db_pool.get().unwrap();
        assert_eq!(crate::regenesis::seal::dissent_marker(&conn), Some(9));
    }

    for (function, payload) in [
        ("regenesis_commit", commit_payload([7u8; 32], 10, 20260800)),
        ("regenesis_abort", abort_payload()),
        ("regenesis_start", start_payload(20260800)),
    ] {
        let err = apply(&node, function, payload).unwrap_err();
        assert!(matches!(err, DatabaseError::ProcessingError), "{function}");
    }
}

// Should: vote for an honest regenesis commit (matching hash, bound
// height, riding alone) and refuse every malformed shape — wrong local
// hash, wrong terminal height, shared block — and refuse ANY block once
// the epoch is sealed.
// Impact: this is the byzantine-tolerant layer — a proposer cannot seal
// a mesh onto a snapshot its validators cannot reproduce, cannot bind
// the seal to the wrong height, cannot smuggle work into the final
// block, and cannot decide anything past the seal.
#[test]
fn commit_block_vote_iff_match_and_shape() {
    use super::byzantine::validate_at_height_1;
    use hopnet_consensus::traits::ValidationVerdict;

    let network = MockNetwork::setup_with_validators(1);
    let node = &network.nodes[0];

    // Enter the moratorium (committed state).
    {
        let mut conn = node.app_state.db_pool.get().unwrap();
        let db_tx = conn.transaction().unwrap();
        set_moratorium_tx(&db_tx, 20260800).unwrap();
        db_tx.commit().unwrap();
    }

    // The honest identity: blake3 over the canonical artifact bytes —
    // what every validator recomputes locally at vote time.
    let honest = {
        let mut conn = node.app_state.db_pool.get().unwrap();
        let db_tx = conn.transaction().unwrap();
        let hash = crate::db::snapshot::compute_artifact_hash_tx(&db_tx).unwrap();
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(hash.as_bytes());
        bytes
    };

    let commit_tx = |hash: [u8; 32], seal_height: u64| {
        Transaction::new(
            "regenesis_commit".to_string(),
            commit_payload(hash, seal_height, 20260800),
            node.node_id,
            &node.signing_key,
        )
        .unwrap()
    };

    // Honest commit at the actual block height, alone: votable.
    assert_eq!(
        validate_at_height_1(&node.app_state, vec![commit_tx(honest, 1)]),
        ValidationVerdict::Valid,
        "honest seal must be votable"
    );

    // Wrong hash: vote-iff-match refuses.
    assert_eq!(
        validate_at_height_1(&node.app_state, vec![commit_tx([0xAB; 32], 1)]),
        ValidationVerdict::Invalid,
        "hash mismatch must refuse the vote"
    );

    // seal_height not bound to the block height: refused.
    assert_eq!(
        validate_at_height_1(&node.app_state, vec![commit_tx(honest, 7)]),
        ValidationVerdict::Invalid,
        "unbound seal_height must be refused"
    );

    // The commit must ride alone.
    let bystander = Transaction::new(
        "node_staged_version".to_string(),
        vec![],
        node.node_id,
        &node.signing_key,
    )
    .unwrap();
    assert_eq!(
        validate_at_height_1(&node.app_state, vec![commit_tx(honest, 1), bystander]),
        ValidationVerdict::Invalid,
        "commit sharing a block must be refused"
    );

    // Seal the epoch; nothing further is votable — not even an otherwise
    // well-formed ordinary transaction.
    {
        let mut conn = node.app_state.db_pool.get().unwrap();
        let db_tx = conn.transaction().unwrap();
        crate::db::regenesis::set_sealed_tx(&db_tx, &honest, 1).unwrap();
        db_tx.commit().unwrap();
    }
    let ordinary = Transaction::new(
        "node_staged_version".to_string(),
        vec![],
        node.node_id,
        &node.signing_key,
    )
    .unwrap();
    assert_eq!(
        validate_at_height_1(&node.app_state, vec![ordinary]),
        ValidationVerdict::Invalid,
        "no block may decide past the seal"
    );
}

// Should: write the snapshot artifact from sealed local state when the
// recomputed artifact hash matches the committed one — the certified
// bytes themselves — and refuse loudly when this replica's recompute
// diverges from what the mesh certified.
// Impact: the seal's hash identity covers the EXPORTED subset only, so
// it survives the seal transition (only divergence-only state changes
// when the commit applies) and a diverged replica can never publish a
// wrong artifact for joiners.
#[test]
fn seal_artifact_written_only_on_hash_match() {
    let node = MockNode::new(3);
    register_node(&node);
    seat_with_version(&node, 3, 20260800);

    apply(&node, "regenesis_start", start_payload(20260800)).unwrap();
    let honest = {
        let mut conn = node.app_state.db_pool.get().unwrap();
        let db_tx = conn.transaction().unwrap();
        let hash = crate::db::snapshot::compute_artifact_hash_tx(&db_tx).unwrap();
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(hash.as_bytes());
        bytes
    };
    apply(
        &node,
        "regenesis_commit",
        commit_payload(honest, 9, 20260800),
    )
    .unwrap();

    // RFC-020 S6: an honest apply (matching hash) leaves no dissent.
    {
        let conn = node.app_state.db_pool.get().unwrap();
        assert!(crate::regenesis::seal::dissent_marker(&conn).is_none());
    }

    let dir = std::env::temp_dir().join(format!("hopnet-seal-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("regenesis-snapshot.bin");
    let written = crate::regenesis::seal::write_seal_artifact_to(&node.app_state, &path).unwrap();
    let bytes = std::fs::read(&written).unwrap();
    assert!(bytes.starts_with(b"HOPSNAP\0"), "artifact magic");
    assert_eq!(
        blake3::hash(&bytes).as_bytes(),
        &honest,
        "bytes are the certified bytes"
    );

    // Tamper the committed hash: the writer must refuse (this replica
    // would be the diverged one).
    {
        let conn = node.app_state.db_pool.get().unwrap();
        conn.execute("UPDATE regenesis_state SET snapshot_hash = X'AB'", [])
            .unwrap();
    }
    let err = crate::regenesis::seal::write_seal_artifact_to(&node.app_state, &path).unwrap_err();
    assert!(err.contains("diverged"), "{err}");
    std::fs::remove_dir_all(&dir).ok();
}

// Impact: the status view is the ONLY surface a headless operator (or
// the orchestrator) has for "is this node parked awaiting a binary
// swap" — a wrong awaiting_upgrade would send someone hunting a hung
// process that is actually waiting for them.
// Should: report the epoch, the effective running version, and
// awaiting_upgrade = sealed-for-a-version-this-binary-does-not-run.
// Should not: claim awaiting_upgrade while sealed at the version this
// binary already runs (that node restarts itself).
#[test]
fn status_view_reports_epoch_and_awaiting_upgrade() {
    // Asserts on `awaiting_upgrade`, which compares the sealed target
    // against `effective_running_code()` — so pin the claimed version
    // to the hardcoded 20260800 target (a release bump of the real
    // binary flipped not-awaiting into awaiting at 2026.8.1), and hold
    // the lock so no other test's override leaks in.
    let _env = crate::test_env::lock_env();
    crate::test_env::set(&_env, "HOPNET_UPGRADE_VERSION_OVERRIDE", "2026.8.0");
    let node = MockNode::new(4);
    register_node(&node);
    seat_with_version(&node, 4, 20260800);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let status = |node: &MockNode| {
        rt.block_on(crate::regenesis::routes::get_regenesis_status(
            axum::extract::State(node.app_state.clone()),
        ))
        .unwrap()
        .0
    };

    let view = status(&node);
    assert_eq!(view.phase, "normal");
    assert_eq!(view.epoch, "1");
    assert_eq!(
        view.running_version,
        crate::version::format_code(crate::version::effective_running_code())
    );
    assert!(!view.awaiting_upgrade);
    assert!(!view.rollback_retained);

    // Seal at the version this binary runs: NOT awaiting an upgrade.
    apply(&node, "regenesis_start", start_payload(20260800)).unwrap();
    apply(
        &node,
        "regenesis_commit",
        commit_payload([7u8; 32], 9, 20260800),
    )
    .unwrap();
    let view = status(&node);
    assert_eq!(view.phase, "sealed");
    assert!(!view.awaiting_upgrade);

    // Sealed for a DIFFERENT version: parked awaiting the swap.
    {
        let conn = node.app_state.db_pool.get().unwrap();
        conn.execute(
            "UPDATE regenesis_state SET target_version_code = 20990100",
            [],
        )
        .unwrap();
    }
    let view = status(&node);
    assert!(view.awaiting_upgrade);
    assert_eq!(view.target_version.as_deref(), Some("2099.1.0"));
}

// Impact: re-trust is the operator's only way out when churn moved past
// the overlap window, so it has to be reachable and honest about what it
// did — but it must never accept a peer this node cannot even name.
// Should: refuse an unknown peer, and accept a known one with 202 (the
// fetch runs in the background and can take minutes).
// Should not: start a fetch without a well-formed fingerprint — that is
// the request's only trust anchor, so a malformed one has to be a 400
// rather than a join that proceeds unanchored.
#[test]
fn retrust_route_requires_a_known_peer() {
    use axum::response::IntoResponse as _;

    let node = MockNode::new(6);
    register_node(&node);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let good_fp = "a".repeat(64);
    let retrust = |node_id: i32, expect_chain_id: &str| {
        let expect_chain_id = expect_chain_id.to_string();
        rt.block_on(async {
            crate::regenesis::routes::post_regenesis_retrust(
                axum::extract::State(node.app_state.clone()),
                // No signing node in the extensions: the operator/JWT path,
                // which the seat gate deliberately leaves alone.
                None,
                axum::Json(crate::regenesis::routes::RetrustRequest {
                    node_id,
                    expect_chain_id,
                }),
            )
            .await
            .into_response()
        })
    };

    // The fingerprint is checked before the peer even gets looked up: an
    // unanchored request has nothing to verify against, so it never
    // becomes a fetch.
    for bad in [
        "",
        "not-hex",
        &"a".repeat(63),
        &"a".repeat(65),
        &"z".repeat(64),
    ] {
        assert_eq!(
            retrust(6, bad).status(),
            axum::http::StatusCode::BAD_REQUEST,
            "fingerprint {bad:?} must be refused"
        );
    }

    assert_eq!(
        retrust(999, &good_fp).status(),
        axum::http::StatusCode::NOT_FOUND
    );
    // Node 6 is registered and the fingerprint parses, so the request is
    // well formed. The join itself then fails in the background (no
    // reachable peer, and the fingerprint would not match), which is the
    // spawn's business, not the route's.
    assert_eq!(
        retrust(6, &good_fp).status(),
        axum::http::StatusCode::ACCEPTED
    );
}

// Impact: rollback DISCARDS an epoch's database while the window is
// open, so the one thing the route must never do is act on a node that
// has nothing to abandon. The destructive half is covered end to end by
// the orchestrator scenario; this pins the guard.
// Should: refuse with 409 on a node that is neither sealed nor holding
// a retained database.
// Should not: write the rollback marker on a refusal.
#[test]
fn rollback_route_refuses_when_there_is_no_boundary() {
    // This decides on `sealed_path()`, which resolves from the
    // process-global XDG_DATA_HOME: unlocked, it could see a live boundary
    // e2e's retained-epoch file, answer 202 instead of the asserted 409,
    // and write a real rollback marker into that e2e's data directory.
    let _env = crate::test_env::lock_env();
    use axum::response::IntoResponse as _;

    let node = MockNode::new(7);
    register_node(&node);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let resp = rt.block_on(async {
        crate::regenesis::routes::post_regenesis_rollback(
            axum::extract::State(node.app_state.clone()),
            None,
        )
        .await
        .into_response()
    });
    assert_eq!(resp.status(), axum::http::StatusCode::CONFLICT);
    assert!(
        !crate::regenesis::boot::rollback_marker_path(&crate::db::shared::get_database_path())
            .exists(),
        "a refused request must leave no marker behind"
    );
}

// Impact: `jwt_or_rpc_auth_middleware` accepts a signature from ANY row in
// `nodes`, and `nodes` is append-only in production — no non-test path
// deletes from it — so a node that left voluntarily or was voted out kept
// a working credential. start/abort are safe because their handlers call
// `require_seated` inside consensus; rollback and retrust submit nothing
// and so had no equivalent. This is that gap, closed.
// Should: refuse a signing node that is registered but NOT seated, on both
//   locally-acting routes, before either does any work.
// Should not: refuse when no signing node is present — the JWT/operator
//   path is deliberately untouched (no admin role exists to check, and it
//   is how rollback is actually driven).
#[test]
fn local_boundary_routes_require_a_seated_signer() {
    // `rollback_available` resolves `sealed_path()` from XDG_DATA_HOME.
    let _env = crate::test_env::lock_env();
    use axum::response::IntoResponse as _;

    let node = MockNode::new(11);
    register_node(&node);
    // Registered above, deliberately never seated.
    let unseated = axum::Extension(crate::consensus::routes::AuthenticatedNode { node_id: 11 });

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let rollback = rt.block_on(async {
        crate::regenesis::routes::post_regenesis_rollback(
            axum::extract::State(node.app_state.clone()),
            Some(unseated.clone()),
        )
        .await
        .into_response()
    });
    assert_eq!(rollback.status(), axum::http::StatusCode::FORBIDDEN);

    // Refused ahead of the fingerprint parse, so even a well-formed
    // request from an unseated node never reaches the join.
    let retrust = rt.block_on(async {
        crate::regenesis::routes::post_regenesis_retrust(
            axum::extract::State(node.app_state.clone()),
            Some(unseated.clone()),
            axum::Json(crate::regenesis::routes::RetrustRequest {
                node_id: 11,
                expect_chain_id: "a".repeat(64),
            }),
        )
        .await
        .into_response()
    });
    assert_eq!(retrust.status(), axum::http::StatusCode::FORBIDDEN);

    // Seat it and the gate stops firing: rollback falls through to its own
    // "nothing to abandon" refusal, which is the next check, not this one.
    seat_with_version(&node, 11, 20260800);
    let seated = rt.block_on(async {
        crate::regenesis::routes::post_regenesis_rollback(
            axum::extract::State(node.app_state.clone()),
            Some(unseated),
        )
        .await
        .into_response()
    });
    assert_eq!(seated.status(), axum::http::StatusCode::CONFLICT);
}

// Impact: the (epoch, version) handshake is what turns silent cross-
// epoch signature failures into diagnosable refusals — and the fetch
// gate is the hook S7's epoch join extends into a lineage answer.
// Should: refuse a DecidedFetch from a different epoch with a
// structured error, serve a same-epoch one (engine parked or not), and
// answer status pings with this node's (epoch, version).
#[test]
fn handshake_carries_epoch_and_refuses_mismatched_fetch() {
    // Reads `effective_running_code()` for the handshake pong.
    let _env = crate::test_env::lock_env();
    use crate::consensus::evidence::{StatusRequest, StatusResponse};
    use crate::consensus::malachite::gossip::{ConsensusNetRequest, ConsensusNetResponse};

    let node = MockNode::new(5);
    register_node(&node);
    let peer = hopnet_comms::PeerRef {
        node_id: 42,
        pubkey: [0u8; 32],
    };
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    // Fetch from another epoch: refused before any DB read, with the
    // STRUCTURED refusal the S7 epoch-join classification pivots on.
    let scope = crate::net::scopes::ConsensusScope {
        app_state: node.app_state.clone(),
    };
    let resp = rt.block_on(scope.serve(
        peer,
        crate::net::encode_payload(&ConsensusNetRequest::DecidedFetch {
            from_height: 1,
            to_height: 1,
            epoch: 2,
        }),
    ));
    match resp {
        ConsensusNetResponse::EpochMismatch { local_epoch } => {
            assert_eq!(local_epoch, 1)
        }
        other => panic!("expected structured refusal, got {other:?}"),
    }

    // Same epoch: served from the DB even with NO engine (a parked node
    // answering a laggard its decided history).
    let resp = rt.block_on(scope.serve(
        peer,
        crate::net::encode_payload(&ConsensusNetRequest::DecidedFetch {
            from_height: 1,
            to_height: 1,
            epoch: 1,
        }),
    ));
    assert!(
        matches!(resp, ConsensusNetResponse::Decided { ref items } if items.is_empty()),
        "expected empty Decided, got {resp:?}"
    );

    // Status ping answers with OUR (epoch, version).
    let status = crate::consensus::evidence::StatusScope {
        app_state: node.app_state.clone(),
    };
    let ping = bincode::serde::encode_to_vec(
        &StatusRequest::Ping {
            decided_height: 3,
            epoch: 9,
            version_code: 20990100,
        },
        bincode::config::standard(),
    )
    .unwrap();
    let raw = rt.block_on(hopnet_comms::RpcHandler::handle(&status, peer, ping));
    let (
        StatusResponse::Pong {
            epoch,
            version_code,
            floor,
            head,
            ..
        },
        _,
    ) = bincode::serde::decode_from_slice(&raw, bincode::config::standard()).unwrap();
    assert_eq!(epoch, 1);
    assert_eq!(version_code, crate::version::effective_running_code());
    // The Pong is the policy readout (RFC-025): the served window rides
    // every answer.
    assert_eq!(floor, hopnet_comms::alpn::compat_floor(hopnet_comms::alpn::COMPAT_HEAD));
    assert_eq!(head, hopnet_comms::alpn::COMPAT_HEAD);
}

// Impact: the schema-evolution parity gate transposed to the wire — a
// generation-1 reshape that strands generation-0 peers fails here at
// mint time, not in the field.
// Should: serve a generation-0-encoded Ping through the head handler
// via the G0 adapter and produce a response the frozen generation-0
// decoder reads back exactly (three fields, correct epoch and version).
#[test]
fn status_g0_roundtrip_through_the_head_adapter() {
    // Reads `effective_running_code()` for the pong.
    let _env = crate::test_env::lock_env();
    use crate::consensus::status_compat_g0 as g0;

    let node = MockNode::new(6);
    register_node(&node);
    let peer = hopnet_comms::PeerRef {
        node_id: 43,
        pubkey: [0u8; 32],
    };
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let adapter = crate::consensus::evidence::StatusCompatG0 {
        inner: std::sync::Arc::new(crate::consensus::evidence::StatusScope {
            app_state: node.app_state.clone(),
        }),
    };
    let g0_ping = bincode::serde::encode_to_vec(
        &g0::StatusRequest::Ping {
            decided_height: 3,
            epoch: 1,
            version_code: 20990100,
        },
        bincode::config::standard(),
    )
    .unwrap();
    let raw = rt.block_on(hopnet_comms::RpcHandler::handle(&adapter, peer, g0_ping));
    let (g0::StatusResponse::Pong {
        decided_height,
        epoch,
        version_code,
    }, consumed) =
        bincode::serde::decode_from_slice(&raw, bincode::config::standard()).unwrap();
    assert_eq!(consumed, raw.len(), "no trailing bytes for the old decoder");
    assert_eq!(decided_height, 0);
    assert_eq!(epoch, 1);
    assert_eq!(version_code, crate::version::effective_running_code());
}
