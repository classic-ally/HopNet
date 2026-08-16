//! Transient-storage-error classification at the two consensus seams:
//! `build_value`'s preflight must bucket a transient handler failure as
//! `RejectReason::Transient` (restage), never `Permanent` (409 → EEXIST at
//! rsync), and `validate_block` must return `Undetermined` (host retries on
//! an IMMEDIATE transaction), never `Invalid` (a nil vote / false SyncInvalid
//! determinism alarm).
//!
//! The handlers here are test-only inventory registrations that fail with
//! `DatabaseError::Transient` exactly once — a real per-transaction
//! SQLITE_BUSY cannot be manufactured under the preflight's IMMEDIATE
//! build transaction, so the classified error is injected at the handler
//! seam (the real-contention story is covered by hopnet-consensus's
//! `immediate_rollback_transaction_survives_contention_deferred_does_not`
//! and hopnet-drive's `busy_repro_tests`).

use std::sync::atomic::{AtomicU32, Ordering};

use crate::consensus::malachite::app::build_value;
use crate::consensus::queue::RejectReason;
use crate::consensus::tests::MockNetwork;
use crate::consensus::tests::byzantine::validate_at_height_1;
use crate::consensus::types::Transaction;
use crate::handlers::{HandlerCtx, HandlerResult, TransactionHandler, TxMeta};
use hopnet_consensus::Round;
use hopnet_consensus::context::Height;
use hopnet_consensus::traits::ValidationVerdict;

/// Fails with a transient storage error on the first process() call, then
/// succeeds — the shape of a lock collision that clears by the next height.
macro_rules! transient_once_handler {
    ($ty:ident, $name:literal, $counter:ident) => {
        static $counter: AtomicU32 = AtomicU32::new(0);
        struct $ty;
        impl TransactionHandler for $ty {
            fn name(&self) -> &'static str {
                $name
            }
            fn process(
                &self,
                _meta: &TxMeta<'_>,
                _execute: bool,
                _ctx: &HandlerCtx<'_>,
                _db_tx: &rusqlite::Transaction<'_>,
            ) -> HandlerResult {
                if $counter.fetch_add(1, Ordering::SeqCst) == 0 {
                    Err(crate::db::DatabaseError::Transient(
                        rusqlite::ErrorCode::DatabaseBusy,
                    ))
                } else {
                    Ok(())
                }
            }
        }
        inventory::submit! {
            &$ty as &dyn TransactionHandler
        }
    };
}

transient_once_handler!(
    PreflightTransientOnce,
    "test.transient_once_preflight",
    PREFLIGHT_CALLS
);
transient_once_handler!(
    ValidateTransientOnce,
    "test.transient_once_validate",
    VALIDATE_CALLS
);

fn signed_tx(network: &MockNetwork, function: &str) -> Transaction {
    let node = &network.nodes[0];
    Transaction::new(
        function.to_string(),
        Vec::new(),
        node.node_id,
        &node.signing_key,
    )
    .expect("sign transaction")
}

// Impact: this is the regression guard for the rsync data loss — a transient
// SQLITE_BUSY during the proposer's preflight became TxSubmitError::Rejected,
// the mount answered 409, the FUSE client surfaced EEXIST, and rsync unlinked
// its temp file and dropped the file (~0.6% of a live migration).
// Should: bucket a transient handler failure as Transient, not Permanent.
// Should: include the same transaction in a later proposal once the
// transient condition has cleared.
// Should not: put a transiently-failed transaction into the built block.
#[test]
fn preflight_transient_failure_is_bucketed_for_restage_then_proposes() {
    let network = MockNetwork::setup_with_validators(1);
    let app_state = &network.nodes[0].app_state;
    let mut conn = app_state.db_pool.get().expect("pool");

    let tx = signed_tx(&network, "test.transient_once_preflight");

    // First build: the handler reports transient contention.
    let built = build_value(
        app_state,
        &mut conn,
        Height(1),
        Round::new(0),
        vec![tx.clone()],
    )
    .expect("build_value");
    assert_eq!(built.rejected.len(), 1);
    assert!(
        matches!(built.rejected[0], (0, RejectReason::Transient(_))),
        "transient handler failure must not be a permanent rejection: {:?}",
        built.rejected[0]
    );
    assert!(
        built.block.data.transactions.is_empty(),
        "a transiently-failed tx must not ride in this block"
    );

    // Second build (the restage's next height): contention cleared.
    let rebuilt = build_value(app_state, &mut conn, Height(2), Round::new(0), vec![tx])
        .expect("build_value retry");
    assert!(rebuilt.rejected.is_empty(), "{:?}", rebuilt.rejected);
    assert_eq!(rebuilt.block.data.transactions.len(), 1);
}

// Impact: a transient storage error during the validation dry-run used to
// collapse into Validity::Invalid — a nil vote on a valid block, and on the
// sync path a false "determinism violation" alarm. The verdict must instead
// be Undetermined so the host retries on an IMMEDIATE transaction.
// Should: return Undetermined (not Invalid) for a transient handler failure.
// Should: return Valid on the retry once the transient condition clears.
#[test]
fn validate_block_returns_undetermined_for_transient_storage_failure() {
    let network = MockNetwork::setup_with_validators(1);
    let node = &network.nodes[0];
    let tx = signed_tx(&network, "test.transient_once_validate");

    let first = validate_at_height_1(&node.app_state, vec![tx.clone()]);
    assert!(
        matches!(first, ValidationVerdict::Undetermined(_)),
        "transient handler failure must be Undetermined, got {first:?}"
    );

    let second = validate_at_height_1(&node.app_state, vec![tx]);
    assert_eq!(
        second,
        ValidationVerdict::Valid,
        "the retry must produce a real verdict once contention clears"
    );
}
