//! Forwarded-verdict routing on the submitter side: a proposer's rejection
//! of a transaction THIS node has already committed is a stale verdict, and
//! resolves as committed. The proposer-side half of the same defence — a
//! re-forwarded copy joins the pooled entry instead of duplicating it —
//! is unit-tested beside `PendingPool` in `queue.rs`.

use crate::consensus::queue::{
    ConsensusResult, PendingPool, QueuedTransaction, process_forward_results,
};
use crate::consensus::rpc::TransactionForwardResult;
use crate::consensus::tests::MockNetwork;
use crate::consensus::types::Transaction;

fn signed_tx(network: &MockNetwork) -> Transaction {
    let node = &network.nodes[0];
    Transaction::new(
        "test.noop".to_string(),
        Vec::new(),
        node.node_id,
        &node.signing_key,
    )
    .expect("sign transaction")
}

// Impact: regenesis-cutover (2026-09-27) — the fourth node's insert_node
// was forwarded, pooled, retried after a decide without it, and the retry
// was preflight-rejected as a duplicate while the original committed at
// height 32; the API answered 500 for a registered node. A committed
// transaction cannot be rejected, whatever the proposer's verdict says.
// Should: resolve a rejected transaction whose nonce is committed here as
// committed, through the local settler, without counting the rejection.
// Should: still reject a genuinely uncommitted transaction on a
// three-validator mesh, where one proposer's rejection is final.
#[test]
fn rejection_of_a_locally_committed_nonce_resolves_as_committed() {
    let network = MockNetwork::setup_with_validators(3);
    let app_state = &network.nodes[0].app_state;
    let mut conn = app_state.db_pool.get().expect("pool");

    let committed_tx = signed_tx(&network);
    let fresh_tx = signed_tx(&network);
    {
        let db_tx = conn.transaction().expect("begin");
        crate::db::consensus::insert_tx_nonces_tx(
            &db_tx,
            std::slice::from_ref(&committed_tx.nonce),
        )
        .expect("insert nonce");
        crate::db::shared::commit_timed(db_tx).expect("commit");
    }

    let pool = PendingPool::default();
    let (committed_entry, mut committed_rx) = QueuedTransaction::new(committed_tx);
    let (fresh_entry, mut fresh_rx) = QueuedTransaction::new(fresh_tx);
    let rejected = || TransactionForwardResult::Rejected {
        reason: "ProcessingError".into(),
    };

    let (retries, _) = process_forward_results(
        vec![committed_entry, fresh_entry],
        vec![rejected(), rejected()],
        2,
        &mut conn,
        &pool,
    );
    assert!(
        retries.is_empty(),
        "nothing to retry: one settles, one is final"
    );

    // The committed one waits for the local settler, like a Committed verdict.
    assert_eq!(pool.staged_len(), 1);
    pool.settle(&conn, 5);
    assert!(matches!(
        committed_rx.try_recv().expect("resolved"),
        ConsensusResult::Committed { .. }
    ));

    // The fresh one is rejected outright: three validators tolerate zero
    // Byzantine proposers, so one rejection is the verdict.
    assert!(matches!(
        fresh_rx.try_recv().expect("resolved"),
        ConsensusResult::Rejected(reason) if reason == "ProcessingError"
    ));
}
