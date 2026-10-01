//! The proposer's preflight is time-boxed: once its IMMEDIATE transaction
//! has held the database's write lock past the budget, the remaining
//! candidates are deferred to the next block instead of extending the hold.
//! Regression guard for the 2026-10-01 crash loop, where a cold-cache
//! preflight held the lock past the consensus connection's busy timeout and
//! the vote WAL append behind it became fatal.

use crate::consensus::malachite::app::{build_value, build_value_with_budget};
use crate::consensus::tests::MockNetwork;
use crate::consensus::types::Transaction;
use crate::handlers::{HandlerCtx, HandlerResult, TransactionHandler, TxMeta};
use hopnet_consensus::Round;
use hopnet_consensus::context::Height;

/// A handler that always accepts — the cheapest candidate the dispatch table
/// can dry-run, so the test measures only the time-box.
struct BudgetNoop;

impl TransactionHandler for BudgetNoop {
    fn name(&self) -> &'static str {
        "test.budget_noop"
    }

    fn process(
        &self,
        _meta: &TxMeta<'_>,
        _execute: bool,
        _ctx: &HandlerCtx<'_>,
        _db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        Ok(())
    }
}

inventory::submit! {
    &BudgetNoop as &dyn TransactionHandler
}

fn candidates(network: &MockNetwork, n: u8) -> Vec<Transaction> {
    let node = &network.nodes[0];
    (0..n)
        .map(|i| {
            Transaction::new(
                "test.budget_noop".to_string(),
                vec![i],
                node.node_id,
                &node.signing_key,
            )
            .expect("sign transaction")
        })
        .collect()
}

// Should: stop admitting candidates once the preflight has held the write
// lock past its budget, defer the remainder for the next block, and still
// build a block from the admitted prefix.
// Should not: reject the deferred candidates — they were never judged.
// Impact: every writer on the node waits behind the preflight's lock, the
// consensus shell's own WAL appends included; a hold that outlives the
// shell's busy timeout is what crash-looped thor at height 82752.
#[test]
fn preflight_defers_candidates_past_the_lock_budget() {
    let network = MockNetwork::setup_with_validators(1);
    let app_state = &network.nodes[0].app_state;
    let mut conn = app_state.db_pool.get().expect("pool");

    let built = build_value_with_budget(
        app_state,
        &mut conn,
        Height(1),
        Round::new(0),
        candidates(&network, 3),
        std::time::Duration::ZERO,
    )
    .expect("build_value");

    assert_eq!(
        built.block.data.transactions.len(),
        1,
        "the first candidate is always admitted"
    );
    assert_eq!(
        built.deferred,
        vec![1, 2],
        "the rest wait for the next block"
    );
    assert!(built.rejected.is_empty(), "{:?}", built.rejected);
}

// Should: admit every candidate while the preflight stays within its budget.
#[test]
fn preflight_within_budget_admits_everything() {
    let network = MockNetwork::setup_with_validators(1);
    let app_state = &network.nodes[0].app_state;
    let mut conn = app_state.db_pool.get().expect("pool");

    let built = build_value(
        app_state,
        &mut conn,
        Height(1),
        Round::new(0),
        candidates(&network, 3),
    )
    .expect("build_value");

    assert_eq!(built.block.data.transactions.len(), 3);
    assert!(built.deferred.is_empty(), "{:?}", built.deferred);
    assert!(built.rejected.is_empty(), "{:?}", built.rejected);
}
