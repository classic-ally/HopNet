//! Proposer-side staleness: `build_value` must never propose a transaction
//! that every Live validator will refuse as older than `MAX_TRANSACTION_AGE`.

use crate::consensus::dispatch::{MAX_TRANSACTION_AGE, PROPOSAL_AGE_MARGIN};
use crate::consensus::malachite::app::build_value;
use crate::consensus::queue::RejectReason;
use crate::consensus::tests::MockNetwork;
use crate::consensus::types::Transaction;
use crate::handlers::{HandlerCtx, HandlerResult, TransactionHandler, TxMeta};
use hopnet_consensus::Round;
use hopnet_consensus::context::Height;

struct AlwaysOk;
impl TransactionHandler for AlwaysOk {
    fn name(&self) -> &'static str {
        "test.stale_proposal_ok"
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
    &AlwaysOk as &dyn TransactionHandler
}

/// A signed transaction whose nonce was minted `age` ago. The nonce is not
/// covered by the signature, so back-dating it models a transaction that sat
/// in the queue that long.
fn tx_aged(network: &MockNetwork, function: &str, age: chrono::TimeDelta) -> Transaction {
    let node = &network.nodes[0];
    let mut tx = Transaction::new(
        function.to_string(),
        Vec::new(),
        node.node_id,
        &node.signing_key,
    )
    .expect("sign transaction");
    let minted = (chrono::Utc::now() - age).timestamp() as u64;
    tx.nonce = hopnet_common::CustomUUID::new(Some(&uuid::Timestamp::from_unix(
        uuid::NoContext,
        minted,
        0,
    )));
    tx
}

fn is_stale_rejection(reason: &RejectReason) -> bool {
    matches!(reason, RejectReason::Permanent(why) if why.starts_with("stale transaction"))
}

// Impact: validators refuse any block carrying a transaction older than
// MAX_TRANSACTION_AGE, so a proposer that includes one loses the round and
// gets the same entry back for the next. Two proposers each holding a stale
// validator_vote_out rejected each other's blocks for 29 rounds.
// Should: reject a transaction past the proposable age as permanently stale.
// Should: still propose the fresh transactions batched alongside it.
#[test]
fn stale_candidate_is_rejected_and_fresh_ones_still_propose() {
    let network = MockNetwork::setup_with_validators(1);
    let app_state = &network.nodes[0].app_state;
    let mut conn = app_state.db_pool.get().expect("pool");

    let stale = tx_aged(&network, "test.stale_proposal_ok", MAX_TRANSACTION_AGE);
    let fresh = tx_aged(
        &network,
        "test.stale_proposal_ok",
        chrono::TimeDelta::zero(),
    );

    let built = build_value(
        app_state,
        &mut conn,
        Height(1),
        Round::new(0),
        vec![stale, fresh],
    )
    .expect("build_value");

    assert_eq!(built.rejected.len(), 1, "{:?}", built.rejected);
    assert_eq!(built.rejected[0].0, 0);
    assert!(
        is_stale_rejection(&built.rejected[0].1),
        "{:?}",
        built.rejected[0]
    );
    assert_eq!(built.block.data.transactions.len(), 1);
}

// Should: refuse a transaction inside the skew margin below MAX_TRANSACTION_AGE,
// which a validator with a slightly faster clock would already call stale.
#[test]
fn nearly_stale_candidate_is_not_proposed() {
    let network = MockNetwork::setup_with_validators(1);
    let app_state = &network.nodes[0].app_state;
    let mut conn = app_state.db_pool.get().expect("pool");

    let age = MAX_TRANSACTION_AGE - PROPOSAL_AGE_MARGIN + chrono::TimeDelta::minutes(1);
    let nearly = tx_aged(&network, "test.stale_proposal_ok", age);

    let built = build_value(app_state, &mut conn, Height(1), Round::new(0), vec![nearly])
        .expect("build_value");

    assert!(
        built
            .rejected
            .first()
            .is_some_and(|(_, r)| is_stale_rejection(r))
    );
    assert!(built.block.data.transactions.is_empty());
}

// Should not: let a stale membership transition claim the solo-block slot and
// push fresh work to a later height.
#[test]
fn stale_vote_out_does_not_hold_the_solo_slot() {
    let network = MockNetwork::setup_with_validators(1);
    let app_state = &network.nodes[0].app_state;
    let mut conn = app_state.db_pool.get().expect("pool");

    let stale_vote_out = tx_aged(&network, "validator_vote_out", MAX_TRANSACTION_AGE);
    let fresh = tx_aged(
        &network,
        "test.stale_proposal_ok",
        chrono::TimeDelta::zero(),
    );

    let built = build_value(
        app_state,
        &mut conn,
        Height(1),
        Round::new(0),
        vec![stale_vote_out, fresh],
    )
    .expect("build_value");

    assert!(
        built.deferred.is_empty(),
        "fresh work deferred: {:?}",
        built.deferred
    );
    assert!(
        built
            .rejected
            .iter()
            .any(|(i, r)| *i == 0 && is_stale_rejection(r)),
        "{:?}",
        built.rejected
    );
    assert_eq!(built.block.data.transactions.len(), 1);
}
