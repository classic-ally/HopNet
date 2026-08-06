//! Context determinism: proposer selection and validator-set ordering.
//! These are pure functions every node evaluates independently — any
//! divergence between nodes here manifests as consensus liveness failure.

mod common;

use common::{pubkey, valset};
use hopnet_consensus::context::Validator;
use hopnet_consensus::{Address, Height, HopNetContext, HopNetValidatorSet};
use malachitebft_core_types::{Context, Round, ValidatorSet as _};

// Should: rotate the proposer through every validator as heights advance.
// Should not: skip or favour any validator at round 0.
// Impact: a rotation gap would let one node monopolize proposals or starve
// the mesh when the skipped node holds pending transactions.
#[test]
fn proposer_rotates_through_all_validators_across_heights() {
    let vs = valset(5);
    let ctx = HopNetContext;

    let proposers: Vec<i32> = (1..=10)
        .map(|h| ctx.select_proposer(&vs, Height(h), Round::new(0)).address.0)
        .collect();

    assert_eq!(proposers, vec![1, 2, 3, 4, 0, 1, 2, 3, 4, 0]);
}

// Should: advance the proposer on every timed-out round within a height.
// Should not: re-select the same proposer for consecutive rounds.
// Impact: reproduces the bespoke engine's view-skip semantics — a crashed
// proposer must not be re-elected for the retry round.
#[test]
fn proposer_advances_per_round_within_a_height() {
    let vs = valset(3);
    let ctx = HopNetContext;

    let by_round: Vec<i32> = (0..4)
        .map(|r| ctx.select_proposer(&vs, Height(7), Round::new(r)).address.0)
        .collect();

    // (7 + r) % 3
    assert_eq!(by_round, vec![1, 2, 0, 1]);
}

// Should: produce the identical validator ordering regardless of the order
// rows arrive from SQLite.
// Should not: depend on insertion order anywhere.
// Impact: two nodes disagreeing on validator order disagree on every
// proposer; the mesh would stall while appearing network-flaky.
#[test]
fn validator_set_order_is_insertion_invariant() {
    let ids = [4, 0, 3, 1, 2];
    let a = HopNetValidatorSet::new(ids.iter().map(|&i| Validator::new(i, pubkey(i))).collect());
    let b = valset(5);

    assert_eq!(a, b);
    let order: Vec<i32> = a.validators().iter().map(|v| v.address.0).collect();
    assert_eq!(order, vec![0, 1, 2, 3, 4]);
}

// Should: order by voting power descending before address.
// Should not: let equal-power ties break by anything but address ascending.
// Impact: keeps the sort future-proof for weighted meshes while degenerating
// to node_id-asc (the bespoke ROW_NUMBER order) at uniform power.
#[test]
fn validator_set_sorts_power_desc_then_address_asc() {
    let mut v_heavy = Validator::new(9, pubkey(9));
    v_heavy.voting_power = 5;
    let set = HopNetValidatorSet::new(vec![
        Validator::new(2, pubkey(2)),
        v_heavy.clone(),
        Validator::new(1, pubkey(1)),
    ]);

    let order: Vec<i32> = set.validators().iter().map(|v| v.address.0).collect();
    assert_eq!(order, vec![9, 1, 2]);
    assert_eq!(set.total_voting_power(), 7);
}

// Should: reject duplicate validator addresses at construction.
// Should not: silently keep one copy (double-counting risk downstream).
// Impact: a duplicate row would double one validator's effective power in
// every quorum computation — construction is the single choke point.
#[test]
#[should_panic(expected = "duplicate validator address")]
fn validator_set_rejects_duplicate_addresses() {
    let _ = HopNetValidatorSet::new(vec![
        Validator::new(1, pubkey(1)),
        Validator::new(1, pubkey(1)),
    ]);
}

// Should: resolve validators by address and by index consistently.
// Should not: return a validator whose address differs from the query.
// Impact: address→pubkey resolution feeds signature verification; a mismatch
// would verify votes against the wrong key.
#[test]
fn validator_lookup_by_address_and_index_agree() {
    let vs = valset(4);
    for (i, v) in vs.validators().iter().enumerate() {
        assert_eq!(vs.get_by_index(i).unwrap(), v);
        assert_eq!(vs.get_by_address(&v.address).unwrap(), v);
    }
    assert!(vs.get_by_address(&Address(99)).is_none());
    assert!(vs.get_by_index(4).is_none());
}

// Should: round-trip every u64 height through the SQLite i64 mapping
// losslessly, including values above i64::MAX (stored as negative
// INTEGERs by the bit cast).
// Should not: panic, saturate, or bounds-check anywhere in the mapping.
// Impact: the height column is i64; a silent wrap would corrupt the chain
// position after restart. RFC-019 S0: heights are continuous across
// regenesis epochs, so the mapping must cover the full engine range.
#[test]
fn height_db_roundtrip() {
    for h in [
        0u64,
        1,
        12345,
        u32::MAX as u64 + 1,
        i64::MAX as u64,
        i64::MAX as u64 + 1,
        u64::MAX,
    ] {
        assert_eq!(Height::from_db(Height(h).as_db()), Height(h));
    }
}
