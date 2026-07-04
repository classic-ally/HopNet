//! Signing payload properties: injectivity and chain binding.
//! The engine signs whatever bytes we produce — if two distinct messages
//! encode identically, a signature transfers between them (forged
//! equivocation); if payloads aren't chain-bound, they replay across meshes.

mod common;

use std::collections::HashSet;

use common::{chain_id, key, valset};
use hopnet_consensus::context::ConsensusProposal;
use hopnet_consensus::signing::{proposal_sign_bytes, sign_vote, verify_vote, vote_sign_bytes};
use hopnet_consensus::types::{Blake3Hash, Block, BlockData, Transactions};
use hopnet_consensus::{Height, HopNetContext};
use malachitebft_core_types::{Context, NilOrVal, Round};

fn hash(byte: u8) -> Blake3Hash {
    Blake3Hash::from_bytes([byte; 32])
}

// Should: give every distinct (type, height, round, value, voter) vote a
// distinct signing payload, across chain ids.
// Should not: let any two different votes share bytes.
// Impact: a collision lets a prevote signature masquerade as a precommit (or
// one height/round as another) — certificate forgery without key compromise.
#[test]
fn vote_payloads_are_injective() {
    let ctx = HopNetContext;
    let chains = [hash(7), hash(8)];
    let heights = [1u64, 2];
    let rounds = [Round::new(0), Round::new(1)];
    let values = [
        NilOrVal::Nil,
        NilOrVal::Val(hash(1)),
        NilOrVal::Val(hash(2)),
    ];
    let voters = [0i32, 1, -1]; // include a negative id: encoding must not fold signs

    let mut seen: HashSet<Vec<u8>> = HashSet::new();
    let mut count = 0;
    for chain in &chains {
        for &h in &heights {
            for &r in &rounds {
                for v in &values {
                    for &voter in &voters {
                        for vote in [
                            ctx.new_prevote(
                                Height(h),
                                r,
                                v.clone(),
                                hopnet_consensus::Address(voter),
                            ),
                            ctx.new_precommit(
                                Height(h),
                                r,
                                v.clone(),
                                hopnet_consensus::Address(voter),
                            ),
                        ] {
                            assert!(
                                seen.insert(vote_sign_bytes(chain, &vote)),
                                "payload collision: {vote:?} on chain {chain}"
                            );
                            count += 1;
                        }
                    }
                }
            }
        }
    }
    assert_eq!(count, 2 * 2 * 2 * 3 * 3 * 2);
}

// Should: keep vote and proposal payload spaces disjoint.
// Should not: let a proposal signature verify as any vote (or vice versa).
// Impact: cross-type signature transfer would let one signed proposal be
// replayed as a precommit on the same hash.
#[test]
fn vote_and_proposal_payloads_never_collide() {
    let ctx = HopNetContext;
    let block = Block::new(BlockData {
        height: 1,
        round: 0,
        parent_hash: None,
        transactions: Transactions::default(),
    })
    .unwrap();

    let proposal = ConsensusProposal {
        height: Height(1),
        round: Round::new(0),
        value: block.clone(),
        pol_round: Round::Nil,
        proposer: hopnet_consensus::Address(0),
    };
    let vote = ctx.new_precommit(
        Height(1),
        Round::new(0),
        NilOrVal::Val(block.block_hash),
        hopnet_consensus::Address(0),
    );

    assert_ne!(
        proposal_sign_bytes(&chain_id(), &proposal),
        vote_sign_bytes(&chain_id(), &vote)
    );
}

// Should: verify a signature only under the signer's key AND the same chain id.
// Should not: verify under another mesh's chain id (cross-mesh replay) or
// another validator's key.
// Impact: chain binding is what makes a re-genesis or a second mesh immune to
// captured consensus traffic.
#[test]
fn signatures_bind_to_key_and_chain() {
    let ctx = HopNetContext;
    let vs = valset(2);
    let vote = ctx.new_precommit(
        Height(3),
        Round::new(0),
        NilOrVal::Val(hash(9)),
        hopnet_consensus::Address(0),
    );

    let sig = sign_vote(&chain_id(), &key(0), &vote);

    assert!(verify_vote(
        &chain_id(),
        &vs.validators()[0].public_key,
        &vote,
        &sig
    ));
    // Wrong key.
    assert!(!verify_vote(
        &chain_id(),
        &vs.validators()[1].public_key,
        &vote,
        &sig
    ));
    // Wrong chain.
    assert!(!verify_vote(
        &hash(42),
        &vs.validators()[0].public_key,
        &vote,
        &sig
    ));
}

// Should: detect any tampering of block data via Block::verify.
// Should not: accept a block whose stored hash predates a data mutation.
// Impact: block_hash is the Value::Id the whole protocol agrees on; a
// hash/data mismatch must be caught at ingest, not after commit.
#[test]
fn block_verify_catches_tampering() {
    let mut block = Block::new(BlockData {
        height: 5,
        round: 1,
        parent_hash: Some(hash(4)),
        transactions: Transactions::default(),
    })
    .unwrap();
    block.verify().unwrap();

    block.data.height = 6;
    assert!(block.verify().is_err());
}
