//! Codec fidelity: every wire/WAL mirror must round-trip losslessly, and the
//! byte format must not drift silently (golden bytes). A round-trip that
//! drops a field (e.g. valid_round) silently re-opens lock-rule holes; a
//! format drift partitions a mixed-version mesh.

mod common;

use common::{chain_id, key, valset};
use hopnet_consensus::codec::{
    decode, encode, SignedProposal, SignedVote, WireCommitCertificate, WireConsensusMsg,
    WireProposal, WireProposedValue, WireVote, WireWalEntry,
};
use hopnet_consensus::context::ConsensusProposal;
use hopnet_consensus::signing::{sign_proposal, sign_vote};
use hopnet_consensus::types::{Blake3Hash, Block, BlockData, Transaction, Transactions};
use hopnet_consensus::MalachiteThresholds;
use hopnet_consensus::{Address, Height, HopNetContext};
use malachitebft_core_consensus::{ProposedValue, SignedConsensusMsg, WalEntry};
use malachitebft_core_types::{
    CommitCertificate, Context, NilOrVal, Round, SignedMessage, Timeout, TimeoutKind, Validity,
};

fn hash(byte: u8) -> Blake3Hash {
    Blake3Hash::from_bytes([byte; 32])
}

fn test_block() -> Block {
    let tx = Transaction::new("insert_files".into(), vec![1, 2, 3], 0, &key(0)).unwrap();
    Block::new(BlockData {
        height: 4,
        round: 1,
        parent_hash: Some(hash(3)),
        transactions: Transactions(vec![tx]),
    })
    .unwrap()
}

fn test_signed_vote(nil: bool) -> SignedVote {
    let ctx = HopNetContext;
    let value = if nil {
        NilOrVal::Nil
    } else {
        NilOrVal::Val(hash(9))
    };
    let vote = ctx.new_precommit(Height(4), Round::new(2), value, Address(1));
    let sig = sign_vote(&chain_id(), &key(1), &vote);
    SignedMessage::new(vote, sig)
}

// Should: round-trip signed votes (incl. Nil votes and Nil rounds) through
// the wire encoding without any field loss.
// Should not: normalize or default any field on the way through.
// Impact: votes are the protocol's atoms; a lossy field breaks certificate
// verification on the receiving side.
#[test]
fn wire_vote_roundtrip() {
    for nil in [false, true] {
        let sv = test_signed_vote(nil);
        let wire = WireVote::from(&sv);
        let bytes = encode(&wire).unwrap();
        let back: WireVote = decode(&bytes).unwrap();
        assert_eq!(wire, back);
        let sv2: SignedVote = (&back).try_into().unwrap();
        assert_eq!(sv.message, sv2.message);
        assert_eq!(sv.signature, sv2.signature);
    }
}

// Should: round-trip engine-internal signed proposals (WAL-persisted),
// preserving pol_round — including pol_round = Nil.
// Should not: lose the POL round; it carries the lock-rule justification.
// Impact: dropping pol_round in WAL replay is exactly the class of bug the
// old engine had (lock context erased on restart).
#[test]
fn wire_proposal_roundtrip_preserves_pol_round() {
    for pol in [Round::Nil, Round::new(1)] {
        let proposal = ConsensusProposal {
            height: Height(4),
            round: Round::new(2),
            value: test_block(),
            pol_round: pol,
            proposer: Address(0),
        };
        let sig = sign_proposal(&chain_id(), &key(0), &proposal);
        let sp: SignedProposal = SignedMessage::new(proposal, sig);

        let wire = WireProposal::from(&sp);
        let back: SignedProposal = (&decode::<WireProposal>(&encode(&wire).unwrap()).unwrap())
            .try_into()
            .unwrap();
        assert_eq!(sp.message, back.message);
        assert_eq!(sp.signature, back.signature);
    }
}

// Should: carry full blocks through the host wire proposal and let the
// RECEIVER attach its own validity verdict.
// Should not: transport a validity bit (validity is never trusted from the
// wire — that's the whole PartsOnly point).
// Impact: guards the Rule-8 seam — a wire format that shipped validity would
// reintroduce the ProposalOnly auto-Valid hole we switched modes to avoid.
#[test]
fn wire_proposed_value_receiver_owns_validity() {
    let pv = ProposedValue {
        height: Height(4),
        round: Round::new(0),
        valid_round: Round::Nil,
        proposer: Address(2),
        value: test_block(),
        validity: Validity::Valid,
    };
    let wire = WireProposedValue::from(&pv);
    let back = decode::<WireProposedValue>(&encode(&wire).unwrap()).unwrap();

    let as_invalid = back.into_proposed_value(Validity::Invalid).unwrap();
    assert_eq!(as_invalid.value, pv.value);
    assert_eq!(as_invalid.valid_round, pv.valid_round);
    assert_eq!(as_invalid.validity, Validity::Invalid); // receiver's verdict, not sender's
}

// Should: round-trip commit certificates exactly (order and content).
// Should not: reorder or dedup signatures in the codec (verification owns
// that policy).
// Impact: certificates are the sync protocol's proof objects; codec-level
// mutation would make honest certs unverifiable.
#[test]
fn wire_commit_certificate_roundtrip() {
    let ctx = HopNetContext;
    let mut sigs = Vec::new();
    for id in [0, 1, 2] {
        let vote = ctx.new_precommit(
            Height(4),
            Round::new(0),
            NilOrVal::Val(hash(9)),
            Address(id),
        );
        sigs.push(malachitebft_core_types::CommitSignature {
            address: Address(id),
            signature: sign_vote(&chain_id(), &key(id), &vote),
        });
    }
    let cert = CommitCertificate::<HopNetContext> {
        height: Height(4),
        round: Round::new(0),
        value_id: hash(9),
        commit_signatures: sigs,
    };

    let wire = WireCommitCertificate::from(&cert);
    let back: CommitCertificate<HopNetContext> =
        (&decode::<WireCommitCertificate>(&encode(&wire).unwrap()).unwrap())
            .try_into()
            .unwrap();

    assert_eq!(back.height, cert.height);
    assert_eq!(back.value_id, cert.value_id);
    assert_eq!(back.commit_signatures.len(), 3);
    // Re-verify after the round-trip: signatures still bind.
    let vs = valset(3);
    hopnet_consensus::verify::verify_commit_certificate(
        &chain_id(),
        &back,
        &vs,
        hopnet_consensus::QuorumProfile::Bft.thresholds_for(1),
    )
    .unwrap();
}

// Should: round-trip every WAL entry variant, including FinalizeHeight
// timeouts and the validity bit on proposed values.
// Should not: lose validity in the WAL — replay feeds it back to the engine.
// Impact: WAL replay correctness is the no-equivocation-across-restart
// property; a lossy entry silently weakens crash recovery.
#[test]
fn wire_wal_entry_roundtrip_all_variants() {
    let sv = test_signed_vote(false);
    let proposal = ConsensusProposal {
        height: Height(4),
        round: Round::new(0),
        value: test_block(),
        pol_round: Round::Nil,
        proposer: Address(0),
    };
    let sp: SignedProposal = SignedMessage::new(
        proposal.clone(),
        sign_proposal(&chain_id(), &key(0), &proposal),
    );

    let entries: Vec<WalEntry<HopNetContext>> = vec![
        WalEntry::ConsensusMsg(SignedConsensusMsg::Vote(sv)),
        WalEntry::ConsensusMsg(SignedConsensusMsg::Proposal(sp)),
        WalEntry::Timeout(Timeout::new(Round::new(3), TimeoutKind::Prevote)),
        WalEntry::Timeout(Timeout::new(
            Round::new(0),
            TimeoutKind::FinalizeHeight(std::time::Duration::from_millis(1500)),
        )),
        WalEntry::ProposedValue(ProposedValue {
            height: Height(4),
            round: Round::new(0),
            valid_round: Round::new(0),
            proposer: Address(1),
            value: test_block(),
            validity: Validity::Invalid,
        }),
    ];

    for entry in &entries {
        let wire = WireWalEntry::from(entry);
        let bytes = encode(&wire).unwrap();
        let back: WireWalEntry = decode(&bytes).unwrap();
        assert_eq!(wire, back);
        let restored: WalEntry<HopNetContext> = (&back).try_into().unwrap();
        // WalEntry has no PartialEq; compare through the wire form again.
        assert_eq!(WireWalEntry::from(&restored), wire);
    }
}

// Should: keep the wire byte format stable across refactors.
// Should not: let a bincode config or field-order change slip through
// unnoticed.
// Impact: nodes on different builds must parse each other during rolling
// updates; silent format drift partitions the mesh at the transport layer.
#[test]
fn golden_bytes_wire_vote() {
    let ctx = HopNetContext;
    let vote = ctx.new_precommit(
        Height(7),
        Round::new(1),
        NilOrVal::Val(hash(0x11)),
        Address(3),
    );
    // Fixed signature bytes (not a real signature — format test only).
    let sv: SignedVote = SignedMessage::new(
        vote,
        hopnet_consensus::signing::Sig(ed25519_dalek::Signature::from_bytes(&[0x22; 64])),
    );
    let bytes = encode(&WireVote::from(&sv)).unwrap();

    let expected_hex = "01070201201111111111111111111111111111111111111111111111111111111111111111064022222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222";
    assert_eq!(
        hex::encode(&bytes),
        expected_hex,
        "wire format drifted — if intentional, bump the format and update this golden"
    );
}

// Should: expose the message height for host lag detection on every variant.
// Should not: leave any wire message without a height.
// Impact: lag detection keys sync triggering; a height-less message would
// bypass catch-up.
#[test]
fn wire_consensus_msg_reports_height() {
    let sv = test_signed_vote(false);
    let msg = WireConsensusMsg::Vote(WireVote::from(&sv));
    assert_eq!(msg.height(), 4);
}
