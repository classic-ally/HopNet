//! Certificate verification (host-owned, safety-critical) and quorum
//! profiles. Retargets the bespoke engine's quorum.rs threshold-math tests
//! onto ThresholdParams and adds adversarial certificate cases.

mod common;

use common::{chain_id, key, valset};
use hopnet_consensus::signing::sign_vote;
use hopnet_consensus::types::Blake3Hash;
use hopnet_consensus::verify::verify_commit_certificate;
use hopnet_consensus::{Height, HopNetContext, MalachiteThresholds, QuorumProfile};
use malachitebft_core_types::{
    CertificateError, CommitCertificate, CommitSignature, Context, NilOrVal, Round,
};

fn value_id() -> Blake3Hash {
    Blake3Hash::from_bytes([0xAB; 32])
}

/// Build a commit certificate for `value_id()` at height 3 round 0, signed by
/// the given node ids (each with its real key unless forged).
fn commit_cert(signers: &[i32]) -> CommitCertificate<HopNetContext> {
    let ctx = HopNetContext;
    let height = Height(3);
    let round = Round::new(0);
    let sigs = signers
        .iter()
        .map(|&id| {
            let vote = ctx.new_precommit(
                height,
                round,
                NilOrVal::Val(value_id()),
                hopnet_consensus::Address(id),
            );
            CommitSignature {
                address: hopnet_consensus::Address(id),
                signature: sign_vote(&chain_id(), &key(id), &vote),
            }
        })
        .collect();
    CommitCertificate {
        height,
        round,
        value_id: value_id(),
        commit_signatures: sigs,
    }
}

// Should: meet the BFT quorum at exactly >2/3 voting power for n=1..10.
// Should not: decide at exactly 2/3 (strict inequality).
// Impact: retargets the bespoke quorum.rs rounding tests; an off-by-one here
// is the difference between 2f+1 safety and a forgeable quorum.
#[test]
fn bft_quorum_boundaries() {
    let q = QuorumProfile::Bft.thresholds_for(1).quorum;
    // (n, smallest passing weight)
    let expected = [
        (1, 1),
        (2, 2),
        (3, 3),
        (4, 3),
        (5, 4),
        (6, 5),
        (7, 5),
        (8, 6),
        (9, 7),
        (10, 7),
    ];
    for (n, min_pass) in expected {
        assert!(
            !q.is_met(min_pass - 1, n),
            "n={n}: {} must fail",
            min_pass - 1
        );
        assert!(q.is_met(min_pass, n), "n={n}: {min_pass} must pass");
    }
}

// Should: meet the Majority quorum at strict >1/2 for small meshes.
// Should not: decide on exactly half (split brain in a 2-node or 4-node mesh).
// Impact: the home-mesh CFT profile — n=2 needs both nodes, n=3 tolerates
// one crash, n=4 needs 3 (no 2-2 split decisions).
#[test]
fn majority_quorum_boundaries() {
    let q = QuorumProfile::Majority.thresholds_for(1).quorum;
    let expected = [(1, 1), (2, 2), (3, 2), (4, 3), (5, 3), (6, 4), (7, 4)];
    for (n, min_pass) in expected {
        assert!(
            !q.is_met(min_pass - 1, n),
            "n={n}: {} must fail",
            min_pass - 1
        );
        assert!(q.is_met(min_pass, n), "n={n}: {min_pass} must pass");
    }
}

// Should: accept a certificate with a valid quorum of real signatures.
// Should not: reject on any well-formed input (no false negatives).
// Impact: false rejection here stalls sync and decide paths mesh-wide.
#[test]
fn commit_certificate_valid_quorum_passes() {
    let vs = valset(4);
    let cert = commit_cert(&[0, 1, 2]);
    verify_commit_certificate(
        &chain_id(),
        &cert,
        &vs,
        QuorumProfile::Bft.thresholds_for(1),
    )
    .unwrap();
}

// Should: reject a certificate below quorum with NotEnoughVotingPower.
// Should not: count sub-quorum certificates as decisions.
// Impact: accepting 2-of-4 in BFT mode would let f+1 colluding nodes forge
// commits.
#[test]
fn commit_certificate_sub_quorum_fails() {
    let vs = valset(4);
    let cert = commit_cert(&[0, 1]);
    let err = verify_commit_certificate(
        &chain_id(),
        &cert,
        &vs,
        QuorumProfile::Bft.thresholds_for(1),
    )
    .unwrap_err();
    assert!(matches!(
        err,
        CertificateError::NotEnoughVotingPower {
            signed: 2,
            total: 4,
            ..
        }
    ));
}

// Should: reject duplicate signatures from one validator outright.
// Should not: double-count a repeated signer toward quorum.
// Impact: THE classic certificate-forgery bug — one validator's signature
// repeated 3× must never pass a 3-of-4 quorum.
#[test]
fn commit_certificate_duplicate_signer_rejected() {
    let vs = valset(4);
    let mut cert = commit_cert(&[0]);
    let dup = cert.commit_signatures[0].clone();
    cert.commit_signatures.push(dup.clone());
    cert.commit_signatures.push(dup);

    let err = verify_commit_certificate(
        &chain_id(),
        &cert,
        &vs,
        QuorumProfile::Bft.thresholds_for(1),
    )
    .unwrap_err();
    assert!(matches!(err, CertificateError::DuplicateVote(a) if a.0 == 0));
}

// Should: reject signatures from addresses outside the validator set.
// Should not: accumulate power for unknown signers.
// Impact: a removed (or never-admitted) node must not influence quorum after
// its effective_height ends — this is what makes historical valset
// verification during sync meaningful.
#[test]
fn commit_certificate_unknown_validator_rejected() {
    let vs = valset(3);
    let cert = commit_cert(&[0, 1, 7]); // node 7 not in the set
    let err = verify_commit_certificate(
        &chain_id(),
        &cert,
        &vs,
        QuorumProfile::Bft.thresholds_for(1),
    )
    .unwrap_err();
    assert!(matches!(err, CertificateError::UnknownValidator(a) if a.0 == 7));
}

// Should: reject a certificate containing a forged signature.
// Should not: let quorum arithmetic proceed past an invalid signature.
// Impact: without per-signature verification a single honest signature could
// be cloned onto other validators' addresses.
#[test]
fn commit_certificate_forged_signature_rejected() {
    let vs = valset(4);
    let mut cert = commit_cert(&[0, 1, 2]);
    // Node 2's entry, but signed with node 3's key.
    let ctx = HopNetContext;
    let vote = ctx.new_precommit(
        Height(3),
        Round::new(0),
        NilOrVal::Val(value_id()),
        hopnet_consensus::Address(2),
    );
    cert.commit_signatures[2].signature = sign_vote(&chain_id(), &key(3), &vote);

    let err = verify_commit_certificate(
        &chain_id(),
        &cert,
        &vs,
        QuorumProfile::Bft.thresholds_for(1),
    )
    .unwrap_err();
    assert!(matches!(err, CertificateError::InvalidCommitSignature(_)));
}

// Should: reject a certificate produced under a different chain id.
// Should not: accept consensus signatures replayed from another mesh.
// Impact: chain binding end-to-end — a captured certificate from mesh A is
// noise on mesh B even with overlapping validator keys.
#[test]
fn commit_certificate_wrong_chain_rejected() {
    let vs = valset(3);
    let cert = commit_cert(&[0, 1]);
    let other_chain = Blake3Hash::from_bytes([0xEE; 32]);
    let err = verify_commit_certificate(
        &other_chain,
        &cert,
        &vs,
        QuorumProfile::Majority.thresholds_for(1),
    )
    .unwrap_err();
    assert!(matches!(err, CertificateError::InvalidCommitSignature(_)));
}

// Should: accept a majority certificate in the CFT profile that BFT rejects.
// Should not: blur the two profiles' thresholds.
// Impact: documents the profile boundary — 2-of-3 decides a home mesh but is
// NOT a BFT quorum.
#[test]
fn profile_boundary_two_of_three() {
    let vs = valset(3);
    let cert = commit_cert(&[0, 1]);

    verify_commit_certificate(
        &chain_id(),
        &cert,
        &vs,
        QuorumProfile::Majority.thresholds_for(1),
    )
    .unwrap();
    assert!(verify_commit_certificate(
        &chain_id(),
        &cert,
        &vs,
        QuorumProfile::Bft.thresholds_for(1)
    )
    .is_err());
}
