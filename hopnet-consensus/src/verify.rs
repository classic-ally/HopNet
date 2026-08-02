//! Host-side certificate verification.
//!
//! Malachite has NO engine-side certificate verifier: it emits
//! `Effect::Verify{Commit,Polka,Round}Certificate` and trusts the host's
//! verdict. This module is therefore safety-critical code we own. Rules:
//!
//! - every signer must be in the given validator set (the set effective at
//!   the certificate's height — historical sets during sync);
//! - duplicate signatures from one validator count ONCE (and are rejected
//!   outright — an honest peer never produces them);
//! - signatures verify against the canonical signing payloads (chain-bound);
//! - accumulated voting power must meet the threshold for the mesh's profile.

use std::collections::BTreeSet;

use malachitebft_core_types::{
    CertificateError, CommitCertificate, NilOrVal, PolkaCertificate, RoundCertificate,
    ThresholdParams, ValidatorSet as _, VoteType, VotingPower,
};

use crate::context::{Address, HopNetContext, HopNetValidatorSet};
use crate::signing::{verify_vote, Sig};
use crate::types::Blake3Hash;
use malachitebft_core_types::Context as _;

type CertError = CertificateError<HopNetContext>;

/// Shared skeleton: dedup signers, resolve each against the validator set,
/// verify the recreated vote signature, accumulate power, check threshold.
fn verify_votes<'a>(
    chain_id: &Blake3Hash,
    valset: &HopNetValidatorSet,
    thresholds: ThresholdParams,
    entries: impl Iterator<Item = (Address, VoteType, NilOrVal<Blake3Hash>, &'a Sig)>,
    height: crate::context::Height,
    round: malachitebft_core_types::Round,
    make_error: impl Fn(Address) -> CertError,
) -> Result<(), CertError> {
    let mut seen: BTreeSet<Address> = BTreeSet::new();
    let mut power: VotingPower = 0;

    for (address, vote_type, value, sig) in entries {
        if !seen.insert(address) {
            return Err(CertificateError::DuplicateVote(address));
        }
        let Some(validator) = valset.get_by_address(&address) else {
            return Err(CertificateError::UnknownValidator(address));
        };
        let vote = match vote_type {
            VoteType::Precommit => HopNetContext.new_precommit(height, round, value, address),
            VoteType::Prevote => HopNetContext.new_prevote(height, round, value, address),
        };
        if !verify_vote(chain_id, &validator.public_key, &vote, sig) {
            return Err(make_error(address));
        }
        power += validator.voting_power;
    }

    let total = valset.total_voting_power();
    if !thresholds.quorum.is_met(power, total) {
        return Err(CertificateError::NotEnoughVotingPower {
            signed: power,
            total,
            expected: thresholds.quorum.min_expected(total),
        });
    }
    Ok(())
}

/// Verify a commit certificate: a quorum of precommits for `value_id` at
/// (height, round), signed by validators of the set effective at that height.
pub fn verify_commit_certificate(
    chain_id: &Blake3Hash,
    cert: &CommitCertificate<HopNetContext>,
    valset: &HopNetValidatorSet,
    thresholds: ThresholdParams,
) -> Result<(), CertError> {
    verify_votes(
        chain_id,
        valset,
        thresholds,
        cert.commit_signatures.iter().map(|cs| {
            (
                cs.address,
                VoteType::Precommit,
                NilOrVal::Val(cert.value_id),
                &cs.signature,
            )
        }),
        cert.height,
        cert.round,
        |addr| {
            let cs = cert
                .commit_signatures
                .iter()
                .find(|cs| cs.address == addr)
                .expect("address came from this certificate")
                .clone();
            CertificateError::InvalidCommitSignature(cs)
        },
    )
}

/// Verify a polka certificate: a quorum of prevotes for `value_id`.
pub fn verify_polka_certificate(
    chain_id: &Blake3Hash,
    cert: &PolkaCertificate<HopNetContext>,
    valset: &HopNetValidatorSet,
    thresholds: ThresholdParams,
) -> Result<(), CertError> {
    verify_votes(
        chain_id,
        valset,
        thresholds,
        cert.polka_signatures.iter().map(|ps| {
            (
                ps.address,
                VoteType::Prevote,
                NilOrVal::Val(cert.value_id),
                &ps.signature,
            )
        }),
        cert.height,
        cert.round,
        |addr| {
            let ps = cert
                .polka_signatures
                .iter()
                .find(|ps| ps.address == addr)
                .expect("address came from this certificate")
                .clone();
            CertificateError::InvalidPolkaSignature(ps)
        },
    )
}

/// Verify a round certificate (skip-round or precommit-any).
///
/// Unlike commit/polka certs, round certificates mix vote values (each signer
/// signed its own view of the round), and the threshold depends on the
/// certificate type: Skip needs the `honest` threshold (f+1), Precommit needs
/// `quorum` (2f+1). Precommit-type certs must contain only precommits.
pub fn verify_round_certificate(
    chain_id: &Blake3Hash,
    cert: &RoundCertificate<HopNetContext>,
    valset: &HopNetValidatorSet,
    thresholds: ThresholdParams,
) -> Result<(), CertError> {
    use malachitebft_core_types::RoundCertificateType;

    let mut seen: BTreeSet<Address> = BTreeSet::new();
    let mut power: VotingPower = 0;

    for rs in &cert.round_signatures {
        if !seen.insert(rs.address) {
            return Err(CertificateError::DuplicateVote(rs.address));
        }
        let Some(validator) = valset.get_by_address(&rs.address) else {
            return Err(CertificateError::UnknownValidator(rs.address));
        };
        if cert.cert_type == RoundCertificateType::Precommit && rs.vote_type == VoteType::Prevote {
            return Err(CertificateError::InvalidVoteType(rs.address));
        }
        let vote = match rs.vote_type {
            VoteType::Precommit => {
                HopNetContext.new_precommit(cert.height, cert.round, rs.value_id, rs.address)
            }
            VoteType::Prevote => {
                HopNetContext.new_prevote(cert.height, cert.round, rs.value_id, rs.address)
            }
        };
        if !verify_vote(chain_id, &validator.public_key, &vote, &rs.signature) {
            return Err(CertificateError::InvalidRoundSignature(rs.clone()));
        }
        power += validator.voting_power;
    }

    let total = valset.total_voting_power();
    let threshold = match cert.cert_type {
        RoundCertificateType::Skip => thresholds.honest,
        RoundCertificateType::Precommit => thresholds.quorum,
    };
    if !threshold.is_met(power, total) {
        return Err(CertificateError::NotEnoughVotingPower {
            signed: power,
            total,
            expected: threshold.min_expected(total),
        });
    }
    Ok(())
}

/// Verify a WIRE-form commit certificate in one call: decode, derive the
/// profile's thresholds from the given set's size, verify. The epoch-
/// boundary seam (RFC-019 S6 lineage gate; S7 joiners): those callers
/// hold wire certificates and a quorum profile, not engine internals —
/// `CommitCertificate` is not re-exported, deliberately.
pub fn verify_wire_certificate(
    chain_id: &Blake3Hash,
    cert: &crate::codec::WireCommitCertificate,
    valset: &HopNetValidatorSet,
    profile: &crate::config::QuorumProfile,
) -> Result<(), String> {
    use crate::config::MalachiteThresholds as _;
    let decoded: CommitCertificate<HopNetContext> = cert
        .try_into()
        .map_err(|e: crate::codec::CodecError| format!("certificate decode: {e:?}"))?;
    let thresholds = profile.thresholds_for(valset.count() as u64);
    verify_commit_certificate(chain_id, &decoded, valset, thresholds)
        .map_err(|e| format!("certificate verification: {e:?}"))
}

/// How many of a wire certificate's signatures verify as precommits on
/// its (height, round, value_id) by members of `trusted` — the RFC-019
/// S7 weak-subjectivity overlap primitive. `verify_wire_certificate` is
/// quorum-of-the-given-set; the overlap rule instead asks how far a
/// certificate's signer set INTERSECTS a set the verifier already
/// trusted (its own last-trusted seated set), and the caller compares
/// the count against that set's Byzantine bound.
///
/// Duplicate signers count once. Unknown or non-verifying signers are
/// SKIPPED, not errors: the certificate was already quorum-verified
/// against its own claimed set — this only measures the intersection.
pub fn count_trusted_signers(
    chain_id: &Blake3Hash,
    cert: &crate::codec::WireCommitCertificate,
    trusted: &HopNetValidatorSet,
) -> usize {
    let Ok(decoded) = CommitCertificate::<HopNetContext>::try_from(cert) else {
        return 0;
    };
    let mut seen: BTreeSet<Address> = BTreeSet::new();
    let mut count = 0;
    for cs in &decoded.commit_signatures {
        if !seen.insert(cs.address) {
            continue;
        }
        let Some(validator) = trusted.get_by_address(&cs.address) else {
            continue;
        };
        // Real final certificates carry the round consensus decided at —
        // the recreated vote must use cert.round, never assume round 0.
        let vote = HopNetContext.new_precommit(
            decoded.height,
            decoded.round,
            NilOrVal::Val(decoded.value_id),
            cs.address,
        );
        if verify_vote(chain_id, &validator.public_key, &vote, &cs.signature) {
            count += 1;
        }
    }
    count
}

/// Fabricate one wire commit signature: a precommit on (height, round 0,
/// value_id) signed by `key` for validator `address`. The certificate-
/// CONSTRUCTION seam for code that must build certificates outside a
/// running engine (epoch-boundary verification tests, S7 join fixtures).
/// The production decide path never calls this — real certificates come
/// from malachite.
pub fn wire_commit_signature(
    chain_id: &Blake3Hash,
    key: &crate::types::PrivKey,
    height: crate::context::Height,
    value_id: Blake3Hash,
    address: i32,
) -> (i32, crate::codec::WireSig) {
    wire_commit_signature_at_round(chain_id, key, height, 0, value_id, address)
}

/// `wire_commit_signature` with an explicit round — real decides can land
/// past round 0, and overlap counting must recreate the vote at the
/// certificate's actual round.
pub fn wire_commit_signature_at_round(
    chain_id: &Blake3Hash,
    key: &crate::types::PrivKey,
    height: crate::context::Height,
    round: u32,
    value_id: Blake3Hash,
    address: i32,
) -> (i32, crate::codec::WireSig) {
    let vote = HopNetContext.new_precommit(
        height,
        malachitebft_core_types::Round::new(round),
        NilOrVal::Val(value_id),
        Address(address),
    );
    let sig = crate::signing::sign_vote(chain_id, key, &vote);
    (address, crate::codec::WireSig(sig.0.to_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::WireCommitCertificate;
    use crate::context::{Height, Validator};
    use crate::types::{PrivKey, PubKey};
    use ed25519_dalek::SigningKey;

    const H: Height = Height(12);

    fn chain() -> Blake3Hash {
        Blake3Hash::from_bytes([7; 32])
    }

    fn value() -> Blake3Hash {
        Blake3Hash::from_bytes([9; 32])
    }

    fn key(seed: u8) -> SigningKey {
        SigningKey::from_bytes(&[seed; 32])
    }

    fn valset(ids: &[u8]) -> HopNetValidatorSet {
        HopNetValidatorSet::new(
            ids.iter()
                .map(|&id| Validator::new(id as i32, PubKey(key(id).verifying_key())))
                .collect(),
        )
    }

    fn cert_signed_by(round: u32, signers: &[(u8, i32)]) -> WireCommitCertificate {
        WireCommitCertificate {
            height: H.0,
            round: round as i64,
            value_id: value(),
            signatures: signers
                .iter()
                .map(|&(seed, addr)| {
                    wire_commit_signature_at_round(
                        &chain(),
                        &PrivKey(key(seed)),
                        H,
                        round,
                        value(),
                        addr,
                    )
                })
                .collect(),
        }
    }

    // Impact: the overlap count is the weak-subjectivity trust decision —
    // over-counting would let a fabricated boundary certificate pass a
    // straggler's Byzantine bound.
    // Should: count exactly the signers that are members of the trusted
    // set with verifying signatures.
    // Should not: count signers outside the trusted set, even with valid
    // signatures over the same payload.
    #[test]
    fn counts_only_trusted_verifying_signers() {
        let trusted = valset(&[1, 2, 3]);
        // Signers 1 and 2 are trusted; 4 signs validly but is unknown.
        let cert = cert_signed_by(0, &[(1, 1), (2, 2), (4, 4)]);
        assert_eq!(count_trusted_signers(&chain(), &cert, &trusted), 2);
    }

    // Should: count a duplicated signer once.
    #[test]
    fn duplicate_signers_count_once() {
        let trusted = valset(&[1, 2]);
        let cert = cert_signed_by(0, &[(1, 1), (1, 1), (2, 2)]);
        assert_eq!(count_trusted_signers(&chain(), &cert, &trusted), 2);
    }

    // Should not: count a signature that does not verify for the claimed
    // address (key A signing under address B).
    #[test]
    fn forged_signature_is_skipped_not_fatal() {
        let trusted = valset(&[1, 2]);
        // Key 3 signs but claims address 2 (a trusted member).
        let mut cert = cert_signed_by(0, &[(1, 1)]);
        let (_, forged) =
            wire_commit_signature_at_round(&chain(), &PrivKey(key(3)), H, 0, value(), 2);
        cert.signatures.push((2, forged));
        assert_eq!(count_trusted_signers(&chain(), &cert, &trusted), 1);
    }

    // Impact: real finals can decide past round 0 — recreating the vote
    // at an assumed round 0 would zero the overlap for exactly the
    // certificates produced under contention.
    // Should: verify signatures at the certificate's actual round.
    #[test]
    fn nonzero_round_certificates_count() {
        let trusted = valset(&[1, 2]);
        let cert = cert_signed_by(3, &[(1, 1), (2, 2)]);
        assert_eq!(count_trusted_signers(&chain(), &cert, &trusted), 2);

        // The same signers at the WRONG round verify as nothing.
        let mut wrong = cert_signed_by(0, &[(1, 1), (2, 2)]);
        wrong.round = 3;
        assert_eq!(count_trusted_signers(&chain(), &wrong, &trusted), 0);
    }
}
