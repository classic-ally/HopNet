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
            VoteType::Precommit => HopNetContext.new_precommit(
                cert.height,
                cert.round,
                rs.value_id.clone(),
                rs.address,
            ),
            VoteType::Prevote => {
                HopNetContext.new_prevote(cert.height, cert.round, rs.value_id.clone(), rs.address)
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
