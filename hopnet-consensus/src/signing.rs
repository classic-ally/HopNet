//! Signing scheme and canonical signing payloads.
//!
//! The engine hands us bare votes/proposals to sign (`Effect::SignVote` /
//! `Effect::SignProposal`); the byte encoding that actually gets signed is
//! entirely host-defined. Two properties are load-bearing:
//!
//! - **Injectivity**: distinct messages must never encode to the same bytes,
//!   or a signature transfers between them (forged equivocation, certificate
//!   confusion). Achieved with a domain tag per message type and fixed-width
//!   fields — no length-prefixed or variable encodings anywhere.
//! - **Chain binding**: every payload starts with the mesh's chain id (the
//!   genesis block hash), so consensus signatures can never replay across
//!   meshes, or across a re-genesis of the same mesh.

use ed25519_dalek::{Signer, Verifier};
use malachitebft_core_consensus::ConsensusMsg;
use malachitebft_core_types::{NilOrVal, VoteType};

use crate::context::{ConsensusProposal, ConsensusVote, HopNetContext};
use crate::types::{Blake3Hash, PrivKey, PubKey};

/// Domain-separation prefix, versioned. Bump on any payload format change.
const DOMAIN: &[u8; 20] = b"hopnet/consensus/v1\0";

const TAG_PREVOTE: u8 = 0x01;
const TAG_PRECOMMIT: u8 = 0x02;
const TAG_PROPOSAL: u8 = 0x03;

/// ed25519_dalek::Signature does not implement `Ord`, which Malachite's
/// `SigningScheme::Signature` requires — newtype over it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Sig(pub ed25519_dalek::Signature);

impl PartialOrd for Sig {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Sig {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.to_bytes().cmp(&other.0.to_bytes())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ed25519Scheme;

impl malachitebft_core_types::SigningScheme for Ed25519Scheme {
    type DecodingError = String;
    type Signature = Sig;
    type PublicKey = PubKey;
    type PrivateKey = PrivKey;

    fn decode_signature(bytes: &[u8]) -> Result<Sig, String> {
        ed25519_dalek::Signature::from_slice(bytes)
            .map(Sig)
            .map_err(|e| e.to_string())
    }

    fn encode_signature(signature: &Sig) -> Vec<u8> {
        signature.0.to_bytes().to_vec()
    }
}

/// Canonical, injective byte encoding of a vote for signing.
///
/// Layout: DOMAIN ‖ chain_id ‖ tag ‖ height(8 LE) ‖ round(8 LE, Nil = -1)
///         ‖ value tag(1) ‖ value hash(32, zeroed for Nil) ‖ voter(4 LE)
pub fn vote_sign_bytes(chain_id: &Blake3Hash, vote: &ConsensusVote) -> Vec<u8> {
    let mut b = Vec::with_capacity(DOMAIN.len() + 32 + 1 + 8 + 8 + 1 + 32 + 4);
    b.extend_from_slice(DOMAIN);
    b.extend_from_slice(chain_id.as_bytes());
    b.push(match vote.typ {
        VoteType::Prevote => TAG_PREVOTE,
        VoteType::Precommit => TAG_PRECOMMIT,
    });
    b.extend_from_slice(&vote.height.0.to_le_bytes());
    b.extend_from_slice(&vote.round.as_i64().to_le_bytes());
    match &vote.value {
        NilOrVal::Nil => {
            b.push(0);
            b.extend_from_slice(&[0u8; 32]);
        }
        NilOrVal::Val(hash) => {
            b.push(1);
            b.extend_from_slice(hash.as_bytes());
        }
    }
    b.extend_from_slice(&vote.voter.0.to_le_bytes());
    b
}

/// Canonical, injective byte encoding of a proposal for signing.
///
/// Signs the block HASH, not the block bytes — the hash is the content
/// address, and re-hashing the whole block here would double work for nothing.
///
/// Layout: DOMAIN ‖ chain_id ‖ tag ‖ height(8 LE) ‖ round(8 LE)
///         ‖ block hash(32) ‖ pol_round(8 LE) ‖ proposer(4 LE)
pub fn proposal_sign_bytes(chain_id: &Blake3Hash, proposal: &ConsensusProposal) -> Vec<u8> {
    let mut b = Vec::with_capacity(DOMAIN.len() + 32 + 1 + 8 + 8 + 32 + 8 + 4);
    b.extend_from_slice(DOMAIN);
    b.extend_from_slice(chain_id.as_bytes());
    b.push(TAG_PROPOSAL);
    b.extend_from_slice(&proposal.height.0.to_le_bytes());
    b.extend_from_slice(&proposal.round.as_i64().to_le_bytes());
    b.extend_from_slice(proposal.value.block_hash.as_bytes());
    b.extend_from_slice(&proposal.pol_round.as_i64().to_le_bytes());
    b.extend_from_slice(&proposal.proposer.0.to_le_bytes());
    b
}

/// Signing bytes for any engine consensus message.
pub fn consensus_msg_sign_bytes(
    chain_id: &Blake3Hash,
    msg: &ConsensusMsg<HopNetContext>,
) -> Vec<u8> {
    match msg {
        ConsensusMsg::Vote(v) => vote_sign_bytes(chain_id, v),
        ConsensusMsg::Proposal(p) => proposal_sign_bytes(chain_id, p),
    }
}

pub fn sign_vote(chain_id: &Blake3Hash, key: &PrivKey, vote: &ConsensusVote) -> Sig {
    Sig(key.sign(&vote_sign_bytes(chain_id, vote)))
}

pub fn sign_proposal(chain_id: &Blake3Hash, key: &PrivKey, proposal: &ConsensusProposal) -> Sig {
    Sig(key.sign(&proposal_sign_bytes(chain_id, proposal)))
}

pub fn verify_vote(
    chain_id: &Blake3Hash,
    pubkey: &PubKey,
    vote: &ConsensusVote,
    sig: &Sig,
) -> bool {
    pubkey
        .verify(&vote_sign_bytes(chain_id, vote), &sig.0)
        .is_ok()
}

pub fn verify_proposal(
    chain_id: &Blake3Hash,
    pubkey: &PubKey,
    proposal: &ConsensusProposal,
    sig: &Sig,
) -> bool {
    pubkey
        .verify(&proposal_sign_bytes(chain_id, proposal), &sig.0)
        .is_ok()
}
