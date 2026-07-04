//! Wire and WAL serialization: mirror structs for every engine type that
//! crosses a process boundary (network) or a restart boundary (WAL).
//!
//! Malachite's generic types don't guarantee serde impls, and depending on
//! their (proto/borsh-flavored) codec story would couple our wire format to
//! upstream internals. Instead every serialized shape is OURS: plain-data
//! mirror structs with `From`/`TryFrom` conversions, encoded with
//! bincode-serde standard config (the encoding HopNet already uses on the
//! wire). Format stability is guarded by golden-bytes tests — if an encoding
//! changes shape, a test fails before a mixed-mesh does.

use serde::{Deserialize, Serialize};

use malachitebft_core_consensus::{ProposedValue, SignedConsensusMsg, WalEntry};
use malachitebft_core_types::{
    CommitCertificate, CommitSignature, NilOrVal, PolkaCertificate, PolkaSignature, Round,
    RoundCertificate, RoundCertificateType, RoundSignature, SignedMessage, Timeout, TimeoutKind,
    Validity, VoteType,
};

use crate::context::{Address, ConsensusProposal, ConsensusVote, Height, HopNetContext};
use crate::signing::Sig;
use crate::types::{Blake3Hash, Block};

pub type SignedVote = SignedMessage<HopNetContext, ConsensusVote>;
pub type SignedProposal = SignedMessage<HopNetContext, ConsensusProposal>;

#[derive(Debug)]
pub enum CodecError {
    Encode(bincode::error::EncodeError),
    Decode(bincode::error::DecodeError),
    /// A field decoded but is semantically invalid (bad signature bytes,
    /// unknown discriminant, malformed round).
    Invalid(&'static str),
}

impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CodecError::Encode(e) => write!(f, "encode: {e}"),
            CodecError::Decode(e) => write!(f, "decode: {e}"),
            CodecError::Invalid(what) => write!(f, "invalid field: {what}"),
        }
    }
}

impl std::error::Error for CodecError {}

// ---------------------------------------------------------------------------
// Scalar mirrors

/// Round on the wire: -1 = Nil, else the round number.
fn round_to_wire(r: Round) -> i64 {
    r.as_i64()
}

fn round_from_wire(v: i64) -> Result<Round, CodecError> {
    match v {
        -1 => Ok(Round::Nil),
        n if n >= 0 && n <= u32::MAX as i64 => Ok(Round::new(n as u32)),
        _ => Err(CodecError::Invalid("round")),
    }
}

/// 64-byte signature on the wire. serde only derives array impls up to 32
/// bytes, so this newtype serializes as a bytes blob.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WireSig(pub [u8; 64]);

impl Serialize for WireSig {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for WireSig {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = WireSig;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("64 signature bytes")
            }
            fn visit_bytes<E: serde::de::Error>(self, v: &[u8]) -> Result<WireSig, E> {
                let arr: [u8; 64] = v
                    .try_into()
                    .map_err(|_| E::custom("signature must be exactly 64 bytes"))?;
                Ok(WireSig(arr))
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<WireSig, A::Error> {
                let mut bytes = Vec::with_capacity(64);
                while let Some(b) = seq.next_element::<u8>()? {
                    bytes.push(b);
                }
                self.visit_bytes(&bytes)
            }
        }
        deserializer.deserialize_bytes(V)
    }
}

fn sig_to_wire(s: &Sig) -> WireSig {
    WireSig(s.0.to_bytes())
}

fn sig_from_wire(b: &WireSig) -> Sig {
    Sig(ed25519_dalek::Signature::from_bytes(&b.0))
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum WireVoteType {
    Prevote,
    Precommit,
}

impl From<VoteType> for WireVoteType {
    fn from(t: VoteType) -> Self {
        match t {
            VoteType::Prevote => WireVoteType::Prevote,
            VoteType::Precommit => WireVoteType::Precommit,
        }
    }
}

impl From<WireVoteType> for VoteType {
    fn from(t: WireVoteType) -> Self {
        match t {
            WireVoteType::Prevote => VoteType::Prevote,
            WireVoteType::Precommit => VoteType::Precommit,
        }
    }
}

// ---------------------------------------------------------------------------
// Votes and proposals

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct WireVote {
    pub typ: WireVoteType,
    pub height: u64,
    pub round: i64,
    /// None = Nil vote.
    pub value: Option<Blake3Hash>,
    pub voter: i32,
    pub signature: WireSig,
}

impl From<&SignedVote> for WireVote {
    fn from(sv: &SignedVote) -> Self {
        let v = &sv.message;
        WireVote {
            typ: v.typ.into(),
            height: v.height.0,
            round: round_to_wire(v.round),
            value: match &v.value {
                NilOrVal::Nil => None,
                NilOrVal::Val(h) => Some(*h),
            },
            voter: v.voter.0,
            signature: sig_to_wire(&sv.signature),
        }
    }
}

impl TryFrom<&WireVote> for SignedVote {
    type Error = CodecError;

    fn try_from(w: &WireVote) -> Result<Self, CodecError> {
        let vote = ConsensusVote {
            typ: w.typ.into(),
            height: Height(w.height),
            round: round_from_wire(w.round)?,
            value: match w.value {
                None => NilOrVal::Nil,
                Some(h) => NilOrVal::Val(h),
            },
            voter: Address(w.voter),
            extension: None,
        };
        Ok(SignedMessage::new(vote, sig_from_wire(&w.signature)))
    }
}

/// Engine-internal signed proposal. Never broadcast in PartsOnly mode, but
/// WAL entries contain them (the engine persists its own proposals).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct WireProposal {
    pub height: u64,
    pub round: i64,
    pub block: Block,
    pub pol_round: i64,
    pub proposer: i32,
    pub signature: WireSig,
}

impl From<&SignedProposal> for WireProposal {
    fn from(sp: &SignedProposal) -> Self {
        let p = &sp.message;
        WireProposal {
            height: p.height.0,
            round: round_to_wire(p.round),
            block: p.value.clone(),
            pol_round: round_to_wire(p.pol_round),
            proposer: p.proposer.0,
            signature: sig_to_wire(&sp.signature),
        }
    }
}

impl TryFrom<&WireProposal> for SignedProposal {
    type Error = CodecError;

    fn try_from(w: &WireProposal) -> Result<Self, CodecError> {
        let proposal = ConsensusProposal {
            height: Height(w.height),
            round: round_from_wire(w.round)?,
            value: w.block.clone(),
            pol_round: round_from_wire(w.pol_round)?,
            proposer: Address(w.proposer),
        };
        Ok(SignedMessage::new(proposal, sig_from_wire(&w.signature)))
    }
}

/// The host's actual wire proposal (PartsOnly mode): full block + the fields
/// the receiver needs to reconstruct a `ProposedValue` after validating.
/// `validity` is NOT on the wire — the RECEIVER decides it (Rule-8 dry-run).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct WireProposedValue {
    pub height: u64,
    pub round: i64,
    pub valid_round: i64,
    pub proposer: i32,
    pub block: Block,
}

impl From<&ProposedValue<HopNetContext>> for WireProposedValue {
    fn from(pv: &ProposedValue<HopNetContext>) -> Self {
        WireProposedValue {
            height: pv.height.0,
            round: round_to_wire(pv.round),
            valid_round: round_to_wire(pv.valid_round),
            proposer: pv.proposer.0,
            block: pv.value.clone(),
        }
    }
}

impl WireProposedValue {
    /// Reconstruct with the receiver's own validity verdict.
    pub fn into_proposed_value(
        self,
        validity: Validity,
    ) -> Result<ProposedValue<HopNetContext>, CodecError> {
        Ok(ProposedValue {
            height: Height(self.height),
            round: round_from_wire(self.round)?,
            valid_round: round_from_wire(self.valid_round)?,
            proposer: Address(self.proposer),
            value: self.block,
            validity,
        })
    }
}

// ---------------------------------------------------------------------------
// Certificates

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct WireCommitCertificate {
    pub height: u64,
    pub round: i64,
    pub value_id: Blake3Hash,
    /// (validator address, signature)
    pub signatures: Vec<(i32, WireSig)>,
}

impl From<&CommitCertificate<HopNetContext>> for WireCommitCertificate {
    fn from(c: &CommitCertificate<HopNetContext>) -> Self {
        WireCommitCertificate {
            height: c.height.0,
            round: round_to_wire(c.round),
            value_id: c.value_id,
            signatures: c
                .commit_signatures
                .iter()
                .map(|cs| (cs.address.0, sig_to_wire(&cs.signature)))
                .collect(),
        }
    }
}

impl TryFrom<&WireCommitCertificate> for CommitCertificate<HopNetContext> {
    type Error = CodecError;

    fn try_from(w: &WireCommitCertificate) -> Result<Self, CodecError> {
        Ok(CommitCertificate {
            height: Height(w.height),
            round: round_from_wire(w.round)?,
            value_id: w.value_id,
            commit_signatures: w
                .signatures
                .iter()
                .map(|(addr, sig)| CommitSignature {
                    address: Address(*addr),
                    signature: sig_from_wire(sig),
                })
                .collect(),
        })
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct WirePolkaCertificate {
    pub height: u64,
    pub round: i64,
    pub value_id: Blake3Hash,
    pub signatures: Vec<(i32, WireSig)>,
}

impl From<&PolkaCertificate<HopNetContext>> for WirePolkaCertificate {
    fn from(c: &PolkaCertificate<HopNetContext>) -> Self {
        WirePolkaCertificate {
            height: c.height.0,
            round: round_to_wire(c.round),
            value_id: c.value_id,
            signatures: c
                .polka_signatures
                .iter()
                .map(|ps| (ps.address.0, sig_to_wire(&ps.signature)))
                .collect(),
        }
    }
}

impl TryFrom<&WirePolkaCertificate> for PolkaCertificate<HopNetContext> {
    type Error = CodecError;

    fn try_from(w: &WirePolkaCertificate) -> Result<Self, CodecError> {
        Ok(PolkaCertificate {
            height: Height(w.height),
            round: round_from_wire(w.round)?,
            value_id: w.value_id,
            polka_signatures: w
                .signatures
                .iter()
                .map(|(addr, sig)| PolkaSignature {
                    address: Address(*addr),
                    signature: sig_from_wire(sig),
                })
                .collect(),
        })
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum WireRoundCertificateType {
    Skip,
    Precommit,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct WireRoundSignature {
    pub vote_type: WireVoteType,
    /// None = Nil.
    pub value_id: Option<Blake3Hash>,
    pub address: i32,
    pub signature: WireSig,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct WireRoundCertificate {
    pub height: u64,
    pub round: i64,
    pub cert_type: WireRoundCertificateType,
    pub signatures: Vec<WireRoundSignature>,
}

impl From<&RoundCertificate<HopNetContext>> for WireRoundCertificate {
    fn from(c: &RoundCertificate<HopNetContext>) -> Self {
        WireRoundCertificate {
            height: c.height.0,
            round: round_to_wire(c.round),
            cert_type: match c.cert_type {
                RoundCertificateType::Skip => WireRoundCertificateType::Skip,
                RoundCertificateType::Precommit => WireRoundCertificateType::Precommit,
            },
            signatures: c
                .round_signatures
                .iter()
                .map(|rs| WireRoundSignature {
                    vote_type: rs.vote_type.into(),
                    value_id: match &rs.value_id {
                        NilOrVal::Nil => None,
                        NilOrVal::Val(h) => Some(*h),
                    },
                    address: rs.address.0,
                    signature: sig_to_wire(&rs.signature),
                })
                .collect(),
        }
    }
}

impl TryFrom<&WireRoundCertificate> for RoundCertificate<HopNetContext> {
    type Error = CodecError;

    fn try_from(w: &WireRoundCertificate) -> Result<Self, CodecError> {
        Ok(RoundCertificate {
            height: Height(w.height),
            round: round_from_wire(w.round)?,
            cert_type: match w.cert_type {
                WireRoundCertificateType::Skip => RoundCertificateType::Skip,
                WireRoundCertificateType::Precommit => RoundCertificateType::Precommit,
            },
            round_signatures: w
                .signatures
                .iter()
                .map(|rs| {
                    Ok(RoundSignature {
                        vote_type: rs.vote_type.into(),
                        value_id: match rs.value_id {
                            None => NilOrVal::Nil,
                            Some(h) => NilOrVal::Val(h),
                        },
                        address: Address(rs.address),
                        signature: sig_from_wire(&rs.signature),
                    })
                })
                .collect::<Result<Vec<_>, CodecError>>()?,
        })
    }
}

// ---------------------------------------------------------------------------
// Top-level wire messages

/// Everything consensus puts on the iroh wire.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum WireConsensusMsg {
    Vote(WireVote),
    /// Host-published full value (PartsOnly mode).
    ProposedValue(WireProposedValue),
    /// Liveness: republished vote.
    LivenessVote(WireVote),
    LivenessPolka(WirePolkaCertificate),
    LivenessSkipRound(WireRoundCertificate),
}

impl WireConsensusMsg {
    /// Height carried by the message — the host's lag-detection key.
    pub fn height(&self) -> u64 {
        match self {
            WireConsensusMsg::Vote(v) | WireConsensusMsg::LivenessVote(v) => v.height,
            WireConsensusMsg::ProposedValue(pv) => pv.height,
            WireConsensusMsg::LivenessPolka(c) => c.height,
            WireConsensusMsg::LivenessSkipRound(c) => c.height,
        }
    }
}

/// From an engine publish effect. Engine proposals are intentionally
/// unrepresentable on the wire (PartsOnly): hitting one is a host bug.
impl TryFrom<&SignedConsensusMsg<HopNetContext>> for WireConsensusMsg {
    type Error = CodecError;

    fn try_from(msg: &SignedConsensusMsg<HopNetContext>) -> Result<Self, CodecError> {
        match msg {
            SignedConsensusMsg::Vote(v) => Ok(WireConsensusMsg::Vote(v.into())),
            SignedConsensusMsg::Proposal(_) => Err(CodecError::Invalid(
                "engine Proposal msg on the wire in PartsOnly mode",
            )),
        }
    }
}

// ---------------------------------------------------------------------------
// WAL entries

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum WireTimeoutKind {
    Propose,
    Prevote,
    Precommit,
    Rebroadcast,
    /// Duration in milliseconds.
    FinalizeHeight(u64),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum WireWalEntry {
    Vote(WireVote),
    Proposal(WireProposal),
    Timeout {
        kind: WireTimeoutKind,
        round: i64,
    },
    ProposedValue {
        inner: WireProposedValue,
        valid: bool,
    },
}

impl From<&WalEntry<HopNetContext>> for WireWalEntry {
    fn from(e: &WalEntry<HopNetContext>) -> Self {
        match e {
            WalEntry::ConsensusMsg(SignedConsensusMsg::Vote(v)) => WireWalEntry::Vote(v.into()),
            WalEntry::ConsensusMsg(SignedConsensusMsg::Proposal(p)) => {
                WireWalEntry::Proposal(p.into())
            }
            WalEntry::Timeout(t) => WireWalEntry::Timeout {
                kind: match t.kind {
                    TimeoutKind::Propose => WireTimeoutKind::Propose,
                    TimeoutKind::Prevote => WireTimeoutKind::Prevote,
                    TimeoutKind::Precommit => WireTimeoutKind::Precommit,
                    TimeoutKind::Rebroadcast => WireTimeoutKind::Rebroadcast,
                    TimeoutKind::FinalizeHeight(d) => {
                        WireTimeoutKind::FinalizeHeight(d.as_millis() as u64)
                    }
                },
                round: round_to_wire(t.round),
            },
            WalEntry::ProposedValue(pv) => WireWalEntry::ProposedValue {
                inner: pv.into(),
                valid: pv.validity.to_bool(),
            },
        }
    }
}

impl TryFrom<&WireWalEntry> for WalEntry<HopNetContext> {
    type Error = CodecError;

    fn try_from(w: &WireWalEntry) -> Result<Self, CodecError> {
        Ok(match w {
            WireWalEntry::Vote(v) => {
                WalEntry::ConsensusMsg(SignedConsensusMsg::Vote(v.try_into()?))
            }
            WireWalEntry::Proposal(p) => {
                WalEntry::ConsensusMsg(SignedConsensusMsg::Proposal(p.try_into()?))
            }
            WireWalEntry::Timeout { kind, round } => WalEntry::Timeout(Timeout::new(
                round_from_wire(*round)?,
                match kind {
                    WireTimeoutKind::Propose => TimeoutKind::Propose,
                    WireTimeoutKind::Prevote => TimeoutKind::Prevote,
                    WireTimeoutKind::Precommit => TimeoutKind::Precommit,
                    WireTimeoutKind::Rebroadcast => TimeoutKind::Rebroadcast,
                    WireTimeoutKind::FinalizeHeight(ms) => {
                        TimeoutKind::FinalizeHeight(std::time::Duration::from_millis(*ms))
                    }
                },
            )),
            WireWalEntry::ProposedValue { inner, valid } => WalEntry::ProposedValue(
                inner
                    .clone()
                    .into_proposed_value(Validity::from_bool(*valid))?,
            ),
        })
    }
}

impl WireWalEntry {
    /// Discriminant for the consensus_wal.entry_type column (diagnostics).
    pub fn entry_type(&self) -> i64 {
        match self {
            WireWalEntry::Vote(_) => 0,
            WireWalEntry::Proposal(_) => 1,
            WireWalEntry::Timeout { .. } => 2,
            WireWalEntry::ProposedValue { .. } => 3,
        }
    }
}

// ---------------------------------------------------------------------------
// Byte-level encode/decode

pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, CodecError> {
    bincode::serde::encode_to_vec(value, bincode::config::standard()).map_err(CodecError::Encode)
}

pub fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, CodecError> {
    bincode::serde::decode_from_slice(bytes, bincode::config::standard())
        .map(|(v, _)| v)
        .map_err(CodecError::Decode)
}
