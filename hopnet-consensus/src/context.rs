//! Malachite `Context` implementation over HopNet types.
//!
//! Determinism contract: every method here must be a pure function of its
//! arguments — all nodes evaluate them independently and must agree.
//! `select_proposer` reproduces the bespoke engine's rotation semantics
//! (leader advances every decided block, and skips forward on every timed-out
//! round): `validators[(height + round) % n]`.

use std::fmt;

use malachitebft_core_types::{
    Context, Height as HeightTrait, LinearTimeouts, NilOrVal, Round, SignedExtension, VoteType,
    VotingPower,
};

use crate::signing::Ed25519Scheme;
use crate::types::{Blake3Hash, Block, PubKey};

// ---------------------------------------------------------------------------
// Height

/// Consensus height. Stored in SQLite as i64 via the lossless bit-cast
/// mapping (`hopnet_common::height`) — full u64 range roundtrips;
/// `from_db`/`as_db` never panic.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Height(pub u64);

impl Height {
    /// The genesis / not-yet-produced height (0). Mirrors the `Height` trait
    /// const so callers need not import the trait.
    pub const ZERO: Self = Height(0);
    /// The first consensus height (1).
    pub const INITIAL: Self = Height(1);

    pub fn from_db(v: i64) -> Self {
        Height(hopnet_common::height::height_from_db(v))
    }

    pub fn as_db(&self) -> i64 {
        hopnet_common::height::height_to_db(self.0)
    }
}

impl fmt::Display for Height {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl HeightTrait for Height {
    const ZERO: Self = Height(0);
    const INITIAL: Self = Height(1);

    fn increment_by(&self, n: u64) -> Self {
        Height(self.0 + n)
    }

    fn decrement_by(&self, n: u64) -> Option<Self> {
        self.0.checked_sub(n).map(Height)
    }

    fn as_u64(&self) -> u64 {
        self.0
    }
}

// ---------------------------------------------------------------------------
// Address / Validator / ValidatorSet

/// Validator identity = HopNet node_id. The pubkey lives on `Validator`.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Address(pub i32);

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "node{}", self.0)
    }
}

impl malachitebft_core_types::Address for Address {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Validator {
    pub address: Address,
    pub public_key: PubKey,
    /// Uniform today (every validator weighs 1); the sort and threshold code
    /// handle non-uniform power so weighted meshes stay possible.
    pub voting_power: VotingPower,
}

impl Validator {
    pub fn new(node_id: i32, public_key: PubKey) -> Self {
        Self {
            address: Address(node_id),
            public_key,
            voting_power: 1,
        }
    }
}

impl malachitebft_core_types::Validator<HopNetContext> for Validator {
    fn address(&self) -> &Address {
        &self.address
    }

    fn public_key(&self) -> &PubKey {
        &self.public_key
    }

    fn voting_power(&self) -> VotingPower {
        self.voting_power
    }
}

/// Validator set for one height. Construction sorts (voting power desc, then
/// address asc) and rejects duplicate addresses, so any insertion order yields
/// the same set on every node. With uniform power this degenerates to
/// node_id-asc — the bespoke engine's `ROW_NUMBER() OVER (ORDER BY node_id)`
/// order, preserved exactly.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HopNetValidatorSet(Vec<Validator>);

impl HopNetValidatorSet {
    /// Sorts and dedups; panics on duplicate addresses (a duplicate validator
    /// row is DB corruption, not a runtime condition).
    pub fn new(mut validators: Vec<Validator>) -> Self {
        assert!(!validators.is_empty(), "validator set must be non-empty");
        validators.sort_by(|a, b| {
            b.voting_power
                .cmp(&a.voting_power)
                .then_with(|| a.address.cmp(&b.address))
        });
        for pair in validators.windows(2) {
            assert!(
                pair[0].address != pair[1].address,
                "duplicate validator address {}",
                pair[0].address
            );
        }
        Self(validators)
    }

    pub fn validators(&self) -> &[Validator] {
        &self.0
    }
}

impl malachitebft_core_types::ValidatorSet<HopNetContext> for HopNetValidatorSet {
    fn count(&self) -> usize {
        self.0.len()
    }

    fn total_voting_power(&self) -> VotingPower {
        self.0.iter().map(|v| v.voting_power).sum()
    }

    fn get_by_address(&self, address: &Address) -> Option<&Validator> {
        self.0.iter().find(|v| &v.address == address)
    }

    fn get_by_index(&self, index: usize) -> Option<&Validator> {
        self.0.get(index)
    }
}

// ---------------------------------------------------------------------------
// Proposal / Vote

/// Engine-internal proposal. In PartsOnly mode this never crosses the wire —
/// the host's wire message carries the block; the engine synthesizes this
/// from the `ProposedValue` the host feeds it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsensusProposal {
    pub height: Height,
    pub round: Round,
    pub value: Block,
    pub pol_round: Round,
    pub proposer: Address,
}

impl malachitebft_core_types::Proposal<HopNetContext> for ConsensusProposal {
    fn height(&self) -> Height {
        self.height
    }

    fn round(&self) -> Round {
        self.round
    }

    fn value(&self) -> &Block {
        &self.value
    }

    fn take_value(self) -> Block {
        self.value
    }

    fn pol_round(&self) -> Round {
        self.pol_round
    }

    fn validator_address(&self) -> &Address {
        &self.proposer
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsensusVote {
    pub typ: VoteType,
    pub height: Height,
    pub round: Round,
    pub value: NilOrVal<Blake3Hash>,
    pub voter: Address,
    /// Vote extensions are disabled (Extension = ()); present to satisfy the
    /// Vote trait.
    pub extension: Option<SignedExtension<HopNetContext>>,
}

impl malachitebft_core_types::Vote<HopNetContext> for ConsensusVote {
    fn height(&self) -> Height {
        self.height
    }

    fn round(&self) -> Round {
        self.round
    }

    fn value(&self) -> &NilOrVal<Blake3Hash> {
        &self.value
    }

    fn take_value(self) -> NilOrVal<Blake3Hash> {
        self.value
    }

    fn vote_type(&self) -> VoteType {
        self.typ
    }

    fn validator_address(&self) -> &Address {
        &self.voter
    }

    fn extension(&self) -> Option<&SignedExtension<HopNetContext>> {
        self.extension.as_ref()
    }

    fn take_extension(&mut self) -> Option<SignedExtension<HopNetContext>> {
        self.extension.take()
    }

    fn extend(self, extension: SignedExtension<HopNetContext>) -> Self {
        Self {
            extension: Some(extension),
            ..self
        }
    }
}

/// PartsOnly mode: engine-level proposal parts are never streamed (the host
/// ships whole blocks in its own wire message), so this is a unit placeholder.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NoProposalPart;

impl malachitebft_core_types::ProposalPart<HopNetContext> for NoProposalPart {
    fn is_first(&self) -> bool {
        true
    }

    fn is_last(&self) -> bool {
        true
    }
}

// ---------------------------------------------------------------------------
// Context

#[derive(Clone, Debug, Default)]
pub struct HopNetContext;

impl Context for HopNetContext {
    type Address = Address;
    type Height = Height;
    type ProposalPart = NoProposalPart;
    type Proposal = ConsensusProposal;
    type Validator = Validator;
    type ValidatorSet = HopNetValidatorSet;
    type Timeouts = LinearTimeouts;
    type Value = Block;
    type Vote = ConsensusVote;
    type Extension = ();
    type SigningScheme = Ed25519Scheme;

    fn select_proposer<'a>(
        &self,
        validator_set: &'a HopNetValidatorSet,
        height: Height,
        round: Round,
    ) -> &'a Validator {
        let n = validator_set.0.len() as u64;
        let round = u64::from(round.as_u32().expect("proposer selection at Nil round"));
        let idx = (height.as_u64() + round) % n;
        validator_set.0.get(idx as usize).expect("index < n")
    }

    fn new_proposal(
        &self,
        height: Height,
        round: Round,
        value: Block,
        pol_round: Round,
        address: Address,
    ) -> ConsensusProposal {
        ConsensusProposal {
            height,
            round,
            value,
            pol_round,
            proposer: address,
        }
    }

    fn new_prevote(
        &self,
        height: Height,
        round: Round,
        value_id: NilOrVal<Blake3Hash>,
        address: Address,
    ) -> ConsensusVote {
        ConsensusVote {
            typ: VoteType::Prevote,
            height,
            round,
            value: value_id,
            voter: address,
            extension: None,
        }
    }

    fn new_precommit(
        &self,
        height: Height,
        round: Round,
        value_id: NilOrVal<Blake3Hash>,
        address: Address,
    ) -> ConsensusVote {
        ConsensusVote {
            typ: VoteType::Precommit,
            height,
            round,
            value: value_id,
            voter: address,
            extension: None,
        }
    }
}
