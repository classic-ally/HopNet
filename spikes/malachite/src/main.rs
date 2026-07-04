//! Malachite spike — Stage 0 of the consensus migration.
//!
//! Goals (see plan): drive arc-malachitebft-core-consensus 0.7.0-pre to a
//! 3-node in-memory decide with a hand-rolled Context over ed25519-dalek,
//! fully synchronously (no tokio), in PartsOnly value mode. Findings feed the
//! plan file; this code is disposable.

use std::collections::VecDeque;
use std::fmt;

use ed25519_dalek::{Signer, SigningKey, Verifier, VerifyingKey};
use malachitebft_core_consensus::{
    process, Effect, Input, LocallyProposedValue, Params, ProposedValue, Resumable, Resume,
    SignedConsensusMsg, State,
};
use malachitebft_core_types::{
    Context, Height as HeightTrait, LinearTimeouts, NilOrVal, Round, SignedExtension,
    SignedMessage, SigningScheme, ThresholdParams, Validator as _, ValidatorSet as _, Validity,
    Value as ValueTrait, ValueOrigin, ValuePayload, VoteType, VotingPower,
};

/// ed25519_dalek::Signature lacks Ord (SigningScheme requires it) — newtype it.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Sig(ed25519_dalek::Signature);

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

// ---------------------------------------------------------------- context types

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Height(u64);

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

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Addr(u8);

impl fmt::Display for Addr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "node{}", self.0)
    }
}

impl malachitebft_core_types::Address for Addr {}

/// Toy value: stands in for a HopNet Block. Id = the u64 itself.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct TestValue(u64);

impl fmt::Display for TestValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{}", self.0)
    }
}

impl ValueTrait for TestValue {
    type Id = u64;
    fn id(&self) -> u64 {
        self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TestValidator {
    addr: Addr,
    pubkey: VerifyingKey,
}

impl malachitebft_core_types::Validator<TestCtx> for TestValidator {
    fn address(&self) -> &Addr {
        &self.addr
    }
    fn public_key(&self) -> &VerifyingKey {
        &self.pubkey
    }
    fn voting_power(&self) -> VotingPower {
        1
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TestValidatorSet(Vec<TestValidator>);

impl malachitebft_core_types::ValidatorSet<TestCtx> for TestValidatorSet {
    fn count(&self) -> usize {
        self.0.len()
    }
    fn total_voting_power(&self) -> VotingPower {
        self.0.len() as VotingPower
    }
    fn get_by_address(&self, address: &Addr) -> Option<&TestValidator> {
        self.0.iter().find(|v| &v.addr == address)
    }
    fn get_by_index(&self, index: usize) -> Option<&TestValidator> {
        self.0.get(index)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TestProposal {
    height: Height,
    round: Round,
    value: TestValue,
    pol_round: Round,
    proposer: Addr,
}

impl malachitebft_core_types::Proposal<TestCtx> for TestProposal {
    fn height(&self) -> Height {
        self.height
    }
    fn round(&self) -> Round {
        self.round
    }
    fn value(&self) -> &TestValue {
        &self.value
    }
    fn take_value(self) -> TestValue {
        self.value
    }
    fn pol_round(&self) -> Round {
        self.pol_round
    }
    fn validator_address(&self) -> &Addr {
        &self.proposer
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TestVote {
    typ: VoteType,
    height: Height,
    round: Round,
    value: NilOrVal<u64>,
    voter: Addr,
    extension: Option<SignedExtension<TestCtx>>,
}

impl malachitebft_core_types::Vote<TestCtx> for TestVote {
    fn height(&self) -> Height {
        self.height
    }
    fn round(&self) -> Round {
        self.round
    }
    fn value(&self) -> &NilOrVal<u64> {
        &self.value
    }
    fn take_value(self) -> NilOrVal<u64> {
        self.value
    }
    fn vote_type(&self) -> VoteType {
        self.typ
    }
    fn validator_address(&self) -> &Addr {
        &self.voter
    }
    fn extension(&self) -> Option<&SignedExtension<TestCtx>> {
        self.extension.as_ref()
    }
    fn take_extension(&mut self) -> Option<SignedExtension<TestCtx>> {
        self.extension.take()
    }
    fn extend(self, extension: SignedExtension<TestCtx>) -> Self {
        Self {
            extension: Some(extension),
            ..self
        }
    }
}

/// Unused: PartsOnly mode never streams engine-level parts in this spike
/// (the host ships the full value in its own wire message).
#[derive(Clone, Debug, PartialEq, Eq)]
struct NoPart;

impl malachitebft_core_types::ProposalPart<TestCtx> for NoPart {
    fn is_first(&self) -> bool {
        true
    }
    fn is_last(&self) -> bool {
        true
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Ed25519;

impl SigningScheme for Ed25519 {
    type DecodingError = String;
    type Signature = Sig;
    type PublicKey = VerifyingKey;
    type PrivateKey = SigningKey;

    fn decode_signature(bytes: &[u8]) -> Result<Self::Signature, String> {
        ed25519_dalek::Signature::from_slice(bytes)
            .map(Sig)
            .map_err(|e| e.to_string())
    }
    fn encode_signature(signature: &Self::Signature) -> Vec<u8> {
        signature.0.to_bytes().to_vec()
    }
}

#[derive(Clone, Debug)]
struct TestCtx;

impl Context for TestCtx {
    type Address = Addr;
    type Height = Height;
    type ProposalPart = NoPart;
    type Proposal = TestProposal;
    type Validator = TestValidator;
    type ValidatorSet = TestValidatorSet;
    type Timeouts = LinearTimeouts;
    type Value = TestValue;
    type Vote = TestVote;
    type Extension = ();
    type SigningScheme = Ed25519;

    fn select_proposer<'a>(
        &self,
        validator_set: &'a TestValidatorSet,
        height: Height,
        round: Round,
    ) -> &'a TestValidator {
        // HopNet rotation: advance per height, skip forward per round.
        let n = validator_set.count() as u64;
        let idx = (height.as_u64() + round.as_u32().unwrap_or(0) as u64) % n;
        validator_set.get_by_index(idx as usize).expect("non-empty")
    }

    fn new_proposal(
        &self,
        height: Height,
        round: Round,
        value: TestValue,
        pol_round: Round,
        address: Addr,
    ) -> TestProposal {
        TestProposal {
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
        value_id: NilOrVal<u64>,
        address: Addr,
    ) -> TestVote {
        TestVote {
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
        value_id: NilOrVal<u64>,
        address: Addr,
    ) -> TestVote {
        TestVote {
            typ: VoteType::Precommit,
            height,
            round,
            value: value_id,
            voter: address,
            extension: None,
        }
    }
}

// ---------------------------------------------------------------- signing payloads

fn vote_bytes(v: &TestVote) -> Vec<u8> {
    let mut b = vec![match v.typ {
        VoteType::Prevote => 1u8,
        VoteType::Precommit => 2,
    }];
    b.extend(v.height.0.to_le_bytes());
    b.extend(v.round.as_i64().to_le_bytes());
    match &v.value {
        NilOrVal::Nil => b.push(0),
        NilOrVal::Val(id) => {
            b.push(1);
            b.extend(id.to_le_bytes());
        }
    }
    b.push(v.voter.0);
    b
}

fn proposal_bytes(p: &TestProposal) -> Vec<u8> {
    let mut b = vec![3u8];
    b.extend(p.height.0.to_le_bytes());
    b.extend(p.round.as_i64().to_le_bytes());
    b.extend(p.value.0.to_le_bytes());
    b.extend(p.pol_round.as_i64().to_le_bytes());
    b.push(p.proposer.0);
    b
}

fn consensus_msg_bytes(msg: &malachitebft_core_consensus::ConsensusMsg<TestCtx>) -> Vec<u8> {
    use malachitebft_core_consensus::ConsensusMsg;
    match msg {
        ConsensusMsg::Vote(v) => vote_bytes(v),
        ConsensusMsg::Proposal(p) => proposal_bytes(p),
    }
}

// ---------------------------------------------------------------- in-memory network

/// What travels between nodes.
#[derive(Clone, Debug)]
enum WireMsg {
    /// Engine-published consensus message (votes; proposals never in PartsOnly).
    Consensus(SignedConsensusMsg<TestCtx>),
    /// Host-published full value ("proposal parts" collapsed to one message).
    Value(ProposedValue<TestCtx>),
}

struct Node {
    addr: Addr,
    state: State<TestCtx>,
    signer: SigningKey,
    inbox: VecDeque<WireMsg>,
    outbox: Vec<WireMsg>,
    /// (height, round) GetValue requests to fulfil after process! returns.
    pending_get_value: Vec<(Height, Round)>,
    /// Follow-up inputs (StartHeight after Decide) — drained by the driver.
    pending_inputs: VecDeque<Input<TestCtx>>,
    decided: Vec<(u64, u64)>, // (height, value id)
    wal: Vec<(u64, String)>,  // (height, entry kind) — replay not exercised here
    timeouts_scheduled: usize,
}

fn handle_effect(
    addr: Addr,
    signer: &SigningKey,
    valset: &TestValidatorSet,
    outbox: &mut Vec<WireMsg>,
    pending_get_value: &mut Vec<(Height, Round)>,
    pending_inputs: &mut VecDeque<Input<TestCtx>>,
    decided: &mut Vec<(u64, u64)>,
    wal: &mut Vec<(u64, String)>,
    timeouts_scheduled: &mut usize,
    effect: Effect<TestCtx>,
) -> Result<Resume<TestCtx>, String> {
    match effect {
        Effect::ScheduleTimeout(_t, r) => {
            *timeouts_scheduled += 1;
            Ok(r.resume_with(()))
        }
        Effect::CancelTimeout(_, r) => Ok(r.resume_with(())),
        Effect::CancelAllTimeouts(r) => Ok(r.resume_with(())),
        Effect::StartRound(_h, _rnd, _proposer, _role, r) => Ok(r.resume_with(())),
        Effect::PublishConsensusMsg(msg, r) => {
            outbox.push(WireMsg::Consensus(msg));
            Ok(r.resume_with(()))
        }
        Effect::PublishLivenessMsg(_msg, r) => Ok(r.resume_with(())),
        Effect::RepublishVote(_v, r) => Ok(r.resume_with(())),
        Effect::RepublishRoundCertificate(_c, r) => Ok(r.resume_with(())),
        Effect::GetValue(h, rnd, _timeout, r) => {
            pending_get_value.push((h, rnd));
            Ok(r.resume_with(()))
        }
        Effect::RestreamProposal(_, _, _, _, _, r) => Ok(r.resume_with(())),
        Effect::Decide(cert, _exts, r) => {
            decided.push((cert.height.as_u64(), cert.value_id));
            let next = cert.height.increment();
            pending_inputs.push_back(Input::StartHeight(
                next,
                valset.clone(),
                false,
                None,
            ));
            Ok(r.resume_with(()))
        }
        Effect::Finalize(_cert, _exts, _evidence, r) => Ok(r.resume_with(())),
        Effect::SignVote(vote, r) => {
            let sig = Sig(signer.sign(&vote_bytes(&vote)));
            Ok(r.resume_with(SignedMessage::new(vote, sig)))
        }
        Effect::SignProposal(prop, r) => {
            let sig = Sig(signer.sign(&proposal_bytes(&prop)));
            Ok(r.resume_with(SignedMessage::new(prop, sig)))
        }
        Effect::VerifySignature(signed, pubkey, r) => {
            let bytes = consensus_msg_bytes(&signed.message);
            let ok = pubkey.verify(&bytes, &signed.signature.0).is_ok();
            Ok(r.resume_with(ok))
        }
        Effect::VerifyCommitCertificate(cert, vs, thresholds, r) => {
            // Real impl for the spike: recreate each precommit, verify, count power.
            let mut power: VotingPower = 0;
            for cs in &cert.commit_signatures {
                let Some(val) = vs.get_by_address(&cs.address) else {
                    continue;
                };
                let vote = TestCtx.new_precommit(
                    cert.height,
                    cert.round,
                    NilOrVal::Val(cert.value_id),
                    cs.address,
                );
                if val.pubkey.verify(&vote_bytes(&vote), &cs.signature.0).is_ok() {
                    power += val.voting_power();
                }
            }
            let ok = thresholds
                .quorum
                .is_met(power, vs.total_voting_power());
            if ok {
                Ok(r.resume_with(Ok(())))
            } else {
                Ok(r.resume_with(Err(
                    malachitebft_core_types::CertificateError::NotEnoughVotingPower {
                        signed: power,
                        total: vs.total_voting_power(),
                        expected: thresholds.quorum.min_expected(vs.total_voting_power()),
                    },
                )))
            }
        }
        Effect::VerifyPolkaCertificate(_c, _vs, _t, r) => Ok(r.resume_with(Ok(()))),
        Effect::VerifyRoundCertificate(_c, _vs, _t, r) => Ok(r.resume_with(Ok(()))),
        Effect::WalAppend(h, entry, r) => {
            let kind = match &entry {
                malachitebft_core_consensus::WalEntry::ConsensusMsg(_) => "msg",
                malachitebft_core_consensus::WalEntry::Timeout(_) => "timeout",
                malachitebft_core_consensus::WalEntry::ProposedValue(_) => "value",
            };
            wal.push((h.as_u64(), kind.to_string()));
            Ok(r.resume_with(()))
        }
        Effect::ExtendVote(_h, _r, _id, r) => Ok(r.resume_with(None)),
        Effect::VerifyVoteExtension(_h, _r, _id, _ext, _pk, r) => Ok(r.resume_with(Ok(()))),
        other => {
            let _ = addr;
            Err(format!("unhandled effect: {other:?}"))
        }
    }
}

impl Node {
    fn process(&mut self, input: Input<TestCtx>, valset: &TestValidatorSet) {
        let metrics = ();
        let state = &mut self.state;
        let signer = &self.signer;
        let addr = self.addr;
        let outbox = &mut self.outbox;
        let pgv = &mut self.pending_get_value;
        let pin = &mut self.pending_inputs;
        let decided = &mut self.decided;
        let wal = &mut self.wal;
        let ts = &mut self.timeouts_scheduled;

        let result: Result<(), malachitebft_core_consensus::Error<TestCtx>> = process!(
            input: input,
            state: state,
            metrics: &metrics,
            with: effect => {
                handle_effect(addr, signer, valset, outbox, pgv, pin, decided, wal, ts, effect)
            }
        );
        if let Err(e) = result {
            panic!("node{} process error: {e:?}", addr.0);
        }
    }
}

// ---------------------------------------------------------------- driver

const TARGET_HEIGHT: u64 = 5;

fn main() {
    let keys: Vec<SigningKey> = (0..3u8)
        .map(|i| SigningKey::from_bytes(&[i + 1; 32]))
        .collect();
    let valset = TestValidatorSet(
        keys.iter()
            .enumerate()
            .map(|(i, k)| TestValidator {
                addr: Addr(i as u8),
                pubkey: k.verifying_key(),
            })
            .collect(),
    );

    let mut nodes: Vec<Node> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| {
            let addr = Addr(i as u8);
            let params = Params {
                address: addr,
                threshold_params: ThresholdParams::default(),
                value_payload: ValuePayload::PartsOnly,
                enabled: true,
            };
            Node {
                addr,
                state: State::new(TestCtx, Height::INITIAL, valset.clone(), params, 128),
                signer: k.clone(),
                inbox: VecDeque::new(),
                outbox: Vec::new(),
                pending_get_value: Vec::new(),
                pending_inputs: VecDeque::new(),
                decided: Vec::new(),
                wal: Vec::new(),
                timeouts_scheduled: 0,
            }
        })
        .collect();

    // Kick off height 1 everywhere.
    for n in nodes.iter_mut() {
        n.process(
            Input::StartHeight(Height::INITIAL, valset.clone(), false, None),
            &valset,
        );
    }

    let mut steps = 0usize;
    loop {
        steps += 1;
        if steps > 10_000 {
            panic!("did not converge");
        }

        let mut progressed = false;

        // 1. Fulfil GetValue requests (proposer builds + ships the value).
        for i in 0..nodes.len() {
            let reqs: Vec<_> = nodes[i].pending_get_value.drain(..).collect();
            for (h, rnd) in reqs {
                progressed = true;
                let value = TestValue(h.as_u64() * 100 + i as u64);
                let proposed = ProposedValue {
                    height: h,
                    round: rnd,
                    valid_round: Round::Nil,
                    proposer: nodes[i].addr,
                    value,
                    validity: Validity::Valid,
                };
                // Host ships the full value to peers (PartsOnly wire msg)...
                nodes[i].outbox.push(WireMsg::Value(proposed));
                // ...and feeds its own engine the locally-proposed value.
                nodes[i].process(
                    Input::Propose(LocallyProposedValue::new(h, rnd, value)),
                    &valset,
                );
            }
        }

        // 2. Deliver outboxes (broadcast to all other nodes).
        let mut deliveries: Vec<(usize, WireMsg)> = Vec::new();
        let n_nodes = nodes.len();
        for i in 0..n_nodes {
            let outbox = std::mem::take(&mut nodes[i].outbox);
            for msg in outbox {
                for j in 0..n_nodes {
                    if j != i {
                        deliveries.push((j, msg.clone()));
                    }
                }
            }
        }
        for (j, msg) in deliveries {
            progressed = true;
            nodes[j].inbox.push_back(msg);
        }

        // 3. Each node consumes one inbox message.
        for i in 0..nodes.len() {
            if let Some(msg) = nodes[i].inbox.pop_front() {
                progressed = true;
                let input = match msg {
                    WireMsg::Consensus(SignedConsensusMsg::Vote(v)) => Input::Vote(v),
                    WireMsg::Consensus(SignedConsensusMsg::Proposal(p)) => Input::Proposal(p),
                    WireMsg::Value(pv) => {
                        // Receiving host validates here (Rule-8 slot) — toy: Valid.
                        Input::ProposedValue(pv, ValueOrigin::Consensus)
                    }
                };
                nodes[i].process(input, &valset);
            }
        }

        // 4. Drain follow-up inputs (StartHeight after decide).
        for i in 0..nodes.len() {
            while let Some(input) = nodes[i].pending_inputs.pop_front() {
                progressed = true;
                nodes[i].process(input, &valset);
            }
        }

        if nodes
            .iter()
            .all(|n| n.decided.len() as u64 >= TARGET_HEIGHT)
        {
            break;
        }
        if !progressed {
            let counts: Vec<usize> = nodes.iter().map(|n| n.decided.len()).collect();
            let heights: Vec<Height> = nodes.iter().map(|n| n.state.height()).collect();
            panic!("stalled at step {steps}: decided={counts:?} heights={heights:?}");
        }
    }

    // Agreement check.
    for h in 0..TARGET_HEIGHT as usize {
        let v0 = nodes[0].decided[h];
        for n in &nodes[1..] {
            assert_eq!(n.decided[h], v0, "divergence at height {}", h + 1);
        }
    }

    println!("steps: {steps}");
    for n in &nodes {
        println!(
            "node{}: decided={:?} wal_entries={} timeouts_scheduled={}",
            n.addr.0,
            n.decided,
            n.wal.len(),
            n.timeouts_scheduled
        );
    }
    println!("AGREEMENT OK — {TARGET_HEIGHT} heights, 3 nodes, PartsOnly, sync-driven");
}
