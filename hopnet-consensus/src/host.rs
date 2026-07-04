//! Sans-io consensus host: drives the Malachite engine and routes every
//! effect to the four trait seams (Application, Storage, Gossip, Timers).
//!
//! This type performs NO I/O and NO awaiting of its own — it is a pure
//! step function over the traits. Production wraps it in a tokio shell
//! (DelayQueue timers, spawned value builds); the deterministic simulator
//! wraps it in a virtual-clock loop. The code the fuzzer exercises is the
//! code that ships — that equivalence is the whole point of keeping this
//! layer sans-io.
//!
//! WAL / crash recovery (contract confirmed against the reference engine):
//! `start_height(h, is_restart=false)` fetches persisted WAL entries for `h`
//! and, after feeding `StartHeight`, replays them as ordinary inputs — this
//! is what prevents equivocation across a crash. `is_restart=true` RESETS
//! (truncates) the WAL and skips replay, used for height jumps after sync.
//! During replay the phase is `Recovering`: WAL appends are suppressed
//! (replayed entries must not re-append) and value-build requests are
//! dropped (the replayed value already provides them).

use std::collections::{BTreeMap, VecDeque};

use malachitebft_core_consensus::{
    process, ConsensusMsg, Effect, Input, LivenessMsg, LocallyProposedValue, Params, ProposedValue,
    Resumable, SignedConsensusMsg, State,
};
use malachitebft_core_types::{Round, SignedMessage, Timeout, Validity, ValueOrigin};

use crate::codec::{self, WireCommitCertificate, WireConsensusMsg, WireWalEntry};
use crate::context::{Address, ConsensusVote, Height, HopNetContext, HopNetValidatorSet};
use crate::signing;
use crate::traits::{Application, Gossip, Storage, Timers};
use crate::types::{Blake3Hash, Block, PrivKey};
use crate::verify;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum Phase {
    Running,
    Recovering,
}

/// Signals the host surfaces for the shell to act on between steps.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HostOutput {
    /// The engine wants this node (the proposer) to build a value for
    /// (height, round). The shell must eventually call `propose`.
    NeedValue { height: Height, round: Round },
    /// A value was decided and committed at `height`.
    Decided { height: Height },
}

#[derive(Debug)]
pub enum HostError<E> {
    Engine(malachitebft_core_consensus::Error<HopNetContext>),
    Storage(E),
    Codec(codec::CodecError),
    /// A decided value_id had no matching stored block — a host bug.
    MissingBlock(Height),
}

/// The consensus host. Generic over the four seams so the simulator and
/// production share this exact driver.
pub struct HostCore<A, S, G, T> {
    state: State<HopNetContext>,
    chain_id: Blake3Hash,
    signer: PrivKey,
    my_addr: Address,

    app: A,
    storage: S,
    gossip: G,
    timers: T,

    phase: Phase,
    /// Monotonic WAL sequence within the current height.
    wal_seq: u64,
    /// Blocks seen (proposed locally or received) keyed by (height, hash),
    /// so `Decide` (which carries only value_id) can recover the full block.
    blocks: BTreeMap<(Height, Blake3Hash), Block>,
    /// Inputs queued from inside an effect handler (e.g. StartHeight after
    /// Decide); drained after `process!` returns — never re-entrant.
    pending_inputs: VecDeque<Input<HopNetContext>>,
    outputs: Vec<HostOutput>,
    last_decided: Option<Height>,
}

impl<A, S, G, T> HostCore<A, S, G, T>
where
    S: Storage,
    A: Application<S>,
    G: Gossip,
    T: Timers,
{
    pub fn new(
        chain_id: Blake3Hash,
        signer: PrivKey,
        my_addr: Address,
        params: Params<HopNetContext>,
        initial_height: Height,
        initial_valset: HopNetValidatorSet,
        app: A,
        storage: S,
        gossip: G,
        timers: T,
    ) -> Self {
        let state = State::new(
            HopNetContext,
            initial_height,
            initial_valset,
            params,
            /* queue_capacity */ 128,
        );
        Self {
            state,
            chain_id,
            signer,
            my_addr,
            app,
            storage,
            gossip,
            timers,
            phase: Phase::Running,
            wal_seq: 0,
            blocks: BTreeMap::new(),
            pending_inputs: VecDeque::new(),
            outputs: Vec::new(),
            last_decided: None,
        }
    }

    pub fn address(&self) -> Address {
        self.my_addr
    }

    pub fn height(&self) -> Height {
        self.state.height()
    }

    /// Drain the signals accumulated since the last call.
    pub fn take_outputs(&mut self) -> Vec<HostOutput> {
        std::mem::take(&mut self.outputs)
    }

    // ----------------------------------------------------------------- entry

    /// Begin a height. On a fresh height `is_restart=false` and the WAL is
    /// empty; on crash recovery it fetches and replays persisted entries;
    /// with `is_restart=true` it resets the WAL (height jump after sync).
    pub fn start_height(
        &mut self,
        height: Height,
        is_restart: bool,
    ) -> Result<(), HostError<S::Error>> {
        self.wal_seq = 0;
        let valset = self.app.validator_set(height);

        let entries: Vec<WireWalEntry> = if is_restart {
            self.storage.wal_reset().map_err(HostError::Storage)?;
            Vec::new()
        } else {
            self.storage.wal_fetch(height).map_err(HostError::Storage)?
        };

        if !entries.is_empty() {
            self.phase = Phase::Recovering;
        }

        self.feed(Input::StartHeight(height, valset, is_restart, None))?;

        // Replay persisted entries as ordinary inputs, in order.
        for wire in &entries {
            let entry = recovered_input(wire).map_err(HostError::Codec)?;
            match entry {
                RecoveredInput::Vote(v) => self.feed(Input::Vote(v))?,
                RecoveredInput::Proposal(p) => self.feed(Input::Proposal(p))?,
                RecoveredInput::Timeout(t) => self.feed(Input::TimeoutElapsed(t))?,
                RecoveredInput::ProposedValue(pv) => {
                    self.feed(Input::ProposedValue(pv, ValueOrigin::Consensus))?
                }
            }
        }

        // Recovery-time value-build requests are moot: the replay supplied the
        // value. Drop them so the shell doesn't build a stale proposal.
        if self.phase == Phase::Recovering {
            self.outputs
                .retain(|o| !matches!(o, HostOutput::NeedValue { .. }));
        }
        self.phase = Phase::Running;
        Ok(())
    }

    /// Feed an inbound wire message from a peer.
    pub fn on_wire(&mut self, msg: WireConsensusMsg) -> Result<(), HostError<S::Error>> {
        match msg {
            WireConsensusMsg::Vote(w) | WireConsensusMsg::LivenessVote(w) => {
                let sv = (&w).try_into().map_err(HostError::Codec)?;
                self.feed(Input::Vote(sv))
            }
            WireConsensusMsg::ProposedValue(w) => {
                let block = w.block.clone();
                let height = Height(w.height);
                // Rule-8: the receiver decides validity in a rollback tx.
                // Disjoint borrows so app and storage can be used together.
                let validity = {
                    let Self { app, storage, .. } = self;
                    storage
                        .with_rollback(|tx| app.validate_block(height, &block, tx))
                        .map_err(HostError::Storage)?
                };
                let pv = w.into_proposed_value(validity).map_err(HostError::Codec)?;
                self.remember_block(pv.height, pv.value.clone());
                self.feed(Input::ProposedValue(pv, ValueOrigin::Consensus))
            }
            WireConsensusMsg::LivenessPolka(w) => {
                let cert = (&w).try_into().map_err(HostError::Codec)?;
                self.feed(Input::PolkaCertificate(cert))
            }
            WireConsensusMsg::LivenessSkipRound(w) => {
                let cert = (&w).try_into().map_err(HostError::Codec)?;
                self.feed(Input::RoundCertificate(cert))
            }
        }
    }

    /// The proposer's value, built by the shell in response to `NeedValue`.
    pub fn propose(
        &mut self,
        height: Height,
        round: Round,
        block: Block,
    ) -> Result<(), HostError<S::Error>> {
        // Ship the full value to peers ourselves (PartsOnly: the engine's
        // Proposal message never hits the wire).
        let pv = ProposedValue {
            height,
            round,
            valid_round: Round::Nil,
            proposer: self.my_addr,
            value: block.clone(),
            validity: Validity::Valid,
        };
        let wire = WireConsensusMsg::ProposedValue((&pv).into());
        self.gossip.broadcast(&wire);

        self.remember_block(height, block.clone());
        self.feed(Input::Propose(LocallyProposedValue::new(
            height, round, block,
        )))
    }

    /// A scheduled timeout fired (delivered by the shell's clock).
    pub fn on_timeout(&mut self, timeout: Timeout) -> Result<(), HostError<S::Error>> {
        self.feed(Input::TimeoutElapsed(timeout))
    }

    // --------------------------------------------------------------- driving

    fn remember_block(&mut self, height: Height, block: Block) {
        self.blocks.insert((height, block.block_hash), block);
    }

    fn feed(&mut self, input: Input<HopNetContext>) -> Result<(), HostError<S::Error>> {
        self.pending_inputs.push_back(input);
        while let Some(input) = self.pending_inputs.pop_front() {
            self.drive_once(input)?;
        }
        Ok(())
    }

    fn drive_once(&mut self, input: Input<HopNetContext>) -> Result<(), HostError<S::Error>> {
        // Disjoint field borrows so the effect handler can touch app/storage/
        // gossip/timers while the macro holds `&mut state`.
        let Self {
            state,
            chain_id,
            signer,
            my_addr: _,
            app,
            storage,
            gossip,
            timers,
            phase,
            wal_seq,
            blocks,
            pending_inputs,
            outputs,
            last_decided,
        } = self;

        let metrics = ();
        let result: Result<(), malachitebft_core_consensus::Error<HopNetContext>> = process!(
            input: input,
            state: state,
            metrics: &metrics,
            with: effect => handle_effect(HandlerCtx {
                chain_id,
                signer,
                app,
                storage,
                gossip,
                timers,
                phase: *phase,
                wal_seq,
                blocks,
                pending_inputs,
                outputs,
                last_decided,
            }, effect)
        );
        result.map_err(HostError::Engine)
    }
}

/// Everything the effect handler needs, as disjoint borrows.
struct HandlerCtx<'a, A, S, G, T> {
    chain_id: &'a Blake3Hash,
    signer: &'a PrivKey,
    app: &'a mut A,
    storage: &'a mut S,
    gossip: &'a mut G,
    timers: &'a mut T,
    phase: Phase,
    wal_seq: &'a mut u64,
    blocks: &'a mut BTreeMap<(Height, Blake3Hash), Block>,
    pending_inputs: &'a mut VecDeque<Input<HopNetContext>>,
    outputs: &'a mut Vec<HostOutput>,
    last_decided: &'a mut Option<Height>,
}

fn handle_effect<A, S, G, T>(
    ctx: HandlerCtx<'_, A, S, G, T>,
    effect: Effect<HopNetContext>,
) -> Result<malachitebft_core_consensus::Resume<HopNetContext>, HostError<S::Error>>
where
    S: Storage,
    A: Application<S>,
    G: Gossip,
    T: Timers,
{
    match effect {
        Effect::ScheduleTimeout(t, r) => {
            ctx.timers.schedule(t);
            Ok(r.resume_with(()))
        }
        Effect::CancelTimeout(t, r) => {
            ctx.timers.cancel(&t);
            Ok(r.resume_with(()))
        }
        Effect::CancelAllTimeouts(r) => {
            ctx.timers.cancel_all();
            Ok(r.resume_with(()))
        }
        Effect::StartRound(_h, _round, _proposer, _role, r) => Ok(r.resume_with(())),

        Effect::PublishConsensusMsg(msg, r) => {
            // PartsOnly: only votes cross the wire; engine proposals don't.
            if let SignedConsensusMsg::Vote(ref v) = msg {
                ctx.gossip.broadcast(&WireConsensusMsg::Vote(v.into()));
            }
            Ok(r.resume_with(()))
        }
        Effect::PublishLivenessMsg(msg, r) => {
            let wire = match msg {
                LivenessMsg::Vote(v) => Some(WireConsensusMsg::LivenessVote((&v).into())),
                LivenessMsg::PolkaCertificate(c) => {
                    Some(WireConsensusMsg::LivenessPolka((&c).into()))
                }
                LivenessMsg::SkipRoundCertificate(c) => {
                    Some(WireConsensusMsg::LivenessSkipRound((&c).into()))
                }
            };
            if let Some(w) = wire {
                ctx.gossip.broadcast(&w);
            }
            Ok(r.resume_with(()))
        }
        Effect::RepublishVote(v, r) => {
            ctx.gossip
                .broadcast(&WireConsensusMsg::LivenessVote((&v).into()));
            Ok(r.resume_with(()))
        }
        Effect::RepublishRoundCertificate(c, r) => {
            ctx.gossip
                .broadcast(&WireConsensusMsg::LivenessSkipRound((&c).into()));
            Ok(r.resume_with(()))
        }
        Effect::RestreamProposal(_, _, _, _, _, r) => Ok(r.resume_with(())),

        Effect::GetValue(height, round, _timeout, r) => {
            // Suppress during recovery — the replay already supplies the value.
            if ctx.phase == Phase::Running {
                ctx.outputs.push(HostOutput::NeedValue { height, round });
            }
            Ok(r.resume_with(()))
        }

        Effect::SignVote(vote, r) => {
            let sig = signing::sign_vote(ctx.chain_id, ctx.signer, &vote);
            Ok(r.resume_with(SignedMessage::new(vote, sig)))
        }
        Effect::SignProposal(proposal, r) => {
            let sig = signing::sign_proposal(ctx.chain_id, ctx.signer, &proposal);
            Ok(r.resume_with(SignedMessage::new(proposal, sig)))
        }
        Effect::VerifySignature(signed, pubkey, r) => {
            let ok = verify_consensus_signature(ctx.chain_id, &signed, &pubkey);
            Ok(r.resume_with(ok))
        }
        Effect::VerifyCommitCertificate(cert, valset, thresholds, r) => {
            let res = verify::verify_commit_certificate(ctx.chain_id, &cert, &valset, thresholds);
            Ok(r.resume_with(res))
        }
        Effect::VerifyPolkaCertificate(cert, valset, thresholds, r) => {
            let res = verify::verify_polka_certificate(ctx.chain_id, &cert, &valset, thresholds);
            Ok(r.resume_with(res))
        }
        Effect::VerifyRoundCertificate(cert, valset, thresholds, r) => {
            let res = verify::verify_round_certificate(ctx.chain_id, &cert, &valset, thresholds);
            Ok(r.resume_with(res))
        }

        Effect::WalAppend(height, entry, r) => {
            if ctx.phase != Phase::Recovering {
                let wire = WireWalEntry::from(&entry);
                let seq = *ctx.wal_seq;
                *ctx.wal_seq += 1;
                ctx.storage
                    .wal_append(height, seq, &wire)
                    .map_err(HostError::Storage)?;
            }
            Ok(r.resume_with(()))
        }

        Effect::Decide(cert, _extensions, r) => {
            let height = cert.height;
            let block = ctx
                .blocks
                .get(&(height, cert.value_id))
                .cloned()
                .ok_or(HostError::MissingBlock(height))?;
            let wire_cert = WireCommitCertificate::from(&cert);

            let app = &mut *ctx.app;
            ctx.storage
                .decide_atomically(|tx| {
                    app.apply_block(height, &block, tx)
                        .map_err(S::apply_error)?;
                    S::store_decided_tx(tx, &block, &wire_cert)?;
                    S::truncate_wal_tx(tx, height)?;
                    S::set_last_decided_tx(tx, height)?;
                    Ok(())
                })
                .map_err(HostError::Storage)?;

            *ctx.last_decided = Some(height);
            ctx.app.on_decided(height, &block, &wire_cert);
            ctx.outputs.push(HostOutput::Decided { height });

            // Advance to the next height (queued, not re-entrant).
            let next = malachitebft_core_types::Height::increment(&height);
            let valset = ctx.app.validator_set(next);
            ctx.pending_inputs
                .push_back(Input::StartHeight(next, valset, false, None));
            *ctx.wal_seq = 0;

            Ok(r.resume_with(()))
        }
        Effect::Finalize(_cert, _extensions, _evidence, r) => Ok(r.resume_with(())),

        Effect::ExtendVote(_h, _round, _vid, r) => Ok(r.resume_with(None)),
        Effect::VerifyVoteExtension(_h, _round, _vid, _ext, _pk, r) => Ok(r.resume_with(Ok(()))),

        // Sync value plumbing arrives at Stage 4 (custom decided-value sync).
        // Until then these effects are only emitted if the shell feeds a sync
        // input, which it doesn't yet — acknowledge and move on.
        Effect::ValidSyncValue(_response, _from, r) => Ok(r.resume_with(())),
        Effect::InvalidSyncValue(_peer, _height, _err, r) => Ok(r.resume_with(())),
    }
}

/// Verify the signature on an incoming consensus message using our canonical
/// signing payloads.
fn verify_consensus_signature(
    chain_id: &Blake3Hash,
    signed: &SignedMessage<HopNetContext, ConsensusMsg<HopNetContext>>,
    pubkey: &crate::types::PubKey,
) -> bool {
    let bytes = signing::consensus_msg_sign_bytes(chain_id, &signed.message);
    use ed25519_dalek::Verifier;
    pubkey.verify(&bytes, &signed.signature.0).is_ok()
}

enum RecoveredInput {
    Vote(SignedMessage<HopNetContext, ConsensusVote>),
    Proposal(SignedMessage<HopNetContext, crate::context::ConsensusProposal>),
    Timeout(Timeout),
    ProposedValue(ProposedValue<HopNetContext>),
}

fn recovered_input(wire: &WireWalEntry) -> Result<RecoveredInput, codec::CodecError> {
    use malachitebft_core_consensus::WalEntry;
    let entry: WalEntry<HopNetContext> = wire.try_into()?;
    Ok(match entry {
        WalEntry::ConsensusMsg(SignedConsensusMsg::Vote(v)) => RecoveredInput::Vote(v),
        WalEntry::ConsensusMsg(SignedConsensusMsg::Proposal(p)) => RecoveredInput::Proposal(p),
        WalEntry::Timeout(t) => RecoveredInput::Timeout(t),
        WalEntry::ProposedValue(pv) => RecoveredInput::ProposedValue(pv),
    })
}
