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
    process, ConsensusMsg, Effect, Input, LivenessMsg, LocallyProposedValue, Params, PeerId,
    ProposedValue, Resumable, SignedConsensusMsg, State,
};
use malachitebft_core_types::{
    CommitCertificate, Round, SignedMessage, Timeout, Validity, ValueOrigin, ValueResponse,
};

use crate::codec::{self, WireCommitCertificate, WireConsensusMsg, WireWalEntry};
use crate::context::{Address, ConsensusVote, Height, HopNetContext, HopNetValidatorSet};
use crate::signing;
use crate::traits::{Application, Gossip, Storage, Timers, ValidationOrigin};
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
    /// The engine entered (height, round) with `proposer`. Lets the embedding
    /// application route transaction forwarding to the current proposer.
    RoundStarted {
        height: Height,
        round: Round,
        proposer: Address,
    },
    /// A value was decided and committed at `height`.
    Decided { height: Height },
    /// A synced value at `height` did not apply (bad certificate, undecodable
    /// value, or a validity mismatch). The sync client should try another
    /// peer for this height.
    SyncInvalid { peer: PeerId, height: Height },
}

/// PeerId for a HopNet node: an identity multihash over the node_id bytes.
/// Deterministic and reversible — no libp2p key material involved.
pub fn peer_id_for_node(node_id: i32) -> PeerId {
    // Multihash binary layout: varint(code=0x00 identity), varint(len=4), digest.
    let d = node_id.to_le_bytes();
    PeerId::from_bytes(&[0x00, 0x04, d[0], d[1], d[2], d[3]])
        .expect("identity multihash over 4 bytes is always valid")
}

/// Recover the node_id from a `peer_id_for_node`-shaped PeerId.
pub fn node_id_from_peer(peer: &PeerId) -> Option<i32> {
    let bytes = peer.to_bytes();
    match bytes.as_slice() {
        [0x00, 0x04, d0, d1, d2, d3] => Some(i32::from_le_bytes([*d0, *d1, *d2, *d3])),
        _ => None,
    }
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
    /// On-demand heights: when true, the next height is NOT started after a
    /// decide — it is deferred until `resume_height` (local work arrived or a
    /// peer message at the pending height proves the mesh is active). Keeps an
    /// idle mesh fully quiescent: no timers, no empty blocks, no round churn.
    on_demand: bool,
    /// The deferred height waiting for a wake signal (on-demand mode only).
    deferred_start: Option<Height>,
    /// Wire proposals for FUTURE heights, held UNVALIDATED until the engine
    /// reaches their height. Rule-8 validation is state-dependent (parent
    /// linkage, nonce dedup, handler dry-run against the tip) — validating an
    /// ahead-of-tip proposal against today's tip judges a perfectly good
    /// block Invalid, and that verdict poisons the engine's per-height value
    /// record, wedging the height when we get there. Drained by
    /// `drain_stashed_proposals` at every height entry.
    stashed_proposals: BTreeMap<u64, Vec<codec::WireProposedValue>>,
}

/// How far ahead of the current height a wire proposal may be stashed, and
/// the total stash bound. One height of lookahead is the common race (peer
/// decided h while we are mid-h); sync covers real gaps. The count bound
/// caps memory against a flooding peer (blocks can be large).
const PROPOSAL_STASH_AHEAD: u64 = 4;
const PROPOSAL_STASH_MAX: usize = 16;

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
            on_demand: false,
            deferred_start: None,
            stashed_proposals: BTreeMap::new(),
        }
    }

    /// Enable on-demand heights (builder): heights start only when work
    /// exists. See the wake rules on [`Self::resume_height`].
    pub fn on_demand(mut self) -> Self {
        self.on_demand = true;
        self
    }

    pub fn address(&self) -> Address {
        self.my_addr
    }

    pub fn height(&self) -> Height {
        self.state.height()
    }

    /// The height this core is paused before (on-demand mode). `None` when a
    /// height is actively running.
    pub fn paused_at(&self) -> Option<Height> {
        self.deferred_start
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

        self.feed_strict(Input::StartHeight(height, valset, is_restart, None))?;

        // Replay persisted entries as ordinary inputs, in order (strict).
        // Entries that carry a full block must ALSO re-populate the host's
        // blocks map — a later Decide (live or via sync) resolves value_id
        // through it, and the map died with the crashed process.
        for wire in &entries {
            let entry = recovered_input(wire).map_err(HostError::Codec)?;
            match entry {
                RecoveredInput::Vote(v) => self.feed_strict(Input::Vote(v))?,
                RecoveredInput::Proposal(p) => {
                    self.remember_block(p.message.height, p.message.value.clone());
                    self.feed_strict(Input::Proposal(p))?
                }
                RecoveredInput::Timeout(t) => self.feed_strict(Input::TimeoutElapsed(t))?,
                RecoveredInput::ProposedValue(pv) => {
                    self.remember_block(pv.height, pv.value.clone());
                    self.feed_strict(Input::ProposedValue(pv, ValueOrigin::Consensus))?
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
        // A proposal for this height may have arrived while it was deferred
        // (on-demand) or before the previous height finished — validate it
        // NOW, against the tip it belongs to.
        self.drain_stashed_proposals()
    }

    /// Boot entry for on-demand mode. A height with persisted WAL entries was
    /// ACTIVE at crash time — it must start (and replay) immediately; pausing
    /// it would defer votes we already durably cast. An empty WAL means the
    /// height never started anywhere we know of — defer until a wake signal.
    /// Without on-demand mode this is exactly `start_height`.
    ///
    /// Returns whether the height actually started.
    pub fn start_or_defer(&mut self, height: Height) -> Result<bool, HostError<S::Error>> {
        if !self.on_demand {
            self.start_height(height, false)?;
            return Ok(true);
        }
        let entries = self.storage.wal_fetch(height).map_err(HostError::Storage)?;
        if entries.is_empty() {
            self.deferred_start = Some(height);
            Ok(false)
        } else {
            self.start_height(height, false)?;
            Ok(true)
        }
    }

    /// Wake a paused core (on-demand mode). Idempotent — a no-op when no
    /// height is deferred. Wake signals, enforced by the embedding shell:
    /// local work arrived (we want to propose), a peer message at the pending
    /// height arrived (the mesh is active), or a sync value for the pending
    /// height is ready to apply.
    pub fn resume_height(&mut self) -> Result<(), HostError<S::Error>> {
        if let Some(height) = self.deferred_start.take() {
            self.start_height(height, false)?;
        }
        Ok(())
    }

    /// Feed an inbound wire message from a peer. Malformed bytes are dropped;
    /// otherwise the input flows through `feed`, which is itself lenient to
    /// engine errors (see there).
    pub fn on_wire(&mut self, msg: WireConsensusMsg) -> Result<(), HostError<S::Error>> {
        match msg {
            WireConsensusMsg::Vote(w) | WireConsensusMsg::LivenessVote(w) => {
                let Ok(sv) = (&w).try_into() else {
                    return Ok(()); // malformed — drop
                };
                self.feed(Input::Vote(sv))?;
            }
            WireConsensusMsg::ProposedValue(w) => {
                let current = self.height().0;
                if w.height > current {
                    // Rule-8 validation needs the state AT the proposal's
                    // height — hold the proposal and validate when we get
                    // there (drain_stashed_proposals). Beyond the window the
                    // sync client fetches the DECIDED value instead.
                    let stashed: usize = self.stashed_proposals.values().map(Vec::len).sum();
                    if w.height <= current + PROPOSAL_STASH_AHEAD && stashed < PROPOSAL_STASH_MAX
                    {
                        self.stashed_proposals.entry(w.height).or_default().push(w);
                    }
                    return Ok(());
                }
                if w.height == current {
                    self.ingest_proposal(w)?;
                }
                // w.height < current: stale — the height is already decided
                // here; drop without validating (a dry-run against the wrong
                // tip only produces noise).
            }
            WireConsensusMsg::LivenessPolka(w) => {
                let cert = (&w).try_into().map_err(HostError::Codec)?;
                self.feed(Input::PolkaCertificate(cert))?;
            }
            WireConsensusMsg::LivenessSkipRound(w) => {
                let cert = (&w).try_into().map_err(HostError::Codec)?;
                self.feed(Input::RoundCertificate(cert))?;
            }
        }
        // Any input can complete a decide and advance the height (votes most
        // of all) — give stashed proposals for the new height their shot.
        self.drain_stashed_proposals()
    }

    /// Validate a wire proposal AT the current height (Rule-8, rollback tx)
    /// and feed the verdict-carrying ProposedValue to the engine.
    fn ingest_proposal(&mut self, w: codec::WireProposedValue) -> Result<(), HostError<S::Error>> {
        let block = w.block.clone();
        let height = Height(w.height);
        // Disjoint borrows so app and storage can be used together.
        let validity = {
            let Self { app, storage, .. } = self;
            storage
                .with_rollback(|tx| app.validate_block(height, &block, tx, ValidationOrigin::Live))
                .map_err(HostError::Storage)?
        };
        let pv = w.into_proposed_value(validity).map_err(HostError::Codec)?;
        self.remember_block(pv.height, pv.value.clone());
        self.feed(Input::ProposedValue(pv, ValueOrigin::Consensus))
    }

    /// Re-process stashed future proposals whose height has arrived. Loops:
    /// ingesting a proposal can itself complete a decide (buffered votes) and
    /// advance the height again. Entries below the current height are pruned.
    fn drain_stashed_proposals(&mut self) -> Result<(), HostError<S::Error>> {
        loop {
            let current = self.height().0;
            while let Some(&h) = self.stashed_proposals.keys().next() {
                if h < current {
                    self.stashed_proposals.remove(&h);
                } else {
                    break;
                }
            }
            let Some(batch) = self.stashed_proposals.remove(&current) else {
                return Ok(());
            };
            for w in batch {
                // An earlier item can complete the height — the rest of the
                // batch is then stale, not ingestable.
                if w.height == self.height().0 {
                    self.ingest_proposal(w)?;
                }
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

    /// A decided (block, certificate) pair fetched from `peer` by the sync
    /// client. MUST be fed sequentially, lowest height first: the engine
    /// processes a sync certificate only at its CURRENT height (each resulting
    /// Decide advances it, so a contiguous range applies in one loop).
    ///
    /// Flow: `Input::SyncValueResponse` → engine verifies the certificate
    /// (VerifyCommitCertificate, current valset) → `Effect::ValidSyncValue`
    /// → we validate the block (Rule-8 rollback tx) and feed
    /// `Input::ProposedValue(_, Sync)` → sync decision → the SAME atomic
    /// decide path as live consensus.
    pub fn on_sync_value(
        &mut self,
        peer: PeerId,
        block: Block,
        cert: &WireCommitCertificate,
    ) -> Result<(), HostError<S::Error>> {
        // A syncing node ALSO participates in live consensus (it buffers and
        // processes live gossip). At the sync/live boundary both paths can
        // reach the same height: live consensus decides height N while the
        // sync client is still feeding a SyncValue for N. Re-applying a block
        // the engine already decided re-runs the app (nonces now committed →
        // the app flips Valid→Invalid), tripping the engine's "changed its
        // mind" invariant and wedging it. So drop any sync value for a height
        // we've already passed; the engine only advances forward.
        let current = self.height().0;
        if cert.height < current {
            tracing::debug!(
                sync_height = cert.height,
                current,
                "dropping already-decided sync value (live consensus got there first)"
            );
            return Ok(());
        }
        // Structural pre-checks; the sync client also performs these, so a
        // failure here is defensive — drop, don't feed garbage to the engine.
        if block.verify().is_err() || block.block_hash != cert.value_id {
            tracing::warn!(
                height = cert.height,
                "dropping malformed sync value (hash/cert mismatch)"
            );
            return Ok(());
        }
        let cert: CommitCertificate<HopNetContext> = cert.try_into().map_err(HostError::Codec)?;
        let bytes = codec::encode(&block).map_err(HostError::Codec)?;
        self.feed(Input::SyncValueResponse(ValueResponse::new(
            peer,
            bytes::Bytes::from(bytes),
            cert,
        )))
    }

    // --------------------------------------------------------------- driving

    fn remember_block(&mut self, height: Height, block: Block) {
        self.blocks.insert((height, block.block_hash), block);
    }

    /// Feed a live input and drain any queued follow-ups (e.g. StartHeight
    /// after Decide). LENIENT to engine errors: the reference engine logs and
    /// continues on every live vote/proposal/timeout processing error (a lost
    /// polka certificate makes a reproposal transiently unverifiable, etc.);
    /// only WAL replay is strict. Storage errors always propagate — they are
    /// real durability failures, not transient protocol states.
    fn feed(&mut self, input: Input<HopNetContext>) -> Result<(), HostError<S::Error>> {
        self.pending_inputs.push_back(input);
        while let Some(input) = self.pending_inputs.pop_front() {
            match self.drive_once(input) {
                Ok(()) => {}
                // Drop and continue (matching the reference engine), but say so.
                Err(HostError::Engine(e)) => {
                    tracing::warn!("engine error on live input (dropped): {e}");
                }
                Err(HostError::Codec(e)) => {
                    tracing::warn!("codec error on live input (dropped): {e}");
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Strict variant for WAL replay: an engine error replaying our OWN
    /// persisted entries is a genuine corruption/bug, not a transient state.
    fn feed_strict(&mut self, input: Input<HopNetContext>) -> Result<(), HostError<S::Error>> {
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
            on_demand,
            deferred_start,
            stashed_proposals: _,
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
                on_demand: *on_demand,
                deferred_start,
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
    on_demand: bool,
    deferred_start: &'a mut Option<Height>,
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
        Effect::StartRound(height, round, proposer, _role, r) => {
            ctx.outputs.push(HostOutput::RoundStarted {
                height,
                round,
                proposer,
            });
            Ok(r.resume_with(()))
        }

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
        Effect::RestreamProposal(height, round, valid_round, proposer, value_id, r) => {
            // Re-send the full value (PartsOnly): rebroadcast only re-sends
            // votes, so without this a dropped ProposedValue is never
            // recovered and a lossy network livelocks. We can only restream a
            // value we hold (we proposed it, or received and stored it).
            if let Some(block) = ctx.blocks.get(&(height, value_id)).cloned() {
                let pv = ProposedValue {
                    height,
                    round,
                    valid_round,
                    proposer,
                    value: block,
                    validity: Validity::Valid,
                };
                ctx.gossip
                    .broadcast(&WireConsensusMsg::ProposedValue((&pv).into()));
            }
            Ok(r.resume_with(()))
        }

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

            // Advance to the next height (queued, not re-entrant). In
            // on-demand mode the start is deferred until a wake signal —
            // an idle mesh arms no timers and produces no empty blocks.
            let next = malachitebft_core_types::Height::increment(&height);
            if ctx.on_demand {
                *ctx.deferred_start = Some(next);
            } else {
                let valset = ctx.app.validator_set(next);
                ctx.pending_inputs
                    .push_back(Input::StartHeight(next, valset, false, None));
            }
            *ctx.wal_seq = 0;

            Ok(r.resume_with(()))
        }
        Effect::Finalize(_cert, _extensions, _evidence, r) => Ok(r.resume_with(())),

        Effect::ExtendVote(_h, _round, _vid, r) => Ok(r.resume_with(None)),
        Effect::VerifyVoteExtension(_h, _round, _vid, _ext, _pk, r) => Ok(r.resume_with(Ok(()))),

        // Decided-value sync: the certificate verified against the current
        // valset — decode the value, attach OUR validity verdict (Rule-8, same
        // seam as live proposals), and hand it back as a ProposedValue so the
        // engine takes the sync decision path (same atomic decide as live).
        Effect::ValidSyncValue(value, proposer, r) => {
            let height = value.certificate.height;
            let round = value.certificate.round;
            let decoded = codec::decode::<Block>(&value.value_bytes);
            match decoded {
                Ok(block)
                    if block.verify().is_ok() && block.block_hash == value.certificate.value_id =>
                {
                    let validity = {
                        let app = &mut *ctx.app;
                        ctx.storage
                            .with_rollback(|tx| {
                                app.validate_block(height, &block, tx, ValidationOrigin::Sync)
                            })
                            .map_err(HostError::Storage)?
                    };
                    if validity == Validity::Invalid {
                        // A quorum committed a value our app rejects: an app
                        // determinism violation, not a peer fault. Surface it —
                        // the engine will log and hold at this height.
                        tracing::error!(
                            %height,
                            "sync value failed local validation despite a valid commit certificate"
                        );
                        ctx.outputs.push(HostOutput::SyncInvalid {
                            peer: value.peer,
                            height,
                        });
                    }
                    ctx.blocks.insert((height, block.block_hash), block.clone());
                    let pv = ProposedValue {
                        height,
                        round,
                        valid_round: Round::Nil,
                        proposer,
                        value: block,
                        validity,
                    };
                    ctx.pending_inputs
                        .push_back(Input::ProposedValue(pv, ValueOrigin::Sync));
                }
                _ => {
                    ctx.outputs.push(HostOutput::SyncInvalid {
                        peer: value.peer,
                        height,
                    });
                }
            }
            Ok(r.resume_with(()))
        }
        Effect::InvalidSyncValue(peer, height, err, r) => {
            tracing::warn!(%height, "invalid sync value: {err}");
            ctx.outputs.push(HostOutput::SyncInvalid { peer, height });
            Ok(r.resume_with(()))
        }
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
