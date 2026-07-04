//! Deterministic in-memory harness: fake implementations of the four host
//! seams plus a `Cluster` driver. No tokio, no real time, no I/O — the
//! `Cluster` owns a single logical clock and message bus so a run is a pure
//! function of its inputs. Stage 2 uses it for multi-node decide and
//! crash/replay tests; Stage 3 layers seeded fault injection on top.
//!
//! Persistence survives a simulated crash because each fake is backed by an
//! `Rc<RefCell<_>>` inner: "crash" drops the `HostCore`, "restart" builds a
//! fresh core over handles to the same inner state.

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

use malachitebft_core_consensus::Params;
use malachitebft_core_types::{Timeout, Validity, ValuePayload};

use crate::codec::{WireCommitCertificate, WireConsensusMsg, WireVote, WireWalEntry};
use crate::config::QuorumProfile;
use crate::context::{Address, Height, HopNetValidatorSet, Validator};
use crate::host::{HostCore, HostError, HostOutput};
use crate::signing::Sig;
use crate::traits::{Application, ApplyError, Gossip, Storage, Timers};
use crate::types::{Blake3Hash, Block, BlockData, PrivKey, Transactions};

// ---------------------------------------------------------------------------
// Fakes

#[derive(Debug)]
pub struct MemError(pub String);

#[derive(Default)]
pub struct MemTx {
    applied: Vec<Block>,
    decided: Vec<(Block, WireCommitCertificate)>,
    truncate: Option<Height>,
    last: Option<Height>,
}

#[derive(Default)]
struct StorageInner {
    /// (height, seq, entry) — the write-ahead log.
    wal: Vec<(Height, u64, WireWalEntry)>,
    /// Applied blocks in commit order — the "application state".
    applied: Vec<Block>,
    /// Decided blocks + certificates in height order.
    decided: Vec<(Block, WireCommitCertificate)>,
    last_decided: Option<Height>,
}

#[derive(Clone, Default)]
pub struct MemStorage(Rc<RefCell<StorageInner>>);

impl MemStorage {
    pub fn decided_blocks(&self) -> Vec<(Height, Blake3Hash)> {
        self.0
            .borrow()
            .decided
            .iter()
            .map(|(b, _)| (Height(b.data.height), b.block_hash))
            .collect()
    }

    pub fn wal_len(&self) -> usize {
        self.0.borrow().wal.len()
    }
}

impl Storage for MemStorage {
    type Tx<'a> = MemTx;
    type Error = MemError;

    fn wal_append(
        &mut self,
        height: Height,
        seq: u64,
        entry: &WireWalEntry,
    ) -> Result<(), MemError> {
        self.0.borrow_mut().wal.push((height, seq, entry.clone()));
        Ok(())
    }

    fn wal_fetch(&mut self, height: Height) -> Result<Vec<WireWalEntry>, MemError> {
        let inner = self.0.borrow();
        let mut rows: Vec<_> = inner
            .wal
            .iter()
            .filter(|(h, _, _)| *h == height)
            .cloned()
            .collect();
        rows.sort_by_key(|(_, seq, _)| *seq);
        Ok(rows.into_iter().map(|(_, _, e)| e).collect())
    }

    fn wal_reset(&mut self) -> Result<(), MemError> {
        self.0.borrow_mut().wal.clear();
        Ok(())
    }

    fn decide_atomically<R>(
        &mut self,
        f: impl FnOnce(&mut MemTx) -> Result<R, MemError>,
    ) -> Result<R, MemError> {
        let mut tx = MemTx::default();
        let r = f(&mut tx)?; // on error, tx is dropped — nothing committed
        let mut inner = self.0.borrow_mut();
        inner.applied.extend(tx.applied);
        inner.decided.extend(tx.decided);
        if let Some(h) = tx.truncate {
            inner.wal.retain(|(hh, _, _)| *hh > h);
        }
        if tx.last.is_some() {
            inner.last_decided = tx.last;
        }
        Ok(r)
    }

    fn with_rollback<R>(&mut self, f: impl FnOnce(&mut MemTx) -> R) -> Result<R, MemError> {
        let mut tx = MemTx::default();
        Ok(f(&mut tx)) // tx dropped — rolled back
    }

    fn store_decided_tx(
        tx: &mut MemTx,
        block: &Block,
        cert: &WireCommitCertificate,
    ) -> Result<(), MemError> {
        tx.decided.push((block.clone(), cert.clone()));
        Ok(())
    }

    fn truncate_wal_tx(tx: &mut MemTx, up_to: Height) -> Result<(), MemError> {
        tx.truncate = Some(up_to);
        Ok(())
    }

    fn set_last_decided_tx(tx: &mut MemTx, height: Height) -> Result<(), MemError> {
        tx.last = Some(height);
        Ok(())
    }

    fn last_decided(&mut self) -> Result<Option<Height>, MemError> {
        Ok(self.0.borrow().last_decided)
    }

    fn apply_error(e: ApplyError) -> MemError {
        MemError(e.0)
    }
}

pub struct MemApp {
    valset: HopNetValidatorSet,
    /// If set, blocks at this height validate as Invalid (adversarial tests,
    /// exercised from Stage 3).
    reject_height: Option<Height>,
}

impl MemApp {
    pub fn new(valset: HopNetValidatorSet) -> Self {
        Self {
            valset,
            reject_height: None,
        }
    }

    /// Reject (validate as Invalid) every block proposed at `height`.
    pub fn reject_at(&mut self, height: Height) {
        self.reject_height = Some(height);
    }
}

impl Application<MemStorage> for MemApp {
    fn validate_block(&mut self, height: Height, _block: &Block, _tx: &mut MemTx) -> Validity {
        if self.reject_height == Some(height) {
            Validity::Invalid
        } else {
            Validity::Valid
        }
    }

    fn apply_block(
        &mut self,
        _height: Height,
        block: &Block,
        tx: &mut MemTx,
    ) -> Result<(), ApplyError> {
        tx.applied.push(block.clone());
        Ok(())
    }

    fn validator_set(&mut self, _height: Height) -> HopNetValidatorSet {
        self.valset.clone()
    }

    fn on_decided(&mut self, _height: Height, _block: &Block, _cert: &WireCommitCertificate) {}
}

#[derive(Clone, Default)]
pub struct MemGossip {
    outbox: Rc<RefCell<Vec<WireConsensusMsg>>>,
}

impl Gossip for MemGossip {
    fn broadcast(&mut self, msg: &WireConsensusMsg) {
        self.outbox.borrow_mut().push(msg.clone());
    }
}

#[derive(Clone, Default)]
pub struct MemTimers {
    /// Currently-armed timeouts (scheduled and not yet cancelled/fired). The
    /// Cluster fires these on a stall, modelling "the round timeout elapsed" —
    /// which is what re-delivers votes (rebroadcast) so a recovered or lagging
    /// node reconverges. Stage 3 replaces stall-firing with a virtual clock.
    scheduled: Rc<RefCell<Vec<Timeout>>>,
}

impl MemTimers {
    /// Drain the armed timeouts (the Cluster fires them on a stall).
    fn take_armed(&self) -> Vec<Timeout> {
        std::mem::take(&mut self.scheduled.borrow_mut())
    }

    /// Re-arm a timeout without dedup (used when the Cluster fires only a
    /// subset and puts the rest back).
    fn schedule_raw(&self, timeout: Timeout) {
        self.scheduled.borrow_mut().push(timeout);
    }
}

impl Timers for MemTimers {
    fn schedule(&mut self, timeout: Timeout) {
        // Re-arming the same (kind, round) replaces — one live timer per slot.
        let mut s = self.scheduled.borrow_mut();
        s.retain(|t| t != &timeout);
        s.push(timeout);
    }
    fn cancel(&mut self, timeout: &Timeout) {
        self.scheduled.borrow_mut().retain(|t| t != timeout);
    }
    fn cancel_all(&mut self) {
        self.scheduled.borrow_mut().clear();
    }
}

// ---------------------------------------------------------------------------
// Cluster

type Core = HostCore<MemApp, MemStorage, MemGossip, MemTimers>;

pub struct Node {
    pub core: Core,
    pub storage: MemStorage,
    pub gossip: MemGossip,
    pub timers: MemTimers,
    pub node_id: i32,
    inbox: VecDeque<WireConsensusMsg>,
}

pub struct Cluster {
    pub nodes: Vec<Node>,
    chain_id: Blake3Hash,
    valset: HopNetValidatorSet,
    keys: Vec<PrivKey>,
    /// Equivocation oracle: for each node, the vote it broadcast for a given
    /// (height, round, vote_type). A second, different vote is a violation.
    signed: BTreeMap<(i32, u64, i64, u8), [u8; 32]>,
}

fn key(node_id: i32) -> PrivKey {
    let mut seed = [0u8; 32];
    seed[..4].copy_from_slice(&node_id.to_le_bytes());
    seed[31] = 0xA5;
    PrivKey(ed25519_dalek::SigningKey::from_bytes(&seed))
}

impl Cluster {
    pub fn new(n: i32, profile: QuorumProfile) -> Self {
        let chain_id = Blake3Hash::from_bytes([7u8; 32]);
        let keys: Vec<PrivKey> = (0..n).map(key).collect();
        let valset = HopNetValidatorSet::new(
            (0..n)
                .map(|i| Validator::new(i, keys[i as usize].public()))
                .collect(),
        );

        let nodes = (0..n)
            .map(|i| build_node(i, chain_id, &valset, &keys[i as usize], profile))
            .collect();

        Cluster {
            nodes,
            chain_id,
            valset,
            keys,
            signed: BTreeMap::new(),
        }
    }

    /// Start height 1 on every node.
    pub fn start(&mut self) -> Result<(), HostError<MemError>> {
        for node in &mut self.nodes {
            node.core.start_height(Height::INITIAL, false)?;
        }
        self.record_and_route();
        Ok(())
    }

    /// One scheduler tick: fulfil value requests, then deliver one queued
    /// message per node. Returns whether anything progressed.
    pub fn step(&mut self) -> Result<bool, HostError<MemError>> {
        let mut progressed = false;

        // Fulfil GetValue requests: the proposer builds and proposes a block.
        for i in 0..self.nodes.len() {
            let outs = self.nodes[i].core.take_outputs();
            for out in outs {
                if let HostOutput::NeedValue { height, round } = out {
                    progressed = true;
                    let parent = self.nodes[i]
                        .storage
                        .decided_blocks()
                        .last()
                        .map(|(_, h)| *h);
                    let block = build_block(height, round, self.nodes[i].node_id, parent);
                    self.nodes[i].core.propose(height, round, block)?;
                }
            }
        }
        self.record_and_route();

        // Deliver one inbox message per node.
        for i in 0..self.nodes.len() {
            if let Some(msg) = self.nodes[i].inbox.pop_front() {
                progressed = true;
                self.nodes[i].core.on_wire(msg)?;
            }
        }
        self.record_and_route();

        Ok(progressed)
    }

    /// Run until every node has decided at least `target` heights, or panic.
    ///
    /// On a stall (no messages in flight, no value requests) we fire the armed
    /// timeouts — that models "the round timeout elapsed", which triggers the
    /// rebroadcast that re-delivers votes to a recovered or lagging node. Only
    /// if firing timeouts ALSO makes no progress is it a genuine deadlock.
    pub fn run_to_height(&mut self, target: u64) -> Result<(), HostError<MemError>> {
        for _ in 0..100_000 {
            if self
                .nodes
                .iter()
                .all(|n| n.storage.decided_blocks().len() as u64 >= target)
            {
                return Ok(());
            }
            if !self.step()? {
                let fired = self.fire_timeouts()?;
                if !fired {
                    panic!(
                        "deadlock: decided counts {:?}",
                        self.nodes
                            .iter()
                            .map(|n| n.storage.decided_blocks().len())
                            .collect::<Vec<_>>()
                    );
                }
            }
        }
        panic!("did not converge in 100k steps");
    }

    /// Fire armed timeouts on a stall (stall recovery / GST), tiered by
    /// urgency so we don't advance rounds when a mere rebroadcast unblocks
    /// things. Rebroadcast timeouts re-deliver a node's own last votes to
    /// peers (recovering a crashed node's contribution); only if none are
    /// armed do we fire the round-advancing propose/prevote/precommit
    /// timeouts. This mirrors real timeouts where rebroadcast is the shortest
    /// deadline. Returns whether any timeout fired.
    pub fn fire_timeouts(&mut self) -> Result<bool, HostError<MemError>> {
        use malachitebft_core_types::TimeoutKind;

        // Tier 1: rebroadcast only.
        let mut fired = false;
        for i in 0..self.nodes.len() {
            let armed = self.nodes[i].timers.take_armed();
            let (rebroadcast, rest): (Vec<_>, Vec<_>) = armed
                .into_iter()
                .partition(|t| matches!(t.kind, TimeoutKind::Rebroadcast));
            // Re-arm the ones we're not firing this pass.
            for t in rest {
                self.nodes[i].timers.schedule_raw(t);
            }
            for t in rebroadcast {
                fired = true;
                self.nodes[i].core.on_timeout(t)?;
            }
        }
        if fired {
            self.record_and_route();
            return Ok(true);
        }

        // Tier 2: everything else (round-advancing).
        for i in 0..self.nodes.len() {
            for t in self.nodes[i].timers.take_armed() {
                fired = true;
                self.nodes[i].core.on_timeout(t)?;
            }
        }
        self.record_and_route();
        Ok(fired)
    }

    /// Simulate a crash+restart of node `idx`: drop its core, rebuild over the
    /// SAME storage, and replay the WAL via `start_height`.
    pub fn crash_restart(&mut self, idx: usize) -> Result<(), HostError<MemError>> {
        let node = &mut self.nodes[idx];
        let resume_height = node
            .storage
            .last_decided()
            .unwrap()
            .map(|h| Height(h.0 + 1))
            .unwrap_or(Height::INITIAL);

        let profile = QuorumProfile::Bft; // fixed for the harness
        let params = Params {
            address: Address(node.node_id),
            threshold_params: profile.thresholds(),
            value_payload: ValuePayload::PartsOnly,
            enabled: true,
        };
        // Fresh timers on restart (armed timers are process state, lost on
        // crash — only the WAL persists).
        node.timers = MemTimers::default();
        node.core = HostCore::new(
            self.chain_id,
            self.keys[idx].clone(),
            Address(node.node_id),
            params,
            resume_height,
            self.valset.clone(),
            MemApp::new(self.valset.clone()),
            node.storage.clone(),
            node.gossip.clone(),
            node.timers.clone(),
        );
        node.core.start_height(resume_height, false)?;
        self.record_and_route();
        Ok(())
    }

    /// Assert agreement: every node decided the same block at each height.
    pub fn assert_agreement(&self, up_to: u64) {
        for h in 1..=up_to {
            let idx = (h - 1) as usize;
            let reference = self.nodes[0].storage.decided_blocks()[idx];
            for node in &self.nodes[1..] {
                assert_eq!(
                    node.storage.decided_blocks()[idx],
                    reference,
                    "divergence at height {h}"
                );
            }
        }
    }

    /// Assert agreement across only the heights EVERY node has decided (the
    /// minimum decided count). Used when some node legitimately lags (e.g. a
    /// crashed node awaiting sync) — the heights they all reached must still
    /// match. Returns that minimum.
    pub fn assert_agreement_common(&self) -> u64 {
        let min = self
            .nodes
            .iter()
            .map(|n| n.storage.decided_blocks().len())
            .min()
            .unwrap_or(0);
        self.assert_agreement(min as u64);
        min as u64
    }

    /// Step a bounded number of times (firing timeouts on stalls), for tests
    /// that don't require full convergence.
    pub fn run_bounded(&mut self, steps: usize) -> Result<(), HostError<MemError>> {
        for _ in 0..steps {
            if !self.step()? {
                self.fire_timeouts()?;
            }
        }
        Ok(())
    }

    /// Drain outboxes into peer inboxes, recording votes for the equivocation
    /// oracle on the way through.
    fn record_and_route(&mut self) {
        let n = self.nodes.len();
        let mut deliveries: Vec<(usize, WireConsensusMsg)> = Vec::new();

        for i in 0..n {
            let node_id = self.nodes[i].node_id;
            let msgs: Vec<WireConsensusMsg> =
                self.nodes[i].gossip.outbox.borrow_mut().drain(..).collect();
            for msg in msgs {
                if let WireConsensusMsg::Vote(ref v) = msg {
                    self.check_equivocation(node_id, v);
                }
                for j in 0..n {
                    if j != i {
                        deliveries.push((j, msg.clone()));
                    }
                }
            }
        }

        for (j, msg) in deliveries {
            self.nodes[j].inbox.push_back(msg);
        }
    }

    fn check_equivocation(&mut self, node_id: i32, v: &WireVote) {
        let typ = match v.typ {
            crate::codec::WireVoteType::Prevote => 0u8,
            crate::codec::WireVoteType::Precommit => 1u8,
        };
        let value = v.value.map(|h| *h.as_bytes()).unwrap_or([0u8; 32]);
        let key = (node_id, v.height, v.round, typ);
        if let Some(prev) = self.signed.get(&key) {
            assert_eq!(
                *prev, value,
                "EQUIVOCATION: node {node_id} signed two values for \
                 height={} round={} type={typ}",
                v.height, v.round
            );
        } else {
            self.signed.insert(key, value);
        }
    }
}

fn build_node(
    node_id: i32,
    chain_id: Blake3Hash,
    valset: &HopNetValidatorSet,
    signer: &PrivKey,
    profile: QuorumProfile,
) -> Node {
    let storage = MemStorage::default();
    let gossip = MemGossip::default();
    let timers = MemTimers::default();
    let params = Params {
        address: Address(node_id),
        threshold_params: profile.thresholds(),
        value_payload: ValuePayload::PartsOnly,
        enabled: true,
    };
    let core = HostCore::new(
        chain_id,
        signer.clone(),
        Address(node_id),
        params,
        Height::INITIAL,
        valset.clone(),
        MemApp::new(valset.clone()),
        storage.clone(),
        gossip.clone(),
        timers.clone(),
    );
    Node {
        core,
        storage,
        gossip,
        timers,
        node_id,
        inbox: VecDeque::new(),
    }
}

fn build_block(
    height: Height,
    round: malachitebft_core_types::Round,
    proposer: i32,
    parent: Option<Blake3Hash>,
) -> Block {
    // Distinct per proposer/round so different proposers yield different
    // values; consensus still decides exactly one per height.
    let mut payload = proposer.to_le_bytes().to_vec();
    payload.extend(round.as_i64().to_le_bytes());
    let tx_bytes = payload;
    Block::new(BlockData {
        height: height.0,
        round: round.as_u32().unwrap_or(0),
        parent_hash: parent,
        transactions: Transactions(vec![crate::types::Transaction::new(
            "noop".into(),
            tx_bytes,
            proposer,
            &key(proposer),
        )
        .unwrap()]),
    })
    .unwrap()
}

// Keep Sig referenced (used transitively via codec) without a warning in some
// builds.
const _: fn(&Sig) = |_s| {};
