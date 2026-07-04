//! Deterministic simulation harness: fake host seams plus a virtual-clock
//! `Sim` driver with seeded fault injection. No tokio, no real time, no I/O —
//! a whole run is a pure function of `(n, profile, FaultConfig)`, so a failing
//! seed reproduces exactly.
//!
//! The driver is a single global event queue ordered by virtual time: message
//! deliveries (with fault-injected delay/drop/duplicate/partition) and timer
//! fires (durations from `LinearTimeouts`, on the same time axis) interleave by
//! deadline. The fakes (`MemStorage`/`MemApp`/`MemGossip`/`MemTimers`) are the
//! same ones production will mirror; only this driver is test-only.
//!
//! Invariants checked continuously:
//! - **agreement**: no two nodes ever decide different values at one height;
//! - **contiguity**: each node's decided heights have no gaps;
//! - **no equivocation**: a correct node never signs two different values for
//!   one (height, round, vote_type) — checked at broadcast, so delayed/dropped
//!   votes still count; this is the WAL-replay-correctness oracle across crash.
//!
//! Liveness is SCOPED: a node that crashes and restarts behind the tip needs
//! decided-value sync (Stage 4) to catch up — a virtual clock does not fix
//! that. So safety holds for all nodes always; liveness (reaching a target
//! height) is asserted only for the healthy quorum via `expect_decided`.

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::ops::Range;
use std::rc::Rc;

use malachitebft_core_consensus::Params;
use malachitebft_core_types::{LinearTimeouts, Timeout, Validity, ValuePayload};

use crate::codec::{WireCommitCertificate, WireConsensusMsg, WireVote, WireVoteType, WireWalEntry};
use crate::config::QuorumProfile;
use crate::context::{Address, Height, HopNetValidatorSet, Validator};
use crate::host::{HostCore, HostError, HostOutput};
use crate::traits::{Application, ApplyError, Gossip, Storage, Timers, ValidationOrigin};
use crate::types::{Blake3Hash, Block, BlockData, PrivKey, Transactions};

// ---------------------------------------------------------------------------
// Deterministic PRNG (SplitMix64) — portable, seedable, dependency-free.

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed)
    }
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    /// True with probability `p` in [0, 1].
    fn chance(&mut self, p: f64) -> bool {
        if p <= 0.0 {
            return false;
        }
        if p >= 1.0 {
            return true;
        }
        let unit = (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64;
        unit < p
    }
    /// Uniform in `range` (start if empty).
    fn range(&mut self, range: Range<u64>) -> u64 {
        if range.end <= range.start {
            range.start
        } else {
            range.start + self.next_u64() % (range.end - range.start)
        }
    }
}

// ---------------------------------------------------------------------------
// Fakes: storage, application

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
    wal: Vec<(Height, u64, WireWalEntry)>,
    applied: Vec<Block>,
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
    /// Full decided history (block + certificate) — the sync server's view.
    pub fn decided_full(&self) -> Vec<(Block, WireCommitCertificate)> {
        self.0.borrow().decided.clone()
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
        let r = f(&mut tx)?;
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
        Ok(f(&mut tx))
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
    reject_height: Option<Height>,
}

impl MemApp {
    pub fn new(valset: HopNetValidatorSet) -> Self {
        Self {
            valset,
            reject_height: None,
        }
    }
}

impl Application<MemStorage> for MemApp {
    fn validate_block(
        &mut self,
        height: Height,
        _block: &Block,
        _tx: &mut MemTx,
        _origin: ValidationOrigin,
    ) -> Validity {
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

// ---------------------------------------------------------------------------
// Fakes: gossip, timers

#[derive(Clone, Default)]
pub struct MemGossip {
    outbox: Rc<RefCell<Vec<WireConsensusMsg>>>,
}

impl MemGossip {
    /// Drain accumulated broadcasts (also used by the SQLite storage tests).
    pub fn take_outbox(&self) -> Vec<WireConsensusMsg> {
        std::mem::take(&mut self.outbox.borrow_mut())
    }
}

impl Gossip for MemGossip {
    fn broadcast(&mut self, msg: &WireConsensusMsg) {
        self.outbox.borrow_mut().push(msg.clone());
    }
}

/// Generation-tracked timers. `schedule` bumps a per-timeout generation and
/// records it in `pending` for the driver to enqueue a `Fire` at the right
/// virtual deadline. A `Fire` is honoured only if its generation still matches
/// the armed one (lazy cancellation): re-arming or cancelling supersedes it.
#[derive(Clone, Default)]
pub struct MemTimers {
    armed: Rc<RefCell<HashMap<Timeout, u64>>>,
    pending: Rc<RefCell<Vec<(Timeout, u64)>>>,
    next_gen: Rc<RefCell<u64>>,
}

impl MemTimers {
    fn take_pending(&self) -> Vec<(Timeout, u64)> {
        std::mem::take(&mut self.pending.borrow_mut())
    }
    /// Is this exact (timeout, generation) still the armed one?
    fn is_current(&self, timeout: &Timeout, gen: u64) -> bool {
        self.armed.borrow().get(timeout) == Some(&gen)
    }
    fn disarm(&self, timeout: &Timeout) {
        self.armed.borrow_mut().remove(timeout);
    }
}

impl Timers for MemTimers {
    fn schedule(&mut self, timeout: Timeout) {
        let mut g = self.next_gen.borrow_mut();
        *g += 1;
        let gen = *g;
        self.armed.borrow_mut().insert(timeout, gen);
        self.pending.borrow_mut().push((timeout, gen));
    }
    fn cancel(&mut self, timeout: &Timeout) {
        self.armed.borrow_mut().remove(timeout);
    }
    fn cancel_all(&mut self) {
        self.armed.borrow_mut().clear();
    }
}

// ---------------------------------------------------------------------------
// Fault configuration

#[derive(Clone)]
pub struct FaultConfig {
    pub seed: u64,
    /// Per-message delivery delay range, in virtual milliseconds.
    pub delay: Range<u64>,
    pub drop_p: f64,
    pub duplicate_p: f64,
    /// Bipartitions active over a `[start, end)` vtime window. `side_a` holds
    /// node indices; messages crossing the cut in that window are dropped.
    pub partitions: Vec<Partition>,
    /// `(vtime, node_idx, restart_after_ms)` — node goes down at `vtime`, comes
    /// back `restart_after_ms` later (messages to it are lost while down).
    pub crashes: Vec<(u64, usize, u64)>,
}

#[derive(Clone)]
pub struct Partition {
    pub start: u64,
    pub end: u64,
    pub side_a: Vec<usize>,
}

impl FaultConfig {
    /// No faults: instant, lossless delivery. The happy path.
    pub fn none(seed: u64) -> Self {
        FaultConfig {
            seed,
            delay: 0..1,
            drop_p: 0.0,
            duplicate_p: 0.0,
            partitions: Vec::new(),
            crashes: Vec::new(),
        }
    }
    /// Derive a whole fault schedule from a seed (structure-aware fuzzing):
    /// random delay range, drop/duplicate rates, an optional partition window,
    /// and an optional crash+restart. `n` bounds node indices.
    pub fn from_seed(seed: u64, n: usize) -> Self {
        let mut r = Rng::new(seed ^ 0xD1B5_4A32_D192_ED03);
        let lo = r.range(1..30);
        let hi = lo + 1 + r.range(1..80);
        let drop_p = (r.range(0..40) as f64) / 100.0; // 0–0.39
        let duplicate_p = (r.range(0..20) as f64) / 100.0; // 0–0.19

        let mut partitions = Vec::new();
        if r.chance(0.5) && n >= 3 {
            // Isolate a minority (1 node) so the majority can still decide.
            let victim = (r.range(0..n as u64)) as usize;
            let start = r.range(0..200);
            let end = start + 100 + r.range(0..400);
            partitions.push(Partition {
                start,
                end,
                side_a: vec![victim],
            });
        }

        let mut crashes = Vec::new();
        if r.chance(0.5) {
            let node = (r.range(0..n as u64)) as usize;
            let at = r.range(10..500);
            let after = 100 + r.range(0..600);
            crashes.push((at, node, after));
        }

        FaultConfig {
            seed,
            delay: lo..hi,
            drop_p,
            duplicate_p,
            partitions,
            crashes,
        }
    }

    /// The last vtime at which any fault is active (GST boundary).
    fn gst(&self) -> u64 {
        let mut t = 0;
        for p in &self.partitions {
            t = t.max(p.end.min(MAX_VTIME));
        }
        for (vt, _, after) in &self.crashes {
            t = t.max(vt.saturating_add(*after));
        }
        t
    }
    fn crosses_partition(&self, vtime: u64, a: usize, b: usize) -> bool {
        self.partitions.iter().any(|p| {
            vtime >= p.start && vtime < p.end && (p.side_a.contains(&a) != p.side_a.contains(&b))
        })
    }
}

// ---------------------------------------------------------------------------
// Event queue

enum Event {
    Deliver {
        to: usize,
        from: usize,
        msg: WireConsensusMsg,
    },
    Fire {
        node: usize,
        timeout: Timeout,
        gen: u64,
    },
    Crash {
        node: usize,
        restart_after: u64,
    },
    Restart {
        node: usize,
    },
}

struct Scheduled {
    vtime: u64,
    seq: u64,
    event: Event,
}

impl PartialEq for Scheduled {
    fn eq(&self, other: &Self) -> bool {
        self.vtime == other.vtime && self.seq == other.seq
    }
}
impl Eq for Scheduled {}
impl PartialOrd for Scheduled {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Scheduled {
    // Reverse so BinaryHeap (a max-heap) pops the EARLIEST event.
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .vtime
            .cmp(&self.vtime)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

// ---------------------------------------------------------------------------
// Sim driver

type Core = HostCore<MemApp, MemStorage, MemGossip, MemTimers>;

struct SimNode {
    core: Core,
    storage: MemStorage,
    gossip: MemGossip,
    timers: MemTimers,
    node_id: i32,
    down: bool,
    /// Highest decided height observed, for contiguity checks.
    last_decided_seen: u64,
}

pub struct Sim {
    nodes: Vec<SimNode>,
    chain_id: Blake3Hash,
    valset: HopNetValidatorSet,
    keys: Vec<PrivKey>,
    profile: QuorumProfile,
    faults: FaultConfig,
    rng: Rng,
    queue: BinaryHeap<Scheduled>,
    vtime: u64,
    seq: u64,
    timeouts: LinearTimeouts,
    /// Agreement oracle: the value decided at each height (first writer wins;
    /// any mismatch panics).
    agreed: BTreeMap<u64, Blake3Hash>,
    /// Equivocation oracle: (node, height, round, type) → value bytes.
    signed: BTreeMap<(i32, u64, i64, u8), [u8; 32]>,
}

const MAX_VTIME: u64 = 100_000_000;
const MAX_EVENTS: u64 = 5_000_000;

fn sim_key(node_id: i32) -> PrivKey {
    let mut seed = [0u8; 32];
    seed[..4].copy_from_slice(&node_id.to_le_bytes());
    seed[31] = 0xA5;
    PrivKey(ed25519_dalek::SigningKey::from_bytes(&seed))
}

impl Sim {
    pub fn new(n: i32, profile: QuorumProfile) -> Self {
        Self::with_faults(n, profile, FaultConfig::none(0))
    }

    pub fn with_faults(n: i32, profile: QuorumProfile, faults: FaultConfig) -> Self {
        let chain_id = Blake3Hash::from_bytes([7u8; 32]);
        let keys: Vec<PrivKey> = (0..n).map(sim_key).collect();
        let valset = HopNetValidatorSet::new(
            (0..n)
                .map(|i| Validator::new(i, keys[i as usize].public()))
                .collect(),
        );
        let nodes = (0..n)
            .map(|i| build_node(i, chain_id, &valset, &keys[i as usize], profile))
            .collect();

        let mut sim = Sim {
            nodes,
            chain_id,
            valset,
            keys,
            profile,
            rng: Rng::new(faults.seed),
            queue: BinaryHeap::new(),
            vtime: 0,
            seq: 0,
            timeouts: LinearTimeouts::default(),
            agreed: BTreeMap::new(),
            signed: BTreeMap::new(),
            faults,
        };

        // Schedule crash/restart events.
        let crashes = sim.faults.crashes.clone();
        for (vt, node, after) in crashes {
            sim.push(
                vt,
                Event::Crash {
                    node,
                    restart_after: after,
                },
            );
        }
        sim
    }

    fn push(&mut self, vtime: u64, event: Event) {
        self.seq += 1;
        self.queue.push(Scheduled {
            vtime,
            seq: self.seq,
            event,
        });
    }

    /// Start height 1 on every node and settle the resulting effects.
    pub fn start(&mut self) -> Result<(), HostError<MemError>> {
        for i in 0..self.nodes.len() {
            self.nodes[i].core.start_height(Height::INITIAL, false)?;
            self.settle(i)?;
        }
        Ok(())
    }

    /// After any interaction with node `i`, fulfil its value requests, then
    /// drain its newly-armed timers and outbound messages into the queue.
    fn settle(&mut self, i: usize) -> Result<(), HostError<MemError>> {
        // Fulfil NeedValue / record Decided until the node quiesces.
        loop {
            let outs = self.nodes[i].core.take_outputs();
            if outs.is_empty() {
                break;
            }
            for out in outs {
                match out {
                    HostOutput::NeedValue { height, round } => {
                        let parent = self.nodes[i]
                            .storage
                            .decided_blocks()
                            .last()
                            .map(|(_, h)| *h);
                        let block = build_block(height, round, self.nodes[i].node_id, parent);
                        self.nodes[i].core.propose(height, round, block)?;
                    }
                    HostOutput::Decided { height } => self.on_decided(i, height),
                    HostOutput::SyncInvalid { height, .. } => {
                        panic!("sync value rejected at height {height} — sim peers are honest")
                    }
                }
            }
        }

        // Enqueue newly-armed timers at their real durations.
        let pending = self.nodes[i].timers.take_pending();
        for (timeout, gen) in pending {
            let dur = self.timeouts.duration_for(timeout).as_millis() as u64;
            let at = self.vtime + dur.max(1);
            self.push(
                at,
                Event::Fire {
                    node: i,
                    timeout,
                    gen,
                },
            );
        }

        // Enqueue outbound messages (equivocation-checked at the source, so
        // dropped/delayed votes still count).
        let from = i;
        let msgs = self.nodes[i].gossip.take_outbox();
        for msg in msgs {
            if let WireConsensusMsg::Vote(ref v) = msg {
                self.check_equivocation(self.nodes[i].node_id, v);
            }
            for to in 0..self.nodes.len() {
                if to == from {
                    continue;
                }
                self.schedule_delivery(from, to, msg.clone());
            }
        }
        Ok(())
    }

    fn schedule_delivery(&mut self, from: usize, to: usize, msg: WireConsensusMsg) {
        // Partition and drop are decided at send time on the current clock.
        if self.faults.crosses_partition(self.vtime, from, to) {
            return;
        }
        if self.rng.chance(self.faults.drop_p) {
            return;
        }
        let base_delay = self.rng.range(self.faults.delay.clone());
        self.push(
            self.vtime + base_delay,
            Event::Deliver {
                to,
                from,
                msg: msg.clone(),
            },
        );
        if self.rng.chance(self.faults.duplicate_p) {
            let extra = self.rng.range(self.faults.delay.clone());
            self.push(
                self.vtime + base_delay + extra,
                Event::Deliver { to, from, msg },
            );
        }
    }

    fn on_decided(&mut self, i: usize, height: Height) {
        // Contiguity: heights must advance by exactly 1 per node.
        let h = height.0;
        let prev = self.nodes[i].last_decided_seen;
        assert_eq!(
            h,
            prev + 1,
            "node {} decided height {h} after {prev} — non-contiguous",
            self.nodes[i].node_id
        );
        self.nodes[i].last_decided_seen = h;

        // Agreement: one value per height across all nodes, ever.
        let decided = self.nodes[i].storage.decided_blocks();
        let (_, hash) = decided[(h - 1) as usize];
        match self.agreed.get(&h) {
            Some(existing) => assert_eq!(
                *existing, hash,
                "AGREEMENT VIOLATION at height {h}: node {} decided a different value",
                self.nodes[i].node_id
            ),
            None => {
                self.agreed.insert(h, hash);
            }
        }
    }

    fn check_equivocation(&mut self, node_id: i32, v: &WireVote) {
        let typ = match v.typ {
            WireVoteType::Prevote => 0u8,
            WireVoteType::Precommit => 1u8,
        };
        let value = v.value.map(|h| *h.as_bytes()).unwrap_or([0u8; 32]);
        let key = (node_id, v.height, v.round, typ);
        if let Some(prev) = self.signed.get(&key) {
            assert_eq!(
                *prev, value,
                "EQUIVOCATION: node {node_id} signed two values for height={} round={} type={typ}",
                v.height, v.round
            );
        } else {
            self.signed.insert(key, value);
        }
    }

    /// Run the event loop until `expect_decided` nodes have decided at least
    /// `target` heights. Panics on any invariant violation, on livelock (past
    /// the GST bound with no progress), or on a host error.
    pub fn run(&mut self, target: u64, expect_decided: usize) -> Result<(), HostError<MemError>> {
        let mut events = 0u64;
        let bound = self.faults.gst().saturating_add(MAX_VTIME.min(2_000_000));

        while let Some(item) = self.queue.pop() {
            events += 1;
            assert!(events < MAX_EVENTS, "event budget exhausted (livelock?)");
            self.vtime = item.vtime;
            assert!(
                self.vtime < bound,
                "vtime bound exceeded — no liveness (vtime={}, decided={:?})",
                self.vtime,
                self.nodes
                    .iter()
                    .map(|n| n.last_decided_seen)
                    .collect::<Vec<_>>()
            );

            match item.event {
                Event::Deliver { to, from, msg } => {
                    if self.nodes[to].down {
                        continue; // message lost to a crashed node
                    }
                    // Re-check partition at delivery time too (window may have
                    // opened after send).
                    if self.faults.crosses_partition(self.vtime, from, to) {
                        continue;
                    }
                    self.nodes[to].core.on_wire(msg)?;
                    self.settle(to)?;
                }
                Event::Fire { node, timeout, gen } => {
                    if self.nodes[node].down {
                        continue;
                    }
                    if !self.nodes[node].timers.is_current(&timeout, gen) {
                        continue; // superseded or cancelled
                    }
                    self.nodes[node].timers.disarm(&timeout);
                    self.nodes[node].core.on_timeout(timeout)?;
                    self.settle(node)?;
                }
                Event::Crash {
                    node,
                    restart_after,
                } => {
                    self.nodes[node].down = true;
                    self.push(self.vtime + restart_after, Event::Restart { node });
                }
                Event::Restart { node } => {
                    self.restart(node)?;
                }
            }

            if self.decided_count(target) >= expect_decided {
                return Ok(());
            }
        }

        panic!(
            "event queue drained before {expect_decided} nodes reached height {target}; \
             decided counts {:?}",
            self.nodes
                .iter()
                .map(|n| n.last_decided_seen)
                .collect::<Vec<_>>()
        );
    }

    /// Run up to `max_events` events (or until the queue drains) WITHOUT a
    /// liveness target — for scenarios where some nodes legitimately lag
    /// (message loss drifts heights; catch-up needs Stage-4 sync). The
    /// agreement / contiguity / equivocation oracles still run continuously,
    /// so this asserts SAFETY under the fault, not liveness.
    pub fn run_safety_only(&mut self, max_events: u64) -> Result<(), HostError<MemError>> {
        let mut events = 0u64;
        while let Some(item) = self.queue.pop() {
            events += 1;
            if events > max_events {
                break;
            }
            self.vtime = item.vtime;
            match item.event {
                Event::Deliver { to, from, msg } => {
                    if self.nodes[to].down || self.faults.crosses_partition(self.vtime, from, to) {
                        continue;
                    }
                    self.nodes[to].core.on_wire(msg)?;
                    self.settle(to)?;
                }
                Event::Fire { node, timeout, gen } => {
                    if self.nodes[node].down || !self.nodes[node].timers.is_current(&timeout, gen) {
                        continue;
                    }
                    self.nodes[node].timers.disarm(&timeout);
                    self.nodes[node].core.on_timeout(timeout)?;
                    self.settle(node)?;
                }
                Event::Crash {
                    node,
                    restart_after,
                } => {
                    self.nodes[node].down = true;
                    self.push(self.vtime + restart_after, Event::Restart { node });
                }
                Event::Restart { node } => self.restart(node)?,
            }
        }
        Ok(())
    }

    fn decided_count(&self, target: u64) -> usize {
        self.nodes
            .iter()
            .filter(|n| n.storage.decided_blocks().len() as u64 >= target)
            .count()
    }

    fn restart(&mut self, node: usize) -> Result<(), HostError<MemError>> {
        let resume = self.nodes[node]
            .storage
            .last_decided()
            .unwrap()
            .map(|h| Height(h.0 + 1))
            .unwrap_or(Height::INITIAL);

        // Fresh in-process state (timers), same persisted storage.
        let timers = MemTimers::default();
        let gossip = self.nodes[node].gossip.clone();
        let params = params_for(Address(self.nodes[node].node_id), self.profile);
        self.nodes[node].timers = timers.clone();
        self.nodes[node].down = false;
        self.nodes[node].core = HostCore::new(
            self.chain_id,
            self.keys[node].clone(),
            Address(self.nodes[node].node_id),
            params,
            resume,
            self.valset.clone(),
            MemApp::new(self.valset.clone()),
            self.nodes[node].storage.clone(),
            gossip,
            timers,
        );
        self.nodes[node].core.start_height(resume, false)?;
        self.settle(node)?;
        Ok(())
    }

    /// Decided-value sync: feed `laggard` every decided (block, cert) pair
    /// from `source`'s store, starting at the laggard's current height. Models
    /// the Stage-4 sync client with a perfectly-behaved peer; each pair
    /// decides through the SAME atomic path as live consensus, so the
    /// agreement/contiguity oracles keep running.
    pub fn sync_from_peer(
        &mut self,
        laggard: usize,
        source: usize,
    ) -> Result<(), HostError<MemError>> {
        let peer = crate::host::peer_id_for_node(self.nodes[source].node_id);
        let history = self.nodes[source].storage.decided_full();
        let from = self.nodes[laggard].core.height().0;
        for (block, cert) in history {
            if block.data.height < from {
                continue;
            }
            self.nodes[laggard].core.on_sync_value(peer, block, &cert)?;
            self.settle(laggard)?;
        }
        Ok(())
    }

    // -------- assertions used by tests --------

    pub fn n(&self) -> usize {
        self.nodes.len()
    }

    /// Agreement across the heights EVERY node decided (the minimum). Returns
    /// that minimum. Continuous checks already guarantee no cross-height
    /// divergence; this is a belt-and-suspenders snapshot.
    pub fn assert_agreement_common(&self) -> u64 {
        let min = self
            .nodes
            .iter()
            .map(|n| n.storage.decided_blocks().len())
            .min()
            .unwrap_or(0) as u64;
        for h in 1..=min {
            let reference = self.nodes[0].storage.decided_blocks()[(h - 1) as usize];
            for node in &self.nodes[1..] {
                assert_eq!(
                    node.storage.decided_blocks()[(h - 1) as usize],
                    reference,
                    "divergence at height {h}"
                );
            }
        }
        min
    }

    pub fn decided_height(&self, node: usize) -> u64 {
        self.nodes[node].storage.decided_blocks().len() as u64
    }

    /// The engine's current working height for a node.
    pub fn engine_height(&self, node: usize) -> u64 {
        self.nodes[node].core.height().0
    }
}

fn params_for(address: Address, profile: QuorumProfile) -> Params<crate::context::HopNetContext> {
    Params {
        address,
        threshold_params: profile.thresholds(),
        value_payload: ValuePayload::PartsOnly,
        enabled: true,
    }
}

fn build_node(
    node_id: i32,
    chain_id: Blake3Hash,
    valset: &HopNetValidatorSet,
    signer: &PrivKey,
    profile: QuorumProfile,
) -> SimNode {
    let storage = MemStorage::default();
    let gossip = MemGossip::default();
    let timers = MemTimers::default();
    let core = HostCore::new(
        chain_id,
        signer.clone(),
        Address(node_id),
        params_for(Address(node_id), profile),
        Height::INITIAL,
        valset.clone(),
        MemApp::new(valset.clone()),
        storage.clone(),
        gossip.clone(),
        timers.clone(),
    );
    SimNode {
        core,
        storage,
        gossip,
        timers,
        node_id,
        down: false,
        last_decided_seen: 0,
    }
}

fn build_block(
    height: Height,
    round: malachitebft_core_types::Round,
    proposer: i32,
    parent: Option<Blake3Hash>,
) -> Block {
    let mut tx_bytes = proposer.to_le_bytes().to_vec();
    tx_bytes.extend(round.as_i64().to_le_bytes());
    Block::new(BlockData {
        height: height.0,
        round: round.as_u32().unwrap_or(0),
        parent_hash: parent,
        transactions: Transactions(vec![crate::types::Transaction::new(
            "noop".into(),
            tx_bytes,
            proposer,
            &sim_key(proposer),
        )
        .unwrap()]),
    })
    .unwrap()
}
