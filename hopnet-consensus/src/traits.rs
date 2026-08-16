//! Host trait seams: everything the consensus host needs from the outside
//! world, abstracted so the deterministic simulator and production supply
//! interchangeable implementations. All methods are SYNCHRONOUS — the sans-io
//! dispatch core never awaits; asynchrony (spawning value builds, network
//! sends) lives in the shell around it.

use malachitebft_core_types::Timeout;

use crate::codec::{WireCommitCertificate, WireConsensusMsg, WireWalEntry};
use crate::context::{Height, HopNetValidatorSet};
use crate::types::Block;

/// Consensus persistence: the WAL plus the atomic decide transaction.
///
/// The decide path is the load-bearing part: `decide_atomically` must run the
/// provided closure inside ONE storage transaction — application state
/// mutation, block+certificate persistence, and WAL truncation commit
/// together or not at all (crash consistency across app and consensus state).
///
/// `wal_append` must be DURABLE before it returns: the engine publishes a
/// message only after its WAL entry is appended, and that ordering is the
/// no-equivocation-across-crash guarantee.
pub trait Storage {
    /// Transaction handle passed through to the application during decide.
    type Tx<'a>
    where
        Self: 'a;
    type Error: std::fmt::Debug;

    fn wal_append(
        &mut self,
        height: Height,
        seq: u64,
        entry: &WireWalEntry,
    ) -> Result<(), Self::Error>;
    /// Entries for `height` in seq order (crash recovery).
    fn wal_fetch(&mut self, height: Height) -> Result<Vec<WireWalEntry>, Self::Error>;
    /// Drop ALL WAL entries (engine semantics of is_restart = true).
    fn wal_reset(&mut self) -> Result<(), Self::Error>;

    /// Run `f` inside one atomic transaction.
    fn decide_atomically<R>(
        &mut self,
        f: impl FnOnce(&mut Self::Tx<'_>) -> Result<R, Self::Error>,
    ) -> Result<R, Self::Error>;

    /// Run `f` inside a transaction that is ALWAYS rolled back (validation
    /// dry-runs).
    fn with_rollback<R>(
        &mut self,
        f: impl FnOnce(&mut Self::Tx<'_>) -> R,
    ) -> Result<R, Self::Error>;

    /// Like [`Storage::with_rollback`], but the transaction takes the write
    /// lock up front (IMMEDIATE) under a caller-bounded busy timeout — the
    /// retry path for [`ValidationVerdict::Undetermined`]: an IMMEDIATE
    /// transaction cannot lose its snapshot mid-run, so a verdict is
    /// guaranteed if the lock is acquired within the bound. Default falls
    /// back to `with_rollback` for storages without contention semantics.
    fn with_rollback_immediate<R>(
        &mut self,
        busy_timeout_ms: u32,
        f: impl FnOnce(&mut Self::Tx<'_>) -> R,
    ) -> Result<R, Self::Error> {
        let _ = busy_timeout_ms;
        self.with_rollback(f)
    }

    // Consensus-side writes inside the decide transaction.
    fn store_decided_tx(
        tx: &mut Self::Tx<'_>,
        block: &Block,
        cert: &WireCommitCertificate,
    ) -> Result<(), Self::Error>;
    fn truncate_wal_tx(tx: &mut Self::Tx<'_>, up_to: Height) -> Result<(), Self::Error>;
    fn set_last_decided_tx(tx: &mut Self::Tx<'_>, height: Height) -> Result<(), Self::Error>;

    /// Last decided height, if any (startup).
    fn last_decided(&mut self) -> Result<Option<Height>, Self::Error>;

    /// Lift an application apply failure into the storage error channel, so
    /// the whole decide closure shares one error type.
    fn apply_error(e: ApplyError) -> Self::Error;
}

/// Where a block being validated came from — live checks may be
/// time-dependent; sync checks must not be.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ValidationOrigin {
    /// A live proposal for the current height: full Rule-8 including
    /// time-dependent checks (nonce staleness, committed-nonce dedup).
    Live,
    /// A decided value fetched by sync, already backed by a verified commit
    /// certificate. Time-dependent checks MUST be skipped (a node replaying
    /// week-old history would otherwise reject every block); deterministic
    /// checks (signatures, parent linkage, handler validation) still run.
    Sync,
}

/// The application's Rule-8 dry-run verdict. `Undetermined` is a host-internal
/// state — a node-local storage condition prevented reaching a verdict at all.
/// The host retries it on an IMMEDIATE transaction; it is NEVER fed to the
/// engine as a vote, because a non-deterministic local condition must not be
/// expressible as a judgement on block validity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationVerdict {
    Valid,
    Invalid,
    /// Could not determine validity (transient storage contention).
    Undetermined(String),
}

/// The application seam (ABCI in spirit). Deterministic: same inputs must
/// produce the same verdicts and state on every node — that's what agreement
/// on blocks means. No network, no clocks, no randomness.
///
/// Value BUILDING (the proposer's job) is deliberately absent: the host
/// surfaces `HostOutput::NeedValue` and the shell feeds `Input::Propose`,
/// because building may be asynchronous (queue drain, preflight) and the
/// dispatch core never blocks.
pub trait Application<S: Storage> {
    /// Rule-8 dry-run: signatures, nonce/staleness, execute=false dispatch.
    /// Runs inside a rollback transaction the host opens. Returns a
    /// tri-state verdict — `Undetermined` (transient storage contention)
    /// makes the host retry on an IMMEDIATE transaction rather than vote.
    fn validate_block(
        &mut self,
        height: Height,
        block: &Block,
        tx: &mut S::Tx<'_>,
        origin: ValidationOrigin,
    ) -> ValidationVerdict;

    /// Apply a decided block (execute=true dispatch + nonce insertion) inside
    /// the host's decide transaction.
    fn apply_block(
        &mut self,
        height: Height,
        block: &Block,
        tx: &mut S::Tx<'_>,
    ) -> Result<(), ApplyError>;

    /// Validator set effective at `height` (historical heights during sync).
    fn validator_set(&mut self, height: Height) -> HopNetValidatorSet;

    /// Post-commit hook, outside the transaction: notify submitters, kick
    /// replication, submit follow-up transactions.
    fn on_decided(&mut self, height: Height, block: &Block, cert: &WireCommitCertificate);

    /// RFC-019 seal contract: return true when applying `block` sealed the
    /// epoch at `height`. The engine then treats H as TERMINAL — it neither
    /// defers nor starts H+1; in on-demand mode the quiescent park IS the
    /// halt (regenesis-seal-contract.md, items 1–2). Default: never.
    fn sealed_after(&mut self, _height: Height, _block: &Block) -> bool {
        false
    }
}

#[derive(Debug)]
pub struct ApplyError(pub String);

/// Outbound consensus traffic. `broadcast` is fire-and-forget and must not
/// block: production spawns per-peer sends; the simulator enqueues.
pub trait Gossip {
    fn broadcast(&mut self, msg: &WireConsensusMsg);
}

/// Timer service. The engine requests timeouts; the host's clock decides when
/// `Input::TimeoutElapsed` comes back. Production: DelayQueue; sim: virtual
/// clock — which is exactly why time never appears inside the dispatch core.
pub trait Timers {
    fn schedule(&mut self, timeout: Timeout);
    fn cancel(&mut self, timeout: &Timeout);
    fn cancel_all(&mut self);
}
