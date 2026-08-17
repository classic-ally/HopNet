//! Malachite-engine adapters (migration Stage 4).
//!
//! Everything here is ADDITIVE: the bespoke engine stays live and nothing in
//! `main.rs` spawns these components until the Stage-5 cutover. The pieces:
//!
//! - [`app`]: `HopNetApplication` — the `hopnet_consensus::Application` impl
//!   over the `DISPATCH_TABLE`, plus the proposer's value builder.
//! - [`gossip`]: the "consensus" scope's wire vocabulary and the fire-and-
//!   forget publish of consensus messages over the comms transport (the
//!   server side lives in `net::scopes::ConsensusScope`).
//! - [`sync`]: the decided-value sync client (replaces `catch_up_state` for
//!   the new engine).

pub mod app;
pub mod engine;
pub mod gossip;
pub mod sync;

use hopnet_consensus::shell::{HostInput, RoundInfo};
use tokio::sync::{mpsc, watch};

/// The running engine's handle, installed into `AppState.malachite` by
/// `spawn_engine`. Everything the rest of the process needs to talk to the
/// consensus shell: feed inputs (network dispatch, proposals, sync values),
/// observe decides, and locate the current proposer. The event stream is NOT
/// here — it is single-consumer and lives in the driver task.
#[derive(Clone)]
pub struct EngineHandle {
    pub input_tx: mpsc::Sender<HostInput>,
    /// Last decided height (0 until the first decide).
    pub decided: watch::Receiver<u64>,
    /// Current (height, round, proposer). `None` until the first StartRound.
    pub round: watch::Receiver<Option<RoundInfo>>,
    /// One decided-value sync at a time — shared by the driver's SyncNeeded
    /// arm and the forward-receive lag kick (a forward targeting a height
    /// above ours proves peers decided past us; waiting for timeout-driven
    /// republish costs seconds).
    pub sync_inflight: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// True while the consensus shell thread runs (cleared on any shell
    /// exit). False = the zombie shape of the 2026-08-17 wedge — HTTP up,
    /// chain dead — and /health must not report Ready.
    pub running: std::sync::Arc<std::sync::atomic::AtomicBool>,
}
