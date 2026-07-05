//! Malachite-engine adapters (migration Stage 4).
//!
//! Everything here is ADDITIVE: the bespoke engine stays live and nothing in
//! `main.rs` spawns these components until the Stage-5 cutover. The pieces:
//!
//! - [`app`]: `HopNetApplication` — the `hopnet_consensus::Application` impl
//!   over the `DISPATCH_TABLE`, plus the proposer's value builder.
//! - [`gossip`]: fire-and-forget publish of consensus messages over the
//!   existing `IrohTransport`, and the standalone accept loop that feeds the
//!   consensus shell (merged into `net::handler` at Stage 5).
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
}
