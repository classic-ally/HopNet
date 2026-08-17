//! Production tokio shell around the sans-io [`HostCore`].
//!
//! The core is `!Send` (the engine's generator state), so the shell runs it on
//! a DEDICATED thread with a `current_thread` runtime; the seams are built
//! inside that thread by a `Send` builder closure. Everything crossing the
//! thread boundary is a channel:
//!
//! - inbound:  [`HostInput`] (network messages, built values, sync values)
//! - outbound: gossip broadcasts (`WireConsensusMsg` on the sender you pass),
//!   [`HostEvent`]s (value requests, sync triggers), and a `watch` of the
//!   last decided height.
//!
//! Timers are a `tokio_util` `DelayQueue` with the same lazy-cancellation
//! generation scheme the simulator uses: `schedule` bumps a generation,
//! expiry is honoured only if that generation is still armed. The code paths
//! the deterministic fuzzer exercised in `HostCore` are exactly the ones
//! running here — the shell adds clock and channels, no protocol logic.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use malachitebft_core_types::{LinearTimeouts, Round, Timeout};
use tokio::sync::{mpsc, watch};
use tokio_util::time::DelayQueue;

use crate::codec::{WireCommitCertificate, WireConsensusMsg};
use crate::context::Height;
use crate::host::{peer_id_for_node, HostCore, HostOutput};
use crate::traits::{Application, Gossip, Storage, Timers};
use crate::types::Block;

// ---------------------------------------------------------------------------
// Seams

/// Gossip seam: forwards every broadcast onto an unbounded channel. The
/// embedding application drains it with a publisher task (fire-and-forget
/// per-peer sends in production).
#[derive(Clone)]
pub struct ShellGossip {
    tx: mpsc::UnboundedSender<WireConsensusMsg>,
}

impl Gossip for ShellGossip {
    fn broadcast(&mut self, msg: &WireConsensusMsg) {
        // Receiver dropped = shutting down; nothing useful to do with the msg.
        let _ = self.tx.send(msg.clone());
    }
}

/// Generation-tracked timers (single-threaded; the shell thread owns all
/// clones). `schedule` arms a new generation and records it for the shell to
/// insert into the DelayQueue; cancellation is lazy — a fired entry is
/// honoured only if its generation is still the armed one.
#[derive(Clone, Default)]
pub struct ShellTimers {
    armed: Rc<RefCell<HashMap<Timeout, u64>>>,
    pending: Rc<RefCell<Vec<(Timeout, u64)>>>,
    next_gen: Rc<RefCell<u64>>,
}

impl ShellTimers {
    fn take_pending(&self) -> Vec<(Timeout, u64)> {
        std::mem::take(&mut self.pending.borrow_mut())
    }
    fn is_current(&self, timeout: &Timeout, gen: u64) -> bool {
        self.armed.borrow().get(timeout) == Some(&gen)
    }
    fn disarm(&self, timeout: &Timeout) {
        self.armed.borrow_mut().remove(timeout);
    }
}

impl Timers for ShellTimers {
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
// Channel types

/// Inputs the embedding application feeds the shell.
#[derive(Debug)]
pub enum HostInput {
    /// A consensus message received from `from` (node_id) over the network.
    Wire {
        from: i32,
        msg: WireConsensusMsg,
    },
    /// The value built in response to [`HostEvent::NeedValue`].
    Propose {
        height: Height,
        round: Round,
        block: Block,
    },
    /// A decided (block, certificate) pair fetched by the sync client. Feed
    /// lowest height first; each one decides and advances the engine.
    SyncValue {
        peer_node: i32,
        block: Block,
        cert: WireCommitCertificate,
    },
    /// Wake signal (on-demand heights): local work is staged — start the
    /// pending height so the engine requests a value. Idempotent; a no-op
    /// when a height is already running.
    Resume,
    Shutdown,
}

/// Signals the shell surfaces for the embedding application.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostEvent {
    /// We are the proposer for (height, round): build a value and send
    /// [`HostInput::Propose`].
    NeedValue { height: Height, round: Round },
    /// A peer's message is ahead of our height — trigger decided-value sync
    /// toward `target`, starting with `hint_peer`.
    SyncNeeded { target: Height, hint_peer: i32 },
    /// A synced value at `height` did not apply; try another peer.
    SyncInvalid {
        peer_node: Option<i32>,
        height: Height,
    },
}

/// The engine's current round position — updated on every StartRound. Lets
/// the embedding application route transaction forwarding to the proposer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoundInfo {
    pub height: u64,
    pub round: u32,
    /// node_id of the proposer for this (height, round).
    pub proposer: i32,
}

/// The application's handle to a running consensus shell.
pub struct ConsensusHandle {
    pub input_tx: mpsc::Sender<HostInput>,
    /// Last decided height (0 until the first decide).
    pub decided: watch::Receiver<u64>,
    /// Current (height, round, proposer). `None` until the first StartRound.
    pub round: watch::Receiver<Option<RoundInfo>>,
    /// Event stream (single consumer — move into your driver task).
    pub events: mpsc::UnboundedReceiver<HostEvent>,
    /// True while the shell thread's run loop is alive; cleared on EVERY exit
    /// (clean shutdown, start failure, unwind). Fatal host errors abort the
    /// process instead of clearing it. A node whose flag is false must not
    /// report itself healthy — it serves HTTP but does not participate in
    /// consensus.
    pub running: Arc<AtomicBool>,
}

// ---------------------------------------------------------------------------
// Shell

/// Spawn the consensus host on a dedicated thread.
///
/// `build` constructs the core INSIDE the shell thread (the core is !Send)
/// from the two seams the shell owns; everything else it captures must be
/// `Send` (storage connection, application, keys). `outbound` receives every
/// gossip broadcast. `start_height` is `last_decided + 1` (WAL replay happens
/// inside `start_height`).
pub fn spawn<A, S, F>(
    build: F,
    start_height: Height,
    timeouts: LinearTimeouts,
    outbound: mpsc::UnboundedSender<WireConsensusMsg>,
) -> ConsensusHandle
where
    S: Storage + 'static,
    A: Application<S> + 'static,
    F: FnOnce(ShellGossip, ShellTimers) -> HostCore<A, S, ShellGossip, ShellTimers>
        + Send
        + 'static,
{
    let (input_tx, input_rx) = mpsc::channel::<HostInput>(256);
    let (event_tx, event_rx) = mpsc::unbounded_channel::<HostEvent>();
    let (decided_tx, decided_rx) = watch::channel(0u64);
    let (round_tx, round_rx) = watch::channel(None::<RoundInfo>);
    let running = Arc::new(AtomicBool::new(true));
    let running_in_thread = running.clone();

    std::thread::Builder::new()
        .name("hopnet-consensus".into())
        .spawn(move || {
            /// Clears the liveness flag on every exit path — clean shutdown,
            /// start failure, and unwind (drops before catch_unwind returns;
            /// harmless ahead of an abort()).
            struct RunningGuard(Arc<AtomicBool>);
            impl Drop for RunningGuard {
                fn drop(&mut self) {
                    self.0.store(false, Ordering::SeqCst);
                }
            }
            let _running = RunningGuard(running_in_thread);
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .expect("consensus shell runtime");
            let run = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                rt.block_on(run_shell(
                    build,
                    start_height,
                    timeouts,
                    outbound,
                    input_rx,
                    event_tx,
                    decided_tx,
                    round_tx,
                ));
            }));
            if run.is_err() {
                // A node without its consensus shell is a zombie (HTTP up,
                // chain dead) — crash loudly so supervision restarts it.
                tracing::error!("consensus shell panicked — aborting process");
                std::process::abort();
            }
        })
        .expect("spawn consensus shell thread");

    ConsensusHandle {
        input_tx,
        decided: decided_rx,
        round: round_rx,
        events: event_rx,
        running,
    }
}

/// What the shell loop does with one step result. Extracted pure so the
/// fatal mapping is unit-testable; `abort()` itself stays at the call site
/// (untestable in-process by construction).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StepDisposition {
    Continue,
    CleanShutdown,
    /// Storage/host-invariant failure. The process must die loudly so
    /// supervision restarts it — a shell-less node is a zombie (HTTP up,
    /// chain dead). Transient contention is classified at the validation
    /// sites (`StoreError::is_transient`) and never reaches here.
    Fatal,
}

fn disposition(step: &Result<bool, String>) -> StepDisposition {
    match step {
        Ok(true) => StepDisposition::Continue,
        Ok(false) => StepDisposition::CleanShutdown,
        Err(_) => StepDisposition::Fatal,
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_shell<A, S, F>(
    build: F,
    start_height: Height,
    timeouts: LinearTimeouts,
    outbound: mpsc::UnboundedSender<WireConsensusMsg>,
    mut input_rx: mpsc::Receiver<HostInput>,
    event_tx: mpsc::UnboundedSender<HostEvent>,
    decided_tx: watch::Sender<u64>,
    round_tx: watch::Sender<Option<RoundInfo>>,
) where
    S: Storage,
    A: Application<S>,
    F: FnOnce(ShellGossip, ShellTimers) -> HostCore<A, S, ShellGossip, ShellTimers>,
{
    let timers = ShellTimers::default();
    let gossip = ShellGossip { tx: outbound };
    let mut core = build(gossip, timers.clone());
    let mut dq: DelayQueue<(Timeout, u64)> = DelayQueue::new();

    if start_height.0 > 1 {
        decided_tx.send_replace(start_height.0 - 1);
    }
    // On-demand cores defer the boot start unless the pending height has WAL
    // state (active at crash time — must replay, never pause).
    match core.start_or_defer(start_height) {
        Ok(started) => {
            if !started {
                tracing::debug!(height = start_height.0, "consensus paused pending work");
            }
        }
        Err(e) => {
            tracing::error!("consensus start_height failed: {e:?}");
            return;
        }
    }
    drain(
        &mut core,
        &timers,
        &mut dq,
        &timeouts,
        &event_tx,
        &decided_tx,
        &round_tx,
    );

    loop {
        let step: Result<bool, String> = tokio::select! {
            biased;

            expired = std::future::poll_fn(|cx| dq.poll_expired(cx)), if !dq.is_empty() => {
                if let Some(exp) = expired {
                    let (timeout, gen) = exp.into_inner();
                    if timers.is_current(&timeout, gen) {
                        timers.disarm(&timeout);
                        core.on_timeout(timeout).map(|_| true).map_err(|e| format!("{e:?}"))
                    } else {
                        Ok(true) // superseded or cancelled
                    }
                } else {
                    Ok(true)
                }
            }

            maybe = input_rx.recv() => match maybe {
                None | Some(HostInput::Shutdown) => Ok(false),
                Some(HostInput::Wire { from, msg }) => {
                    // Wake rule (on-demand): a peer message at (or past) the
                    // pending height proves the mesh is active — resume
                    // before feeding so the engine can process it.
                    let msg_height = msg.height();
                    let wake = match core.paused_at() {
                        Some(pending) if msg_height >= pending.0 => {
                            core.resume_height().map_err(|e| format!("{e:?}"))
                        }
                        _ => Ok(()),
                    };
                    // Lag detection: a message from a future height means we
                    // missed decides — kick the sync client before feeding
                    // (the engine buffers/drops what it can't use yet).
                    if msg_height > core.height().0 {
                        let _ = event_tx.send(HostEvent::SyncNeeded {
                            target: Height(msg_height),
                            hint_peer: from,
                        });
                    }
                    wake.and_then(|_| {
                        core.on_wire(msg).map(|_| true).map_err(|e| format!("{e:?}"))
                    })
                }
                Some(HostInput::Propose { height, round, block }) => {
                    core.propose(height, round, block).map(|_| true).map_err(|e| format!("{e:?}"))
                }
                Some(HostInput::SyncValue { peer_node, block, cert }) => {
                    // Sync feeds strictly at the engine's current height; a
                    // paused engine must resume its pending height first (in
                    // on-demand mode every synced decide re-pauses).
                    let wake = match core.paused_at() {
                        Some(pending) if block.data.height >= pending.0 => {
                            core.resume_height().map_err(|e| format!("{e:?}"))
                        }
                        _ => Ok(()),
                    };
                    wake.and_then(|_| {
                        core.on_sync_value(peer_id_for_node(peer_node), block, &cert)
                            .map(|_| true).map_err(|e| format!("{e:?}"))
                    })
                }
                Some(HostInput::Resume) => {
                    core.resume_height().map(|_| true).map_err(|e| format!("{e:?}"))
                }
            },
        };

        match disposition(&step) {
            StepDisposition::Continue => {}
            StepDisposition::CleanShutdown => break,
            StepDisposition::Fatal => {
                // Only storage/host-invariant failures reach here (engine and
                // codec errors are handled leniently inside the core, and
                // transient contention is classified into verdicts at the
                // validation sites). A durability failure is fatal for
                // consensus participation — abort so supervision restarts the
                // process, instead of the 2026-08-17 zombie: a `break` here
                // exited the loop cleanly, the panic guard never fired, and
                // the node served HTTP for 42 minutes with a dead chain.
                tracing::error!(
                    "consensus host fatal error — aborting process: {}",
                    step.unwrap_err()
                );
                std::process::abort();
            }
        }

        drain(
            &mut core,
            &timers,
            &mut dq,
            &timeouts,
            &event_tx,
            &decided_tx,
            &round_tx,
        );
    }
    tracing::info!("consensus shell stopped");
}

/// After every step: surface outputs and arm newly-scheduled timers.
fn drain<A, S>(
    core: &mut HostCore<A, S, ShellGossip, ShellTimers>,
    timers: &ShellTimers,
    dq: &mut DelayQueue<(Timeout, u64)>,
    timeouts: &LinearTimeouts,
    event_tx: &mpsc::UnboundedSender<HostEvent>,
    decided_tx: &watch::Sender<u64>,
    round_tx: &watch::Sender<Option<RoundInfo>>,
) where
    S: Storage,
    A: Application<S>,
{
    for out in core.take_outputs() {
        match out {
            HostOutput::NeedValue { height, round } => {
                let _ = event_tx.send(HostEvent::NeedValue { height, round });
            }
            HostOutput::RoundStarted {
                height,
                round,
                proposer,
            } => {
                round_tx.send_replace(Some(RoundInfo {
                    height: height.0,
                    round: round.as_u32().unwrap_or(0),
                    proposer: proposer.0,
                }));
            }
            HostOutput::Decided { height } => {
                decided_tx.send_replace(height.0);
            }
            HostOutput::SyncInvalid { peer, height } => {
                let _ = event_tx.send(HostEvent::SyncInvalid {
                    peer_node: crate::host::node_id_from_peer(&peer),
                    height,
                });
            }
        }
    }
    for (timeout, gen) in timers.take_pending() {
        dq.insert((timeout, gen), timeouts.duration_for(timeout));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Impact: the 2026-08-17 zombie — a fatal step result mapped to a loop
    // break, so run_shell returned cleanly, the panic guard never fired, and
    // the wedged node kept answering /health Ready. The seam pins that a
    // host error is Fatal (process abort at the call site), never a break.
    // Should: map a step error to Fatal, a false step to CleanShutdown, and
    // a true step to Continue.
    // Should not: give an error result any disposition other than Fatal.
    #[test]
    fn step_error_is_fatal_not_a_clean_stop() {
        assert_eq!(disposition(&Ok(true)), StepDisposition::Continue);
        assert_eq!(disposition(&Ok(false)), StepDisposition::CleanShutdown);
        assert_eq!(
            disposition(&Err("Storage(Db(SqliteFailure(..)))".into())),
            StepDisposition::Fatal
        );
    }
}
