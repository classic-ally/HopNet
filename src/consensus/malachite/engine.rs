//! `spawn_engine`: production wiring of the Malachite consensus stack.
//!
//! Called from the three initialization paths (main.rs restart, genesis
//! setup, join bootstrap) once the node has an identity and an installed
//! consensus genesis. Spawns: the gossip publisher, the shell (HostCore over
//! HopNetApplication + a pool-reserved SqliteStorage), and the driver task
//! that answers the shell's events:
//!
//! - `NeedValue`  → drain the queue's PendingPool → `build_value` → Propose
//! - `SyncNeeded` → decided-value sync client (deduplicated)
//! - PendingPool work signal → `HostInput::Resume` (on-demand heights)
//! - decided watch → settle PendingPool notifiers by committed nonce
//!
//! The engine runs ON-DEMAND: an idle mesh holds StartHeight until work is
//! staged locally or a peer message arrives at the pending height.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::mpsc;

use hopnet_consensus::config::{MalachiteThresholds, QuorumProfile};
use hopnet_consensus::context::{Address, Height};
use hopnet_consensus::host::HostCore;
use hopnet_consensus::shell::{self, HostEvent, HostInput};
use hopnet_consensus::store::{self, SqliteStorage};
use hopnet_consensus::traits::Application;
use hopnet_consensus::types::Blake3Hash;
use hopnet_consensus::{HopNetContext, LinearTimeouts, Params, ValuePayload};

use super::app::{HopNetApplication, build_value};
use super::{EngineHandle, gossip, sync};
use crate::AppState;

type PoolStorage = SqliteStorage<r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>>;

/// Mirror of the `before_decide` barrier's held flag (see the mirror task in
/// `spawn_engine`) — the commit callback spins on it from the shell thread.
static DECIDE_GATE_HELD: AtomicBool = AtomicBool::new(false);

/// Test-mode commit callback: hold every decide's DB commit while the
/// `before_decide` barrier is held, then commit through the timed path.
fn commit_gated(tx: rusqlite::Transaction<'_>) -> rusqlite::Result<()> {
    while DECIDE_GATE_HELD.load(Ordering::SeqCst) {
        std::thread::sleep(Duration::from_millis(5));
    }
    crate::db::shared::commit_timed(tx)
}

/// Max transactions per proposal — matches the queue's historical batch cap.
const MAX_BATCH_SIZE: usize = 100;
/// Linger before building a value, letting forwarded transactions land in
/// the pool (the bespoke leader lingered the same way).
const BATCH_LINGER_MS: u64 = 100;
/// Extra wait when NeedValue finds an empty pool — covers the resume-vs-
/// staging race (forward-wake fires before the forwarded txs are staged).
/// Well under one propose timeout.
const EMPTY_POOL_LINGER_MS: u64 = 500;
/// Inject a `system.cleanup_nonces` transaction every N heights (was: views).
const NONCE_CLEANUP_INTERVAL: u64 = 97;

/// Consensus timeouts, optionally scaled by `HOPNET_CONSENSUS_TIMEOUT_MS`
/// (the round-0 propose timeout in milliseconds; votes and per-round deltas
/// scale proportionally). Orchestrator tests use small values so leader-down
/// round advances happen in seconds instead of the default 3s+.
fn consensus_timeouts() -> LinearTimeouts {
    match std::env::var("HOPNET_CONSENSUS_TIMEOUT_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
    {
        Some(ms) => {
            let propose = Duration::from_millis(ms);
            let vote = Duration::from_millis((ms / 3).max(100));
            let delta = Duration::from_millis((ms / 6).max(50));
            LinearTimeouts {
                propose,
                propose_delta: delta,
                prevote: vote,
                prevote_delta: delta,
                precommit: vote,
                precommit_delta: delta,
                rebroadcast: propose + vote + vote,
            }
        }
        None => LinearTimeouts::default(),
    }
}

/// The current proposal target: `(height, round, proposer node_id)`.
///
/// While a height is running this is the engine's live round info. While
/// PAUSED (on-demand heights — the round watch lags the decided watch) it is
/// the deterministically-computed round-0 proposer of the pending height:
/// `validators[(height + round) % n]` with the valset sorted node_id-asc
/// (uniform voting power makes the crate's power-desc/address-asc sort
/// degenerate to exactly that).
///
/// `None` when the engine isn't running or the validator set is unreadable.
pub fn proposal_target(app_state: &AppState) -> Option<(u64, u32, i32)> {
    let engine = app_state.malachite.get()?;
    let decided = *engine.decided.borrow();
    if let Some(ri) = *engine.round.borrow()
        && ri.height > decided
    {
        return Some((ri.height, ri.round, ri.proposer));
    }
    let pending = decided + 1;
    let conn = app_state.db_pool.get().ok()?;
    let validators = crate::db::consensus::get_validators_with_conn(&conn, pending).ok()?;
    if validators.is_empty() {
        return None;
    }
    let mut ids: Vec<i32> = validators.iter().map(|n| n.node_id).collect();
    ids.sort_unstable();
    let idx = (pending as usize) % ids.len();
    Some((pending, 0, ids[idx]))
}

/// Wire and start the consensus engine. Idempotent — a second call is a
/// no-op. Requires: node identity set (node_id) and the consensus genesis
/// installed (`consensus_meta` populated).
pub fn spawn_engine(app_state: &AppState) -> Result<(), String> {
    if app_state.malachite.get().is_some() {
        return Ok(());
    }
    let node_id = app_state
        .get_node_id()
        .map_err(|_| "spawn_engine: node identity not initialized".to_string())?;

    // --- Engine config from consensus_meta -------------------------------
    let (start_height, chain_id, profile) = {
        let conn = app_state
            .db_pool
            .get()
            .map_err(|e| format!("spawn_engine: pool: {e}"))?;
        let last_decided = store::last_decided_height(&conn)
            .map_err(|e| format!("spawn_engine: {e}"))?
            .ok_or("spawn_engine: no consensus genesis installed")?;
        // RFC-019 seal contract: a sealed epoch never restarts its engine —
        // the node parks awaiting the S6 restart path. The artifact is
        // recomputed on wake if a crash interrupted the seal work
        // (idempotent from sealed local state).
        if let Some(sealed_at) = crate::regenesis::seal::sealed_marker(&conn) {
            tracing::warn!(
                sealed_at,
                "epoch is sealed: consensus engine will not start (awaiting restart)"
            );
            let recompute_state = app_state.clone();
            std::thread::spawn(move || {
                if !crate::regenesis::seal::artifact_path().exists()
                    && let Err(e) = crate::regenesis::seal::write_seal_artifact_to(
                        &recompute_state,
                        &crate::regenesis::seal::artifact_path(),
                    )
                {
                    tracing::error!("seal artifact recompute failed: {e}");
                }
            });
            return Ok(());
        }
        let chain_bytes = store::meta_get(&conn, store::META_CHAIN_ID)
            .map_err(|e| format!("spawn_engine: {e}"))?
            .ok_or("spawn_engine: no chain id in consensus_meta")?;
        let chain_id = Blake3Hash::from_bytes(
            chain_bytes
                .as_slice()
                .try_into()
                .map_err(|_| "spawn_engine: malformed chain id".to_string())?,
        );
        let profile = match store::meta_get(&conn, store::META_QUORUM_PROFILE)
            .map_err(|e| format!("spawn_engine: {e}"))?
        {
            Some(bytes) => {
                let s = String::from_utf8(bytes)
                    .map_err(|_| "spawn_engine: malformed quorum profile".to_string())?;
                QuorumProfile::parse(&s)
                    .ok_or_else(|| format!("spawn_engine: unknown quorum profile {s:?}"))?
            }
            None => QuorumProfile::Auto,
        };
        (Height(last_decided.0 + 1), chain_id, profile)
    };

    // --- Publisher --------------------------------------------------------
    // Runs on the dedicated net runtime: outbound votes/proposals must keep
    // flowing when API load starves the main runtime (peer refresh inside is
    // already spawn_blocking; per-peer send tasks inherit the net runtime).
    let (out_tx, out_rx) = mpsc::unbounded_channel();
    hopnet_comms::net_rt().spawn(gossip::run_publisher(app_state.clone(), node_id, out_rx));

    // --- before_decide commit gate (test_mode) -----------------------------
    // The decide commit runs on the !Send shell thread where async barrier
    // waits can't reach; a mirror task reflects the barrier's held flag into
    // a process-wide gate the commit callback spins on. One engine per
    // process, so a global is accurate.
    if app_state.test_mode {
        let barriers = app_state.consensus_barriers.clone();
        // Queue runtime: the mirror must stay live under load, or barrier
        // state observed by the commit gate goes stale.
        crate::consensus::queue::queue_rt().spawn(async move {
            loop {
                let held = barriers
                    .status(crate::consensus::barriers::names::BEFORE_DECIDE)
                    .map(|s| s.held)
                    .unwrap_or(false);
                DECIDE_GATE_HELD.store(held, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });
    }

    // --- Shell (dedicated thread; storage conn reserved for its lifetime) --
    let storage_conn = app_state
        .db_pool
        .get()
        .map_err(|e| format!("spawn_engine: storage conn: {e}"))?;
    // Second dedicated conn: the application's shell-thread reads (validator
    // sets). Checked out at spawn (quiet startup), held for the engine's
    // lifetime — the shell must never race the pool.
    let app_conn = app_state
        .db_pool
        .get()
        .map_err(|e| format!("spawn_engine: app conn: {e}"))?;
    let commit_fn: hopnet_consensus::store::CommitFn = if app_state.test_mode {
        commit_gated
    } else {
        crate::db::shared::commit_timed
    };
    let signer = hopnet_consensus::types::PrivKey(app_state.private_key.0.clone());
    let params = Params {
        address: Address(node_id),
        threshold_params: profile.thresholds_for(1), // placeholder — HostCore overwrites from the boot valset
        value_payload: ValuePayload::PartsOnly,
        enabled: true,
    };
    let app_state_for_core = app_state.clone();
    let handle = shell::spawn(
        move |gossip_seam, timers| {
            let storage =
                PoolStorage::from_handle(storage_conn, commit_fn).expect("consensus storage");
            let mut app = HopNetApplication::new(app_state_for_core, app_conn);
            let valset = <HopNetApplication as Application<PoolStorage>>::validator_set(
                &mut app,
                start_height,
            );
            HostCore::new(
                chain_id,
                signer,
                Address(node_id),
                profile,
                params,
                start_height,
                valset,
                app,
                storage,
                gossip_seam,
                timers,
            )
            .on_demand()
        },
        start_height,
        consensus_timeouts(),
        out_tx,
    );

    let shell::ConsensusHandle {
        input_tx,
        decided,
        round,
        events,
    } = handle;

    // --- Settler: resolve queue notifiers as heights decide ---------------
    // Queue runtime: settlement must keep pace with decides under API load.
    {
        let pool = app_state.consensus_queue.pending_pool();
        let db_pool = app_state.db_pool.clone();
        let mut decided_watch = decided.clone();
        crate::consensus::queue::queue_rt().spawn(async move {
            loop {
                // Wake on a new decide OR a settle kick (remote-committed
                // entries staged after the decide that applied their block
                // — RFC-018 S6).
                tokio::select! {
                    changed = decided_watch.changed() => {
                        if changed.is_err() {
                            break;
                        }
                    }
                    _ = pool.settle_kicked() => {}
                }
                let h = *decided_watch.borrow_and_update();
                // Retry on pool contention: a skipped settle would orphan the
                // notifiers of committed txs (clients hang to timeout). The
                // FINAL decide of a burst has no later settle to catch them.
                loop {
                    match db_pool.get() {
                        Ok(conn) => {
                            pool.settle(&conn, h);
                            break;
                        }
                        Err(e) => {
                            tracing::warn!("settler: pool conn (retrying): {e}");
                            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                        }
                    }
                    // A newer decide supersedes this settle (nonces are
                    // cumulative) — bail to pick up the latest height.
                    if *decided_watch.borrow() != h {
                        break;
                    }
                }
            }
        });
    }

    // --- Driver ------------------------------------------------------------
    // Third dedicated conn: proposal builds (see handle_need_value).
    let build_conn = app_state
        .db_pool
        .get()
        .map_err(|e| format!("spawn_engine: build conn: {e}"))?;
    let sync_inflight = Arc::new(AtomicBool::new(false));
    spawn_driver(
        app_state.clone(),
        node_id,
        input_tx.clone(),
        decided.clone(),
        events,
        build_conn,
        sync_inflight.clone(),
    );

    // Storage distribution engine (RFC-014): behind the host seams. Data
    // plane on the caller's (main) runtime; placement batcher on queue_rt.
    crate::storage_host::substrate_host::spawn_storage_engine(app_state);

    // Regenesis drain watcher (RFC-019 S5): wakes the on-demand engine to
    // propose the commit once a moratorium drains — including after a
    // crash mid-moratorium, since this spawns on every engine start.
    spawn_drain_watcher(
        app_state.clone(),
        input_tx.clone(),
        app_state.consensus_queue.pending_pool(),
    );

    // Rollback-window cleanup (RFC-019 S6): a freshly transitioned epoch
    // retains the sealed epoch-N database until its own first decide.
    spawn_rollback_cleanup(app_state.clone(), decided.clone());

    let engine = EngineHandle {
        input_tx,
        decided,
        round,
        sync_inflight,
    };
    app_state
        .malachite
        .set(engine)
        .map_err(|_| "spawn_engine: raced another spawn".to_string())?;

    // Non-validator tip-poll (RFC-CONSENSUS-002 S1): a node outside the
    // valset receives no consensus gossip (gossip::peers is valset-only),
    // so it polls peers' decided tips and feeds sync. Cheap when seated:
    // one indexed validators lookup per tick, then sleep.
    spawn_tip_poll(app_state.clone());

    // Evidence probe scheduler (RFC-CONSENSUS-002 S3): the deadline scan
    // that keeps every registered peer's evidence bounded-stale.
    crate::consensus::evidence::spawn_probe_scheduler(app_state.clone());

    tracing::info!(
        start_height = start_height.0,
        profile = profile.as_str(),
        "malachite engine started (on-demand heights)"
    );
    Ok(())
}

/// Base cadence of the non-validator tip-poll; jittered ±20% so a departed
/// cohort doesn't poll in lockstep.
const TIP_POLL_BASE: Duration = Duration::from_secs(5);

/// True when a sync ended because the mesh is in a NEWER epoch. There is
/// nothing to retry in that case — no amount of block sync crosses a
/// boundary — so this starts the epoch join instead (RFC-019 S7) and the
/// caller skips its usual failure logging.
fn pivot_on_epoch_ahead(app_state: &AppState, e: &sync::SyncError) -> bool {
    let sync::SyncError::EpochAhead { peer, peer_epoch } = e else {
        return false;
    };
    tracing::info!(
        peer,
        peer_epoch,
        "sync refused: the mesh crossed an epoch boundary this node missed — starting epoch join"
    );
    let Ok(node_id) = app_state.get_node_id() else {
        return true;
    };
    // Hint the peer that answered: it demonstrably holds the new epoch.
    let peers = sync::peer_list(&app_state.db_pool, node_id, Some(*peer));
    crate::regenesis::join::spawn_epoch_join(
        app_state,
        crate::regenesis::join::JoinAnchor::OwnDb,
        peers,
    );
    true
}

pub(crate) fn spawn_tip_poll(app_state: AppState) {
    crate::consensus::queue::queue_rt().spawn(async move {
        loop {
            let jitter_ns = (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.subsec_nanos())
                .unwrap_or(0)
                % 1000) as f64
                / 1000.0;
            let delay = TIP_POLL_BASE.mul_f64(0.8 + 0.4 * jitter_ns);
            tokio::time::sleep(delay).await;

            let Some(engine) = app_state.malachite.get() else {
                continue;
            };
            let Ok(node_id) = app_state.get_node_id() else {
                continue;
            };
            let pending = engine.decided.borrow().saturating_add(1);
            let am_active = {
                let Ok(conn) = app_state.db_pool.get() else {
                    continue;
                };
                // Fail closed: on a query error assume seated and skip —
                // gossip is then feeding us anyway or the node is broken
                // in ways a poll won't fix.
                hopnet_consensus::validators::is_node_active(&conn, node_id, pending)
                    .unwrap_or(true)
            };
            if am_active {
                // Seated nodes normally skip: live gossip feeds them. ONE
                // exception (RFC-019 S5): during a regenesis boundary the
                // mesh may have already sealed and gone SILENT — a seated
                // straggler that missed the final block would otherwise
                // churn rounds at the boundary height forever, with nothing
                // pushing the seal to it. Poll while the committed phase is
                // not normal; the pulled commit block seals this node too
                // (Sync-origin validation skips vote-iff-match by design).
                let boundary_in_flight = app_state
                    .db_pool
                    .get()
                    .ok()
                    .and_then(|c| crate::db::regenesis::read_regenesis_state(&c).ok())
                    .is_some_and(|s| s.phase != crate::db::regenesis::RegenesisPhase::Normal);
                if !boundary_in_flight {
                    continue;
                }
            }
            if engine.sync_inflight.swap(true, Ordering::SeqCst) {
                continue; // one sync at a time
            }
            let peers = sync::peer_list(&app_state.db_pool, node_id, None);
            let mut decided = engine.decided.clone();
            if let Err(e) = sync::sync_to_tip(
                &app_state.comms,
                app_state.epoch.load(Ordering::Relaxed),
                &engine.input_tx,
                &mut decided,
                &peers,
                Some(app_state.evidence.clone()),
            )
            .await
                && !pivot_on_epoch_ahead(&app_state, &e)
            {
                tracing::debug!("tip-poll sync: {e:?}");
            }
            engine.sync_inflight.store(false, Ordering::SeqCst);
        }
    });
}

/// Join bootstrap: bring a freshly-initialized node (this_node only — empty
/// validators/nodes tables) into the mesh.
///
/// 1. Fetch the genesis pair from a bootstrap validator and install it as
///    TRUSTED (the synthetic certificate has no signatures; there is no
///    validator set before genesis creates one) — the genesis transaction
///    replays through the dispatch table in the same DB transaction.
/// 2. Start the engine (it now has a valset and a chain id).
/// 3. Decided-value-sync to the mesh tip — every post-genesis certificate is
///    engine-verified against the validator sets genesis established.
///
/// Returns the tip height reached. Activation is the caller's next step.
pub async fn bootstrap_join(
    app_state: &AppState,
    join_info: &crate::types::JoinInfo,
) -> Result<u64, String> {
    let peers: Vec<hopnet_comms::PeerRef> = join_info
        .bootstrap_validators
        .iter()
        .map(|n| hopnet_comms::PeerRef {
            node_id: n.node_id,
            pubkey: n.pubkey.0.to_bytes(),
        })
        .collect();

    let already_installed = {
        let conn = app_state.db_pool.get().map_err(|e| e.to_string())?;
        store::last_decided_height(&conn)
            .map_err(|e| e.to_string())?
            .is_some()
    };

    if !already_installed && join_info.epoch >= 2 {
        // The mesh is past its first epoch: the trusted height-0 genesis
        // no longer exists to fetch. Verify the lineage chain and import
        // the boundary snapshot instead — epoch join SUBSUMES the
        // height-0 bootstrap (RFC-019 S7). In process: the database holds
        // only this_node, so there is nothing to swap.
        let data_dir = std::path::Path::new(&crate::db::shared::get_database_path())
            .parent()
            .map(|p| p.to_path_buf())
            .ok_or("database path has no parent directory")?;
        crate::regenesis::join::epoch_join_bootstrap(
            app_state,
            &data_dir,
            join_info.epoch,
            &peers,
            crate::version::effective_running_code(),
        )
        .await
        .map_err(|e| format!("epoch join: {e}"))?;
    } else if !already_installed {
        let profile = QuorumProfile::parse(&join_info.quorum_profile)
            .ok_or_else(|| format!("unknown quorum profile {:?}", join_info.quorum_profile))?;

        let (block, cert) = sync::fetch_genesis(
            &app_state.comms,
            app_state.epoch.load(std::sync::atomic::Ordering::Relaxed),
            &peers,
        )
        .await
        .map_err(|e| format!("genesis fetch: {e}"))?;

        let old_txs = super::app::to_old_transactions(&block.data.transactions)
            .map_err(|e| format!("genesis bridge: {e}"))?;

        let mut conn = app_state.db_pool.get().map_err(|e| e.to_string())?;
        let tx_db = conn.transaction().map_err(|e| e.to_string())?;
        // Same shape as post_initial_setup: process_transaction directly (no
        // nonce insert — the genesis node has no nonce row either, keeping
        // committed_tx_nonces byte-identical across the mesh).
        for t in old_txs.iter() {
            crate::consensus::dispatch::process_transaction(t, app_state, true, 1, &tx_db)
                .map_err(|e| format!("genesis apply: {e:?}"))?;
        }
        store::install_genesis(&tx_db, &block, &cert).map_err(|e| e.to_string())?;
        store::meta_put(
            &tx_db,
            store::META_CHAIN_ID,
            block.block_hash.as_bytes().as_slice(),
        )
        .map_err(|e| e.to_string())?;
        store::meta_put(
            &tx_db,
            store::META_QUORUM_PROFILE,
            profile.as_str().as_bytes(),
        )
        .map_err(|e| e.to_string())?;
        crate::db::shared::commit_timed(tx_db).map_err(|e| format!("genesis commit: {e}"))?;
        tracing::info!(
            "join bootstrap: installed trusted genesis (chain_id {:?})",
            block.block_hash
        );
    }

    spawn_engine(app_state)?;
    let engine = app_state
        .malachite
        .get()
        .ok_or("engine missing after spawn")?;

    let mut decided = engine.decided.clone();
    let reached = sync::sync_to_tip(
        &app_state.comms,
        app_state.epoch.load(std::sync::atomic::Ordering::Relaxed),
        &engine.input_tx,
        &mut decided,
        &peers,
        None, // bootstrap: nodes table still filling; evidence starts post-join
    )
    .await
    .map_err(|e| format!("sync to tip: {e:?}"))?;

    tracing::info!("join bootstrap: synced to height {reached}");
    Ok(reached)
}

/// The app-side event loop: answers NeedValue with built proposals, kicks the
/// sync client on SyncNeeded, and forwards the PendingPool's work signal as
/// a Resume (on-demand wake rule 1).
pub(crate) fn spawn_driver(
    app_state: AppState,
    node_id: i32,
    input_tx: mpsc::Sender<HostInput>,
    decided: tokio::sync::watch::Receiver<u64>,
    mut events: mpsc::UnboundedReceiver<HostEvent>,
    build_conn: r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>,
    sync_inflight: Arc<AtomicBool>,
) {
    let pool = app_state.consensus_queue.pending_pool();
    let mut build_conn = Some(build_conn);

    // Queue runtime: the driver answers NeedValue and fires Resume — if it
    // can't get polled, the engine never advances (the image-10 stall).
    crate::consensus::queue::queue_rt().spawn(async move {
        loop {
            tokio::select! {
                maybe = events.recv() => {
                    let Some(ev) = maybe else { break };
                    match ev {
                        HostEvent::NeedValue { height, round } => {
                            handle_need_value(&app_state, &pool, &input_tx, &mut build_conn, height, round).await;
                        }
                        HostEvent::SyncNeeded { target, hint_peer } => {
                            if sync_inflight.swap(true, Ordering::SeqCst) {
                                continue; // one sync at a time
                            }
                            let app_state = app_state.clone();
                            let input_tx = input_tx.clone();
                            let mut decided = decided.clone();
                            let flag = sync_inflight.clone();
                            tokio::spawn(async move {
                                if let Err(e) = sync::sync_to_target(
                                    &app_state.comms,
                                    app_state.epoch.load(std::sync::atomic::Ordering::Relaxed),
                                    &app_state.db_pool,
                                    node_id,
                                    &input_tx,
                                    &mut decided,
                                    target.0,
                                    Some(hint_peer),
                                    Some(app_state.evidence.clone()),
                                )
                                .await
                                    && !pivot_on_epoch_ahead(&app_state, &e)
                                {
                                    tracing::warn!("decided-value sync failed: {e:?}");
                                }
                                flag.store(false, Ordering::SeqCst);
                            });
                        }
                        HostEvent::SyncInvalid { peer_node, height } => {
                            tracing::warn!(
                                ?peer_node,
                                height = height.0,
                                "sync value rejected"
                            );
                        }
                    }
                }
                _ = pool.work_available() => {
                    // On-demand wake rule 1: staged local work starts the
                    // pending height (idempotent when already running).
                    if input_tx.send(HostInput::Resume).await.is_err() {
                        break;
                    }
                }
            }
        }
        tracing::info!("malachite engine driver stopped");
    });
}

/// Committed regenesis phase, or None on any read failure (treated as
/// "don't act" by every caller — hygiene skips, seal proposal skips).
fn committed_regenesis_phase(app_state: &AppState) -> Option<crate::db::regenesis::RegenesisPhase> {
    let conn = app_state.db_pool.get().ok()?;
    crate::db::regenesis::read_regenesis_state(&conn)
        .ok()
        .map(|s| s.phase)
}

/// Build the proposer-injected `regenesis_commit` (RFC-019 S5): recompute
/// the canonical snapshot over ONE transaction snapshot of the build
/// connection, bind it to the pending height, node-sign. Solo by
/// construction — only called when no other candidate exists.
fn seal_candidate(
    app_state: &AppState,
    conn: &mut r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>,
    seal_height: u64,
) -> Result<crate::consensus::types::Transaction, String> {
    let started = std::time::Instant::now();
    let tx_snapshot = conn
        .transaction()
        .map_err(|e| format!("snapshot tx: {e}"))?;
    let artifact_hash = crate::db::snapshot::compute_artifact_hash_tx(&tx_snapshot)
        .map_err(|e| format!("snapshot compute: {e:?}"))?;
    drop(tx_snapshot);
    let mut snapshot_hash = [0u8; 32];
    snapshot_hash.copy_from_slice(artifact_hash.as_bytes());
    tracing::info!(
        seal_height,
        elapsed_ms = started.elapsed().as_millis() as u64,
        artifact_hash = %artifact_hash.to_hex(),
        "regenesis commit candidate: artifact recomputed (OQ1 timing)"
    );
    // The target is already committed (regenesis_start decided it); copying
    // it into the commit is what puts it inside the certified block, where a
    // joiner can bind the lineage record against it.
    let target_version_code = crate::db::regenesis::read_regenesis_state(conn)
        .map_err(|e| format!("regenesis state: {e:?}"))?
        .target_version_code
        .ok_or("proposing a commit with no committed target version")?;
    let payload = bincode::serde::encode_to_vec(
        &crate::regenesis::RegenesisCommit {
            snapshot_hash,
            seal_height,
            target_version_code,
        },
        bincode::config::standard(),
    )
    .map_err(|e| format!("encode: {e}"))?;
    crate::consensus::dispatch::create_signed_transaction(
        app_state,
        "regenesis_commit".to_string(),
        payload,
    )
    .map_err(|e| format!("sign: {e:?}"))
}

/// RFC-019 S5 (OQ4): an empty pool never fires the work signal, so a
/// drained moratorium would leave the on-demand engine paused with nobody
/// to propose the regenesis commit. This watcher nudges the engine while
/// (and only while) the committed phase is MORATORIUM and this node's
/// pool is drained — `Resume` is idempotent, and the nudge stops on its
/// own once the commit decides (phase leaves MORATORIUM). Lives for the
/// engine's lifetime; outside a moratorium it is one cheap singleton
/// read per second.
/// RFC-019 S6 rollback window: delete the retained epoch-N database
/// (`database.db.sealed`) once this epoch decides its first height past
/// the genesis. Exits immediately when nothing is retained — i.e. on
/// every boot except the first after a boundary. Crash-safe: a decide
/// followed by a crash before deletion is caught by the initial check on
/// the next boot's spawn.
pub(crate) fn spawn_rollback_cleanup(
    app_state: crate::AppState,
    mut decided: tokio::sync::watch::Receiver<u64>,
) {
    let retained = crate::regenesis::boot::sealed_path(&crate::db::shared::get_database_path());
    if !retained.exists() {
        return;
    }
    tokio::spawn(async move {
        let genesis_height = {
            let Ok(conn) = app_state.db_pool.get() else {
                tracing::warn!("rollback cleanup: no db conn; retained database kept");
                return;
            };
            match crate::regenesis::genesis::epoch_genesis_height(&conn) {
                Some(h) => h,
                None => {
                    tracing::warn!(
                        retained = %retained.display(),
                        "retained database exists but this epoch has no genesis height meta; kept"
                    );
                    return;
                }
            }
        };
        loop {
            if *decided.borrow() > genesis_height {
                match std::fs::remove_file(&retained) {
                    Ok(()) => tracing::info!(
                        retained = %retained.display(),
                        "rollback window closed: retained epoch database deleted"
                    ),
                    Err(e) => tracing::warn!(
                        retained = %retained.display(),
                        "rollback window cleanup failed: {e}"
                    ),
                }
                return;
            }
            if decided.changed().await.is_err() {
                return;
            }
        }
    });
}

pub(crate) fn spawn_drain_watcher(
    app_state: AppState,
    input_tx: mpsc::Sender<HostInput>,
    pool: Arc<crate::consensus::queue::PendingPool>,
) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(1000)).await;
            if committed_regenesis_phase(&app_state)
                == Some(crate::db::regenesis::RegenesisPhase::Moratorium)
                && pool.staged_len() == 0
                && pool.inflight_len() == 0
                && input_tx.send(HostInput::Resume).await.is_err()
            {
                return; // engine gone
            }
        }
    });
}

/// NeedValue: linger for forwarded transactions, drain the pool, build the
/// block off the async workers (write gate + Immediate-tx preflight), then
/// hand the proposal to the shell. Preflight rejections resolve their
/// submitters immediately; proposed entries park inflight until settle.
async fn handle_need_value(
    app_state: &AppState,
    pool: &Arc<crate::consensus::queue::PendingPool>,
    input_tx: &mpsc::Sender<HostInput>,
    build_conn: &mut Option<r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>>,
    height: Height,
    round: hopnet_consensus::Round,
) {
    if app_state.test_mode {
        app_state
            .consensus_barriers
            .wait(crate::consensus::barriers::names::BEFORE_PROPOSE)
            .await;
    }
    tokio::time::sleep(Duration::from_millis(BATCH_LINGER_MS)).await;

    // Resume can race forwarded-tx staging: an inbound TransactionForward
    // wakes the engine, NeedValue fires at height entry, and the acked txs
    // land in the pool a few hundred ms later — proposing immediately wastes
    // the height on an empty block. If the pool is empty, wait briefly for
    // the work signal before building.
    if pool.staged_len() == 0 {
        let _ = tokio::time::timeout(
            Duration::from_millis(EMPTY_POOL_LINGER_MS),
            pool.work_available(),
        )
        .await;
    }

    let entries = pool.take_for_proposal(MAX_BATCH_SIZE);
    let mut candidates: Vec<crate::consensus::types::Transaction> =
        entries.iter().map(|e| e.transaction().clone()).collect();

    let regenesis_phase = committed_regenesis_phase(app_state);

    // Periodic nonce-table hygiene, appended AFTER the queue entries so
    // candidate indices still line up with `entries`. Suppressed outside
    // the normal phase (RFC-019 S5): this injection bypasses the queue's
    // admission gate, and hygiene must neither dilute the drain nor break
    // the regenesis commit's solo block. On a read error we skip — a
    // missed cleanup is harmless; the next interval retries.
    if height.0.is_multiple_of(NONCE_CLEANUP_INTERVAL)
        && regenesis_phase == Some(crate::db::regenesis::RegenesisPhase::Normal)
    {
        let cutoff_ts = (chrono::Utc::now() - chrono::Duration::hours(1)).timestamp() as u64;
        let cutoff = hopnet_common::CustomUUID::new(Some(&uuid::Timestamp::from_unix(
            uuid::NoContext,
            cutoff_ts,
            0,
        )));
        if let Ok(payload) = bincode::serde::encode_to_vec(&cutoff, bincode::config::standard())
            && let Ok(tx) = crate::consensus::dispatch::create_signed_transaction(
                app_state,
                "system.cleanup_nonces".to_string(),
                payload,
            )
        {
            candidates.push(tx);
        }
    }

    // RFC-019 S5: once the moratorium holds and this node's pool has fully
    // drained (nothing staged, nothing inflight, no candidate this round),
    // the proposer's next block is the regenesis commit — the SOLE
    // transaction of the final block, carrying the snapshot hash it
    // recomputes over the build connection (proposer-injected, queue-
    // bypassing: the cleanup_nonces precedent).
    let propose_seal = regenesis_phase == Some(crate::db::regenesis::RegenesisPhase::Moratorium)
        && candidates.is_empty()
        && pool.inflight_len() == 0;

    // Dedicated build connection: proposal building must never lose a pool
    // checkout race under load (a failed build wastes the whole round — the
    // image-14 finding: "build conn: timed out" → empty heights). The conn is
    // moved through the blocking task and handed back; if a build panics the
    // conn drops back to the pool and the fallback re-checks one out.
    let taken = build_conn.take();
    let build_state = app_state.clone();
    let joined = tokio::task::spawn_blocking(move || {
        let mut conn = match taken {
            Some(conn) => conn,
            None => build_state
                .db_pool
                .get()
                .map_err(|e| format!("build conn: {e}"))?,
        };
        let mut candidates = candidates;
        if propose_seal {
            match seal_candidate(&build_state, &mut conn, height.0) {
                Ok(tx) => candidates.push(tx),
                // A failed recompute wastes this round only — the drain
                // watcher keeps nudging and the next NeedValue retries.
                Err(e) => tracing::warn!("regenesis commit candidate failed: {e}"),
            }
        }
        let result = build_value(&build_state, &mut conn, height, round, candidates);
        Ok::<_, String>((conn, result))
    })
    .await;

    let built = match joined {
        Ok(Ok((conn, result))) => {
            *build_conn = Some(conn);
            result
        }
        Ok(Err(e)) => {
            tracing::error!("build_value setup failed at height {}: {e}", height.0);
            pool.restage(entries);
            return;
        }
        Err(join_err) => {
            tracing::error!("build task panicked: {join_err}");
            pool.restage(entries);
            return;
        }
    };

    match built {
        Ok(built) => {
            // Resolve preflight verdicts for the queue-backed entries.
            // Solo-block deferrals restage (valid txs that may not share a
            // block with a membership transition); deferred indices beyond
            // entries.len() (the appended system candidate) have no pool
            // entry and are naturally dropped.
            let mut rejected_by_idx: std::collections::HashMap<usize, String> =
                built.rejected.into_iter().collect();
            let deferred_idx: std::collections::HashSet<usize> =
                built.deferred.into_iter().collect();
            let mut inflight = Vec::new();
            let mut restage = Vec::new();
            for (i, entry) in entries.into_iter().enumerate() {
                if deferred_idx.contains(&i) {
                    restage.push(entry);
                    continue;
                }
                match rejected_by_idx.remove(&i) {
                    Some(reason) if reason == "already committed" => {
                        // Nonce is in local committed_tx_nonces — applied
                        // here at some height below the one we're proposing.
                        pool.resolve_committed(entry, height.0.saturating_sub(1));
                    }
                    Some(reason) => pool.reject(entry, reason),
                    None => inflight.push(entry),
                }
            }
            if !restage.is_empty() {
                pool.restage(restage);
            }
            pool.mark_inflight(inflight, height.0);

            if input_tx
                .send(HostInput::Propose {
                    height,
                    round,
                    block: built.block,
                })
                .await
                .is_err()
            {
                tracing::error!("shell gone — dropping proposal at height {}", height.0);
            }
        }
        Err(e) => {
            tracing::error!("build_value failed at height {}: {e}", height.0);
            pool.restage(entries);
        }
    }
}

/// Kick a decided-value sync toward `target` because inbound traffic proved
/// peers are ahead (e.g. a TransactionForward for a height above ours).
/// Shares the driver's one-sync-at-a-time flag; no-op while a sync runs or
/// when the engine isn't active. Cuts multi-second timeout-republish waits
/// out of the idle-mesh catch-up path.
pub fn kick_sync_if_behind(app_state: &AppState, target: u64, hint_peer: i32) {
    let Some(engine) = app_state.malachite.get() else {
        return;
    };
    if *engine.decided.borrow() >= target {
        return;
    }
    if engine.sync_inflight.swap(true, Ordering::SeqCst) {
        return; // one sync at a time
    }
    let node_id = match app_state.get_node_id() {
        Ok(id) => id,
        Err(_) => {
            engine.sync_inflight.store(false, Ordering::SeqCst);
            return;
        }
    };
    let comms = app_state.comms.clone();
    let epoch = app_state.epoch.load(Ordering::Relaxed);
    let db_pool = app_state.db_pool.clone();
    let input_tx = engine.input_tx.clone();
    let mut decided = engine.decided.clone();
    let flag = engine.sync_inflight.clone();
    let evidence = app_state.evidence.clone();
    let kick_state = app_state.clone();
    crate::consensus::queue::queue_rt().spawn(async move {
        if let Err(e) = sync::sync_to_target(
            &comms,
            epoch,
            &db_pool,
            node_id,
            &input_tx,
            &mut decided,
            target,
            Some(hint_peer),
            Some(evidence),
        )
        .await
            && !pivot_on_epoch_ahead(&kick_state, &e)
        {
            tracing::debug!("lag-kick sync toward {target} did not complete: {e:?}");
        }
        flag.store(false, Ordering::SeqCst);
    });
}
