//! Per-peer liveness evidence (RFC-CONSENSUS-002 S3).
//!
//! The refresh set is exactly the spec's two classes (RFC-CONSENSUS-001,
//! Evidence & validation): REACHABILITY evidence — our own authenticated
//! exchanges, hooked at every RPC scope intake and host-owned outbound
//! completion — and CONTRIBUTION evidence — signatures inside committed
//! certificates, swept at on_decided. Relayed artifacts never refresh:
//! every hook records the TRANSPORT peer, which is always first-hand.
//!
//! Purity contract: classification (`live_estimate`, `bright_span`,
//! `contact_age`) is a pure function of a snapshot + policy + committed
//! sets — never of in-flight probe state, never under the map lock. The
//! probe is a DEADLINE, not a schedule: it fires when evidence age reaches
//! T_probe(band), so a busy mesh probes almost never. Suspicion attaches
//! to the unanswered probe (T_unresponsive = T_probe + g), never to the
//! deadline itself.
//!
//! The map is in-memory by design: restart loss is correct semantics — a
//! window you have not probed you cannot attest, so a rebooted observer
//! extends first-deadline grace to everyone and re-learns.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use axum::{extract::State, response::IntoResponse};
use hopnet_comms::Rpc;
use parking_lot::Mutex;

use hopnet_consensus::config::QuorumProfile;
use hopnet_consensus::membership::{Band, ConsensusPolicy, band};

/// Per-peer evidence record. `last_contact` is the freshness anchor for
/// dark(X); `bright_since` anchors the S5 bright span;
/// `probes_since_contact` carries the S4 attestation floor (>= 2 probe
/// attempts since last contact).
#[derive(Debug, Clone, Copy)]
pub struct PeerEvidence {
    pub last_contact: Instant,
    pub last_probe_at: Option<Instant>,
    pub probes_since_contact: u32,
    pub bright_since: Option<Instant>,
    pub last_known_height: Option<u64>,
}

/// Copy-out view — snapshots never alias the live map.
pub type PeerEvidenceView = PeerEvidence;

pub struct EvidenceMap {
    /// Map creation instant: nodes with no entry age from here
    /// (live-until-first-deadline).
    origin: Instant,
    /// Decided height when the evidence began (first scheduler pass wins);
    /// the proven-quorum ceiling's pre-boot arm.
    boot_height: once_cell::sync::OnceCell<u64>,
    /// bright_since reset gap in millis: the band-independent upper bound
    /// t_unresponsive(Lazy). Initialized from the default policy; the
    /// probe scheduler refreshes it from the replicated policy each scan.
    reset_gap_ms: AtomicU64,
    inner: Mutex<HashMap<i32, PeerEvidence>>,
}

impl Default for EvidenceMap {
    fn default() -> Self {
        Self::new()
    }
}

impl EvidenceMap {
    pub fn new() -> Self {
        let default_gap = ConsensusPolicy::default().t_unresponsive(Band::Lazy);
        Self {
            origin: Instant::now(),
            boot_height: once_cell::sync::OnceCell::new(),
            reset_gap_ms: AtomicU64::new(default_gap.as_millis() as u64),
            inner: Mutex::new(HashMap::new()),
        }
    }

    pub fn origin(&self) -> Instant {
        self.origin
    }

    /// Set once by the probe scheduler's first pass (idempotent).
    pub fn set_boot_height(&self, h: u64) {
        let _ = self.boot_height.set(h);
    }

    pub fn boot_height(&self) -> Option<u64> {
        self.boot_height.get().copied()
    }

    /// Scheduler-maintained: t_unresponsive(Lazy) from the replicated
    /// policy (idempotent atomic store).
    pub fn set_reset_gap(&self, gap: Duration) {
        self.reset_gap_ms
            .store(gap.as_millis() as u64, Ordering::Relaxed);
    }

    /// Reachability or contribution evidence: an authenticated exchange
    /// with (or a committed vote from) this node happened NOW.
    pub fn record_contact(&self, node_id: i32) {
        self.record_at(node_id, None, Instant::now());
    }

    /// Contact that also proved a decided height (status-probe response,
    /// certificate signature at its height).
    pub fn record_contact_with_height(&self, node_id: i32, height: u64) {
        self.record_at(node_id, Some(height), Instant::now());
    }

    /// A probe ATTEMPT was fired (recorded before the send — the record is
    /// the attempt, never in-flight state).
    pub fn record_probe_sent(&self, node_id: i32) {
        self.record_probe_sent_at(node_id, Instant::now());
    }

    /// Snapshot for classification and the debug route; sorted by node_id.
    pub fn snapshot(&self) -> Vec<(i32, PeerEvidenceView)> {
        let map = self.inner.lock();
        let mut out: Vec<(i32, PeerEvidenceView)> = map.iter().map(|(k, v)| (*k, *v)).collect();
        out.sort_unstable_by_key(|(id, _)| *id);
        out
    }

    pub(crate) fn record_at(&self, node_id: i32, height: Option<u64>, now: Instant) {
        let reset_gap = Duration::from_millis(self.reset_gap_ms.load(Ordering::Relaxed));
        let mut map = self.inner.lock();
        let entry = map.entry(node_id).or_insert(PeerEvidence {
            last_contact: now,
            last_probe_at: None,
            probes_since_contact: 0,
            bright_since: Some(now),
            last_known_height: None,
        });
        // Reset rule: a gap no band would tolerate restarts the bright
        // span; sub-gap silences are the hysteresis grace for blips.
        if entry.bright_since.is_none()
            || now.saturating_duration_since(entry.last_contact) > reset_gap
        {
            entry.bright_since = Some(now);
        }
        entry.last_contact = now;
        entry.probes_since_contact = 0;
        if height.is_some() {
            entry.last_known_height = height;
        }
    }

    pub(crate) fn record_probe_sent_at(&self, node_id: i32, now: Instant) {
        let origin = self.origin;
        let mut map = self.inner.lock();
        let entry = map.entry(node_id).or_insert(PeerEvidence {
            // Never-contacted nodes keep aging from origin (the probe
            // attempt is not contact).
            last_contact: origin,
            last_probe_at: None,
            probes_since_contact: 0,
            bright_since: None,
            last_known_height: None,
        });
        entry.last_probe_at = Some(now);
        entry.probes_since_contact = entry.probes_since_contact.saturating_add(1);
    }
}

/// Evidence age: now − last_contact, or now − origin for a node with no
/// entry (live-until-first-deadline from map creation).
pub fn contact_age(view: Option<&PeerEvidenceView>, origin: Instant, now: Instant) -> Duration {
    match view {
        Some(v) => now.saturating_duration_since(v.last_contact),
        None => now.saturating_duration_since(origin),
    }
}

/// Observed bright span: now − bright_since, but only while the node is in
/// contact under `band`'s deadline; a currently-dark node's span is zero.
pub fn bright_span(
    view: Option<&PeerEvidenceView>,
    origin: Instant,
    policy: &ConsensusPolicy,
    current_band: Band,
    now: Instant,
) -> Duration {
    let Some(v) = view else {
        return Duration::ZERO;
    };
    if contact_age(Some(v), origin, now) > policy.t_unresponsive(current_band) {
        return Duration::ZERO;
    }
    match v.bright_since {
        Some(since) => now.saturating_duration_since(since),
        None => Duration::ZERO,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiveEstimate {
    pub live: u64,
    pub quorum: u64,
    /// live − quorum, signed: negative when (subjectively) stalled.
    pub headroom: i64,
    pub band: Band,
    /// Fixpoint iterations taken (≤ 3).
    pub iterations: u32,
}

/// Band fixpoint ratchet (RFC-CONSENSUS-001 Headroom schedule): classify
/// against the LAZY deadline, compute H and the band, reclassify against
/// that band's deadline, repeat until stable. Terminates in ≤ 3
/// iterations: bands only tighten (t_unresponsive shrinks ⇒ the live set
/// shrinks ⇒ H non-increasing ⇒ the band is monotone toward Cliff).
/// `self_id` always counts live — a node is its own evidence.
pub fn live_estimate(
    snapshot: &[(i32, PeerEvidenceView)],
    origin: Instant,
    policy: &ConsensusPolicy,
    profile: QuorumProfile,
    seated: &[i32],
    self_id: i32,
    now: Instant,
) -> LiveEstimate {
    let lookup = |id: i32| -> Option<&PeerEvidenceView> {
        snapshot
            .binary_search_by_key(&id, |(k, _)| *k)
            .ok()
            .map(|i| &snapshot[i].1)
    };
    let quorum = profile.quorum(seated.len() as u64);

    let mut current = Band::Lazy;
    let mut iterations = 0u32;
    loop {
        iterations += 1;
        let deadline = policy.t_unresponsive(current);
        let live = seated
            .iter()
            .filter(|id| **id == self_id || contact_age(lookup(**id), origin, now) <= deadline)
            .count() as u64;
        let headroom = live as i64 - quorum as i64;
        let next = band(headroom);
        if next == current || iterations >= 3 {
            return LiveEstimate {
                live,
                quorum,
                headroom,
                band: next,
                iterations,
            };
        }
        current = next;
    }
}

// ============================================================================
// Status probe (the deadline probe): a tiny authenticated RPC returning the
// peer's decided height — reachability evidence that also feeds the S5
// catch-up gate (last_known_height).
// ============================================================================

#[derive(serde::Serialize, serde::Deserialize)]
pub enum StatusRequest {
    /// Carries the PROBER's decided height: a probe teaches both sides —
    /// the responder learns the prober's height here, the prober learns
    /// the responder's from the Pong. Without this, steady-state probe
    /// circularity (each side's probes keeping the other's view fresh)
    /// leaves exactly one probe direction per pair and the responder
    /// heightless.
    ///
    /// Also the hello of the (epoch, version) handshake (RFC-019 S6):
    /// both sides learn each other's identity and log a structured
    /// refusal on mismatch — turning the silent signature-domain failure
    /// (chain_id is mixed into every vote) into a diagnosable one. The
    /// responder still answers and records contact: reachability is a
    /// transport fact, orthogonal to epoch membership.
    Ping {
        decided_height: u64,
        epoch: u64,
        version_code: u32,
    },
}

#[derive(serde::Serialize, serde::Deserialize)]
pub enum StatusResponse {
    /// Current decided height (0 pre-genesis/pre-engine — reachability is
    /// a transport property; a zero height just fails catch-up gates),
    /// plus the responder's (epoch, version) — see Ping.
    Pong {
        decided_height: u64,
        epoch: u64,
        version_code: u32,
    },
}

/// Inbound status scope: mesh plane, inline on the net runtime — one
/// watch borrow + one mutex write, no DB, no spawn.
pub struct StatusScope {
    pub app_state: crate::AppState,
}

impl hopnet_comms::RpcHandler for StatusScope {
    fn handle(
        &self,
        peer: hopnet_comms::PeerRef,
        _payload: Vec<u8>,
    ) -> hopnet_comms::BoxFuture<'_, Vec<u8>> {
        let app_state = self.app_state.clone();
        Box::pin(async move {
            // An inbound probe is itself an authenticated exchange — and it
            // carries the prober's height (see StatusRequest::Ping).
            let my_epoch = app_state.epoch.load(std::sync::atomic::Ordering::Relaxed);
            match bincode::serde::decode_from_slice::<StatusRequest, _>(
                &_payload,
                bincode::config::standard(),
            ) {
                Ok((
                    StatusRequest::Ping {
                        decided_height,
                        epoch,
                        version_code,
                    },
                    _,
                )) => {
                    app_state
                        .evidence
                        .record_contact_with_height(peer.node_id, decided_height);
                    if epoch != my_epoch {
                        tracing::warn!(
                            peer = peer.node_id,
                            peer_epoch = epoch,
                            peer_version = %crate::version::format_code(version_code),
                            local_epoch = my_epoch,
                            "handshake: peer is in a different epoch (needs epoch join / upgrade)"
                        );
                    }
                }
                Err(_) => app_state.evidence.record_contact(peer.node_id),
            }
            let decided_height = app_state
                .malachite
                .get()
                .map(|e| *e.decided.borrow())
                .unwrap_or(0);
            bincode::serde::encode_to_vec(
                &StatusResponse::Pong {
                    decided_height,
                    epoch: my_epoch,
                    version_code: crate::version::effective_running_code(),
                },
                bincode::config::standard(),
            )
            .unwrap_or_default()
        })
    }
}

/// Fire one status probe; `timeout` = the policy grace g (one source of
/// truth — the comms transport default is only the floor).
pub async fn status_probe(
    comms: &hopnet_comms::IrohComms,
    peer: &hopnet_comms::PeerRef,
    my_decided_height: u64,
    my_epoch: u64,
    timeout: Duration,
) -> Result<(u64, u64), String> {
    let payload = bincode::serde::encode_to_vec(
        &StatusRequest::Ping {
            decided_height: my_decided_height,
            epoch: my_epoch,
            version_code: crate::version::effective_running_code(),
        },
        bincode::config::standard(),
    )
    .map_err(|e| format!("encode: {e}"))?;
    let raw = comms
        .rpc(peer, "status", payload, timeout)
        .await
        .map_err(|e| format!("rpc: {e:?}"))?;
    let (resp, _) =
        bincode::serde::decode_from_slice::<StatusResponse, _>(&raw, bincode::config::standard())
            .map_err(|e| format!("decode: {e}"))?;
    let StatusResponse::Pong {
        decided_height,
        epoch,
        version_code,
    } = resp;
    if epoch != my_epoch {
        tracing::warn!(
            peer = peer.node_id,
            peer_epoch = epoch,
            peer_version = %crate::version::format_code(version_code),
            local_epoch = my_epoch,
            "handshake: peer answered from a different epoch"
        );
    }
    // The peer's epoch escapes to the caller (RFC-019 S7): a node that
    // slept through a boundary while the mesh stayed quiet has nothing
    // else to learn from — no sync to fail, no gossip to arrive. The
    // pong is its only signal that it must rejoin.
    Ok((decided_height, epoch))
}

// ============================================================================
// Probe scheduler: a 1s deadline SCAN (the probe deadline itself comes from
// the policy band). Runs on queue_rt beside the tip-poll.
// ============================================================================

const PROBE_SCAN_INTERVAL: Duration = Duration::from_secs(1);

/// Deterministic per-peer deadline jitter, DOWNWARD only (0.85–0.95 of
/// T_probe): desynchronizes peers with zero mutable state while
/// guaranteeing the probe fires strictly BEFORE the un-jittered deadline —
/// an upward jitter would push worst-case evidence age (deadline + scan
/// quantization + RTT) past T_unresponsive = T_probe + g, making live
/// nodes flicker unresponsive once per probe cycle.
fn jitter(node_id: i32) -> f64 {
    let h = (node_id as u32).wrapping_mul(2654435761);
    0.85 + 0.1 * f64::from(h % 1024) / 1024.0
}

pub fn spawn_probe_scheduler(app_state: crate::AppState) {
    crate::consensus::queue::queue_rt().spawn(async move {
        // Vote-out proposal cooldown anchor (RFC-CONSENSUS-002 S4).
        let mut last_voteout_at: Option<Instant> = None;
        let mut last_seat_at: Option<Instant> = None;
        loop {
            tokio::time::sleep(PROBE_SCAN_INTERVAL).await;

            let Ok(my_id) = app_state.get_node_id() else {
                continue;
            };
            let Some(engine) = app_state.malachite.get() else {
                continue;
            };
            let decided = *engine.decided.borrow();
            app_state.evidence.set_boot_height(decided);

            let (policy, profile, seated, seat_starts, pool) = {
                let Ok(conn) = app_state.db_pool.get() else {
                    continue;
                };
                let policy = hopnet_consensus::store::read_policy(&conn).unwrap_or_default();
                app_state
                    .evidence
                    .set_reset_gap(policy.t_unresponsive(Band::Lazy));
                let profile = hopnet_consensus::store::meta_get(
                    &conn,
                    hopnet_consensus::store::META_QUORUM_PROFILE,
                )
                .ok()
                .flatten()
                .and_then(|b| String::from_utf8(b).ok())
                .and_then(|s| QuorumProfile::parse(&s))
                .unwrap_or(QuorumProfile::Auto);
                let pending = decided.saturating_add(1);
                let seated: Vec<i32> =
                    crate::db::consensus::get_validators_with_conn(&conn, pending)
                        .map(|v| v.into_iter().map(|n| n.node_id).collect())
                        .unwrap_or_default();
                // Seat starts (proven pre-boot arm) + the candidate pool
                // (registered minus seated, with each one's last departure).
                let mut seat_starts: Vec<(i32, u64)> = Vec::with_capacity(seated.len());
                for id in &seated {
                    if let Ok(Some(h)) =
                        hopnet_consensus::validators::activation_height(&conn, *id, pending)
                    {
                        seat_starts.push((*id, h));
                    }
                }
                let registered: Vec<i32> = conn
                    .prepare_cached("SELECT node_id FROM nodes ORDER BY node_id")
                    .and_then(|mut st| {
                        st.query_map([], |row| row.get::<_, i32>(0))
                            .and_then(|rows| rows.collect::<Result<Vec<_>, _>>())
                    })
                    .unwrap_or_default();
                let pool: Vec<(i32, Option<hopnet_consensus::validators::DepartureKind>)> =
                    registered
                        .iter()
                        .filter(|id| !seated.contains(id) && **id != my_id)
                        .map(|id| {
                            let dep =
                                hopnet_consensus::validators::last_departure(&conn, *id, pending)
                                    .unwrap_or(None);
                            (*id, dep)
                        })
                        .collect();
                (policy, profile, seated, seat_starts, pool)
            };
            if seated.is_empty() {
                continue; // pre-genesis / mid-bootstrap
            }

            let now = Instant::now();
            let snap = app_state.evidence.snapshot();
            let est = live_estimate(
                &snap,
                app_state.evidence.origin(),
                &policy,
                profile,
                &seated,
                my_id,
                now,
            );
            let t_probe = policy.t_probe(est.band);
            let my_epoch = app_state.epoch.load(Ordering::Relaxed);

            // Targets = every registered node except self (pool nodes are
            // probed too — reputation for candidates, spec Decisions).
            let peers =
                crate::consensus::malachite::sync::peer_list(&app_state.db_pool, my_id, None);
            for peer in peers {
                let view = snap
                    .binary_search_by_key(&peer.node_id, |(k, _)| *k)
                    .ok()
                    .map(|i| snap[i].1);
                let anchor = match view {
                    Some(v) => {
                        let contact = v.last_contact;
                        match v.last_probe_at {
                            Some(p) if p > contact => p,
                            _ => contact,
                        }
                    }
                    None => app_state.evidence.origin(),
                };
                let deadline = t_probe.mul_f64(jitter(peer.node_id));
                if now.saturating_duration_since(anchor) < deadline {
                    continue;
                }
                // The record IS the attempt — before the send, so
                // classification never depends on in-flight state.
                app_state.evidence.record_probe_sent(peer.node_id);
                let comms = app_state.comms.clone();
                let evidence = app_state.evidence.clone();
                let g = policy.grace;
                let probe_state = app_state.clone();
                tokio::spawn(async move {
                    if let Ok((h, peer_epoch)) =
                        status_probe(&comms, &peer, decided, my_epoch, g).await
                    {
                        evidence.record_contact_with_height(peer.node_id, h);
                        // The offline-through-regenesis case (RFC-019 S7):
                        // this node woke in a sealed epoch with a quiet
                        // mesh around it. Nothing will sync and nothing
                        // will gossip — the pong is the only thing that
                        // tells it to rejoin.
                        if peer_epoch > my_epoch {
                            crate::regenesis::join::spawn_epoch_join(
                                &probe_state,
                                crate::regenesis::join::JoinAnchor::OwnDb,
                                vec![peer],
                            );
                        }
                    }
                    // Failure: the silence is already recorded as the
                    // unanswered probe.
                });
            }

            // Vote-out proposer scan (RFC-CONSENSUS-002 S4): longest-dark
            // first, one at a time. Duplicates are harmless (committed-
            // nonce dedup + the target-not-seated objective check kill
            // replays); the cooldown just avoids spamming. Satisfies
            // RECOVERY's fairness obligation — at least one live validator
            // eventually proposes — with no coordination protocol.
            if seated.contains(&my_id) {
                let t_out = policy.t_out(est.band);
                let cooled =
                    last_voteout_at.is_none_or(|t| now.saturating_duration_since(t) >= t_out / 2);
                if cooled {
                    let target = seated
                        .iter()
                        .filter(|id| **id != my_id)
                        .filter_map(|id| {
                            let view = snap
                                .binary_search_by_key(id, |(k, _)| *k)
                                .ok()
                                .map(|i| snap[i].1);
                            let age = contact_age(view.as_ref(), app_state.evidence.origin(), now);
                            let probes = view.map(|v| v.probes_since_contact).unwrap_or(0);
                            (age >= t_out
                                && probes >= hopnet_consensus::membership::ATTESTATION_PROBE_FLOOR)
                                .then_some((*id, age))
                        })
                        .max_by_key(|(_, age)| *age);
                    if let Some((target, age)) = target {
                        last_voteout_at = Some(now);
                        let payload = bincode::serde::encode_to_vec(
                            &crate::consensus::handlers::VoteOutRequest { node_id: target },
                            bincode::config::standard(),
                        )
                        .unwrap_or_default();
                        match crate::consensus::dispatch::create_signed_transaction(
                            &app_state,
                            "validator_vote_out".to_string(),
                            payload,
                        ) {
                            Ok(tx) => {
                                // submit awaits the commit (120s bound) —
                                // and the target being dead may mean the
                                // mesh is stalled. Fire-and-forget so the
                                // scan never blocks.
                                let queue = app_state.consensus_queue.clone();
                                tokio::spawn(async move {
                                    match queue.submit(tx).await {
                                        Ok(()) => tracing::info!(
                                            "vote-out of node {target} committed (dark {age:?})"
                                        ),
                                        Err(e) => tracing::debug!(
                                            "vote-out of node {target} not committed: {e}"
                                        ),
                                    }
                                });
                            }
                            Err(e) => tracing::warn!("vote-out sign failed: {e:?}"),
                        }
                    }
                }
            }

            // Seat-proposal scan (RFC-CONSENSUS-002 S5): a seated validator
            // proposes the largest brightest-first gaining batch from the
            // pool. Nodes never request seats — this is the only initiator.
            if seated.contains(&my_id) && !pool.is_empty() {
                let seat_cooldown = policy.t_probe(est.band);
                let cooled =
                    last_seat_at.is_none_or(|t| now.saturating_duration_since(t) >= seat_cooldown);
                if cooled {
                    let inp = crate::consensus::membership_guards::GuardInputs {
                        snapshot: &snap,
                        origin: app_state.evidence.origin(),
                        now,
                        policy: &policy,
                        profile,
                        seated: &seated,
                        my_id,
                        boot_height: app_state.evidence.boot_height(),
                        seat_starts: &seat_starts,
                        pending_height: decided.saturating_add(1),
                    };
                    if let Some(batch) =
                        crate::consensus::membership_guards::plan_seating_batch(&inp, &pool)
                    {
                        last_seat_at = Some(now);
                        let payload = bincode::serde::encode_to_vec(
                            &crate::consensus::handlers::ActivationRequest {
                                members: batch.clone(),
                            },
                            bincode::config::standard(),
                        )
                        .unwrap_or_default();
                        match crate::consensus::dispatch::create_signed_transaction(
                            &app_state,
                            "validator_activation".to_string(),
                            payload,
                        ) {
                            Ok(tx) => {
                                let queue = app_state.consensus_queue.clone();
                                tokio::spawn(async move {
                                    match queue.submit(tx).await {
                                        Ok(()) => tracing::info!("seated batch {batch:?}"),
                                        Err(e) => tracing::debug!(
                                            "seat batch {batch:?} not committed: {e}"
                                        ),
                                    }
                                });
                            }
                            Err(e) => tracing::warn!("seat sign failed: {e:?}"),
                        }
                    }
                }
            }
        }
    });
}

// ============================================================================
// Debug route: GET /consensus/evidence
// ============================================================================

/// The DB-side inputs `live_estimate` needs, read in one scoped checkout.
pub struct EvidenceInputs {
    pub policy: ConsensusPolicy,
    /// The CONFIGURED profile from consensus_meta — still Auto if unpinned.
    /// Resolve with `profile.profile_at(v)` where the effective one is wanted.
    pub profile: QuorumProfile,
    pub seated: Vec<i32>,
    pub registered: Vec<i32>,
}

/// Read policy, quorum profile, seated validators and the registered node
/// universe together.
///
/// Shared so the evidence route and the resilience view cannot read a
/// different set of inputs and then disagree about the same mesh. Errors are
/// swallowed into defaults exactly as the route did before extraction — this
/// is diagnostics, and a missing policy row should not 500 the pane.
pub fn evidence_inputs(
    conn: &r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>,
    decided: u64,
) -> EvidenceInputs {
    let policy = hopnet_consensus::store::read_policy(conn).unwrap_or_default();
    let profile =
        hopnet_consensus::store::meta_get(conn, hopnet_consensus::store::META_QUORUM_PROFILE)
            .ok()
            .flatten()
            .and_then(|b| String::from_utf8(b).ok())
            .and_then(|s| QuorumProfile::parse(&s))
            .unwrap_or(QuorumProfile::Auto);
    let pending = decided.saturating_add(1);
    let seated: Vec<i32> = crate::db::consensus::get_validators_with_conn(conn, pending)
        .map(|v| v.into_iter().map(|n| n.node_id).collect())
        .unwrap_or_default();
    let registered: Vec<i32> = conn
        .prepare_cached("SELECT node_id FROM nodes ORDER BY node_id")
        .and_then(|mut s| {
            s.query_map([], |row| row.get::<_, i32>(0))
                .and_then(|rows| rows.collect::<Result<Vec<_>, _>>())
        })
        .unwrap_or_default();

    EvidenceInputs {
        policy,
        profile,
        seated,
        registered,
    }
}

pub async fn get_evidence(State(app_state): State<crate::AppState>) -> impl IntoResponse {
    use axum::http::StatusCode;

    let Ok(my_id) = app_state.get_node_id() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "node identity not initialized",
        )
            .into_response();
    };
    let decided = app_state
        .malachite
        .get()
        .map(|e| *e.decided.borrow())
        .unwrap_or(0);

    let EvidenceInputs {
        policy,
        profile,
        seated,
        registered,
    } = {
        let Ok(conn) = app_state.db_pool.get() else {
            return (StatusCode::INTERNAL_SERVER_ERROR, "db pool").into_response();
        };
        evidence_inputs(&conn, decided)
    };

    let now = Instant::now();
    let origin = app_state.evidence.origin();
    let snap = app_state.evidence.snapshot();
    let est = live_estimate(&snap, origin, &policy, profile, &seated, my_id, now);

    let nodes: Vec<serde_json::Value> = registered
        .iter()
        .map(|id| {
            let view = snap
                .binary_search_by_key(id, |(k, _)| *k)
                .ok()
                .map(|i| snap[i].1);
            let age = contact_age(view.as_ref(), origin, now);
            let span = bright_span(view.as_ref(), origin, &policy, est.band, now);
            serde_json::json!({
                "node_id": id,
                "seated": seated.contains(id),
                "self": *id == my_id,
                "age_ms": age.as_millis() as u64,
                "synthetic_age": view.is_none(),
                "probes_since_contact": view.map(|v| v.probes_since_contact).unwrap_or(0),
                "last_probe_age_ms": view
                    .and_then(|v| v.last_probe_at)
                    .map(|p| now.saturating_duration_since(p).as_millis() as u64),
                "bright_span_ms": span.as_millis() as u64,
                "last_known_height": view.and_then(|v| v.last_known_height),
            })
        })
        .collect();

    axum::Json(serde_json::json!({
        "summary": {
            "live": est.live,
            "quorum": est.quorum,
            "headroom": est.headroom,
            "band": format!("{:?}", est.band),
            "iterations": est.iterations,
            "v": seated.len(),
            "decided_height": decided,
            "t_probe_ms": policy.t_probe(est.band).as_millis() as u64,
            "t_unresponsive_ms": policy.t_unresponsive(est.band).as_millis() as u64,
            "t_out_ms": policy.t_out(est.band).as_millis() as u64,
        },
        "nodes": nodes,
    }))
    .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> ConsensusPolicy {
        ConsensusPolicy::default() // t_unresponsive Lazy/Fast/Cliff = 125/65/35 s
    }

    fn view(last_contact: Instant) -> PeerEvidenceView {
        PeerEvidence {
            last_contact,
            last_probe_at: None,
            probes_since_contact: 0,
            bright_since: Some(last_contact),
            last_known_height: None,
        }
    }

    // Should: classification be a pure function of the snapshot — same
    // inputs, same outputs; later map mutation never changes a prior
    // snapshot's classification.
    // Impact: attestations must be functions of recorded evidence only
    // (spec purity contract).
    #[test]
    fn classification_is_pure() {
        let map = EvidenceMap::new();
        let origin = map.origin();
        let now = origin + Duration::from_secs(10);
        map.record_at(2, None, origin + Duration::from_secs(9));
        let snap = map.snapshot();

        let est1 = live_estimate(
            &snap,
            origin,
            &policy(),
            QuorumProfile::Majority,
            &[1, 2, 3],
            1,
            now,
        );
        let est2 = live_estimate(
            &snap,
            origin,
            &policy(),
            QuorumProfile::Majority,
            &[1, 2, 3],
            1,
            now,
        );
        assert_eq!(est1, est2);

        map.record_at(2, None, now); // mutate AFTER snapshot
        let est3 = live_estimate(
            &snap,
            origin,
            &policy(),
            QuorumProfile::Majority,
            &[1, 2, 3],
            1,
            now,
        );
        assert_eq!(est1, est3);
    }

    // Should: the fixpoint ratchet cascade — dropping a node compresses
    // the band, which drops another — converge at the documented worst
    // case of 3 iterations; a calm snapshot converges in 1.
    // Impact: the H estimate every guard reads.
    #[test]
    fn fixpoint_ratchet_cascades_and_converges() {
        let origin = Instant::now();
        let now = origin + Duration::from_secs(1000);
        // Majority, seated [1..5], self 1, quorum(5)=3.
        // Ages: node2 130s (> Lazy 125), node3 70s (Fast< x <=Lazy),
        // node4 40s (Cliff< x <=Fast), node5 1s.
        let snap = vec![
            (2, view(now - Duration::from_secs(130))),
            (3, view(now - Duration::from_secs(70))),
            (4, view(now - Duration::from_secs(40))),
            (5, view(now - Duration::from_secs(1))),
        ];
        let est = live_estimate(
            &snap,
            origin,
            &policy(),
            QuorumProfile::Majority,
            &[1, 2, 3, 4, 5],
            1,
            now,
        );
        assert_eq!(est.live, 2); // self + node5
        assert_eq!(est.headroom, -1);
        assert_eq!(est.band, Band::Cliff);
        assert_eq!(est.iterations, 3);

        // Calm mesh at H >= 2: the Lazy hypothesis confirms immediately.
        let calm = vec![
            (2, view(now - Duration::from_secs(1))),
            (3, view(now - Duration::from_secs(2))),
            (4, view(now - Duration::from_secs(3))),
            (5, view(now - Duration::from_secs(4))),
        ];
        let est = live_estimate(
            &calm,
            origin,
            &policy(),
            QuorumProfile::Majority,
            &[1, 2, 3, 4, 5],
            1,
            now,
        );
        assert_eq!(est.iterations, 1);
        assert_eq!(est.band, Band::Lazy); // live 5, quorum 3, H 2

        // Calm mesh at H = 1 takes exactly two: Lazy hypothesis, then the
        // Fast confirmation pass.
        let calm3 = vec![
            (2, view(now - Duration::from_secs(1))),
            (3, view(now - Duration::from_secs(2))),
        ];
        let est = live_estimate(
            &calm3,
            origin,
            &policy(),
            QuorumProfile::Majority,
            &[1, 2, 3],
            1,
            now,
        );
        assert_eq!(est.iterations, 2);
        assert_eq!(est.band, Band::Fast); // live 3, quorum 2, H 1
    }

    // Should: the fixpoint never exceed 3 iterations over arbitrary age
    // vectors (bands only tighten — monotone, no oscillation).
    #[test]
    fn fixpoint_terminates_at_three() {
        let origin = Instant::now();
        let now = origin + Duration::from_secs(10_000);
        let mut seed = 0x9E3779B97F4A7C15u64;
        for _ in 0..1000 {
            let mut snap = Vec::new();
            let n = 3 + (seed % 8) as i32;
            for id in 2..=n {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                let age = seed % 200;
                snap.push((id, view(now - Duration::from_secs(age))));
            }
            let seated: Vec<i32> = (1..=n).collect();
            let est = live_estimate(
                &snap,
                origin,
                &policy(),
                QuorumProfile::Bft,
                &seated,
                1,
                now,
            );
            assert!(est.iterations <= 3);
        }
    }

    // Should: ordinary contact never reset the bright span; a gap beyond
    // the reset threshold restart it; first contact set it; a
    // probe-created entry gain it on first real contact.
    // Impact: the S5 admission span — over-crediting is the cheap
    // direction (one readmission round trip), stall safety lives in the
    // ceiling.
    #[test]
    fn bright_since_reset_rule() {
        let map = EvidenceMap::new();
        map.set_reset_gap(Duration::from_secs(10));
        let t0 = Instant::now();

        map.record_at(7, None, t0);
        map.record_at(7, None, t0 + Duration::from_secs(5));
        let since = map.snapshot()[0].1.bright_since.unwrap();
        assert_eq!(since, t0, "ordinary contact must not reset the span");

        let late = t0 + Duration::from_secs(5) + Duration::from_secs(11);
        map.record_at(7, None, late);
        assert_eq!(map.snapshot()[0].1.bright_since.unwrap(), late);

        // Probe-created entry: bright_since None until real contact.
        map.record_probe_sent_at(8, t0);
        let e8 = map.snapshot().iter().find(|(id, _)| *id == 8).unwrap().1;
        assert!(e8.bright_since.is_none());
        map.record_at(8, None, t0 + Duration::from_secs(1));
        let e8 = map.snapshot().iter().find(|(id, _)| *id == 8).unwrap().1;
        assert!(e8.bright_since.is_some());
    }

    // Should: a seated node with no evidence entry count live until the
    // first deadline measured from the map origin, then drop.
    // Impact: restart semantics — a rebooted observer extends
    // first-deadline grace and re-learns; it can never attest what it has
    // not probed.
    #[test]
    fn unknown_node_default_liveness() {
        let map = EvidenceMap::new();
        let origin = map.origin();
        let p = policy();

        let early = origin + Duration::from_secs(5);
        let est = live_estimate(
            &map.snapshot(),
            origin,
            &p,
            QuorumProfile::Majority,
            &[1, 9],
            1,
            early,
        );
        assert_eq!(est.live, 2);

        let late = origin + p.t_unresponsive(Band::Lazy) + Duration::from_secs(1);
        let est = live_estimate(
            &map.snapshot(),
            origin,
            &p,
            QuorumProfile::Majority,
            &[1, 9],
            1,
            late,
        );
        assert_eq!(est.live, 1, "unknown node drops after the first deadline");
    }

    // Should: probe attempts accumulate since last contact and reset on
    // contact (the S4 attestation floor input).
    #[test]
    fn probe_bookkeeping() {
        let map = EvidenceMap::new();
        let t0 = Instant::now();
        map.record_probe_sent_at(3, t0);
        map.record_probe_sent_at(3, t0 + Duration::from_secs(30));
        assert_eq!(map.snapshot()[0].1.probes_since_contact, 2);
        map.record_at(3, None, t0 + Duration::from_secs(31));
        assert_eq!(map.snapshot()[0].1.probes_since_contact, 0);
    }

    // Should: a known height survive height-less contacts (sticky).
    #[test]
    fn height_sticky() {
        let map = EvidenceMap::new();
        let t0 = Instant::now();
        map.record_at(1, Some(5), t0);
        map.record_at(1, None, t0 + Duration::from_secs(1));
        assert_eq!(map.snapshot()[0].1.last_known_height, Some(5));
    }

    // Should: bright_span read zero for currently-dark nodes and for
    // probe-only entries; read the span for in-contact nodes.
    #[test]
    fn bright_span_gating() {
        let origin = Instant::now();
        let now = origin + Duration::from_secs(500);
        let p = policy();

        let fresh = view(now - Duration::from_secs(10));
        let mut fresh_span = fresh;
        fresh_span.bright_since = Some(now - Duration::from_secs(400));
        assert_eq!(
            bright_span(Some(&fresh_span), origin, &p, Band::Lazy, now),
            Duration::from_secs(400)
        );

        let dark = view(now - Duration::from_secs(200)); // > Lazy 125s
        assert_eq!(
            bright_span(Some(&dark), origin, &p, Band::Lazy, now),
            Duration::ZERO
        );
        assert_eq!(
            bright_span(None, origin, &p, Band::Lazy, now),
            Duration::ZERO
        );
    }
}
