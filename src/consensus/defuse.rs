//! The AlpnRejected defuser (RFC-025 §Rejection & Diagnosability).
//!
//! A locked-family refusal means: alive, reachable, and either
//! version-mismatched or a different mesh entirely. This module resolves
//! which, against the peer's latest Pong — the evidence cache when
//! fresh, one status probe otherwise — and turns the answer into the
//! named states the RFC requires. Callers sit on runtimes that must not
//! await (queue_rt, the sync driver), so the entry point is spawn-shaped
//! and a per-peer cooldown elects one resolver however many dials are
//! failing.

use std::time::{Duration, Instant};

use crate::consensus::evidence::{self, PongStamp, SelfView, absorb_pong, status_probe};

/// What a cached (or absent) Pong stamp tells the defuser to do.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum DefuseDecision {
    /// Fresh stamp, versions differ: the refusal is named — log the
    /// skew (and the epoch, which may pivot us into the epoch join).
    Skew { peer_version: u32, peer_epoch: u64 },
    /// No stamp, or stale: one status probe answers it.
    Probe,
    /// Fresh stamp, versions match: a genuine transport anomaly — the
    /// caller's existing retry/evict handling already proceeded.
    Anomaly,
}

/// Pure decision core: fresh-enough is one probe cadence — anything
/// older, ask again.
pub(crate) fn defuse_from_stamp(
    stamp: Option<PongStamp>,
    my_version: u32,
    freshness: Duration,
    now: Instant,
) -> DefuseDecision {
    match stamp {
        Some(s) if now.saturating_duration_since(s.at) <= freshness => {
            if s.version_code == my_version {
                DefuseDecision::Anomaly
            } else {
                DefuseDecision::Skew {
                    peer_version: s.version_code,
                    peer_epoch: s.epoch,
                }
            }
        }
        _ => DefuseDecision::Probe,
    }
}

/// Resolve a locked-family `AlpnRejected` from `peer` into a named
/// state. Fire-and-forget: the failing dial's own handling (retry
/// cadence, candidate rotation) proceeds regardless — the defuser only
/// names why.
pub(crate) fn defuse_alpn_rejection(app_state: &crate::AppState, peer: hopnet_comms::PeerRef) {
    let app_state = app_state.clone();
    // queue_rt: the policy read is blocking DB work.
    crate::consensus::queue::queue_rt().spawn(async move {
        let my_version = crate::version::effective_running_code();
        let my_epoch = app_state.epoch.load(std::sync::atomic::Ordering::Relaxed);
        // The replicated policy's Lazy probe cadence bounds both the
        // stamp freshness and the cooldown — the largest band, so the
        // defuser never probes more often than the scheduler would.
        let policy = app_state
            .db_pool
            .get()
            .ok()
            .map(|conn| hopnet_consensus::store::read_policy(&conn).unwrap_or_default())
            .unwrap_or_default();
        let cadence = policy.t_probe(hopnet_consensus::membership::Band::Lazy);
        let now = Instant::now();

        match defuse_from_stamp(
            app_state.evidence.last_pong(peer.node_id),
            my_version,
            cadence,
            now,
        ) {
            DefuseDecision::Skew {
                peer_version,
                peer_epoch,
            } => {
                scream_skew(peer.node_id, peer_version, my_version, peer_epoch);
                if peer_epoch > my_epoch {
                    // The straggler fast path (RFC-025): the defuser
                    // pivots into the epoch join without waiting for the
                    // prober. CAS-guarded, idempotent.
                    crate::regenesis::join::spawn_epoch_join(
                        &app_state,
                        crate::regenesis::join::JoinAnchor::OwnDb,
                        vec![peer],
                    );
                }
            }
            DefuseDecision::Anomaly => {
                tracing::debug!(
                    peer = peer.node_id,
                    "locked dial refused but versions match — transport anomaly, \
                     existing handling proceeds"
                );
            }
            DefuseDecision::Probe => {
                if !app_state.evidence.try_begin_defuse(peer.node_id, cadence, now) {
                    return;
                }
                let decided = app_state
                    .malachite
                    .get()
                    .map(|e| *e.decided.borrow())
                    .unwrap_or(0);
                let me = SelfView {
                    epoch: my_epoch,
                    decided,
                    version_code: my_version,
                    compat_floor: hopnet_comms::alpn::compat_floor(hopnet_comms::alpn::COMPAT_HEAD),
                    compat_head: hopnet_comms::alpn::COMPAT_HEAD,
                };
                match status_probe(&app_state.comms, &peer, decided, my_epoch, policy.grace).await {
                    Ok(pong) => {
                        let action = absorb_pong(
                            &app_state.evidence,
                            &me,
                            peer.node_id,
                            &pong,
                            Instant::now(),
                        );
                        if pong.version_code != my_version {
                            scream_skew(peer.node_id, pong.version_code, my_version, pong.epoch);
                        }
                        if action == evidence::PongAction::EpochJoin {
                            crate::regenesis::join::spawn_epoch_join(
                                &app_state,
                                crate::regenesis::join::JoinAnchor::OwnDb,
                                vec![peer],
                            );
                        }
                    }
                    // The compat dial failed too: wrong magic or not a
                    // HopNet node — opaque by design (RFC-025 §The ALPN
                    // Scheme). Refusal vs timeout is indistinguishable
                    // here on purpose; no typed error needed.
                    Err(e) => {
                        tracing::debug!(
                            peer = peer.node_id,
                            error = %e,
                            "locked dial refused and the status probe failed — \
                             foreign or unreachable, staying opaque"
                        );
                    }
                }
            }
        }
    });
}

/// The RFC's SCREAM (one shape, shared with the prober's arm): error
/// level, both codes named, never a warn that scrolls away.
fn scream_skew(peer: i32, peer_version: u32, local_version: u32, peer_epoch: u64) {
    tracing::error!(
        peer,
        peer_version = %crate::version::format_code(peer_version),
        local_version = %crate::version::format_code(local_version),
        peer_epoch,
        "version skew: locked dial refused at the transport — \
         unsupported state (RFC-025)"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stamp(at: Instant, version_code: u32) -> PongStamp {
        PongStamp {
            at,
            epoch: 1,
            version_code,
            window: Some((0, 1)),
        }
    }

    // Impact: the defuser's whole value is not guessing — a fresh cached
    // Pong answers without a probe, a stale one never does.
    // Should: name the skew from a fresh mismatched stamp, call a fresh
    // matched stamp an anomaly, and probe on absent or stale stamps.
    #[test]
    fn decision_follows_freshness_and_version() {
        let now = Instant::now();
        let fresh = now - Duration::from_secs(5);
        let stale = now - Duration::from_secs(120);
        let freshness = Duration::from_secs(60);

        assert_eq!(
            defuse_from_stamp(Some(stamp(fresh, 20270101)), 20260806, freshness, now),
            DefuseDecision::Skew {
                peer_version: 20270101,
                peer_epoch: 1
            }
        );
        assert_eq!(
            defuse_from_stamp(Some(stamp(fresh, 20260806)), 20260806, freshness, now),
            DefuseDecision::Anomaly
        );
        assert_eq!(
            defuse_from_stamp(Some(stamp(stale, 20270101)), 20260806, freshness, now),
            DefuseDecision::Probe
        );
        assert_eq!(
            defuse_from_stamp(None, 20260806, freshness, now),
            DefuseDecision::Probe
        );
    }

    // Impact: concurrent failing dials must elect ONE resolver — the
    // cooldown is the election, atomically under the map lock.
    // Should: grant the first claim and refuse a second inside the
    // cooldown; grant again once the cooldown passes.
    #[test]
    fn defuse_cooldown_elects_one_resolver() {
        let map = evidence::EvidenceMap::new();
        let cooldown = Duration::from_secs(30);
        let t0 = map.origin() + Duration::from_secs(1);
        assert!(map.try_begin_defuse(9, cooldown, t0));
        assert!(!map.try_begin_defuse(9, cooldown, t0 + Duration::from_secs(1)));
        assert!(map.try_begin_defuse(9, cooldown, t0 + Duration::from_secs(31)));
    }
}
