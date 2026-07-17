//! Subjective membership validation (RFC-CONSENSUS-002 S4).
//!
//! Runs at `ValidationOrigin::Live` ONLY — called from validate_inner's
//! Live block and build_value's preflight, never from the handlers. The
//! handlers stay objective and deterministic over committed state, so
//! sync replay can never wedge on evidence it cannot re-derive (the RT1
//! rule); the Live-only invariant is enforced at one structural choke
//! point instead of per-handler convention.
//!
//! Wall clock is legal here: validation is an opinion (RFC-CONSENSUS-001,
//! Evidence & validation) — each approver attests from its OWN evidence.

use std::time::Instant;

use hopnet_consensus::config::QuorumProfile;
use hopnet_consensus::membership::{ATTESTATION_PROBE_FLOOR, ConsensusPolicy, exposure};
use hopnet_consensus::validators::DepartureKind;

use crate::consensus::evidence::{PeerEvidenceView, bright_span, contact_age, live_estimate};

/// Everything a subjective predicate reads — pure inputs, unit-testable
/// with hand-built snapshots.
pub struct GuardInputs<'a> {
    pub snapshot: &'a [(i32, PeerEvidenceView)],
    pub origin: Instant,
    pub now: Instant,
    pub policy: &'a ConsensusPolicy,
    pub profile: QuorumProfile,
    /// Valset at the pending height.
    pub seated: &'a [i32],
    pub my_id: i32,
    /// Decided height when my evidence began (proven pre-boot arm);
    /// None before the first scheduler pass.
    pub boot_height: Option<i64>,
    /// (node_id, activation effective_height) for every seated member.
    pub seat_starts: &'a [(i32, i32)],
    /// committed + 1 — the catch-up anchor.
    pub pending_height: i64,
}

fn lookup<'a>(snap: &'a [(i32, PeerEvidenceView)], id: i32) -> Option<&'a PeerEvidenceView> {
    snap.binary_search_by_key(&id, |(k, _)| *k)
        .ok()
        .map(|i| &snap[i].1)
}

/// dark(target): contact age ≥ t_out(my band) with the attestation floor
/// (≥ 2 probes since last contact) met.
pub fn check_vote_out(inp: &GuardInputs<'_>, target: i32) -> Result<(), String> {
    // Target dissent, degenerate form: my own liveness contradicts the
    // attestation trivially — and self usually has no evidence entry, so
    // the synthetic origin-age could otherwise let a node attest its own
    // darkness.
    if target == inp.my_id {
        return Err("refusing to attest own darkness".into());
    }
    let est = live_estimate(
        inp.snapshot,
        inp.origin,
        inp.policy,
        inp.profile,
        inp.seated,
        inp.my_id,
        inp.now,
    );
    let view = lookup(inp.snapshot, target);
    let age = contact_age(view, inp.origin, inp.now);
    let window = inp.policy.t_out(est.band);
    if age < window {
        return Err(format!(
            "target {target} not dark by my evidence: age {age:?} < t_out {window:?} ({:?})",
            est.band
        ));
    }
    let probes = view.map(|v| v.probes_since_contact).unwrap_or(0);
    if probes < ATTESTATION_PROBE_FLOOR {
        return Err(format!(
            "attestation floor: {probes} probes since contact (< {ATTESTATION_PROBE_FLOOR})"
        ));
    }
    Ok(())
}

/// Leave safety: by MY evidence, the survivors still hold quorum —
/// live(seated \ leaver) ≥ quorum(v − 1).
pub fn check_leave(inp: &GuardInputs<'_>, leaver: i32) -> Result<(), String> {
    let survivors: Vec<i32> = inp
        .seated
        .iter()
        .copied()
        .filter(|id| *id != leaver)
        .collect();
    if survivors.is_empty() {
        return Err("set floor".into()); // the handler's objective v>1 also refuses
    }
    let est = live_estimate(
        inp.snapshot,
        inp.origin,
        inp.policy,
        inp.profile,
        &survivors,
        inp.my_id,
        inp.now,
    );
    if est.headroom < 0 {
        return Err(format!(
            "survivors would not hold quorum: live {} < quorum {}",
            est.live, est.quorum
        ));
    }
    Ok(())
}

/// |alive ∩ proven| among the seated set, by MY evidence — the
/// proven-quorum ceiling's input. proven(X) = seated since before my
/// evidence began (activation height ≤ my boot height) ∨ continuously
/// bright ≥ p_prove by my own observation. Genesis seats are pre-boot
/// for everyone — proven by fiat. Self counts via process uptime.
pub fn proven_live(inp: &GuardInputs<'_>, current_band: hopnet_consensus::membership::Band) -> u64 {
    inp.seated
        .iter()
        .filter(|id| {
            let view = lookup(inp.snapshot, **id);
            let alive = **id == inp.my_id
                || contact_age(view, inp.origin, inp.now)
                    <= inp.policy.t_unresponsive(current_band);
            if !alive {
                return false;
            }
            let pre_boot = match (
                inp.boot_height,
                inp.seat_starts.iter().find(|(n, _)| n == *id),
            ) {
                (Some(boot), Some((_, start))) => i64::from(*start) <= boot,
                _ => false,
            };
            if pre_boot {
                return true;
            }
            let span = if **id == inp.my_id {
                // Own seat: process uptime approximates the span.
                inp.now.saturating_duration_since(inp.origin)
            } else {
                bright_span(view, inp.origin, inp.policy, current_band, inp.now)
            };
            span >= inp.policy.p_prove
        })
        .count() as u64
}

/// One batch member as the guard sees it.
pub struct BatchMember {
    pub node_id: i32,
    pub last_departure: Option<DepartureKind>,
}

/// Batch admission gate (RFC-CONSENSUS-001 Admission & readmission):
/// joint posture, joint proven-quorum ceiling (waiver inside), and per
/// member the liveness floor, the S_min span, and the catch-up bound.
/// ELIGIBILITY ONLY — never ranking: any eligible batch passes
/// regardless of which candidates the proposer picked (spec Selection).
pub fn check_activation(
    inp: &GuardInputs<'_>,
    members: &[BatchMember],
) -> Result<(), String> {
    let est = live_estimate(
        inp.snapshot,
        inp.origin,
        inp.policy,
        inp.profile,
        inp.seated,
        inp.my_id,
        inp.now,
    );
    let v = inp.seated.len() as u64;
    let b = members.len() as u64;

    if !hopnet_consensus::membership::posture_ok(inp.profile, v, b) {
        return Err(format!(
            "posture: seating {b} at v={v} is lateral with no equivocation gain"
        ));
    }
    let proven = proven_live(inp, est.band);
    if !hopnet_consensus::membership::ceiling_ok(inp.profile, v, b, proven) {
        return Err(format!(
            "ceiling: quorum inflation exceeds the proven cushion (proven_live {proven}, v={v}, b={b})"
        ));
    }

    let exposed = exposure(inp.profile, v, b) > 0;
    for m in members {
        let view = lookup(inp.snapshot, m.node_id);
        // Liveness floor — explicit: a voluntary exposure-free member's
        // required span is zero, so dark-reads-zero no longer covers it.
        let age = contact_age(view, inp.origin, inp.now);
        if age > inp.policy.t_unresponsive(est.band) {
            return Err(format!("member {} is not currently live", m.node_id));
        }
        let required = inp
            .policy
            .req_span(exposed, m.last_departure, est.headroom);
        let span = bright_span(view, inp.origin, inp.policy, est.band, inp.now);
        if span < required {
            return Err(format!(
                "member {}: bright span {span:?} < required {required:?} (exposed={exposed}, dep={:?}, H={})",
                m.node_id, m.last_departure, est.headroom
            ));
        }
        // Catch-up: I must have OBSERVED the member near the tip.
        let known = view.and_then(|vw| vw.last_known_height);
        match known {
            Some(h)
                if h >= inp
                    .pending_height
                    .saturating_sub(hopnet_consensus::membership::CATCH_UP_TOLERANCE) => {}
            _ => {
                return Err(format!(
                    "member {}: not caught up by my evidence (known {known:?}, pending {})",
                    m.node_id, inp.pending_height
                ));
            }
        }
    }
    Ok(())
}

/// Proposer-side planner: the brightest-first largest-gaining batch
/// (spec Selection + Seating rule). Ranking lives ONLY here — approvers
/// verify eligibility. Returns payload members sorted ascending.
pub fn plan_seating_batch(
    inp: &GuardInputs<'_>,
    pool: &[(i32, Option<DepartureKind>)],
) -> Option<Vec<i32>> {
    let est = live_estimate(
        inp.snapshot,
        inp.origin,
        inp.policy,
        inp.profile,
        inp.seated,
        inp.my_id,
        inp.now,
    );
    let v = inp.seated.len() as u64;

    // Viable: alive now + caught up by my evidence, with the span attached.
    let mut viable: Vec<(i32, Option<DepartureKind>, std::time::Duration)> = pool
        .iter()
        .filter_map(|(id, dep)| {
            let view = lookup(inp.snapshot, *id);
            let alive = contact_age(view, inp.origin, inp.now)
                <= inp.policy.t_unresponsive(est.band);
            let caught_up = view
                .and_then(|vw| vw.last_known_height)
                .is_some_and(|h| {
                    h >= inp
                        .pending_height
                        .saturating_sub(hopnet_consensus::membership::CATCH_UP_TOLERANCE)
                });
            if !alive || !caught_up {
                return None;
            }
            let span = bright_span(view, inp.origin, inp.policy, est.band, inp.now);
            Some((*id, *dep, span))
        })
        .collect();
    // Brightest first, ties by node_id.
    viable.sort_by(|a, b| b.2.cmp(&a.2).then(a.0.cmp(&b.0)));

    let max_b = viable.len().min(hopnet_consensus::membership::B_MAX);
    let proven = proven_live(inp, est.band);
    for b in (1..=max_b).rev() {
        let bu = b as u64;
        if !hopnet_consensus::membership::posture_ok(inp.profile, v, bu)
            || !hopnet_consensus::membership::ceiling_ok(inp.profile, v, bu, proven)
        {
            continue;
        }
        let exposed = exposure(inp.profile, v, bu) > 0;
        let eligible: Vec<i32> = viable
            .iter()
            .filter(|(_, dep, span)| {
                *span >= inp.policy.req_span(exposed, *dep, est.headroom)
            })
            .map(|(id, _, _)| *id)
            .collect();
        if eligible.len() >= b {
            let mut batch: Vec<i32> = eligible[..b].to_vec();
            batch.sort_unstable();
            return Some(batch);
        }
    }
    None
}

/// Assembler: committed-state context + an evidence snapshot, dispatched
/// by function name. Callers guarantee `ValidationOrigin::Live`.
pub fn subjective_membership_check(
    app_state: &crate::AppState,
    db_tx: &rusqlite::Transaction<'_>,
    function: &str,
    payload: &[u8],
    _submitter_node: i32,
) -> Result<(), String> {
    let my_id = app_state
        .get_node_id()
        .map_err(|_| "node identity not initialized".to_string())?;
    let policy = hopnet_consensus::store::read_policy(db_tx).unwrap_or_default();
    let profile = hopnet_consensus::store::meta_get(
        db_tx,
        hopnet_consensus::store::META_QUORUM_PROFILE,
    )
    .ok()
    .flatten()
    .and_then(|b| String::from_utf8(b).ok())
    .and_then(|s| QuorumProfile::parse(&s))
    .unwrap_or(QuorumProfile::Auto);
    let committed = crate::db::consensus::get_current_consensus_height(db_tx)
        .map_err(|e| format!("{e:?}"))?;
    let pending = committed.saturating_add(1);
    let seated: Vec<i32> = hopnet_consensus::validators::get_validators(db_tx, pending)
        .map_err(|e| format!("{e}"))?
        .into_iter()
        .map(|v| v.node_id)
        .collect();
    let mut seat_starts: Vec<(i32, i32)> = Vec::with_capacity(seated.len());
    for id in &seated {
        if let Ok(Some(h)) = hopnet_consensus::validators::activation_height(db_tx, *id, pending)
        {
            seat_starts.push((*id, h));
        }
    }
    let snapshot = app_state.evidence.snapshot();
    let inp = GuardInputs {
        snapshot: &snapshot,
        origin: app_state.evidence.origin(),
        now: Instant::now(),
        policy: &policy,
        profile,
        seated: &seated,
        my_id,
        boot_height: app_state.evidence.boot_height(),
        seat_starts: &seat_starts,
        pending_height: i64::from(pending),
    };
    let cfg = bincode::config::standard();
    match function {
        "validator_vote_out" => {
            let (req, _) = bincode::serde::decode_from_slice::<
                crate::consensus::handlers::VoteOutRequest,
                _,
            >(payload, cfg)
            .map_err(|_| "vote-out payload".to_string())?;
            check_vote_out(&inp, req.node_id)
        }
        "validator_leave" => {
            let (req, _) = bincode::serde::decode_from_slice::<
                crate::consensus::handlers::LeaveRequest,
                _,
            >(payload, cfg)
            .map_err(|_| "leave payload".to_string())?;
            check_leave(&inp, req.node_id)
        }
        "validator_activation" => {
            let (req, _) = bincode::serde::decode_from_slice::<
                crate::consensus::handlers::ActivationRequest,
                _,
            >(payload, cfg)
            .map_err(|_| "activation payload".to_string())?;
            let mut members = Vec::with_capacity(req.members.len());
            for m in &req.members {
                let last_departure =
                    crate::db::consensus::last_departure(db_tx, *m, pending)
                        .map_err(|e| format!("{e:?}"))?;
                members.push(BatchMember {
                    node_id: *m,
                    last_departure,
                });
            }
            check_activation(&inp, &members)
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consensus::evidence::PeerEvidence;
    use std::time::Duration;

    fn policy_small() -> ConsensusPolicy {
        // probe_base 2s, grace 1s: t_out cliff/fast/lazy = 5/9/17 s.
        ConsensusPolicy::from_rows(&[
            ("probe_base".to_string(), "2".to_string()),
            ("grace".to_string(), "1".to_string()),
            ("s_full".to_string(), "6".to_string()),
            ("p_prove".to_string(), "6".to_string()),
        ])
    }

    fn view_at(last_contact: Instant, probes: u32, bright_since: Option<Instant>) -> PeerEvidence {
        PeerEvidence {
            last_contact,
            last_probe_at: None,
            probes_since_contact: probes,
            bright_since,
            // Fresh height so the catch-up gate passes; tests overriding
            // catch-up set this explicitly.
            last_known_height: Some(200),
        }
    }

    struct Fixture {
        origin: Instant,
        now: Instant,
        policy: ConsensusPolicy,
    }

    fn fixture() -> Fixture {
        let origin = Instant::now();
        Fixture {
            origin,
            now: origin + Duration::from_secs(1000),
            policy: policy_small(),
        }
    }

    fn inputs<'a>(
        f: &'a Fixture,
        snapshot: &'a [(i32, PeerEvidenceView)],
        seated: &'a [i32],
    ) -> GuardInputs<'a> {
        GuardInputs {
            snapshot,
            origin: f.origin,
            now: f.now,
            policy: &f.policy,
            profile: QuorumProfile::Majority,
            seated,
            my_id: 1,
            boot_height: None,
            seat_starts: &[],
            pending_height: 100,
        }
    }

    // Should: attest a target dark past t_out with the probe floor met;
    // refuse a fresh target, an under-probed one, and self.
    // Impact: the vote-out attestation — wrongful yes-votes steal
    // headroom; missing floors attest unprobed windows.
    #[test]
    fn vote_out_attestation_matrix() {
        let f = fixture();
        // seated [1,2,3]; node 2 fresh, node 3 dark with 2 probes.
        let snap = vec![
            (2, view_at(f.now - Duration::from_secs(1), 0, Some(f.origin))),
            (3, view_at(f.now - Duration::from_secs(60), 2, None)),
        ];
        let seated = [1, 2, 3];
        let inp = inputs(&f, &snap, &seated);

        assert!(check_vote_out(&inp, 3).is_ok(), "dark target passes");
        assert!(check_vote_out(&inp, 2).is_err(), "fresh target refused");
        assert!(check_vote_out(&inp, 1).is_err(), "self refused");

        // Under-probed: dark age but only one probe.
        let snap2 = vec![
            (2, view_at(f.now - Duration::from_secs(1), 0, Some(f.origin))),
            (3, view_at(f.now - Duration::from_secs(60), 1, None)),
        ];
        let inp2 = inputs(&f, &snap2, &seated);
        assert!(check_vote_out(&inp2, 3).is_err(), "one probe refused");
    }

    // Should: refuse a leave whose survivors, by my evidence, would not
    // hold quorum; pass when the survivors are live.
    // Impact: INV-NO-HARM's leave clause — the guard the S1 interim
    // version could not express.
    #[test]
    fn leave_survivor_math() {
        let f = fixture();
        let seated = [1, 2, 3];
        // Node 3 dark: if node 2 leaves, survivors {1,3} have live 1 <
        // quorum(2) = 2 — refuse.
        let snap = vec![
            (2, view_at(f.now - Duration::from_secs(1), 0, Some(f.origin))),
            (3, view_at(f.now - Duration::from_secs(120), 3, None)),
        ];
        let inp = inputs(&f, &snap, &seated);
        assert!(check_leave(&inp, 2).is_err());

        // Everyone live: node 2 leaving leaves {1,3} live 2 >= 2 — pass.
        let snap2 = vec![
            (2, view_at(f.now - Duration::from_secs(1), 0, Some(f.origin))),
            (3, view_at(f.now - Duration::from_secs(1), 0, Some(f.origin))),
        ];
        let inp2 = inputs(&f, &snap2, &seated);
        assert!(check_leave(&inp2, 2).is_ok());
    }

    // Should: gate a BATCH by joint posture + proven ceiling and per
    // member span/liveness/catch-up; approve any eligible batch without
    // ranking.
    // Impact: the mesh-initiated seating gate.
    fn member(id: i32, dep: Option<DepartureKind>) -> BatchMember {
        BatchMember { node_id: id, last_departure: dep }
    }

    #[test]
    fn activation_batch_gate_matrix() {
        let f = fixture();

        // Majority v=3: a single seat (3->4) is lateral => posture refuses.
        let seated3 = [1, 2, 3];
        let bright: Vec<(i32, PeerEvidenceView)> = (2..=5)
            .map(|id| (id, view_at(f.now - Duration::from_secs(1), 0, Some(f.origin))))
            .collect();
        let inp3 = inputs(&f, &bright, &seated3);
        assert!(
            check_activation(&inp3, &[member(4, None)]).is_err(),
            "lateral single refused"
        );
        // A batch of 2 (3->5) gains under majority => posture ok.
        assert!(check_activation(&inp3, &[member(4, None), member(5, None)]).is_ok());

        // Majority v=2 (waiver tol(2)=0): exposure-free 2->3, voted-out
        // member needs the cliff floor 2s; 3s bright passes, 1s fails.
        let seated2 = [1, 2];
        let ok = vec![
            (2, view_at(f.now - Duration::from_secs(1), 0, Some(f.origin))),
            (3, view_at(f.now - Duration::from_secs(1), 0, Some(f.now - Duration::from_secs(3)))),
        ];
        let inp = inputs(&f, &ok, &seated2);
        assert!(check_activation(&inp, &[member(3, Some(DepartureKind::VotedOut))]).is_ok());

        let short = vec![
            (2, view_at(f.now - Duration::from_secs(1), 0, Some(f.origin))),
            (3, view_at(f.now - Duration::from_secs(1), 0, Some(f.now - Duration::from_secs(1)))),
        ];
        let inp = inputs(&f, &short, &seated2);
        assert!(check_activation(&inp, &[member(3, Some(DepartureKind::VotedOut))]).is_err());
        // Voluntary + exposure-free: zero required, passes with a short span.
        assert!(check_activation(&inp, &[member(3, Some(DepartureKind::Voluntary))]).is_ok());

        // Currently-dark member: liveness floor refuses even at required 0.
        let dark = vec![
            (2, view_at(f.now - Duration::from_secs(1), 0, Some(f.origin))),
            (3, view_at(f.now - Duration::from_secs(120), 4, Some(f.origin))),
        ];
        let inp = inputs(&f, &dark, &seated2);
        assert!(check_activation(&inp, &[member(3, Some(DepartureKind::Voluntary))]).is_err());

        // Catch-up: a member whose height I never learned is refused.
        let mut unknown = view_at(f.now - Duration::from_secs(1), 0, Some(f.origin));
        unknown.last_known_height = None;
        let snap = vec![
            (2, view_at(f.now - Duration::from_secs(1), 0, Some(f.origin))),
            (3, unknown),
        ];
        let inp = inputs(&f, &snap, &seated2);
        assert!(check_activation(&inp, &[member(3, None)]).is_err());
    }
}
