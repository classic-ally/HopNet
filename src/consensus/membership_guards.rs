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

/// Readmission S_min gate for the LEGACY single activation (batch is S5).
///
/// SCOPE (transitional): only candidates with a committed prior departure
/// are gated — a never-seated candidate keeps the S1 join path (catch-up
/// check in the handler), because the legacy self-request IS the join
/// bootstrap until S5's mesh-initiated seating replaces it.
pub fn check_activation(
    inp: &GuardInputs<'_>,
    candidate: i32,
    last_dep: Option<DepartureKind>,
) -> Result<(), String> {
    let Some(dep) = last_dep else {
        return Ok(());
    };
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
    let exposed = exposure(inp.profile, v, 1) > 0;
    let required = inp.policy.req_span(exposed, Some(dep), est.headroom);
    // bright_span reads zero for a currently-dark candidate, so the
    // "answers my probe now" floor rides for free.
    let span = bright_span(
        lookup(inp.snapshot, candidate),
        inp.origin,
        inp.policy,
        est.band,
        inp.now,
    );
    if span < required {
        return Err(format!(
            "bright span {span:?} < required {required:?} (exposed={exposed}, dep={dep:?}, H={})",
            est.headroom
        ));
    }
    Ok(())
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
    .unwrap_or(QuorumProfile::Bft);
    let committed = crate::db::consensus::get_current_consensus_height(db_tx)
        .map_err(|e| format!("{e:?}"))?;
    let pending = committed.saturating_add(1);
    let seated: Vec<i32> = hopnet_consensus::validators::get_validators(db_tx, pending)
        .map_err(|e| format!("{e}"))?
        .into_iter()
        .map(|v| v.node_id)
        .collect();
    let snapshot = app_state.evidence.snapshot();
    let inp = GuardInputs {
        snapshot: &snapshot,
        origin: app_state.evidence.origin(),
        now: Instant::now(),
        policy: &policy,
        profile,
        seated: &seated,
        my_id,
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
            let last_dep = crate::db::consensus::last_departure(db_tx, req.node_id, pending)
                .map_err(|e| format!("{e:?}"))?;
            check_activation(&inp, req.node_id, last_dep)
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
            last_known_height: None,
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

    // Should: gate readmission by departure kind and span — never-seated
    // ungated; voted-out pays the floor at the cliff and s_full when
    // comfortable; voluntary pays nothing exposure-free; a currently-dark
    // candidate always fails (span reads zero).
    // Impact: the S_min gate — the flap bound and the pool's evidence
    // quality.
    #[test]
    fn activation_gate_matrix() {
        let f = fixture();
        // Majority v=2 seated [1,2]: candidate 3 seating is 2->3, quorum
        // flat (2) => exposure-free.
        let seated = [1, 2];

        // Never-seated: ungated regardless of evidence.
        let empty: Vec<(i32, PeerEvidenceView)> = Vec::new();
        let inp = inputs(&f, &empty, &seated);
        assert!(check_activation(&inp, 3, None).is_ok());

        // Voted-out, bright 3s at cliff/fast floors: H(live 2, q 2) = 0
        // => floor = s_floor(Cliff) = 2s; span 3s passes.
        let snap = vec![
            (2, view_at(f.now - Duration::from_secs(1), 0, Some(f.origin))),
            (
                3,
                view_at(
                    f.now - Duration::from_secs(1),
                    0,
                    Some(f.now - Duration::from_secs(3)),
                ),
            ),
        ];
        let inp = inputs(&f, &snap, &seated);
        assert!(check_activation(&inp, 3, Some(DepartureKind::VotedOut)).is_ok());

        // Voted-out but span 1s < floor 2s: refused.
        let snap2 = vec![
            (2, view_at(f.now - Duration::from_secs(1), 0, Some(f.origin))),
            (
                3,
                view_at(
                    f.now - Duration::from_secs(1),
                    0,
                    Some(f.now - Duration::from_secs(1)),
                ),
            ),
        ];
        let inp2 = inputs(&f, &snap2, &seated);
        assert!(check_activation(&inp2, 3, Some(DepartureKind::VotedOut)).is_err());

        // Voluntary, exposure-free: zero span required.
        assert!(check_activation(&inp2, 3, Some(DepartureKind::Voluntary)).is_ok());

        // Currently-dark candidate: span reads zero — refused for
        // voted-out even with an old bright_since.
        let snap3 = vec![
            (2, view_at(f.now - Duration::from_secs(1), 0, Some(f.origin))),
            (
                3,
                view_at(f.now - Duration::from_secs(120), 4, Some(f.origin)),
            ),
        ];
        let inp3 = inputs(&f, &snap3, &seated);
        assert!(check_activation(&inp3, 3, Some(DepartureKind::VotedOut)).is_err());
    }
}
