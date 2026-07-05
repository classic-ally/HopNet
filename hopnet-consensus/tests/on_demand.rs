//! On-demand heights: the shell/sim holds StartHeight until work exists.
//! These tests pin the WAKE RULES — host code the Quint-verified engine does
//! not cover: (1) local work starts our height regardless of proposer;
//! (2) an inbound message at the pending height resumes a paused node.

use hopnet_consensus::config::QuorumProfile;
use hopnet_consensus::sim::{FaultConfig, Sim};

// Should: a freshly-started on-demand mesh arms no timers, sends no messages,
// and decides nothing until work arrives.
// Should not: produce empty blocks or round-churn while idle.
// Impact: an idle home mesh is fully quiescent — zero disk and network churn.
#[test]
fn idle_mesh_stays_quiescent_until_work() {
    let mut sim = Sim::new_on_demand(3, QuorumProfile::Bft);
    sim.start().unwrap();

    // Nothing is scheduled: the event loop drains immediately with no decides.
    sim.run_safety_only(1_000_000).unwrap();
    for node in 0..3 {
        assert_eq!(sim.decided_height(node), 0, "node {node} decided while idle");
        assert_eq!(
            sim.paused_at(node),
            Some(1),
            "node {node} should be paused before height 1"
        );
    }
}

// Should: work at ONE node (not the round-0 proposer) wakes the whole mesh —
// its votes resume the others (wake rule 2) — and the height decides; the
// mesh then pauses again at the next height.
// Should not: deadlock because the work-holder isn't the proposer.
// Impact: wake rule 1 + 2 end-to-end; the on-demand happy path.
#[test]
fn work_at_non_proposer_wakes_mesh_and_decides() {
    let mut sim = Sim::new_on_demand(3, QuorumProfile::Bft);
    sim.start().unwrap();

    // select_proposer(h=1, r=0) = validators[(1+0) % 3] = node 1.
    // Give the work to node 0: it must start the height on its own, time out
    // round 0 locally, and its votes must wake nodes 1 and 2.
    sim.schedule_work(10, 0);
    sim.run(1, 3).unwrap();
    sim.assert_agreement_common();

    for node in 0..3 {
        assert_eq!(
            sim.paused_at(node),
            Some(2),
            "node {node} should re-pause before height 2 after deciding 1"
        );
    }
}

// Should: an idle mesh whose pending round-0 proposer is CRASHED still
// decides once work arrives elsewhere — the awake node's timeouts advance
// rounds past the dead proposer.
// Should not: deadlock with all timers unarmed (the failure mode the wake
// rules exist to prevent).
// Impact: risk #18 — liveness of on-demand heights under proposer crash.
#[test]
fn idle_proposer_crash_does_not_deadlock() {
    let faults = FaultConfig {
        // Crash node 1 (proposer of height 1 round 0) immediately; keep it
        // down past the horizon of this test.
        crashes: vec![(1, 1, 50_000_000)],
        ..FaultConfig::none(42)
    };
    let mut sim = Sim::with_faults_on_demand(3, QuorumProfile::Majority, faults);
    sim.start().unwrap();

    sim.schedule_work(10, 0);
    // Majority profile: quorum 2 of 3 — nodes 0 and 2 must decide.
    sim.run(1, 2).unwrap();
    sim.assert_agreement_common();
}

// Should: duplicate work signals are harmless (resume is idempotent), and a
// second batch of work advances the chain by exactly one more height.
// Should not: double-start a height or decide extra heights.
// Impact: the driver may fire Resume freely without protocol bookkeeping.
#[test]
fn resume_is_idempotent_and_chain_advances_on_demand() {
    let mut sim = Sim::new_on_demand(3, QuorumProfile::Bft);
    sim.start().unwrap();

    sim.schedule_work(10, 1);
    sim.schedule_work(11, 1); // duplicate — must be a no-op
    sim.schedule_work(12, 2); // concurrent wake on another node
    sim.run(1, 3).unwrap();

    // Second round of work → exactly height 2 (contiguity oracle enforces
    // the "exactly" part).
    sim.schedule_work(1_000_000, 2);
    sim.run(2, 3).unwrap();
    sim.assert_agreement_common();

    for node in 0..3 {
        assert_eq!(sim.paused_at(node), Some(3));
    }
}

// Should: a node that crashes WHILE PAUSED restarts paused (its pending
// height has no WAL state) and rejoins on the next wake.
// Should not: start the pending height at boot and round-churn alone.
// Impact: the boot rule (empty WAL ⇒ defer) across crash/restart.
#[test]
fn restart_while_paused_stays_paused_then_rejoins() {
    let faults = FaultConfig {
        // Well after height 1 decides; back up 1s later.
        crashes: vec![(200_000, 2, 1_000)],
        ..FaultConfig::none(7)
    };
    let mut sim = Sim::with_faults_on_demand(3, QuorumProfile::Bft, faults);
    sim.start().unwrap();

    sim.schedule_work(10, 1);
    sim.run(1, 3).unwrap();

    // Wake the mesh again after node 2's crash+restart cycle.
    sim.schedule_work(300_000, 0);
    sim.run(2, 3).unwrap();
    sim.assert_agreement_common();
}
