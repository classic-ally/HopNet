//! RFC-019 S5 drain/seal scenarios over the deterministic sim — the
//! Evidence row's "drain reaches quiescence under faults; seal is
//! terminal" coverage. The sim models the boundary's SHAPE (admission
//! closing = work events stopping; the commit = the terminal height;
//! vote-iff-match dissent = a rejecting validator); the real-code paths
//! are covered by the main crate's byzantine/handler tests and the
//! orchestrator regenesis-seal scenario.

use hopnet_consensus::config::QuorumProfile;
use hopnet_consensus::sim::{FaultConfig, Sim};

/// SplitMix64 — deterministic per-seed scheduling for the sweeps.
fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

// Should: reach quiescence once admission closes, under seeded delay
// and duplication schedules — after the last staged work event, every
// node ends parked at a pending height with the event queue drained,
// and the safety oracles (agreement, contiguity, no-equivocation) hold
// throughout.
// Impact: the moratorium's structural promise — a finite pool drains
// and the on-demand engine goes quiescent on its own — holds under the
// reordering regime. Faults that STRAND a node behind a majority
// (permanent loss, partitions, crash-with-progress) are out of the
// drain's scope by design: recovering a laggard is decided-value
// sync's job, modeled only manually in this harness and covered for
// real by the malachite_integration laggard test; permanent loss
// denies liveness to any consensus protocol (the safety sweep's
// regime).
#[test]
fn drain_reaches_quiescence_under_faults_a() {
    drain_sweep(0..4);
}
#[test]
fn drain_reaches_quiescence_under_faults_b() {
    drain_sweep(4..8);
}
#[test]
fn drain_reaches_quiescence_under_faults_c() {
    drain_sweep(8..12);
}
#[test]
fn drain_reaches_quiescence_under_faults_d() {
    drain_sweep(12..16);
}

fn drain_sweep(seeds: std::ops::Range<u64>) {
    const CAP: u64 = 400_000;
    for seed in seeds {
        let n = 3 + (seed % 3) as i32; // 3..=5
        let mut fr = seed ^ 0xFA17_5EED;
        let faults = FaultConfig {
            seed,
            delay: 1..(10 + (splitmix(&mut fr) % 120)),
            drop_p: 0.0,
            duplicate_p: (splitmix(&mut fr) % 20) as f64 / 100.0,
            partitions: Vec::new(),
            crashes: Vec::new(),
        };
        let mut sim = Sim::with_faults_on_demand(n, QuorumProfile::Auto, faults);
        sim.start().unwrap();

        // Admission open: staged work arrives until it doesn't — closing
        // admission IS the moratorium, so the drain is whatever remains.
        let mut r = seed ^ 0xD0A1_57AB;
        for k in 0..12 {
            let vt = splitmix(&mut r) % 30_000;
            sim.schedule_work(vt, (k % n) as usize);
        }

        // Quiescence is a STATE FIXPOINT, not a drained event queue: a
        // parked core can keep re-arming timer noise (generation-stale
        // fires — a pre-existing harness/core quirk, independent of the
        // boundary), so the probe is: every node parked at a pending
        // height, and further event processing changes NOTHING.
        let _ = sim.run_safety_only(CAP).unwrap();
        sim.assert_agreement_common();
        let state = |sim: &Sim| {
            (0..n as usize)
                .map(|i| (sim.decided_height(i), sim.paused_at(i)))
                .collect::<Vec<_>>()
        };
        let settled = state(&sim);
        for (i, (decided, paused)) in settled.iter().enumerate() {
            assert!(
                paused.is_some(),
                "seed {seed}: node {i} did not quiesce (decided {decided})"
            );
        }
        let _ = sim.run_safety_only(50_000).unwrap();
        assert_eq!(
            settled,
            state(&sim),
            "seed {seed}: state kept moving after the drain"
        );
    }
}

// Should: treat the terminal height as final under seeded faults — no
// node ever decides past it, and every node that reaches it halts
// (parked with NO pending height, unlike ordinary quiescence).
// Impact: seal contract items 1–2 at the engine layer, fuzzed: crashes,
// partitions, and replays cannot push a sealed mesh past H.
#[test]
fn seal_is_terminal_under_faults_a() {
    terminal_sweep(0..3);
}
#[test]
fn seal_is_terminal_under_faults_b() {
    terminal_sweep(3..6);
}
#[test]
fn seal_is_terminal_under_faults_c() {
    terminal_sweep(6..9);
}
#[test]
fn seal_is_terminal_under_faults_d() {
    terminal_sweep(9..12);
}

fn terminal_sweep(seeds: std::ops::Range<u64>) {
    const TERMINAL: u64 = 4;
    for seed in seeds {
        let faults = FaultConfig::from_seed(seed, 3);
        let mut sim = Sim::with_faults_on_demand(3, QuorumProfile::Auto, faults);
        sim.start().unwrap();
        sim.seal_all_at(TERMINAL);

        // Enough staged work to blow far past the terminal without a seal.
        let mut r = seed ^ 0x5EA1_5EA1;
        for k in 0..16 {
            let vt = splitmix(&mut r) % 40_000;
            sim.schedule_work(vt, (k % 3) as usize);
        }

        sim.run_safety_only(80_000).unwrap();
        sim.assert_agreement_common();
        assert!(
            sim.max_agreed_height() <= TERMINAL,
            "seed {seed}: decided past the seal ({})",
            sim.max_agreed_height()
        );
        for i in 0..3 {
            if !sim.is_down(i) && sim.decided_height(i) == TERMINAL {
                assert_eq!(
                    sim.paused_at(i),
                    None,
                    "seed {seed}: node {i} reached the terminal but armed a next height"
                );
            }
        }
    }
}

// Should: fail to seal when a validator's vote-iff-match dissents and the
// quorum needs it (BFT at v=3 needs all three) — the boundary stalls
// BEFORE the terminal, nothing is lost, and the same mesh without the
// dissent seals normally.
// Impact: a proposer cannot seal a mesh onto a snapshot its validators
// cannot reproduce; a failed commit leaves the old chain intact (the
// spec's "nothing is ever lost by a failed commit — nobody crossed").
#[test]
fn no_seal_without_quorum_match() {
    const TERMINAL: u64 = 3;

    // Control: same schedule, no dissent — the mesh seals at the terminal.
    let mut control = Sim::new_on_demand(3, QuorumProfile::Bft);
    control.start().unwrap();
    control.seal_all_at(TERMINAL);
    for k in 0..8 {
        control.schedule_work(1 + k * 500, (k % 3) as usize);
    }
    control.run_safety_only(40_000).unwrap();
    assert_eq!(control.max_agreed_height(), TERMINAL, "control must seal");

    // Dissent: two nodes refuse every block at the terminal height — a
    // proposer never validates its OWN block, so whichever node proposes
    // at the terminal, a needed voter still refuses (BFT(3) needs all
    // three precommits).
    let mut sim = Sim::new_on_demand(3, QuorumProfile::Bft);
    sim.start().unwrap();
    sim.seal_all_at(TERMINAL);
    sim.set_reject_height(0, TERMINAL);
    sim.set_reject_height(1, TERMINAL);
    for k in 0..8 {
        sim.schedule_work(1 + k * 500, (k % 3) as usize);
    }
    sim.run_safety_only(40_000).unwrap();
    sim.assert_agreement_common();
    assert_eq!(
        sim.max_agreed_height(),
        TERMINAL - 1,
        "dissenting voters under BFT(3) must block the seal"
    );
}

// Should: keep a sealed mesh inert — wake signals after the halt find no
// deferred height and decide nothing, no matter how much work arrives.
// Impact: the quiescent park IS the halt; there is no code path from a
// wake signal back to a running engine past the terminal.
#[test]
fn post_seal_wake_is_inert() {
    const TERMINAL: u64 = 2;
    let mut sim = Sim::new_on_demand(3, QuorumProfile::Auto);
    sim.start().unwrap();
    sim.seal_all_at(TERMINAL);
    for k in 0..6 {
        sim.schedule_work(1 + k * 400, (k % 3) as usize);
    }
    sim.run_safety_only(40_000).unwrap();
    for i in 0..3 {
        assert_eq!(sim.decided_height(i), TERMINAL);
        assert_eq!(sim.paused_at(i), None, "node {i} must be halted, not paused");
    }

    // Late wakes: nothing moves.
    for i in 0..3 {
        sim.schedule_work(1, i);
    }
    sim.run_safety_only(10_000).unwrap();
    for i in 0..3 {
        assert_eq!(sim.decided_height(i), TERMINAL, "node {i} decided past the seal");
    }
}
