//! Seeded fault-injection sweep — the safety fuzz oracle. Each seed derives a
//! full fault schedule (delay, loss, duplication, minority partition, crash +
//! restart) and runs the mesh under it; the agreement, contiguity, and
//! equivocation oracles inside `Sim` panic on the first violation. A failing
//! seed is printed and reproduces deterministically (SplitMix64, no wall-clock
//! or thread nondeterminism).
//!
//! This is the PR-tier corpus (fast, bounded). A cargo-fuzz target over the
//! same `Sim::run_safety_only` path is the nightly-tier extension.

use hopnet_consensus::config::QuorumProfile;
use hopnet_consensus::sim::{FaultConfig, Partition, Sim};

// Should: never violate agreement, contiguity, or no-equivocation across a
// broad sweep of randomized fault schedules and mesh sizes.
// Should not: let ANY seed produce divergence or a double-sign.
// Impact: the core safety guarantee of the migration, fuzzed. Liveness is not
// asserted here (loss/partition legitimately strand nodes pre-sync); safety is
// universal.
//
// Every sim is a pure function of its seed, so the sweep is chunked across
// test fns purely for wall-clock: cargo's harness parallelizes BETWEEN tests
// (one thread per fn), and per-event cost is real ed25519 signing — a single
// sequential fn made this the ~13-minute long pole of the whole corpus.
fn sweep_bft(seeds: std::ops::Range<u64>) {
    for seed in seeds {
        let n = 3 + (seed % 3) as i32; // 3, 4, or 5 nodes
        let faults = FaultConfig::from_seed(seed, n as usize);
        let mut sim = Sim::with_faults(n, QuorumProfile::Bft, faults);
        sim.start().unwrap();
        sim.run_safety_only(20_000).unwrap();
        sim.assert_agreement_common();
    }
}

#[test]
fn safety_sweep_bft_a() {
    sweep_bft(0..30);
}
#[test]
fn safety_sweep_bft_b() {
    sweep_bft(30..60);
}
#[test]
fn safety_sweep_bft_c() {
    sweep_bft(60..90);
}
#[test]
fn safety_sweep_bft_d() {
    sweep_bft(90..120);
}

// Should: hold the same safety guarantees under the crash-fault majority
// profile.
// Should not: let the weaker quorum threshold admit a divergence under faults.
// Impact: the CFT home-mesh profile fuzzed alongside BFT — the majority
// threshold must still give quorum intersection (safety) under the
// no-equivocation assumption the fakes honour.
fn sweep_cft(seeds: std::ops::Range<u64>) {
    for seed in seeds {
        let n = 3 + (seed % 2) as i32; // 3 or 4 nodes
        let faults = FaultConfig::from_seed(seed.wrapping_mul(2_654_435_761), n as usize);
        let mut sim = Sim::with_faults(n, QuorumProfile::Majority, faults);
        sim.start().unwrap();
        sim.run_safety_only(20_000).unwrap();
        sim.assert_agreement_common();
    }
}

#[test]
fn safety_sweep_cft_a() {
    sweep_cft(0..20);
}
#[test]
fn safety_sweep_cft_b() {
    sweep_cft(20..40);
}
#[test]
fn safety_sweep_cft_c() {
    sweep_cft(40..60);
}
#[test]
fn safety_sweep_cft_d() {
    sweep_cft(60..80);
}

// Should: FIRE the liveness oracle when a mesh genuinely cannot make progress.
// Should not: silently pass a stalled mesh.
// Impact: proves the oracle has teeth — a permanent partition that denies a
// node any quorum must be caught, not ignored, when we demand it decide.
#[test]
#[should_panic(expected = "no liveness")]
fn liveness_oracle_has_teeth() {
    // n=3 BFT → quorum = 3 (every node required). Isolating node 2 for the
    // whole run means nodes 0,1 (2 < 3) can NEVER reach quorum → no height
    // ever decides → the vtime bound trips the liveness assertion. (Bound is
    // finite; a huge-but-capped partition window keeps it permanent.)
    let faults = FaultConfig {
        seed: 1,
        delay: 1..5,
        drop_p: 0.0,
        duplicate_p: 0.0,
        // Permanent isolation (never heals): gst() caps at MAX_VTIME so the
        // liveness bound stays finite while the cut outlasts it.
        partitions: vec![Partition {
            start: 0,
            end: u64::MAX,
            side_a: vec![2],
        }],
        crashes: Vec::new(),
    };
    let mut sim = Sim::with_faults(3, QuorumProfile::Bft, faults);
    sim.start().unwrap();
    sim.run(1, 3).unwrap();
}
