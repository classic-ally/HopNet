//! AUTO quorum profile — the V_bft seam (RFC-CONSENSUS-002 S6).
//!
//! These exercise per-height threshold SELECTION end-to-end through a real
//! consensus run: the driver malachite builds is fed the composite
//! threshold for the committed set size, so a 7-node AUTO mesh runs BFT
//! (quorum 5) and a 6-or-fewer AUTO mesh runs majority.

use hopnet_consensus::sim::{FaultConfig, Sim};
use hopnet_consensus::QuorumProfile;

// Should: a full mesh decide under AUTO on BOTH sides of the seam — a
// 6-node mesh at majority thresholds, a 7-node mesh at BFT thresholds.
// Should not: stall because the driver got the wrong threshold ratio.
// Impact: proves HostCore installs the composite threshold from the boot
// valset (the seam's core mechanism).
#[test]
fn auto_decides_both_sides_of_the_seam() {
    // v=6 < V_bft: AUTO = majority (quorum 4). Full mesh decides.
    let mut below = Sim::new(6, QuorumProfile::Auto);
    below.start().unwrap();
    below.run(6, 6).unwrap();

    // v=7 >= V_bft: AUTO = BFT (quorum 5). Full mesh decides.
    let mut at = Sim::new(7, QuorumProfile::Auto);
    at.start().unwrap();
    at.run(6, 7).unwrap();
}

// Should: the AUTO threshold at v=7 be BFT — losing 3 of 7 (4 live) is
// below the BFT quorum (5), so consensus STALLS; the same crash under a
// pinned Majority profile (quorum 4) keeps deciding.
// Impact: the observable difference between AUTO's Byzantine hardening at
// v>=7 and plain majority — the whole point of the composite.
#[test]
fn auto_at_seam_needs_bft_quorum() {
    // Permanent partition: 3 nodes cut off from the other 4 for the whole
    // run. The 4-side can talk among itself — 4 votes. Under majority
    // (quorum 4) that decides; under BFT (quorum 5) it cannot.
    let partition = hopnet_consensus::sim::Partition {
        start: 0,
        end: u64::MAX,
        side_a: vec![4, 5, 6],
    };
    let faults = FaultConfig {
        partitions: vec![partition.clone()],
        ..FaultConfig::none(1)
    };

    // AUTO(7) = BFT quorum 5; the 4-side can't reach it → stalls.
    let mut auto = Sim::with_faults(7, QuorumProfile::Auto, faults);
    auto.start().unwrap();
    auto.run_safety_only(50_000).unwrap();
    let auto_max = (0..auto.n()).map(|i| auto.decided_height(i)).max().unwrap();

    // Pinned Majority(7) = quorum 4; the 4-side reaches it → decides.
    let faults_maj = FaultConfig {
        partitions: vec![partition],
        ..FaultConfig::none(1)
    };
    let mut maj = Sim::with_faults(7, QuorumProfile::Majority, faults_maj);
    maj.start().unwrap();
    maj.run_safety_only(50_000).unwrap();
    let maj_max = (0..maj.n()).map(|i| maj.decided_height(i)).max().unwrap();

    assert!(
        maj_max > auto_max,
        "majority (q4) must decide via the 4-side where AUTO-BFT (q5) cannot: maj={maj_max} auto={auto_max}"
    );
}
