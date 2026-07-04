//! Host-loop integration over the virtual-clock simulator: multi-node decide,
//! fault injection (delay / drop / partition), crash+restart recovery, and the
//! CFT quorum profile. The agreement, contiguity, and equivocation oracles run
//! continuously inside `Sim` and fire on the FIRST violation — those oracles
//! are the safety guarantees the whole migration exists to secure.

use hopnet_consensus::config::QuorumProfile;
use hopnet_consensus::sim::{FaultConfig, Partition, Sim};

// Should: reach agreement across all nodes for many consecutive heights.
// Should not: diverge, stall, or skip a height.
// Impact: the baseline liveness+agreement property; the no-fault seed 0 of the
// regression corpus.
#[test]
fn three_nodes_decide_many_heights() {
    let mut sim = Sim::new(3, QuorumProfile::Bft);
    sim.start().unwrap();
    sim.run(8, 3).unwrap();
    sim.assert_agreement_common();
}

// Should: still agree under randomized message delay and reordering.
// Should not: let out-of-order delivery produce divergence.
// Impact: reordering is the most common real-network perturbation; the engine
// must be order-insensitive and our host must not add ordering assumptions.
#[test]
fn agreement_under_delay_and_reorder() {
    let faults = FaultConfig {
        seed: 42,
        delay: 1..50, // wide range → messages reorder relative to each other
        drop_p: 0.0,
        duplicate_p: 0.1,
        partitions: Vec::new(),
        crashes: Vec::new(),
    };
    let mut sim = Sim::with_faults(4, QuorumProfile::Bft, faults);
    sim.start().unwrap();
    sim.run(6, 4).unwrap();
    sim.assert_agreement_common();
}

// Should: never let message loss produce DIVERGENCE or equivocation, even as
// nodes drift to different heights.
// Should not: assert full liveness — a node that loses a decision's messages
// falls behind, and catching up needs decided-value sync (Stage 4). At Stage 3
// loss guarantees SAFETY (agreement on shared heights, no equivocation), not
// that every node reaches the tip.
// Impact: separates the two failure modes cleanly — loss costs liveness (fixed
// by sync later), never safety.
#[test]
fn message_loss_preserves_safety() {
    let faults = FaultConfig {
        seed: 7,
        delay: 1..20,
        drop_p: 0.3,
        duplicate_p: 0.0,
        partitions: Vec::new(),
        crashes: Vec::new(),
    };
    let mut sim = Sim::with_faults(4, QuorumProfile::Bft, faults);
    sim.start().unwrap();
    // The oracles run continuously and panic on the first violation.
    sim.run_safety_only(50_000).unwrap();
    // Whatever heights the nodes managed to reach, they agree on the shared
    // prefix, and at least the healthy majority made some progress.
    let common = sim.assert_agreement_common();
    let max = (0..sim.n()).map(|i| sim.decided_height(i)).max().unwrap();
    assert!(max >= 1, "no node made any progress under loss");
    let _ = common;
}

// Should: recover and reconverge after a network partition heals.
// Should not: let the two sides decide conflicting values during the split.
// Impact: partition tolerance is the defining BFT property; a minority side
// (1 of 4) cannot reach quorum, so it cannot decide anything conflicting, and
// once healed everyone agrees.
#[test]
fn partition_then_heal() {
    let faults = FaultConfig {
        seed: 99,
        delay: 1..10,
        drop_p: 0.0,
        duplicate_p: 0.0,
        // Isolate node 3 for an initial window, then heal.
        partitions: vec![Partition {
            start: 0,
            end: 300,
            side_a: vec![3],
        }],
        crashes: Vec::new(),
    };
    let mut sim = Sim::with_faults(4, QuorumProfile::Bft, faults);
    sim.start().unwrap();
    // The isolated node (1 of 4) cannot reach quorum, so it decides nothing
    // during the split and — lacking sync (Stage 4) — stays behind after the
    // heal. Only the 3-node majority reaches the target; safety holds for all.
    sim.run(5, 3).unwrap();
    sim.assert_agreement_common();
}

// Should: keep the healthy quorum deciding when a node crashes, and let the
// restarted node replay its WAL without EQUIVOCATING.
// Should not: let the crash or the replay corrupt agreement or produce a
// second signature for a slot the node already voted on.
// Impact: THE safety-critical crash property. Full catch-up of the laggard
// needs decided-value sync (Stage 4); here we assert safety (agreement +
// no-equivocation) plus liveness for the surviving quorum.
#[test]
fn crash_restart_healthy_quorum_progresses_no_equivocation() {
    let faults = FaultConfig {
        seed: 5,
        delay: 1..10,
        drop_p: 0.0,
        duplicate_p: 0.0,
        partitions: Vec::new(),
        // Crash node 2 early, restart it later. n=4 BFT quorum=3 → the other
        // three retain quorum and keep deciding.
        crashes: vec![(50, 2, 400)],
    };
    let mut sim = Sim::with_faults(4, QuorumProfile::Bft, faults);
    sim.start().unwrap();
    // Expect the 3-node surviving quorum to reach height 6.
    sim.run(6, 3).unwrap();
    // Safety holds for every height all nodes share.
    sim.assert_agreement_common();
}

// Should: decide in a 3-node mesh under the majority (CFT) profile.
// Should not: require a 2/3 supermajority.
// Impact: the home-mesh profile end-to-end through the real host loop.
#[test]
fn cft_profile_three_node_mesh_decides() {
    let mut sim = Sim::new(3, QuorumProfile::Majority);
    sim.start().unwrap();
    sim.run(5, 3).unwrap();
    sim.assert_agreement_common();
}

// Should: decide in a 2-node mesh under the majority profile (quorum = 2).
// Should not: deadlock.
// Impact: smallest useful home mesh.
#[test]
fn cft_profile_two_node_mesh_decides() {
    let mut sim = Sim::new(2, QuorumProfile::Majority);
    sim.start().unwrap();
    sim.run(4, 2).unwrap();
    sim.assert_agreement_common();
}
