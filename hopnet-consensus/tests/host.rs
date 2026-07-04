//! Host-loop integration over the in-memory simulator: multi-node decide,
//! crash/WAL-replay recovery, and the CFT quorum profile. The equivocation
//! oracle (a correct node never signs two values for one height/round/type)
//! runs continuously inside the Cluster and fires on the FIRST violation —
//! that oracle is the safety guarantee the whole migration exists to secure.

use hopnet_consensus::config::QuorumProfile;
use hopnet_consensus::sim::Cluster;

// Should: reach agreement across all nodes for many consecutive heights,
// each node deciding the identical block.
// Should not: diverge, stall, or skip a height.
// Impact: the baseline liveness+agreement property; without it nothing else
// matters.
#[test]
fn three_nodes_decide_many_heights() {
    let mut c = Cluster::new(3, QuorumProfile::Bft);
    c.start().unwrap();
    c.run_to_height(8).unwrap();
    c.assert_agreement(8);
}

// Should: keep deciding after a node crashes and restarts mid-run, with the
// restarted node replaying its WAL and rejoining without re-signing.
// Should not: let the restarted node equivocate (sign a different value for a
// height/round it already voted on before the crash).
// Impact: THE safety-critical property — WAL-replay correctness across a
// crash. The Cluster's sign-tracking oracle asserts no equivocation through
// the restart.
#[test]
fn crash_restart_replays_wal_without_equivocation() {
    // The Stage-2 crash property: a node that crashes mid-height and restarts
    // must (a) replay its WAL without EQUIVOCATING — never sign a second,
    // different value for a height/round/type it already voted on — and (b)
    // not corrupt any already-decided history. Full catch-up of a lagged node
    // needs decided-value sync (Stage 4); here we assert the safety property,
    // not reconvergence.
    //
    // The equivocation oracle runs continuously inside the Cluster and panics
    // on the first violation, so simply exercising the restarted node past its
    // crash point is the test.
    let mut c = Cluster::new(4, QuorumProfile::Bft);
    c.start().unwrap();
    c.run_to_height(3).unwrap();
    c.assert_agreement(3);

    // Crash into a genuine mid-height WAL (voted/proposed, not yet decided).
    let mut crashed = false;
    for _ in 0..1000 {
        c.step().unwrap();
        if c.nodes[2].storage.wal_len() > 0 {
            c.crash_restart(2).unwrap();
            crashed = true;
            break;
        }
    }
    assert!(crashed, "never observed a mid-height WAL to crash into");

    // Drive the mesh past the crash. The oracle asserts no-equivocation
    // throughout; the healthy majority keeps deciding.
    c.run_bounded(2000).unwrap();

    // Every height that ALL nodes reached still agrees, and the pre-crash
    // history (heights 1..3) was not corrupted by the replay.
    let common = c.assert_agreement_common();
    assert!(common >= 3, "pre-crash decided history must survive replay");
}

// Should: restart cleanly even when nothing was mid-flight (empty WAL for the
// resume height).
// Should not: fail or replay stale entries when there is nothing to replay.
// Impact: the common restart path (clean shutdown at a height boundary) must
// be as safe as the mid-height one.
#[test]
fn restart_with_empty_wal_is_a_noop_resume() {
    let mut c = Cluster::new(3, QuorumProfile::Bft);
    c.start().unwrap();
    c.run_to_height(4).unwrap();
    c.crash_restart(0).unwrap();
    c.run_to_height(7).unwrap();
    c.assert_agreement(7);
}

// Should: decide in a 3-node mesh under the majority (CFT) quorum profile.
// Should not: require a 2/3 supermajority — the home-mesh profile decides at
// >1/2.
// Impact: exercises the CFT threshold end-to-end through the real host loop,
// not just the certificate-verification unit tests.
#[test]
fn cft_profile_three_node_mesh_decides() {
    let mut c = Cluster::new(3, QuorumProfile::Majority);
    c.start().unwrap();
    c.run_to_height(5).unwrap();
    c.assert_agreement(5);
}

// Should: decide in a 2-node mesh under the majority profile (quorum = 2).
// Should not: deadlock — both nodes present means quorum is met.
// Impact: the smallest useful home mesh; confirms n=2 is live under CFT.
#[test]
fn cft_profile_two_node_mesh_decides() {
    let mut c = Cluster::new(2, QuorumProfile::Majority);
    c.start().unwrap();
    c.run_to_height(4).unwrap();
    c.assert_agreement(4);
}
