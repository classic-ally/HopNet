//! Consensus↔storage coupling contract (sans-io, no consensus dependency).
//!
//! The durability watermark sizes its fault budget `B(v) = v − quorum(v)` off
//! the SAME quorum math the consensus engine uses — both call
//! `hopnet_common::quorum::QuorumProfile::quorum`, so they cannot drift
//! (that parity is structural, not asserted here). What this file locks is the
//! BEHAVIOUR that coupling must produce: storage must buffer the burst the
//! ACTIVE profile survives, the AUTO seam must line up with the engine's
//! `profile_at`, and the operating watermark must stay above the durability
//! cliff wherever the active profile tolerates a fault.
//!
//! This is the fast (`cargo test`) mirror of what previously only the Docker
//! orchestrator (`vote_out`, `tier_membership`, `reencode`) exercised.

use hopnet_common::quorum::QuorumProfile;
use hopnet_storage::membership::{
    derive_view, watermark, watermark_floor, StoragePolicy, ViewNode,
};
use hopnet_storage::placement::{select_nodes_for_blob, MetricsRow};
use std::collections::HashMap;

const K: usize = 10; // hopnet_storage::rs::ORIGINAL_FRAGMENTS_PER_CHUNK
const V_BFT: u64 = hopnet_common::quorum::V_BFT;

// Should: storage never buffer LESS than the active profile's fault budget
// warrants — the Majority arm (which tolerates more loss than BFT at the same
// v) must produce a watermark ≥ the BFT arm at every mesh size.
// Should not: let a majority-profile mesh inherit BFT's smaller reserve (the
// pre-fix bug: hard-coded 2v/3+1 under-provisioned v∈{3,5,6}).
// Impact: a burst consensus survives dropping storage below K = permanent
// loss; this is the property that fix guards.
#[test]
fn majority_buffers_at_least_as_much_as_bft() {
    for v in 1..=30usize {
        let maj = watermark(v, QuorumProfile::Majority);
        let bft = watermark(v, QuorumProfile::Bft);
        assert!(
            maj >= bft,
            "v={v}: majority watermark {maj} < bft {bft} — storage under-buffers the crash-fault mesh"
        );
    }
}

// Should: AUTO resolve to the engine's per-height arm at the watermark — the
// Majority reserve below V_BFT, the BFT reserve at and above. This is the
// storage-side reflection of `QuorumProfile::profile_at`.
// Impact: if AUTO's watermark diverged from the engine's committed profile,
// storage would size durability for a fault model the mesh is not running.
#[test]
fn auto_tracks_the_seam() {
    for v in 1..=30usize {
        let auto = watermark(v, QuorumProfile::Auto);
        let expected = if (v as u64) >= V_BFT {
            watermark(v, QuorumProfile::Bft)
        } else {
            watermark(v, QuorumProfile::Majority)
        };
        assert_eq!(auto, expected, "v={v}: AUTO watermark off-seam");
    }
}

// Should: keep the operating watermark at or above the durability cliff
// (K + ⌈N/v⌉) for every profile wherever that profile tolerates ≥1 fault
// (B(v) ≥ 1), so urgent re-encode always fires while the control plane is
// still live. The boundary is profile-dependent: v≥3 under Majority/AUTO,
// v≥4 under BFT.
// Impact: a W below the floor at a fault-tolerant size marks chunks lazy
// while the mesh is one member from unreconstructable.
#[test]
fn watermark_above_floor_where_active_profile_is_fault_tolerant() {
    for profile in [
        QuorumProfile::Auto,
        QuorumProfile::Bft,
        QuorumProfile::Majority,
    ] {
        for v in 1..=30usize {
            let fault_tolerant = v.saturating_sub(profile.quorum(v as u64) as usize) >= 1;
            if !fault_tolerant {
                continue;
            }
            let w = watermark(v, profile);
            let floor = watermark_floor(v);
            assert!(w >= floor, "{profile:?} v={v}: W={w} < floor={floor}");
        }
    }
}

// Should: at the small-mesh sizes where AUTO runs Majority (v∈{3,5,6}), the
// active-profile watermark strictly exceed the old BFT-only value — the exact
// durability gap the fix closes. Pins the regression so a revert to hard-coded
// BFT quorum fails here.
// Impact: documents and guards the corrected v∈{3,5,6} under-provisioning.
#[test]
fn small_majority_meshes_buffer_more_than_old_bft_formula() {
    for v in [3usize, 5, 6] {
        let auto = watermark(v, QuorumProfile::Auto);
        let bft = watermark(v, QuorumProfile::Bft);
        assert!(
            auto > bft,
            "v={v}: AUTO {auto} not greater than BFT {bft} — the fix did not raise the reserve"
        );
        // And the corrected watermark keeps v=3 off the cliff (BFT gave K).
        if v == 3 {
            assert!(auto > K, "v=3: AUTO watermark {auto} did not clear K={K}");
        }
    }
}

// ---------------------------------------------------------------------------
// The §408 decoupling: consensus churn moves zero bytes.
//
// `derive_view` is the pure kernel of the host's storage_view(). It takes NO
// validator set — storage membership derives from availability, and the only
// consensus-derived input is the quorum `profile`, which sizes the watermark's
// fault budget and nothing else. These tests assert that: a change in the
// consensus state (profile) leaves the member set, weights, tiers, online set,
// AND the resulting placement byte-identical — only the watermark moves.
// ---------------------------------------------------------------------------

fn metrics(node_id: i32) -> MetricsRow {
    MetricsRow {
        node_id,
        trust_factor: 1.0,
        availability_score: 1.0,
        // Vary throughput a little so weights/placement are non-degenerate.
        throughput_score: 0.5 + (node_id as f64) * 0.03,
        latency_score: 0.9,
        stability_score: 0.9,
        storage_multiplier: 1.0,
    }
}

type AvailabilityFixture = HashMap<i32, Vec<(i64, bool)>>;

/// `n` fully-online nodes (absence 0 → all are storage members) with a dense
/// all-available grid, plus a matching profile-agnostic node universe.
fn scenario(n: i32) -> (Vec<ViewNode>, AvailabilityFixture, i64) {
    let step = 600i64;
    let mut nodes = Vec::new();
    let mut grid = HashMap::new();
    for id in 1..=n {
        nodes.push(ViewNode {
            node_id: id,
            pubkey: [id as u8; 32],
            metrics: metrics(id),
        });
        // 10 buckets, all available → current absence 0, ample history.
        grid.insert(id, (0..10).map(|b| (b * step, true)).collect());
    }
    (nodes, grid, step)
}

// Should: the derived member set, weights, tiers, and online set be identical
// under every quorum profile — membership is a function of availability, not
// of the consensus fault model.
// Should not: let the profile leak into anything but the watermark.
// Impact: this is the three-timescale decoupling the whole design rests on;
// if it broke, a validator churn (which at most changes the profile/count)
// would reshuffle placement and move real bytes.
#[test]
fn profile_moves_watermark_only_not_membership() {
    let (nodes, grid, step) = scenario(8);
    let policy = StoragePolicy::default();

    let bft = derive_view(100, nodes.clone(), &grid, step, &policy, QuorumProfile::Bft);
    let maj = derive_view(
        100,
        nodes.clone(),
        &grid,
        step,
        &policy,
        QuorumProfile::Majority,
    );
    let auto = derive_view(
        100,
        nodes.clone(),
        &grid,
        step,
        &policy,
        QuorumProfile::Auto,
    );

    let ids = |v: &hopnet_storage::traits::StorageView| {
        let mut m: Vec<i32> = v.members.iter().map(|p| p.node_id).collect();
        m.sort();
        m
    };
    // Member set identical across all profiles.
    assert_eq!(ids(&bft), ids(&maj), "member set moved with profile");
    assert_eq!(ids(&bft), ids(&auto));
    // Weights, tiers, online identical.
    assert_eq!(bft.weights, maj.weights, "weights moved with profile");
    assert_eq!(bft.tiers, maj.tiers, "tiers moved with profile");
    let mut on_bft = bft.online.clone();
    on_bft.sort();
    let mut on_maj = maj.online.clone();
    on_maj.sort();
    assert_eq!(on_bft, on_maj, "online set moved with profile");

    // The watermark — and ONLY the watermark — reflects the fault model.
    // At v=8, majority (q=5, B=3) buffers more than BFT (q=6, B=2).
    assert!(
        maj.watermark > bft.watermark,
        "watermark should track the profile's fault budget: maj={} bft={}",
        maj.watermark,
        bft.watermark
    );
    // AUTO at v=8 (≥ V_BFT) resolves to BFT.
    assert_eq!(auto.watermark, bft.watermark);
}

// Should: placement over the derived members be identical across profiles —
// the get/put fragment layout does not shift when the consensus fault model
// changes. This is the literal "moves zero bytes" claim.
// Impact: a profile-sensitive placement would re-home fragments on every AUTO
// seam crossing, defeating the decoupling.
#[test]
fn placement_is_profile_invariant() {
    let (nodes, grid, step) = scenario(9);
    let policy = StoragePolicy::default();
    let seed = [7u8; 32];

    let place = |profile| {
        let view = derive_view(100, nodes.clone(), &grid, step, &policy, profile);
        let mut selected: Vec<i32> =
            select_nodes_for_blob(view.members.clone(), view.metrics.clone(), &seed)
                .iter()
                .map(|p| p.node_id)
                .collect();
        selected.sort();
        selected
    };

    let bft = place(QuorumProfile::Bft);
    assert_eq!(
        bft,
        place(QuorumProfile::Majority),
        "placement moved under majority"
    );
    assert_eq!(
        bft,
        place(QuorumProfile::Auto),
        "placement moved under auto"
    );
    assert!(!bft.is_empty(), "sanity: some nodes were selected");
}
