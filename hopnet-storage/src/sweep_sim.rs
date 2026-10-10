//! A deterministic model of the rolling sweep's attestation refresh across
//! a mesh, for sizing and guarding the refresh policy (test-only).
//!
//! What it keeps from production: 256 shards visited once a rotation,
//! paced over `rotation_secs`; the decision (`sweep::refresh_due`) and the
//! rotation estimate (`sweep::estimate_rotation_heights`) themselves; page
//! buffers flushed when full or a minute old and stamped at their floor
//! height; and, the part a mesh test can't reach, heights made by the
//! pages themselves on top of background traffic — on a quiet mesh nearly
//! every height is an attestation page (pages and heights run about one
//! to one), so the refresh feeds back into its own clock.
//!
//! What it leaves out: decide latency (inside `REFRESH_MARGIN_HEIGHTS`),
//! several pages sharing a block, and the belief side. A failed sweep is a
//! whole rotation whose pages never land. Fragment counts and page size
//! are scaled down together; heights are not.

use std::collections::VecDeque;

use hopnet_common::Blake3Hash;

use crate::sweep::{
    estimate_rotation_heights, refresh_due, refresh_jitter, refresh_page_due, InventoryStamp,
    RefreshDue, RefreshJitter, RefreshPolicy,
};

const SHARDS: usize = 256;
/// The sweep's buffer age before the refresh: `engine::policy::
/// EVIDENCE_MAX_AGE_SECS` (behind the engine feature).
const BUFFER_AGE_SECS: u64 = 60;
/// The shortest slice of a rotation in progress worth extrapolating.
const MIN_SAMPLE_SECS: u64 = 300;
/// Completed rotations the estimate looks back over.
const HISTORY: usize = 3;
const HOUR: u64 = 3600;
const DAY: u64 = 24 * HOUR;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// Every present fragment, every rotation: the behavior before.
    EveryRotation,
    Refresh {
        cap: bool,
        jitter: bool,
    },
}

#[derive(Debug, Clone)]
struct NodeSpec {
    fragments: usize,
    rotation_secs: u64,
    /// Rotations (0-based) whose pages all fail.
    failed_rotations: Vec<u64>,
}

impl NodeSpec {
    fn hourly(fragments: usize) -> Self {
        NodeSpec {
            fragments,
            rotation_secs: HOUR,
            failed_rotations: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Burst {
    node: usize,
    at_secs: u64,
    fragments: usize,
}

#[derive(Debug, Clone)]
struct Scenario {
    nodes: Vec<NodeSpec>,
    page_size: usize,
    window: u64,
    /// Background heights per hour from each start time on, ascending.
    traffic: Vec<(u64, u64)>,
    secs: u64,
    mode: Mode,
    burst: Option<Burst>,
}

impl Scenario {
    /// Three nodes at the live mesh's shape, scaled 1:100 (2.2M, 2.3M and
    /// 0.8M fragments; 8192-hash pages), quiet background traffic.
    fn quiet_mesh(mode: Mode, days: u64) -> Self {
        Scenario {
            nodes: vec![
                NodeSpec::hourly(22_000),
                NodeSpec::hourly(23_000),
                NodeSpec::hourly(8_000),
            ],
            page_size: 82,
            window: crate::lifecycle::ATTESTATION_RECENCY_HEIGHTS,
            traffic: vec![(0, 30)],
            secs: days * DAY,
            mode,
            burst: None,
        }
    }

    fn background_at(&self, t: u64) -> u64 {
        self.traffic
            .iter()
            .take_while(|(from, _)| *from <= t)
            .last()
            .map_or(0, |(_, rate)| *rate)
    }
}

#[derive(Debug, Clone, Copy)]
struct Frag {
    jitter: RefreshJitter,
    verified: u64,
    stamped_at: u64,
    burst: bool,
}

struct Node {
    spec: NodeSpec,
    frags: Vec<Frag>,
    by_shard: Vec<Vec<u32>>,
    steps: u64,
    rotation: u64,
    rotation_started: (u64, u64),
    completed: VecDeque<u64>,
    buffer: Vec<u32>,
    buffer_floor: Option<u64>,
    buffer_opened: Option<u64>,
}

impl Node {
    fn next_step_at(&self) -> u64 {
        self.steps * self.spec.rotation_secs / SHARDS as u64
    }

    fn add(&mut self, node_id: i32, count: usize, height: u64, t: u64, burst: bool) {
        for _ in 0..count {
            let index = self.frags.len() as u32;
            let mut seed = node_id.to_le_bytes().to_vec();
            seed.extend_from_slice(&index.to_le_bytes());
            let hash = Blake3Hash::from_bytes(*blake3::hash(&seed).as_bytes());
            self.by_shard[usize::from(crate::sweep::shard_of(&hash))].push(index);
            self.frags.push(Frag {
                jitter: refresh_jitter(node_id, &hash),
                verified: height,
                stamped_at: t,
                burst,
            });
        }
    }

    fn policy(&self, mode: Mode, window: u64, height: u64, t: u64) -> RefreshPolicy {
        let Mode::Refresh { cap, jitter } = mode else {
            return RefreshPolicy::EVERY_ROTATION;
        };
        let completed: Vec<u64> = self.completed.iter().copied().collect();
        let Some(rotation) = estimate_rotation_heights(
            &completed,
            height - self.rotation_started.1,
            t - self.rotation_started.0,
            self.spec.rotation_secs,
            MIN_SAMPLE_SECS,
        ) else {
            return RefreshPolicy::EVERY_ROTATION;
        };
        let mut policy = RefreshPolicy::for_rotation(window, rotation);
        if !cap {
            policy.max_age_secs = u64::MAX;
            policy.cap_jitter_secs = 0;
        }
        if !jitter {
            policy.jitter_heights = 0;
            policy.cap_jitter_secs = 0;
        }
        policy
    }
}

#[derive(Debug, Default)]
struct Outcome {
    /// The oldest any stamp got, in heights, before its refresh or at the
    /// end: what a confirm or surplus check could have seen.
    max_age: u64,
    /// Pages decided, mesh-wide, per hour.
    pages_by_hour: Vec<u64>,
    /// Fragment stamps applied, mesh-wide, per hour: every node's apply
    /// cost, row by row.
    attested_by_hour: Vec<u64>,
    /// Heights decided (pages and background) per hour.
    heights_by_hour: Vec<u64>,
    /// Burst fragments refreshed per rotation of the burst node.
    burst_refreshed_by_rotation: Vec<u64>,
}

impl Outcome {
    fn pages_per_hour_over(&self, hours: std::ops::Range<usize>) -> f64 {
        let n = hours.len() as f64;
        self.pages_by_hour[hours].iter().sum::<u64>() as f64 / n
    }

    fn attested_per_hour_over(&self, hours: std::ops::Range<usize>) -> f64 {
        let n = hours.len() as f64;
        self.attested_by_hour[hours].iter().sum::<u64>() as f64 / n
    }

    fn heights_per_hour_over(&self, hours: std::ops::Range<usize>) -> f64 {
        let n = hours.len() as f64;
        self.heights_by_hour[hours].iter().sum::<u64>() as f64 / n
    }
}

fn run(scenario: &Scenario) -> Outcome {
    let hours = scenario.secs.div_ceil(HOUR) as usize;
    let mut out = Outcome {
        pages_by_hour: vec![0; hours],
        attested_by_hour: vec![0; hours],
        heights_by_hour: vec![0; hours],
        ..Default::default()
    };
    let mut height = 0u64;
    let mut background = 0u64;
    let mut nodes: Vec<Node> = scenario
        .nodes
        .iter()
        .enumerate()
        .map(|(id, spec)| {
            let mut node = Node {
                spec: spec.clone(),
                frags: Vec::with_capacity(spec.fragments),
                by_shard: vec![Vec::new(); SHARDS],
                steps: 0,
                rotation: 0,
                rotation_started: (0, 0),
                completed: VecDeque::new(),
                buffer: Vec::new(),
                buffer_floor: None,
                buffer_opened: None,
            };
            node.add(id as i32, spec.fragments, 0, 0, false);
            node
        })
        .collect();

    for t in 0..scenario.secs {
        let hour = (t / HOUR) as usize;
        background += scenario.background_at(t);
        while background >= HOUR {
            background -= HOUR;
            height += 1;
            out.heights_by_hour[hour] += 1;
        }
        if let Some(burst) = scenario.burst.filter(|b| b.at_secs == t) {
            nodes[burst.node].add(burst.node as i32, burst.fragments, height, t, true);
        }
        for (id, node) in nodes.iter_mut().enumerate() {
            if node.next_step_at() > t {
                continue;
            }
            let shard = (node.steps % SHARDS as u64) as usize;
            let policy = node.policy(scenario.mode, scenario.window, height, t);
            for &i in &node.by_shard[shard] {
                let f = node.frags[i as usize];
                let stamp = InventoryStamp {
                    verified_height: Some(f.verified),
                    provenance: Some(0),
                    suspect: false,
                };
                let due = refresh_due(
                    Some(&stamp),
                    height,
                    Some(t - f.stamped_at),
                    f.jitter,
                    &policy,
                );
                if due != RefreshDue::No {
                    node.buffer.push(i);
                    node.buffer_floor = Some(node.buffer_floor.map_or(height, |h| h.min(height)));
                    node.buffer_opened.get_or_insert(t);
                }
            }
            let failed = node.spec.failed_rotations.contains(&node.rotation);
            loop {
                let waited_secs = node.buffer_opened.map_or(0, |o| t - o);
                let due = match scenario.mode {
                    Mode::EveryRotation => {
                        !node.buffer.is_empty()
                            && (node.buffer.len() >= scenario.page_size
                                || waited_secs >= BUFFER_AGE_SECS)
                    }
                    Mode::Refresh { .. } => refresh_page_due(
                        node.buffer.len(),
                        scenario.page_size,
                        waited_secs,
                        node.buffer_floor.map_or(0, |f| height - f),
                    ),
                };
                if !due {
                    break;
                }
                let n = scenario.page_size.min(node.buffer.len());
                let page: Vec<u32> = node.buffer.drain(..n).collect();
                let (floor, opened) = (node.buffer_floor.unwrap(), node.buffer_opened.unwrap());
                if node.buffer.is_empty() {
                    node.buffer_floor = None;
                    node.buffer_opened = None;
                }
                if failed {
                    continue;
                }
                height += 1;
                out.heights_by_hour[hour] += 1;
                out.pages_by_hour[hour] += 1;
                out.attested_by_hour[hour] += page.len() as u64;
                for i in page {
                    let f = &mut node.frags[i as usize];
                    out.max_age = out.max_age.max(height - 1 - f.verified);
                    f.verified = floor;
                    f.stamped_at = opened;
                    if f.burst && scenario.burst.is_some_and(|b| b.node == id) {
                        let r = node.rotation as usize;
                        if out.burst_refreshed_by_rotation.len() <= r {
                            out.burst_refreshed_by_rotation.resize(r + 1, 0);
                        }
                        out.burst_refreshed_by_rotation[r] += 1;
                    }
                }
            }
            node.steps += 1;
            if node.steps % SHARDS as u64 == 0 {
                node.completed.push_back(height - node.rotation_started.1);
                if node.completed.len() > HISTORY {
                    node.completed.pop_front();
                }
                node.rotation += 1;
                node.rotation_started = (t, height);
            }
        }
    }
    for node in &nodes {
        for f in &node.frags {
            out.max_age = out.max_age.max(height - f.verified);
        }
    }
    out
}

const REFRESH: Mode = Mode::Refresh {
    cap: true,
    jitter: true,
};

// Impact: the guarantee the refresh trades volume against. The recency
// rule is consensus; stamps the refresh lets age past it make blobs
// unconfirmable and surplus copies unreleasable mesh-wide.
// Should: keep every stamp inside the window over a quiet week, with a
// slow node and a failed sweep on each node.
#[test]
fn refresh_keeps_every_stamp_inside_the_window_through_failed_sweeps() {
    let mut scenario = Scenario::quiet_mesh(REFRESH, 7);
    scenario.nodes[1].rotation_secs = 90 * 60;
    scenario.nodes[0].failed_rotations = vec![30, 31];
    scenario.nodes[1].failed_rotations = vec![60];
    scenario.nodes[2].failed_rotations = vec![100];
    let out = run(&scenario);
    assert!(
        out.max_age <= scenario.window,
        "a stamp reached {} heights against a {}-height window",
        out.max_age,
        scenario.window
    );
}

// Impact: the hard case, and the one that set the rotation floor. A quiet
// mesh lets height ages grow, then a drain (~45 heights a minute in all,
// the 2026-09-27 rehearsal the window was sized for) ages every stamp
// ~2,700 heights an hour; a failed sweep right at the surge doubles the
// wait. Budgeting for the quiet mesh's own rotations put a stamp at 9,008
// heights here.
// Should: keep every stamp inside the window when traffic jumps from quiet
// to drain, with a failed sweep in the surge's first rotation.
#[test]
fn refresh_keeps_every_stamp_inside_the_window_through_a_drain_surge() {
    let mut scenario = Scenario::quiet_mesh(REFRESH, 5);
    // Background on top of the refresh pages, which run ~700 an hour
    // while the surge makes everything due.
    scenario.traffic = vec![(0, 30), (3 * DAY, 2_000)];
    scenario.nodes[0].failed_rotations = vec![72];
    let out = run(&scenario);
    let surge = (3 * 24 + 2) as usize..(5 * 24) as usize;
    eprintln!(
        "max age {} heights; surge heights/h {:.0}",
        out.max_age,
        out.heights_per_hour_over(surge)
    );
    assert!(
        out.max_age <= scenario.window,
        "a stamp reached {} heights against a {}-height window",
        out.max_age,
        scenario.window
    );
}

// Impact: what overloads a spindle node is the apply cost of every node
// re-stamping every fragment hourly — rows, not pages.
// Should: cut the fragment stamps a quiet mesh applies by at least tenfold
// against attesting everything every rotation.
// Should: settle the mesh's height rate instead of letting it oscillate.
#[test]
fn refresh_cuts_quiet_mesh_volume_by_an_order_of_magnitude() {
    let days = 5;
    let last_two_days = ((days - 2) * 24) as usize..(days * 24) as usize;
    let before = run(&Scenario::quiet_mesh(Mode::EveryRotation, days));
    let after = run(&Scenario::quiet_mesh(REFRESH, days));
    let ratio = |f: fn(&Outcome, std::ops::Range<usize>) -> f64| {
        f(&before, last_two_days.clone()) / f(&after, last_two_days.clone())
    };
    let stamps = ratio(Outcome::attested_per_hour_over);
    eprintln!(
        "stamps/h {:.0} -> {:.0} ({stamps:.1}x); pages/h {:.0} -> {:.1} ({:.1}x); heights/h {:.0} -> {:.1}",
        before.attested_per_hour_over(last_two_days.clone()),
        after.attested_per_hour_over(last_two_days.clone()),
        before.pages_per_hour_over(last_two_days.clone()),
        after.pages_per_hour_over(last_two_days.clone()),
        ratio(Outcome::pages_per_hour_over),
        before.heights_per_hour_over(last_two_days.clone()),
        after.heights_per_hour_over(last_two_days.clone()),
    );
    assert!(stamps >= 10.0, "only {stamps:.1}x fewer fragment stamps");

    let day = |d: u64| ((d - 1) * 24) as usize..(d * 24) as usize;
    let (d4, d5) = (
        after.heights_per_hour_over(day(4)),
        after.heights_per_hour_over(day(5)),
    );
    assert!(
        (d4 - d5).abs() <= d4.max(d5) * 0.25,
        "heights/h swung from {d4:.1} to {d5:.1} between days"
    );
}

// Impact: a big upload lands its stamps together; without jitter they all
// come due under the cap in the same rotation a day later, and again every
// day after — one hour of drain-sized pages on an otherwise quiet mesh.
// Should: spread a big upload's cap refresh over several rotations.
// Should: cut the burst's busiest rotation at least threefold against no
// jitter.
#[test]
fn jitter_spreads_a_big_upload_across_rotations() {
    let burst = Burst {
        node: 0,
        at_secs: DAY + 600,
        fragments: 20_000,
    };
    let busiest = |jitter: bool| {
        let mut scenario = Scenario::quiet_mesh(Mode::Refresh { cap: true, jitter }, 4);
        scenario.burst = Some(burst);
        let out = run(&scenario);
        let after_upload =
            &out.burst_refreshed_by_rotation[25.min(out.burst_refreshed_by_rotation.len())..];
        let busiest = after_upload.iter().copied().max().unwrap_or(0);
        let rotations = after_upload.iter().filter(|n| **n > 0).count();
        (busiest, rotations)
    };
    let (flat, flat_rotations) = busiest(false);
    let (spread, spread_rotations) = busiest(true);
    eprintln!(
        "burst's busiest rotation: {flat} without jitter ({flat_rotations} rotations), {spread} with ({spread_rotations} rotations)"
    );
    assert!(
        spread_rotations >= 4,
        "spread over only {spread_rotations} rotations"
    );
    assert!(
        spread * 3 <= flat,
        "busiest rotation {spread} with jitter against {flat} without"
    );
}

// Sizing aid, not a gate: how the refresh's saving holds up as the mesh
// stores more. Every refresh page is a height, so a mesh storing enough
// fragments refreshes itself into a drain no threshold can outrun.
// `cargo test -p hopnet-storage --release --lib sweep_sim::volume_by_data_scale -- --ignored --nocapture`
#[test]
#[ignore]
fn volume_by_data_scale() {
    let days = 4;
    let last_day = ((days - 1) * 24) as usize..(days * 24) as usize;
    for scale in [1, 2, 4, 8] {
        let scaled = |mode| {
            let mut scenario = Scenario::quiet_mesh(mode, days);
            for node in &mut scenario.nodes {
                node.fragments *= scale;
            }
            run(&scenario)
        };
        let (before, after) = (scaled(Mode::EveryRotation), scaled(REFRESH));
        eprintln!(
            "{scale}x data: stamps/h {:.0} -> {:.0} ({:.1}x), heights/h {:.0} -> {:.0}, max age {}",
            before.attested_per_hour_over(last_day.clone()),
            after.attested_per_hour_over(last_day.clone()),
            before.attested_per_hour_over(last_day.clone())
                / after.attested_per_hour_over(last_day.clone()),
            before.heights_per_hour_over(last_day.clone()),
            after.heights_per_hour_over(last_day.clone()),
            after.max_age,
        );
    }
}
