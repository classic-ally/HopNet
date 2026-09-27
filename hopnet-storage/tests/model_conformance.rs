//! Model conformance (RFC-STORAGE-003 S2–S4): the protection predicate,
//! the eviction planner, the reconciler's duty ladder and the whole engine
//! tick replayed against exported Quint witness traces.
//!
//! `spec/traces/*.itf.json` are ITF traces of `scaled_bal` witnesses
//! (see spec/README.md for the regeneration command). For every state of
//! every trace this test rebuilds the Rust `Protection` memo from the
//! trace's `(confirmedView, targetView)` history — declares add an
//! in-flight epoch, confirms reset — using the model's own placement
//! scoring, and asserts:
//!   1. the memo equals the model's `protectedBy` row for every class;
//!   2. `protects(f, n)` equals the model's `protected(f, n)`;
//!   3. the planner evicts (f, n) under full pressure iff the model's
//!      `evictable(f, n)` holds.
//!
//! This is the first rung of replaying Quint executions against real
//! code: "the code resembles the model" becomes "the code agrees with
//! the model on every checked execution".

use std::collections::{BTreeMap, BTreeSet};

use hopnet_storage::eviction::{plan_evictions, DiskPressure, EvictionCandidate};
use hopnet_storage::placement::{assign_classes_by_score, qnt_mix};
use hopnet_storage::protection::{Protection, ProtectionEpochs};
use hopnet_storage::reconcile::{plan, ChunkState, ClassState, Duty};
use serde_json::Value;

const N_FRAGS: u32 = 6;
const VAR_PREFIX: &str = "scaled_bal::storage_policy::";
/// The model's UP / DOWN statuses (`pure val UP = 0`, `DOWN = 1`).
const UP: i64 = 0;
const DOWN: i64 = 1;
/// `scaled_bal`'s policy constants.
const K: usize = 2;
const W: usize = 3;
const DELTA: i64 = 3;

// --- ITF reading -----------------------------------------------------------

fn bigint(v: &Value) -> i64 {
    v.get("#bigint")
        .and_then(Value::as_str)
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("not a #bigint: {v}"))
}

fn set(v: &Value) -> BTreeSet<i64> {
    v.get("#set")
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("not a #set: {v}"))
        .iter()
        .map(bigint)
        .collect()
}

fn map_of_sets(v: &Value) -> BTreeMap<i64, BTreeSet<i64>> {
    v.get("#map")
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("not a #map: {v}"))
        .iter()
        .map(|pair| (bigint(&pair[0]), set(&pair[1])))
        .collect()
}

fn map_of_ints(v: &Value) -> BTreeMap<i64, i64> {
    v.get("#map")
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("not a #map: {v}"))
        .iter()
        .map(|pair| (bigint(&pair[0]), bigint(&pair[1])))
        .collect()
}

struct State {
    calm: i64,
    member_view: BTreeSet<i64>,
    confirmed_view: BTreeSet<i64>,
    target_view: BTreeSet<i64>,
    protected_by: BTreeMap<i64, BTreeSet<i64>>,
    copies: BTreeMap<i64, BTreeSet<i64>>,
    inv_view: BTreeMap<i64, BTreeSet<i64>>,
    status: BTreeMap<i64, i64>,
    down_for: BTreeMap<i64, i64>,
    deleted: bool,
}

fn read_trace(path: &std::path::Path) -> Vec<State> {
    let text = std::fs::read_to_string(path).unwrap();
    let json: Value = serde_json::from_str(&text).unwrap();
    let var = |state: &Value, name: &str| -> Value {
        state
            .get(format!("{VAR_PREFIX}{name}"))
            .cloned()
            .unwrap_or_else(|| panic!("{}: missing variable {name}", path.display()))
    };
    json["states"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| State {
            calm: bigint(&var(s, "calm")),
            member_view: set(&var(s, "memberView")),
            confirmed_view: set(&var(s, "confirmedView")),
            target_view: set(&var(s, "targetView")),
            protected_by: map_of_sets(&var(s, "protectedBy")),
            copies: map_of_sets(&var(s, "copies")),
            inv_view: map_of_sets(&var(s, "invView")),
            status: map_of_ints(&var(s, "status")),
            down_for: map_of_ints(&var(s, "downFor")),
            deleted: var(s, "deleted").as_bool().unwrap(),
        })
        .collect()
}

// --- The model's placement (BAL_TABLE via mix scoring, WEIGHT4) -----------

fn model_assignment(view: &BTreeSet<i64>) -> Vec<i32> {
    let weights: BTreeMap<i32, i64> = [(1, 3), (2, 2), (3, 1), (4, 1)].into();
    let members: Vec<i32> = view.iter().map(|n| *n as i32).collect();
    assign_classes_by_score(&members, N_FRAGS, |n, f| {
        (qnt_mix(n as i64, f as i64) / weights[&n]) as u64
    })
}

/// Walk a trace, rebuilding the in-flight epoch set the model's
/// `protectedBy` memo summarizes: declare (target moved) adds the new
/// target; confirm (confirmed moved onto the target) resets.
fn epochs_along(states: &[State]) -> Vec<ProtectionEpochs> {
    let mut out = Vec::with_capacity(states.len());
    let mut in_flight: Vec<BTreeSet<i64>> = Vec::new();
    for (i, s) in states.iter().enumerate() {
        if i == 0 {
            if s.confirmed_view != s.target_view {
                in_flight.push(s.target_view.clone());
            }
        } else {
            let prev = &states[i - 1];
            if s.confirmed_view != prev.confirmed_view {
                in_flight.clear();
            }
            if s.target_view != prev.target_view && s.target_view != s.confirmed_view {
                in_flight.push(s.target_view.clone());
            }
        }
        out.push(ProtectionEpochs {
            confirmed: if s.confirmed_view.is_empty() {
                None
            } else {
                Some(model_assignment(&s.confirmed_view))
            },
            in_flight: in_flight.iter().map(model_assignment).collect(),
        });
    }
    out
}

fn traces() -> Vec<std::path::PathBuf> {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("spec/traces");
    let mut paths: Vec<_> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "json"))
        .collect();
    paths.sort();
    assert!(!paths.is_empty(), "no traces in {}", dir.display());
    paths
}

// Impact: the transfer of the model's checked safety to the Rust guard —
// every state of every witness, including the supersede counterexample
// that forced the in-flight epoch set in S0.
// Should: rebuild the model's protectedBy memo exactly from the
// (confirmed, target) history, and agree with `protected(f, n)` for every
// class and node.
#[test]
fn protection_memo_and_guard_match_the_model() {
    let mut states_checked = 0;
    for path in traces() {
        let states = read_trace(&path);
        let epochs = epochs_along(&states);
        for (i, (s, e)) in states.iter().zip(&epochs).enumerate() {
            let p = Protection::from_epochs(e);
            for f in 0..N_FRAGS {
                let model_holders: BTreeSet<i32> = s.protected_by[&(f as i64)]
                    .iter()
                    .map(|n| *n as i32)
                    .collect();
                assert_eq!(
                    p.holders(f),
                    &model_holders,
                    "{} state {i} class {f}: memo differs (confirmed {:?}, target {:?})",
                    path.display(),
                    s.confirmed_view,
                    s.target_view
                );
                for n in 1..=4i64 {
                    let model =
                        s.confirmed_view.is_empty() || s.protected_by[&(f as i64)].contains(&n);
                    assert_eq!(
                        p.protects(f, n as i32, false),
                        model,
                        "{} state {i}: protected({f}, {n})",
                        path.display()
                    );
                }
            }
            states_checked += 1;
        }
    }
    assert!(
        states_checked > 50,
        "only {states_checked} states — traces missing?"
    );
}

// Should: plan an eviction of copy (f, n) under full pressure exactly when
// the model's `evictable(f, n)` holds — the predicate plus the attested-
// other-holder belt, deleted blobs aside (the orphan flow's case).
// Impact: pins the planner's filter to the model's envEvict guard, not
// just the predicate in isolation.
#[test]
fn planner_matches_the_models_evictable() {
    let full = DiskPressure {
        used_bytes: 100,
        total_bytes: 100,
        high_pct: 0,
        low_pct: 0,
    };
    for path in traces() {
        let states = read_trace(&path);
        let epochs = epochs_along(&states);
        for (i, (s, e)) in states.iter().zip(&epochs).enumerate() {
            if s.deleted {
                continue;
            }
            let p = Protection::from_epochs(e);
            for f in 0..N_FRAGS as i64 {
                for n in 1..=4i64 {
                    let holds = s.copies[&f].contains(&n);
                    let up = s.status[&n] == UP;
                    let others = s.inv_view[&f].iter().filter(|m| **m != n).count();
                    let model_protected =
                        s.confirmed_view.is_empty() || s.protected_by[&f].contains(&n);
                    let model_evictable = holds && up && !model_protected && others > 0;
                    if !(holds && up) {
                        continue; // the planner only sees copies this node holds
                    }
                    let candidate = EvictionCandidate {
                        fragment_hash: hopnet_common::Blake3Hash::from_bytes([f as u8; 32]),
                        blob_id: "blob".into(),
                        size_bytes: 1,
                        protected: p.protects(f as u32, n as i32, false),
                        other_member_holders: others,
                    };
                    let planned = !plan_evictions(vec![candidate], &full).is_empty();
                    assert_eq!(
                        planned,
                        model_evictable,
                        "{} state {i}: evictable({f}, {n})",
                        path.display()
                    );
                }
            }
        }
    }
}

// Impact: the S0 Apalache finding, pinned in Rust — a supersede-declare
// must not strip the first destination's freshly pulled copy of
// protection; it had no fast-suite witness before this.
// Should: in the supersede trace, keep every class's FIRST in-flight
// destination protected after the target moves again, until confirm.
#[test]
fn supersede_keeps_the_first_destination_protected() {
    let path = traces()
        .into_iter()
        .find(|p| p.to_string_lossy().contains("supersedeMidFlight"))
        .expect("supersede trace present");
    let states = read_trace(&path);
    let epochs = epochs_along(&states);
    let mut checked = false;
    for (i, e) in epochs.iter().enumerate() {
        if e.in_flight.len() < 2 {
            continue;
        }
        // Two epochs in flight: the first destination and its superseder.
        let p = Protection::from_epochs(e);
        let first = &e.in_flight[0];
        for (class, node) in first.iter().enumerate() {
            assert!(
                p.protects(class as u32, *node, false),
                "state {i}: first destination of class {class} (node {node}) lost protection"
            );
        }
        checked = true;
    }
    assert!(
        checked,
        "the supersede trace never had two epochs in flight"
    );
}

// Impact: the S3 reconciler's rungs are the model's re-encode and pull
// rungs, per node — the duty derivation is what discharges "the pull rung
// fires for owed classes" in the assumption table.
// Should: for every state and every up node, owe exactly the model's
// `pullNeedy` classes assigned to that node, and re-encode exactly the
// model's `reencodeReady` classes assigned to it (only while K classes are
// live, as the model's rung guard requires).
#[test]
fn duty_ladder_matches_the_models_rungs() {
    let mut states_checked = 0;
    for path in traces() {
        let states = read_trace(&path);
        for (i, s) in states.iter().enumerate() {
            if s.deleted {
                continue;
            }
            let assignment = model_assignment(&s.target_view);
            let up: BTreeSet<i64> = s
                .status
                .iter()
                .filter(|(_, st)| **st == UP)
                .map(|(n, _)| *n)
                .collect();
            let hopeful: BTreeSet<i64> = s
                .status
                .iter()
                .filter(|(n, st)| **st == DOWN && s.down_for[n] < DELTA)
                .map(|(n, _)| *n)
                .collect();
            let chunk = ChunkState {
                classes: (0..N_FRAGS as i64)
                    .map(|f| ClassState {
                        holders: s.copies[&f].iter().map(|n| *n as i32).collect(),
                        responsible: assignment[f as usize],
                    })
                    .collect(),
                up: up.iter().map(|n| *n as i32).collect(),
                hopeful_down: hopeful.iter().map(|n| *n as i32).collect(),
                k: K,
                watermark: W,
            };

            // The model's rung sets, transcribed from storage_policy.qnt.
            let live = |f: i64| s.copies[&f].iter().any(|h| up.contains(h));
            let live_count = (0..N_FRAGS as i64).filter(|f| live(*f)).count();
            let resp = |f: i64| assignment[f as usize] as i64;
            let model_pull: BTreeSet<i64> = (0..N_FRAGS as i64)
                .filter(|f| !s.copies[f].contains(&resp(*f)) && up.contains(&resp(*f)) && live(*f))
                .collect();
            let model_reencode: BTreeSet<i64> = if live_count >= K {
                (0..N_FRAGS as i64)
                    .filter(|f| !live(*f) && up.contains(&resp(*f)))
                    .filter(|f| live_count < W || !s.copies[f].iter().any(|h| hopeful.contains(h)))
                    .collect()
            } else {
                BTreeSet::new()
            };

            for n in &up {
                let duties = plan(&chunk, *n as i32);
                let mut rust_pull = BTreeSet::new();
                let mut rust_reencode = BTreeSet::new();
                for d in duties {
                    match d {
                        Duty::Pull { class } => {
                            rust_pull.insert(class as i64);
                        }
                        Duty::Reencode { classes } => {
                            rust_reencode.extend(classes.into_iter().map(|c| c as i64));
                        }
                    }
                }
                let mine = |set: &BTreeSet<i64>| -> BTreeSet<i64> {
                    set.iter().copied().filter(|f| resp(*f) == *n).collect()
                };
                assert_eq!(
                    rust_pull,
                    mine(&model_pull),
                    "{} state {i} node {n}: pull duties",
                    path.display()
                );
                assert_eq!(
                    rust_reencode,
                    mine(&model_reencode),
                    "{} state {i} node {n}: re-encode duties",
                    path.display()
                );
            }
            states_checked += 1;
        }
    }
    assert!(
        states_checked > 50,
        "only {states_checked} states — traces missing?"
    );
}

// --- Full tick replay (S4) ------------------------------------------------

fn model_state(s: &State) -> hopnet_storage::tick::ModelState {
    use hopnet_storage::tick::{ModelState, Status};
    let status = s
        .status
        .iter()
        .map(|(n, st)| {
            let st = match *st {
                0 => Status::Up,
                1 => Status::Down,
                _ => Status::Gone,
            };
            (*n as i32, st)
        })
        .collect();
    let sets = |m: &BTreeMap<i64, BTreeSet<i64>>| -> BTreeMap<u32, BTreeSet<i32>> {
        m.iter()
            .map(|(f, ns)| (*f as u32, ns.iter().map(|n| *n as i32).collect()))
            .collect()
    };
    ModelState {
        status,
        down_for: s.down_for.iter().map(|(n, d)| (*n as i32, *d)).collect(),
        member_view: s.member_view.iter().map(|n| *n as i32).collect(),
        confirmed_view: s.confirmed_view.iter().map(|n| *n as i32).collect(),
        target_view: s.target_view.iter().map(|n| *n as i32).collect(),
        copies: sets(&s.copies),
        inv_view: sets(&s.inv_view),
        protected_by: sets(&s.protected_by),
        deleted: s.deleted,
    }
}

// Impact: the harness the RFC promised — the engine tick replayed against
// every exported model execution, env actions injected between ticks,
// state agreement asserted after every engine step. The code cannot
// drift from the checked model on any witnessed execution without this
// failing.
// Should: reproduce the model's next state from its previous one on every
// engineTick step of every trace (view sync, declare, re-encode, pull,
// belief sync, confirm — one mutation per tick, in the model's order).
#[test]
fn engine_tick_replays_every_trace_step() {
    let place = |view: &BTreeSet<i32>| -> Vec<i32> {
        model_assignment(&view.iter().map(|n| *n as i64).collect())
    };
    let params = hopnet_storage::tick::Params {
        nodes: (1..=4).collect(),
        n_frags: N_FRAGS,
        k: K,
        watermark: W,
        delta: DELTA,
        auto_pull: true,
        auto_reencode: true,
        place: &place,
    };
    let mut ticks_checked = 0;
    let mut env_steps = 0;
    for path in traces() {
        let states = read_trace(&path);
        for (i, pair) in states.windows(2).enumerate() {
            let (prev, next) = (&pair[0], &pair[1]);
            if next.calm != prev.calm + 1 {
                env_steps += 1; // an adversary move: adopted, not replayed
                continue;
            }
            let expected = model_state(next);
            let got = hopnet_storage::tick::step(&model_state(prev), &params);
            assert_eq!(
                got,
                expected,
                "{} step {i}→{}: engine tick diverged from the model",
                path.display(),
                i + 1
            );
            ticks_checked += 1;
        }
    }
    assert!(
        ticks_checked > 50,
        "only {ticks_checked} ticks replayed — traces missing?"
    );
    assert!(env_steps > 0, "no env actions in the traces?");
}
