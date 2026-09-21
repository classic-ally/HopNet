//! Model conformance (RFC-STORAGE-003 S2): the protection predicate and
//! the eviction planner replayed against exported Quint witness traces.
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
use serde_json::Value;

const N_FRAGS: u32 = 6;
const VAR_PREFIX: &str = "scaled_bal::storage_policy::";
/// The model's UP status (`pure val UP = 0`).
const UP: i64 = 0;

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
    confirmed_view: BTreeSet<i64>,
    target_view: BTreeSet<i64>,
    protected_by: BTreeMap<i64, BTreeSet<i64>>,
    copies: BTreeMap<i64, BTreeSet<i64>>,
    inv_view: BTreeMap<i64, BTreeSet<i64>>,
    status: BTreeMap<i64, i64>,
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
            confirmed_view: set(&var(s, "confirmedView")),
            target_view: set(&var(s, "targetView")),
            protected_by: map_of_sets(&var(s, "protectedBy")),
            copies: map_of_sets(&var(s, "copies")),
            inv_view: map_of_sets(&var(s, "invView")),
            status: map_of_ints(&var(s, "status")),
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
