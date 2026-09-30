//! The model's engine tick in Rust (RFC-STORAGE-003 S4 trace conformance).
//!
//! `storage_policy.qnt`'s `engineTick` is one deterministic, prioritized
//! mutation over a single chunk: view sync > declare > re-encode > pull >
//! belief sync > confirm. This module is that ladder, over the SAME
//! predicates production runs — `reconcile::plan` for the movers,
//! `protection` for the memo, the confirm-readiness rule of
//! `lifecycle::confirm_ready` — so the trace-replay harness can drive it
//! step by step against exported model traces and assert state agreement
//! after every engine tick. Only what the model abstracts away is
//! harness-side here: the global "least class" choice among nodes' duties,
//! the decay-gate bookkeeping (`downFor`), and the injected placement
//! table. Nothing in this module runs in production; it exists so the
//! production functions cannot drift from the checked model unnoticed.

use std::collections::{BTreeMap, BTreeSet};

use crate::protection::{Protection, ProtectionEpochs};
use crate::reconcile::{plan, ChunkState, ClassState, Duty};

/// The model's node statuses (`pure val UP = 0`, `DOWN = 1`, `GONE = 2`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Up,
    Down,
    Gone,
}

/// The model's per-chunk state (the variables `engineTick` reads and
/// writes; fault budgets and cost counters stay with the harness).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelState {
    pub status: BTreeMap<i32, Status>,
    pub down_for: BTreeMap<i32, i64>,
    pub member_view: BTreeSet<i32>,
    /// `Set()` encodes NULL — never confirmed.
    pub confirmed_view: BTreeSet<i32>,
    pub target_view: BTreeSet<i32>,
    /// Class → holders (truth).
    pub copies: BTreeMap<u32, BTreeSet<i32>>,
    /// Class → believed holders.
    pub inv_view: BTreeMap<u32, BTreeSet<i32>>,
    /// Class → protected holders (the model's `protectedBy` memo).
    pub protected_by: BTreeMap<u32, BTreeSet<i32>>,
    pub deleted: bool,
}

/// The injected constants and placement table of one model config.
pub struct Params<'a> {
    pub nodes: BTreeSet<i32>,
    pub n_frags: u32,
    pub k: usize,
    pub watermark: usize,
    pub delta: i64,
    pub auto_pull: bool,
    pub auto_reencode: bool,
    /// The model's `PLACE`: member view → class → responsible node.
    pub place: &'a dyn Fn(&BTreeSet<i32>) -> Vec<i32>,
}

impl ModelState {
    fn up(&self) -> BTreeSet<i32> {
        self.status
            .iter()
            .filter(|(_, s)| **s == Status::Up)
            .map(|(n, _)| *n)
            .collect()
    }

    /// The model's `viewTarget`: up, or down inside the decay window.
    pub fn view_target(&self, p: &Params) -> BTreeSet<i32> {
        p.nodes
            .iter()
            .copied()
            .filter(|n| {
                self.status[n] == Status::Up || self.down_for.get(n).copied().unwrap_or(0) < p.delta
            })
            .collect()
    }

    /// The model's `nextDownFor`.
    fn next_down_for(&self, p: &Params) -> BTreeMap<i32, i64> {
        self.down_for
            .iter()
            .map(|(n, d)| {
                let next = if self.status[n] == Status::Up {
                    0
                } else if *d < p.delta {
                    d + 1
                } else {
                    *d
                };
                (*n, next)
            })
            .collect()
    }

    /// The chunk as the reconciler sees it under the target view.
    pub fn chunk_state(&self, p: &Params) -> ChunkState {
        let assignment = (p.place)(&self.target_view);
        let up = self.up();
        let hopeful_down: BTreeSet<i32> = self
            .status
            .iter()
            .filter(|(n, s)| **s == Status::Down && self.down_for[n] < p.delta)
            .map(|(n, _)| *n)
            .collect();
        ChunkState {
            classes: (0..p.n_frags)
                .map(|f| ClassState {
                    holders: self.copies.get(&f).cloned().unwrap_or_default(),
                    responsible: assignment[f as usize],
                })
                .collect(),
            up,
            hopeful_down,
            k: p.k,
            watermark: p.watermark,
        }
    }

    /// The model's `converged`.
    pub fn converged(&self, p: &Params) -> bool {
        let assignment = (p.place)(&self.target_view);
        let conformant = (0..p.n_frags).all(|f| {
            self.copies
                .get(&f)
                .is_some_and(|h| h.contains(&assignment[f as usize]))
        });
        conformant
            && self.confirmed_view == self.target_view
            && (p.place)(&self.target_view) == (p.place)(&self.member_view)
            && self.inv_view == self.copies
    }

    /// The memo under this state's epochs, from the production predicate.
    fn memo(
        &self,
        p: &Params,
        confirmed: &BTreeSet<i32>,
        in_flight: &[BTreeSet<i32>],
    ) -> BTreeMap<u32, BTreeSet<i32>> {
        let epochs = ProtectionEpochs {
            confirmed: if confirmed.is_empty() {
                None
            } else {
                Some((p.place)(confirmed))
            },
            in_flight: in_flight.iter().map(|v| (p.place)(v)).collect(),
        };
        let prot = Protection::from_epochs(&epochs);
        (0..p.n_frags)
            .map(|f| (f, prot.holders(f).clone()))
            .collect()
    }
}

/// One engine tick: the ladder, first enabled rung wins. Returns the next
/// state (the harness compares it to the trace's).
pub fn step(s: &ModelState, p: &Params) -> ModelState {
    let mut next = s.clone();
    next.down_for = s.next_down_for(p);

    // 1. View sync.
    let view_target = s.view_target(p);
    if s.member_view != view_target && !view_target.is_empty() {
        next.member_view = view_target;
        return next;
    }
    // 2. Declare: the goal went stale. The new epoch JOINS the protected
    // set; earlier destinations stay protected until confirm.
    if !s.deleted && s.target_view != s.member_view {
        next.target_view = s.member_view.clone();
        let new_assignment = (p.place)(&s.member_view);
        for (f, holders) in next.protected_by.iter_mut() {
            holders.insert(new_assignment[*f as usize]);
        }
        return next;
    }
    let chunk = s.chunk_state(p);
    let up = s.up();
    // Duties over every up node, as the model's global rungs see them.
    let mut reencode_ready: BTreeSet<u32> = BTreeSet::new();
    let mut pull_needy: BTreeSet<u32> = BTreeSet::new();
    for n in &up {
        for duty in plan(&chunk, *n) {
            match duty {
                Duty::Reencode { classes } => reencode_ready.extend(classes),
                Duty::Pull { class } => {
                    pull_needy.insert(class);
                }
            }
        }
    }
    let assignment = (p.place)(&s.target_view);
    // 3. Re-encode (least ready class; needs K live — `plan` gates it).
    if p.auto_reencode && !s.deleted {
        if let Some(f) = reencode_ready.iter().next().copied() {
            next.copies
                .entry(f)
                .or_default()
                .insert(assignment[f as usize]);
            return next;
        }
    }
    // 4. Pull (least needy class).
    if p.auto_pull && !s.deleted {
        if let Some(f) = pull_needy.iter().next().copied() {
            next.copies
                .entry(f)
                .or_default()
                .insert(assignment[f as usize]);
            return next;
        }
    }
    // 5. Belief sync.
    if s.inv_view != s.copies {
        next.inv_view = s.copies.clone();
        return next;
    }
    // 6. Confirm: evidence complete — reads BELIEF, as ConfirmPlacement
    // validation reads attested rows (`lifecycle::confirm_ready`'s rule).
    if !s.deleted
        && s.confirmed_view != s.target_view
        && (0..p.n_frags).all(|f| {
            s.inv_view
                .get(&f)
                .is_some_and(|b| b.contains(&assignment[f as usize]))
        })
    {
        next.confirmed_view = s.target_view.clone();
        next.protected_by = s.memo(p, &s.target_view, &[]);
        return next;
    }
    next
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(view: &BTreeSet<i32>) -> Vec<i32> {
        // Two classes: class 0 → smallest member, class 1 → largest.
        let mut v: Vec<i32> = view.iter().copied().collect();
        v.sort_unstable();
        vec![v[0], *v.last().unwrap()]
    }

    fn params(table: &dyn Fn(&BTreeSet<i32>) -> Vec<i32>) -> Params<'_> {
        Params {
            nodes: [1, 2, 3].into_iter().collect(),
            n_frags: 2,
            k: 1,
            watermark: 2,
            delta: 2,
            auto_pull: true,
            auto_reencode: true,
            place: table,
        }
    }

    fn birth() -> ModelState {
        let nodes = [1, 2, 3];
        ModelState {
            status: nodes.iter().map(|n| (*n, Status::Up)).collect(),
            down_for: nodes.iter().map(|n| (*n, 0)).collect(),
            member_view: nodes.into_iter().collect(),
            confirmed_view: BTreeSet::new(),
            target_view: nodes.into_iter().collect(),
            copies: (0..2).map(|f| (f, [2].into_iter().collect())).collect(),
            inv_view: (0..2).map(|f| (f, [2].into_iter().collect())).collect(),
            protected_by: [
                (0, [1].into_iter().collect()),
                (1, [3].into_iter().collect()),
            ]
            .into_iter()
            .collect(),
            deleted: false,
        }
    }

    // Should: walk a birth to the converged fixpoint through the ladder —
    // pull, pull, belief sync, confirm — mutating exactly one thing per
    // tick, in the model's order.
    #[test]
    fn birth_converges_through_the_ladder() {
        let p = params(&table);
        let mut s = birth();
        s = step(&s, &p);
        assert_eq!(s.copies[&0], [1, 2].into_iter().collect());
        s = step(&s, &p);
        assert_eq!(s.copies[&1], [2, 3].into_iter().collect());
        s = step(&s, &p);
        assert_eq!(s.inv_view, s.copies);
        assert!(s.confirmed_view.is_empty());
        s = step(&s, &p);
        assert_eq!(s.confirmed_view, s.target_view);
        assert_eq!(s.protected_by[&0], [1].into_iter().collect());
        assert!(s.converged(&p));
        assert_eq!(step(&s, &p), {
            let mut fixed = s.clone();
            fixed.down_for = s.next_down_for(&p);
            fixed
        });
    }

    // Impact: the model's hope is `status == DOWN and downFor < DELTA` —
    // a node exactly at DELTA is hopeless, and a GONE node never hopeful
    // whatever its clock. The 2026-09-27 mutation run flipped both
    // comparisons unnoticed: no trace sits on the boundary.
    // Should: count a down node as hopeful strictly inside the decay
    // window, and never a gone one.
    #[test]
    fn hope_ends_exactly_at_delta_and_never_covers_gone() {
        let p = params(&table);
        let mut s = birth();
        s.status.insert(1, Status::Down);
        s.down_for.insert(1, 1); // inside the window (DELTA 2)
        s.status.insert(3, Status::Gone);
        s.down_for.insert(3, 0);
        assert_eq!(
            s.chunk_state(&p).hopeful_down,
            [1].into_iter().collect(),
            "down inside the window is hopeful; gone never is"
        );
        s.down_for.insert(1, 2); // exactly DELTA: hopeless
        assert!(s.chunk_state(&p).hopeful_down.is_empty());
    }

    // Impact: the re-encode rung's guard is `AUTO_REENCODE and not(deleted)`;
    // the exported deleted-blob trace has no dead class, so only a direct
    // witness discriminates each conjunct (the 2026-09-27 mutation run
    // left `||` alive here).
    // Should: rebuild a dead class when allowed and the blob lives.
    // Should not: rebuild it for a deleted blob, or with re-encode off.
    #[test]
    fn reencode_needs_both_the_policy_and_a_live_blob() {
        let p = params(&table);
        let mut s = birth();
        s.copies.insert(1, BTreeSet::new()); // class 1 dead; class 0 live = K
        s.inv_view = s.copies.clone();
        assert_eq!(
            step(&s, &p).copies[&1],
            [3].into_iter().collect(),
            "dead class rebuilt on its responsible"
        );

        let mut gone = s.clone();
        gone.deleted = true;
        assert!(
            step(&gone, &p).copies[&1].is_empty(),
            "deleted: no bytes move"
        );

        let off = Params {
            auto_reencode: false,
            ..params(&table)
        };
        assert!(step(&s, &off).copies[&1].is_empty(), "re-encode off: waits");
    }

    // Should: sync the view first when a node decays past DELTA, then
    // declare (the new epoch joins protection), before any mover runs.
    #[test]
    fn decay_then_declare_precede_the_movers() {
        let p = params(&table);
        let mut s = birth();
        for _ in 0..4 {
            s = step(&s, &p);
        }
        s.status.insert(3, Status::Down);
        s.down_for.insert(3, 2); // already past DELTA
        let synced = step(&s, &p);
        assert_eq!(synced.member_view, [1, 2].into_iter().collect());
        assert_eq!(synced.target_view, s.target_view, "declare waits a tick");
        let declared = step(&synced, &p);
        assert_eq!(declared.target_view, [1, 2].into_iter().collect());
        assert_eq!(
            declared.protected_by[&1],
            [2, 3].into_iter().collect(),
            "old destination stays protected, new one joins"
        );
    }
}
