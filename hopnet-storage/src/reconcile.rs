//! The reconciler's duty ladder (RFC-STORAGE-003 S3), pure and sans-io.
//!
//! One blob-chunk, one node: given the target assignment, who holds what,
//! and who is up, decide what THIS node owes — the model's engine rungs
//! for the classes it is responsible for, in the model's order:
//!
//! - re-encode first (liveness deficit): a dead class (no up holder) whose
//!   responsible is this node, and which is ready — the chunk is below the
//!   watermark, or no holder is merely asleep inside its decay tier (the
//!   model's `reencodeReady` guard: urgency overrides hope);
//! - then pull: a class this node is responsible for under the target and
//!   does not hold, with at least one up holder to fetch from (`pullNeedy`).
//!
//! A re-encode needs K live classes to read from (the model gates that rung
//! on `liveClassCount >= K`); a pull needs only one live holder of its own
//! class. Nothing is owed for classes assigned elsewhere. The deputy rule
//! the RFC adds below the watermark (the lowest-live-class responsible
//! rebuilds a surplus copy when the responsible is down) is a strict
//! superset the caller layers on; this ladder is the model verbatim.

use std::collections::BTreeSet;

/// One fragment class as the reconciler sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassState {
    /// Nodes believed to hold a copy (inventory rows, or truth in tests).
    pub holders: BTreeSet<i32>,
    /// The responsible node under the TARGET view.
    pub responsible: i32,
}

/// One chunk's state for one tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkState {
    pub classes: Vec<ClassState>,
    /// Nodes currently up (online).
    pub up: BTreeSet<i32>,
    /// Nodes down but still inside their decay tier — a copy there may
    /// come back, so a lazy class waits for it.
    pub hopeful_down: BTreeSet<i32>,
    /// Reconstruction threshold.
    pub k: usize,
    /// Urgency watermark: below this many live classes, hope is overridden.
    pub watermark: usize,
}

/// What this node owes for the chunk, in ladder order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Duty {
    /// Regenerate these dead classes from any K live ones (this node is
    /// their responsible and they are ready).
    Reencode { classes: Vec<u32> },
    /// Fetch this class from a live holder (this node is responsible and
    /// does not hold it).
    Pull { class: u32 },
}

impl ChunkState {
    fn live(&self, class: &ClassState) -> bool {
        class.holders.iter().any(|h| self.up.contains(h))
    }

    /// Classes with at least one up holder.
    pub fn live_class_count(&self) -> usize {
        self.classes.iter().filter(|c| self.live(c)).count()
    }

    /// The model's `available`: reconstructible from online nodes.
    pub fn available(&self) -> bool {
        self.live_class_count() >= self.k
    }
}

/// The ladder for `me`. Empty when the chunk is unreadable or this node
/// owes nothing.
pub fn plan(state: &ChunkState, me: i32) -> Vec<Duty> {
    let live_count = state.live_class_count();
    let mut duties = Vec::new();

    // Re-encode rung: dead classes I am responsible for, once ready — and
    // only while K live classes exist to rebuild from.
    if state.available() {
        let ready: Vec<u32> = state
            .classes
            .iter()
            .enumerate()
            .filter(|(_, c)| c.responsible == me && !state.live(c))
            .filter(|(_, c)| {
                let hopeful = c.holders.iter().any(|h| state.hopeful_down.contains(h));
                live_count < state.watermark || !hopeful
            })
            .map(|(i, _)| i as u32)
            .collect();
        if !ready.is_empty() {
            duties.push(Duty::Reencode { classes: ready });
        }
    }

    // Pull rung: live classes I am responsible for and do not hold.
    for (i, c) in state.classes.iter().enumerate() {
        if c.responsible == me && !c.holders.contains(&me) && state.live(c) {
            duties.push(Duty::Pull { class: i as u32 });
        }
    }
    duties
}

/// The deputy rule (RFC-STORAGE-003 Mechanisms, "Recovery is the fetch
/// fallback"): below the watermark, dead classes whose responsible is not
/// up would otherwise wait out that node's decay gate while the chunk sits
/// in the danger zone. The responsible of the lowest LIVE class rebuilds
/// them instead, as a surplus, belief-protected copy — today's urgent
/// semantics. A strict superset of the model's ladder (the model's
/// re-encoder is always the responsible); never fires above the
/// watermark, and never while fewer than K classes are live.
pub fn deputy(state: &ChunkState, me: i32) -> Option<Vec<u32>> {
    if !state.available() || state.live_class_count() >= state.watermark {
        return None;
    }
    let deputy = state
        .classes
        .iter()
        .find(|c| state.live(c))
        .map(|c| c.responsible)?;
    if deputy != me {
        return None;
    }
    let orphaned: Vec<u32> = state
        .classes
        .iter()
        .enumerate()
        .filter(|(_, c)| !state.live(c) && !state.up.contains(&c.responsible))
        .map(|(i, _)| i as u32)
        .collect();
    (!orphaned.is_empty()).then_some(orphaned)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(v: &[i32]) -> BTreeSet<i32> {
        v.iter().copied().collect()
    }

    // Should: below the watermark, have the responsible of the lowest live
    // class rebuild dead classes whose own responsible is down; nobody
    // else, nothing above the watermark, nothing below K.
    #[test]
    fn deputy_rebuilds_for_a_down_responsible_below_watermark() {
        // W = 3, K = 2. Live: class 1 (node 2), class 2 (node 3). Dead:
        // class 0 (responsible 4, down), class 3 (responsible 2, up).
        let s = chunk(
            &[(&[4], 4), (&[2], 2), (&[3], 3), (&[4], 2)],
            &[1, 2, 3],
            &[],
        );
        assert_eq!(
            deputy(&s, 2),
            Some(vec![0]),
            "node 2 owns the lowest live class"
        );
        assert_eq!(deputy(&s, 3), None);
        assert_eq!(deputy(&s, 1), None);
        // Above the watermark: the decay gate decides, no deputy.
        let calm = chunk(
            &[(&[4], 4), (&[2], 2), (&[3], 3), (&[1], 1)],
            &[1, 2, 3],
            &[4],
        );
        assert_eq!(deputy(&calm, 2), None);
        // Below K: nothing to rebuild from.
        let dark = chunk(&[(&[4], 4), (&[2], 2), (&[4], 3)], &[1, 2, 3], &[]);
        assert_eq!(deputy(&dark, 2), None);
    }

    fn chunk(classes: &[(&[i32], i32)], up: &[i32], hopeful: &[i32]) -> ChunkState {
        ChunkState {
            classes: classes
                .iter()
                .map(|(h, r)| ClassState {
                    holders: set(h),
                    responsible: *r,
                })
                .collect(),
            up: set(up),
            hopeful_down: set(hopeful),
            k: 2,
            watermark: 3,
        }
    }

    // Should: owe a pull for every class this node is responsible for and
    // does not hold, when a live holder exists; nothing for others' classes.
    #[test]
    fn pulls_owed_classes_with_a_live_source() {
        let s = chunk(
            &[(&[1], 2), (&[1], 3), (&[1, 2], 2), (&[1], 1)],
            &[1, 2, 3],
            &[],
        );
        assert_eq!(plan(&s, 2), vec![Duty::Pull { class: 0 }]);
        assert_eq!(plan(&s, 3), vec![Duty::Pull { class: 1 }]);
        assert!(plan(&s, 1).is_empty());
    }

    // Should: withhold re-encode while fewer than K classes are live (there
    // is nothing to rebuild from) but still pull a class whose own holder
    // is up — the model gates only the re-encode rung on K.
    #[test]
    fn below_k_pulls_but_never_rebuilds() {
        // Classes 0,1 dead (holder 1 gone), class 2 live on 3; K = 2.
        let s = chunk(&[(&[1], 2), (&[1], 2), (&[3], 2)], &[2, 3], &[]);
        assert_eq!(plan(&s, 2), vec![Duty::Pull { class: 2 }]);
    }

    // Should: re-encode a dead class this node is responsible for once no
    // hopeful holder remains, before any pull; wait while a holder may
    // still return above the watermark.
    // Impact: urgency overrides hope only below W — the decay gate is what
    // keeps a weekend-asleep laptop from triggering a mesh-wide rebuild.
    #[test]
    fn reencode_waits_for_hope_above_watermark_only() {
        // Class 0 dead (holder 4 asleep, hopeful); classes 1..3 live: 3 ≥ W.
        let s = chunk(
            &[(&[4], 1), (&[2], 2), (&[3], 3), (&[2], 1), (&[3], 1)],
            &[1, 2, 3],
            &[4],
        );
        let duties = plan(&s, 1);
        assert!(
            !duties.iter().any(|d| matches!(d, Duty::Reencode { .. })),
            "hopeful holder above W: wait"
        );
        assert_eq!(
            duties,
            vec![Duty::Pull { class: 3 }, Duty::Pull { class: 4 }]
        );

        // Same, but holder 4 is gone (not hopeful): rebuild first.
        let gone = chunk(
            &[(&[4], 1), (&[2], 2), (&[3], 3), (&[2], 1), (&[3], 1)],
            &[1, 2, 3],
            &[],
        );
        assert_eq!(plan(&gone, 1)[0], Duty::Reencode { classes: vec![0] });

        // Below W (2 live < 3), hope no longer counts.
        let urgent = chunk(&[(&[4], 1), (&[2], 2), (&[3], 3)], &[1, 2, 3], &[4]);
        assert_eq!(plan(&urgent, 1), vec![Duty::Reencode { classes: vec![0] }]);
    }

    // Impact: the model's urgency floor is `liveClassCount < W`, strict —
    // exactly W live classes is not yet urgent. The 2026-09-27 mutation
    // run flipped it to `<=` unnoticed: no trace sits on the boundary.
    // Should: at exactly W live classes, let a hopeful holder defer the
    // rebuild; one fewer live class and hope no longer counts.
    #[test]
    fn urgency_floor_is_strict_at_the_watermark() {
        // Three live classes (1, 2, 3) = W; class 0 dead on hopeful 4.
        let at_w = chunk(
            &[(&[4], 1), (&[2], 2), (&[3], 3), (&[2], 1)],
            &[1, 2, 3],
            &[4],
        );
        assert_eq!(at_w.live_class_count(), 3);
        assert_eq!(
            plan(&at_w, 1),
            vec![Duty::Pull { class: 3 }],
            "at W: hope defers the rebuild"
        );
        // Two live classes < W: the same dead class is rebuilt now.
        let below = chunk(&[(&[4], 1), (&[2], 2), (&[3], 3)], &[1, 2, 3], &[4]);
        assert_eq!(plan(&below, 1), vec![Duty::Reencode { classes: vec![0] }]);
    }
}
