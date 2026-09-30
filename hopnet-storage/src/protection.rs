//! The protection predicate (RFC-STORAGE-003 S2) — THE eviction guard.
//!
//! A copy of class `f` held by node `n` is protected iff any of:
//! - the blob was never confirmed (`placement_height` NULL): no obligation
//!   has ever lapsed, so every existing copy is load-bearing whoever holds
//!   it — what keeps the origin's copies safe between birth and the first
//!   confirmation;
//! - `n` is responsible for `f` under the CONFIRMED epoch's assignment —
//!   the standing obligation;
//! - `n` is responsible for `f` under ANY declared-but-unconfirmed epoch
//!   (the transition record's snapshots in `(placement_height, desired]`,
//!   the current target always among them). Covering only the newest
//!   target is not enough: a supersede-declare would strip the previous
//!   destination's freshly pulled copy of protection. Protection lapses
//!   only at confirm, exactly as obligations do;
//! - the copy is pinned (implementation-only; absent from the model).
//!
//! Everything else is surplus. This is `storage_policy.qnt`'s `protected`
//! verbatim — `confirmedView == Set() or protectedBy.get(f).contains(n)` —
//! over the per-class holder memo the model keeps as `protectedBy`. It is
//! pure over ASSIGNMENTS: how a view becomes an assignment is the
//! placement function's business (`lifecycle::ViewSnapshot::assignment` in
//! production, the model's `mix` scoring in the conformance test), so the
//! same predicate is checked against exported model traces and run by the
//! evictor. Single-sourced by design: computing the guard anywhere else is
//! the only way to reintroduce the loss.

use std::collections::BTreeSet;

/// Class → responsible node under one storage view (index = class).
pub type Assignment = Vec<i32>;

/// The epochs whose obligations protect a blob's copies.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProtectionEpochs {
    /// Assignment under the confirmed epoch; `None` = never confirmed.
    pub confirmed: Option<Assignment>,
    /// Assignments under every declared-but-unconfirmed epoch — empty when
    /// quiescent, the current target always included when in flight.
    pub in_flight: Vec<Assignment>,
}

/// The model's `protectedBy` memo for one blob: per class, the union of
/// responsible nodes over the confirmed epoch and every in-flight epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Protection {
    never_confirmed: bool,
    holders_by_class: Vec<BTreeSet<i32>>,
}

impl Protection {
    /// Build the memo. Classes are indexed by position; epochs with
    /// differing lengths (should not happen — one blob, one layout) are
    /// unioned up to the longest.
    pub fn from_epochs(epochs: &ProtectionEpochs) -> Self {
        let n = epochs
            .confirmed
            .iter()
            .chain(epochs.in_flight.iter())
            .map(|a| a.len())
            .max()
            .unwrap_or(0);
        let mut holders_by_class = vec![BTreeSet::new(); n];
        for assignment in epochs.confirmed.iter().chain(epochs.in_flight.iter()) {
            for (class, node) in assignment.iter().enumerate() {
                holders_by_class[class].insert(*node);
            }
        }
        Self {
            never_confirmed: epochs.confirmed.is_none(),
            holders_by_class,
        }
    }

    /// Whether the record could not answer for this blob — treat as
    /// protected everywhere (never evict on an unanswerable question).
    pub fn unknown() -> Self {
        Self {
            never_confirmed: true,
            holders_by_class: Vec::new(),
        }
    }

    /// The guard. A class beyond the memo's range fails closed.
    pub fn protects(&self, class: u32, node: i32, pinned: bool) -> bool {
        if pinned || self.never_confirmed {
            return true;
        }
        match self.holders_by_class.get(class as usize) {
            Some(holders) => holders.contains(&node),
            None => true,
        }
    }

    /// The protected holders of one class (the memo row), for parity
    /// checks and observability. Empty beyond range.
    pub fn holders(&self, class: u32) -> &BTreeSet<i32> {
        static EMPTY: BTreeSet<i32> = BTreeSet::new();
        self.holders_by_class.get(class as usize).unwrap_or(&EMPTY)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Should: protect every copy of a never-confirmed blob, whoever holds
    // it — the birth window has no lapsed obligations.
    // Impact: closes the uploader-nukes-the-only-copy hole: mid-
    // distribution the origin's copies are the only real bytes.
    #[test]
    fn never_confirmed_protects_everyone() {
        let p = Protection::from_epochs(&ProtectionEpochs {
            confirmed: None,
            in_flight: vec![vec![2, 3, 4]],
        });
        for class in 0..3 {
            for node in 1..=4 {
                assert!(p.protects(class, node, false));
            }
        }
    }

    // Should: protect exactly the confirmed assignment when quiescent.
    // Should not: protect a surplus holder the confirmed epoch does not
    // name.
    #[test]
    fn confirmed_only_protects_the_standing_obligation() {
        let p = Protection::from_epochs(&ProtectionEpochs {
            confirmed: Some(vec![2, 1, 3]),
            in_flight: vec![],
        });
        assert!(p.protects(0, 2, false));
        assert!(p.protects(1, 1, false));
        assert!(p.protects(2, 3, false));
        assert!(!p.protects(0, 1, false));
        assert!(!p.protects(2, 4, false));
    }

    // Impact: the S0 counterexample — a supersede-declare must not strip
    // the earlier destination's freshly pulled copy of protection.
    // Should: protect the union of the confirmed epoch and EVERY in-flight
    // epoch, not just the newest.
    #[test]
    fn in_flight_epochs_union_with_confirmed() {
        let p = Protection::from_epochs(&ProtectionEpochs {
            confirmed: Some(vec![1, 1]),
            in_flight: vec![vec![2, 3], vec![4, 3]],
        });
        assert_eq!(p.holders(0), &[1, 2, 4].into_iter().collect());
        assert_eq!(p.holders(1), &[1, 3].into_iter().collect());
        assert!(p.protects(0, 2, false), "first destination stays protected");
        assert!(p.protects(0, 4, false));
        assert!(!p.protects(1, 2, false));
    }

    // Should: protect a pinned copy regardless of epochs, and fail closed
    // for a class beyond the memo or an unanswerable record.
    #[test]
    fn pin_clause_and_fail_closed() {
        let p = Protection::from_epochs(&ProtectionEpochs {
            confirmed: Some(vec![1]),
            in_flight: vec![],
        });
        assert!(p.protects(0, 4, true));
        assert!(
            p.protects(9, 4, false),
            "class beyond the memo fails closed"
        );
        assert!(Protection::unknown().protects(0, 4, false));
    }
}
