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
///
/// Rows are packed: class `c`'s holders are `holders[starts[c]..starts[c + 1]]`,
/// sorted and deduplicated. The sweep holds one memo per blob of a shard,
/// and a set per class cost an allocation per class where this costs two
/// per blob.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Protection {
    never_confirmed: bool,
    starts: Vec<u32>,
    holders: Vec<i32>,
}

impl Protection {
    /// Build the memo. Classes are indexed by position; epochs with
    /// differing lengths (should not happen — one blob, one layout) are
    /// unioned up to the longest.
    pub fn from_epochs(epochs: &ProtectionEpochs) -> Self {
        let assignments = || epochs.confirmed.iter().chain(epochs.in_flight.iter());
        let n = assignments().map(|a| a.len()).max().unwrap_or(0);
        let mut starts = Vec::with_capacity(n + 1);
        let mut holders = Vec::with_capacity(n * assignments().count());
        for class in 0..n {
            let start = holders.len();
            starts.push(start as u32);
            holders.extend(assignments().filter_map(|a| a.get(class).copied()));
            let row = &mut holders[start..];
            row.sort_unstable();
            // Deduplicate the row in place: keep each node's first copy.
            let mut kept = 0;
            for i in 0..row.len() {
                if kept == 0 || row[i] != row[kept - 1] {
                    row[kept] = row[i];
                    kept += 1;
                }
            }
            holders.truncate(start + kept);
        }
        starts.push(holders.len() as u32);
        Self {
            never_confirmed: epochs.confirmed.is_none(),
            starts,
            holders,
        }
    }

    /// Whether the record could not answer for this blob — treat as
    /// protected everywhere (never evict on an unanswerable question).
    pub fn unknown() -> Self {
        Self {
            never_confirmed: true,
            starts: Vec::new(),
            holders: Vec::new(),
        }
    }

    /// One class's row, `None` beyond the memo's range.
    fn row(&self, class: u32) -> Option<&[i32]> {
        let class = class as usize;
        let (start, end) = (*self.starts.get(class)?, *self.starts.get(class + 1)?);
        Some(&self.holders[start as usize..end as usize])
    }

    /// The guard. A class beyond the memo's range fails closed.
    pub fn protects(&self, class: u32, node: i32, pinned: bool) -> bool {
        if pinned || self.never_confirmed {
            return true;
        }
        match self.row(class) {
            Some(holders) => holders.contains(&node),
            None => true,
        }
    }

    /// The protected holders of one class (the memo row, ascending), for
    /// parity checks and observability. Empty beyond range.
    pub fn holders(&self, class: u32) -> &[i32] {
        self.row(class).unwrap_or(&[])
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

    // Should: union epochs of differing lengths up to the longest, so a
    // class only the longer epoch names is held by that epoch's node.
    // Should: fail closed for a class beyond every epoch.
    #[test]
    fn epochs_of_differing_lengths_union_up_to_the_longest() {
        let p = Protection::from_epochs(&ProtectionEpochs {
            confirmed: Some(vec![1, 2]),
            in_flight: vec![vec![2, 2, 4]],
        });
        assert_eq!(p.holders(0), [1, 2]);
        assert_eq!(p.holders(1), [2]);
        assert_eq!(p.holders(2), [4]);
        assert!(p.holders(3).is_empty());
        assert!(!p.protects(2, 1, false));
        assert!(p.protects(3, 1, false));
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
        assert_eq!(p.holders(0), [1, 2, 4]);
        assert_eq!(p.holders(1), [1, 3]);
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
