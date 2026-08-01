//! The admissibility seam (RFC-019 S5): ONE predicate answering "is this
//! tx function admissible for NEW submissions given committed state?",
//! consulted at the queue chokepoint (every client route and internal
//! cron funnels through `ConsensusQueue::submit`/`submit_batch`) and at
//! the proposer's own injection site (`system.cleanup_nonces`).
//!
//! Deliberately NOT consulted: `enqueue_forwarded` and the pending pool —
//! already-accepted work drains through the moratorium by design
//! (spec: "accepted work completes consensus"; Quint model: `drainOne`
//! carries no phase guard, `admitOne` does).
//!
//! This is also the future insertion point for dynamic module
//! enable/disable (RFC-020 territory): new committed-state inputs join
//! `admissible_in_phase`; every chokepoint already asks here.

use crate::db::DatabaseError;
use crate::db::regenesis::{RegenesisPhase, RegenesisState, read_regenesis_state};

/// Functions still admissible as NEW submissions during the moratorium —
/// the boundary's own ops. `regenesis_start` is absent on purpose (no
/// second start), and SEALED admits nothing (recovery is forward-only).
pub const MORATORIUM_ADMISSIBLE: &[&str] = &["regenesis_commit", "regenesis_abort"];

/// The pure phase rule.
pub fn admissible_in_phase(phase: RegenesisPhase, function: &str) -> bool {
    match phase {
        RegenesisPhase::Normal => true,
        RegenesisPhase::Moratorium => MORATORIUM_ADMISSIBLE.contains(&function),
        RegenesisPhase::Sealed => false,
    }
}

/// Committed-state read + rule. `None` = admissible; `Some(state)` =
/// refuse, with the state for the structured 503 body.
pub fn admission_refusal(
    conn: &rusqlite::Connection,
    function: &str,
) -> Result<Option<RegenesisState>, DatabaseError> {
    let state = read_regenesis_state(conn)?;
    if admissible_in_phase(state.phase, function) {
        Ok(None)
    } else {
        Ok(Some(state))
    }
}

/// Display name for a phase (503 bodies, status view, logs).
pub fn phase_str(phase: RegenesisPhase) -> &'static str {
    match phase {
        RegenesisPhase::Normal => "normal",
        RegenesisPhase::Moratorium => "moratorium",
        RegenesisPhase::Sealed => "sealed",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Should: admit everything in the normal phase, and refuse every
    // internal submitter's function once the moratorium holds.
    // Impact: the freeze must close over our OWN crons (metrics,
    // self-check, version attestation) too, or the pool never drains.
    #[test]
    fn moratorium_refuses_every_ordinary_function() {
        let internal_submitters = [
            "submit_metrics",
            "self_check_fragments",
            "node_staged_version",
            "validator_vote_out",
            "validator_activation",
            "validator_leave",
            "system.cleanup_nonces",
            "update_takeout_status",
            "insert_files",
        ];
        for f in internal_submitters {
            assert!(admissible_in_phase(RegenesisPhase::Normal, f));
            assert!(!admissible_in_phase(RegenesisPhase::Moratorium, f));
            assert!(!admissible_in_phase(RegenesisPhase::Sealed, f));
        }
    }

    // Should: keep the boundary's own ops admissible during the
    // moratorium — commit and abort ride the same closed gate.
    // Should not: admit a second start, or anything at all once sealed.
    #[test]
    fn boundary_ops_pass_the_moratorium_only() {
        assert!(admissible_in_phase(
            RegenesisPhase::Moratorium,
            "regenesis_commit"
        ));
        assert!(admissible_in_phase(
            RegenesisPhase::Moratorium,
            "regenesis_abort"
        ));
        assert!(!admissible_in_phase(
            RegenesisPhase::Moratorium,
            "regenesis_start"
        ));
        assert!(!admissible_in_phase(
            RegenesisPhase::Sealed,
            "regenesis_commit"
        ));
        assert!(!admissible_in_phase(
            RegenesisPhase::Sealed,
            "regenesis_abort"
        ));
    }
}
