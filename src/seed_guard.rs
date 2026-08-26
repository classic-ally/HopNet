//! `hopnet seed-guard` — the wrapper's clamp decision (RFC-025).
//!
//! The NixOS module's ExecStartPre seeding asks THIS before advancing
//! the profile to a newer flake pin: exit 0 = seed, exit 3 = held (the
//! mesh agreement pins the runnable version), exit 1/2 = error/usage
//! (the wrapper treats any non-zero as "don't seed", so failures
//! degrade to holding). The decision is pure and unit-tested; bash
//! stays dumb.
//!
//! Reads the `agreed-version` and `awaiting-upgrade` markers beside the
//! database (via `paths::data_dir()`, so systemd's XDG_DATA_HOME
//! resolves identically to the daemon). DELIBERATELY override-blind:
//! the candidate is explicit and the markers are files — never read
//! `HOPNET_UPGRADE_VERSION_OVERRIDE` here, it exists to lie about a
//! running process, not about what may be seeded.

use crate::regenesis::boot;

/// A marker's three observable states. `Malformed` is distinct from
/// `Absent` on purpose: the daemon degrades a corrupt marker to
/// "absent" for availability, but the guard HOLDS on garbage — seeding
/// past an unreadable agreement is the unsafe direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarkerState {
    Absent,
    Valid(u32),
    Malformed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// Exit 0: seeding the candidate is within the agreement (or the
    /// node never joined a mesh — newest-wins).
    Allow,
    /// Exit 3: the candidate is beyond what the mesh agreed (or a
    /// marker is unreadable — conservative).
    Hold,
    /// Exit 2: the candidate itself is not a CalVer.
    Usage,
}

/// The pure decision. The awaiting-upgrade marker outranks the
/// agreement: a parked node's required target IS the sanctioned next
/// version, so the ceiling rises to it — that is the legitimate manual
/// upgrade path for a parked node.
pub fn decide(candidate: Option<u32>, agreed: MarkerState, awaiting: MarkerState) -> Decision {
    let Some(candidate) = candidate else {
        return Decision::Usage;
    };
    match (awaiting, agreed) {
        (MarkerState::Malformed, _) | (_, MarkerState::Malformed) => Decision::Hold,
        (MarkerState::Valid(required), _) => {
            if candidate <= required {
                Decision::Allow
            } else {
                Decision::Hold
            }
        }
        (MarkerState::Absent, MarkerState::Valid(agreed)) => {
            if candidate <= agreed {
                Decision::Allow
            } else {
                Decision::Hold
            }
        }
        // Never joined: newest-wins, exactly as before the clamp.
        (MarkerState::Absent, MarkerState::Absent) => Decision::Allow,
    }
}

fn marker_state(path: &std::path::Path) -> MarkerState {
    match std::fs::read_to_string(path) {
        Ok(content) => match crate::version::parse_code(content.trim()) {
            Some(code) => MarkerState::Valid(code),
            None => MarkerState::Malformed,
        },
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => MarkerState::Absent,
        Err(_) => MarkerState::Malformed,
    }
}

/// The CLI entry: parse, read the markers, decide, print one line.
pub fn run(candidate: &str) -> i32 {
    let db_path = crate::db::shared::get_database_path();
    let agreed = marker_state(&boot::agreed_version_path(&db_path));
    let awaiting = marker_state(&boot::awaiting_upgrade_path(&db_path));
    let code = crate::version::parse_code(candidate);
    match decide(code, agreed, awaiting) {
        Decision::Allow => {
            println!("seed-guard: allow {candidate} (agreed={agreed:?}, awaiting={awaiting:?})");
            0
        }
        Decision::Hold => {
            eprintln!(
                "seed-guard: HELD {candidate} — beyond the mesh agreement \
                 (agreed={agreed:?}, awaiting={awaiting:?})"
            );
            3
        }
        Decision::Usage => {
            eprintln!("seed-guard: candidate {candidate:?} is not a CalVer version");
            2
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const AGREED: u32 = 20260806;

    // Impact: this table IS the wrapper's contract — the nix module
    // seeds exactly when this says Allow, and the never-joined arm is
    // the requirement that a fresh install keeps loading the latest
    // version.
    // Should: allow anything when never joined; clamp to the agreement
    // once joined; let the awaiting-upgrade target raise the ceiling
    // (the parked node's sanctioned manual upgrade).
    // Should not: seed past an unreadable marker — garbage holds.
    #[test]
    fn decision_table() {
        use Decision::*;
        use MarkerState::*;

        // Never joined: newest-wins.
        assert_eq!(decide(Some(AGREED + 100), Absent, Absent), Allow);
        // Joined: at or below the agreement seeds, above holds.
        assert_eq!(decide(Some(AGREED), Valid(AGREED), Absent), Allow);
        assert_eq!(decide(Some(AGREED - 5), Valid(AGREED), Absent), Allow);
        assert_eq!(decide(Some(AGREED + 1), Valid(AGREED), Absent), Hold);
        // Parked awaiting an upgrade: the required target is the
        // ceiling — above the old agreement but at the target seeds.
        assert_eq!(
            decide(Some(AGREED + 1), Valid(AGREED), Valid(AGREED + 1)),
            Allow
        );
        assert_eq!(
            decide(Some(AGREED + 2), Valid(AGREED), Valid(AGREED + 1)),
            Hold
        );
        // Awaiting marker alone still clamps.
        assert_eq!(decide(Some(AGREED + 1), Absent, Valid(AGREED + 1)), Allow);
        assert_eq!(decide(Some(AGREED + 2), Absent, Valid(AGREED + 1)), Hold);
        // Garbage anywhere: hold, both orders.
        assert_eq!(decide(Some(AGREED), Malformed, Absent), Hold);
        assert_eq!(decide(Some(AGREED), Valid(AGREED), Malformed), Hold);
        // A non-CalVer candidate is usage, not a hold.
        assert_eq!(decide(None, Absent, Absent), Usage);
    }
}
