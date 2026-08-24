//! The regenesis boundary protocol (RFC-019 S5): the moratorium → seal
//! state machine over committed state. Phase rules are deterministic
//! reads of the `regenesis_state` singleton (src/db/regenesis.rs), so
//! the proposer's preflight and every validator's apply agree by
//! construction. Handlers never touch clocks, networks, or AppState
//! (RFC-015); the admission gate, drain detection, and the engine-side
//! seal live at the queue/engine layer. The normative boundary model is
//! `epoch_policy` in hopnet-consensus/spec/validator_membership.qnt;
//! the engine obligations are hopnet-consensus/spec/
//! regenesis-seal-contract.md.

pub mod boot;
#[cfg(test)]
pub mod compat_g0;
pub mod compat_g1;
pub mod gate;
pub mod genesis;
pub mod handlers;
pub mod join;
pub mod routes;
pub mod rpc;
pub mod seal;

use serde::{Deserialize, Serialize};

/// `regenesis_start` payload: the version the NEXT epoch requires —
/// always present. A same-version restart (housekeeping) names the
/// running version; an upgrade names a newer one. One precondition, one
/// gate — a sneaky binary swap during a "mere restart" is structurally
/// refused (RFC-019).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegenesisStart {
    pub target_version_code: u32,
}

/// `regenesis_commit` payload: the snapshot identity every validator
/// must reproduce over its own state (vote-iff-match), and the terminal
/// height. `snapshot_hash` is blake3 over the canonical ARTIFACT bytes
/// (Exported tables only — stable across the seal transition, and
/// exactly what a joiner verifies against the certificate; never the
/// manifest top hash, which covers divergence-only tables and moves
/// with every height). `seal_height` is bound to the actual block
/// height at vote time — deterministic with no in-apply height read.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegenesisCommit {
    pub snapshot_hash: [u8; 32],
    pub seal_height: u64,
    /// The version the next epoch requires, carried here so it lands INSIDE
    /// the certified block.
    ///
    /// It originates from `regenesis_start` and is already committed state
    /// by the time the commit is proposed, so this is a copy — but a copy
    /// with a quorum behind it. Without it, `EpochGenesisRecord`'s
    /// `required_version_code` is a field a serving peer chooses freely: a
    /// joiner has no way to tell a real target from an invented one, and a
    /// fabricated value parks it awaiting an upgrade that never comes, or
    /// boots it on a binary the mesh is not running.
    ///
    /// Every validator checks this against its OWN committed target
    /// (deterministically, at both origins), so a proposer that lies is
    /// voted down rather than believed.
    pub target_version_code: u32,
}

/// `regenesis_abort` payload — empty; the phase rule carries everything.
/// The abort window is exactly (start decided, commit decided).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegenesisAbort {}
