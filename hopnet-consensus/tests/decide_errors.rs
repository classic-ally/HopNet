//! A decide that cannot be persisted is never reported as done: transient
//! contention inside the decide transaction is retried, anything else is
//! returned to the shell (which aborts the process so the WAL replay re-runs
//! the decide). Regression tests for the 2026-09-27 production wedge, where a
//! SQLITE_BUSY inside `Effect::Decide` was swallowed by malachite's `process!`
//! macro and one node sat at a single height for four days.

mod common;

use common::{build_block, decided_heights, open_storage, temp_db, FlakyApp};
use hopnet_consensus::config::QuorumProfile;
use hopnet_consensus::context::{Address, Height};
use hopnet_consensus::host::{HostCore, HostError, HostOutput};
use hopnet_consensus::sim::{MemGossip, MemTimers};
use hopnet_consensus::store::{SqliteStorage, StoreError};
use malachitebft_core_types::Round;

type FlakyCore = HostCore<FlakyApp, SqliteStorage, MemGossip, MemTimers>;

/// Single-node Majority core (quorum 1): one `propose` decides synchronously,
/// so the decide effect's outcome is the `propose` result.
fn flaky_core(name: &str, failures: u32, transient: bool) -> (std::path::PathBuf, FlakyCore) {
    let path = temp_db(name);
    let valset = common::valset(1);
    let core = HostCore::new(
        common::chain_id(),
        common::key(0),
        Address(0),
        QuorumProfile::Majority,
        common::params(0, QuorumProfile::Majority),
        Height::INITIAL,
        valset.clone(),
        FlakyApp::new(valset, failures, transient),
        open_storage(&path),
        MemGossip::default(),
        MemTimers::default(),
    );
    (path, core)
}

fn decided_outputs(core: &mut FlakyCore) -> Vec<u64> {
    core.take_outputs()
        .into_iter()
        .filter_map(|o| match o {
            HostOutput::Decided { height } => Some(height.0),
            _ => None,
        })
        .collect()
}

// Should: re-run the decide transaction when the application reports
// transient contention, and decide the height once it succeeds.
// Should not: surface the retried contention as an error or skip the height.
// Impact: the database is shared with every other writer on the node; a lock
// held past the busy timeout is routine under the disk-truth sweep, and a
// decide that gave up on it would be exactly the wedge this file guards.
#[test]
fn decide_retries_a_transient_storage_error() {
    let (path, mut core) = flaky_core("decide-retry", 1, true);
    core.start_height(Height::INITIAL, false).unwrap();
    core.take_outputs();

    let block = build_block(Height::INITIAL, Round::new(0), 0, None);
    core.propose(Height::INITIAL, Round::new(0), block)
        .expect("one transient failure is retried, not returned");

    assert_eq!(decided_outputs(&mut core), vec![1]);
    drop(core);
    let rows = decided_heights(&path);
    assert_eq!(rows.len(), 1, "the retried decide committed exactly once");
    let conn = rusqlite::Connection::open(&path).unwrap();
    let applied: i64 = conn
        .query_row("SELECT COUNT(*) FROM applied", [], |r| r.get(0))
        .unwrap();
    assert_eq!(applied, 1, "the app write landed with the retried decide");
    let _ = std::fs::remove_file(&path);
}

// Should: return the storage error from the input that triggered the decide
// when the application permanently refuses the block.
// Should not: report the height decided, persist anything, or carry on as if
// the effect had succeeded.
// Impact: malachite's process! macro logs a failed effect and resumes the
// engine; without the host parking the error, the engine believes the height
// decided while the database never advances — a silent, permanent wedge
// (production, 2026-09-27: one node four days behind at height 64859).
#[test]
fn decide_storage_failure_is_fatal_not_silent() {
    let (path, mut core) = flaky_core("decide-fatal", 1, false);
    core.start_height(Height::INITIAL, false).unwrap();
    core.take_outputs();

    let block = build_block(Height::INITIAL, Round::new(0), 0, None);
    let err = core
        .propose(Height::INITIAL, Round::new(0), block)
        .expect_err("a permanent apply failure must reach the caller");
    assert!(
        matches!(err, HostError::Storage(StoreError::Apply(_))),
        "unexpected error class: {err:?}"
    );

    assert!(
        decided_outputs(&mut core).is_empty(),
        "no Decided output for a height the database never recorded"
    );
    drop(core);
    assert!(decided_heights(&path).is_empty());
    let _ = std::fs::remove_file(&path);
}

// Should: give up after the bounded number of retries when the contention
// never clears, and return it as a storage error.
// Should not: retry forever on the single-threaded consensus shell.
#[test]
fn decide_retries_are_bounded() {
    let (path, mut core) = flaky_core("decide-bounded", 10, true);
    core.start_height(Height::INITIAL, false).unwrap();
    core.take_outputs();

    let block = build_block(Height::INITIAL, Round::new(0), 0, None);
    let err = core
        .propose(Height::INITIAL, Round::new(0), block)
        .expect_err("contention that outlives the retries is returned");
    assert!(
        matches!(err, HostError::Storage(StoreError::ApplyTransient(_))),
        "unexpected error class: {err:?}"
    );
    assert!(decided_outputs(&mut core).is_empty());
    drop(core);
    assert!(decided_heights(&path).is_empty());
    let _ = std::fs::remove_file(&path);
}
