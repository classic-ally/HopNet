//! A WAL append that cannot be persisted is never silently "done" — and a
//! WAL append that merely hit contention is never fatal. Regression tests for
//! the 2026-10-01 production crash loop: a proposer's cold-cache preflight
//! held the write lock past the connection's 5 s busy_timeout, the vote WAL
//! append got `database is locked`, the host made it fatal (no retry, unlike
//! the decide), the process aborted, and the next boot — proposer at the same
//! height — reproduced it: 24 restarts, the mesh pinned at one height with a
//! single live validator.

mod common;

use std::time::Duration;

use common::{
    build_block, contended_db_with_busy_timeout, decided_heights, hold_write_lock, open_storage,
    temp_db, CallCount, FlakyStorage, SqlApp,
};
use hopnet_consensus::codec::{SignedVote, WireConsensusMsg};
use hopnet_consensus::config::QuorumProfile;
use hopnet_consensus::context::{Address, Height, HopNetContext};
use hopnet_consensus::host::{HostCore, HostError, HostOutput};
use hopnet_consensus::signing::sign_vote;
use hopnet_consensus::sim::{MemGossip, MemTimers};
use hopnet_consensus::store::{SqliteStorage, StoreError};
use malachitebft_core_types::{Context, NilOrVal, Round};

type SqlCore = HostCore<SqlApp, SqliteStorage, MemGossip, MemTimers>;
type FlakyCore = HostCore<SqlApp, FlakyStorage, MemGossip, MemTimers>;

/// Single-node Majority core (quorum 1): one `propose` carries the proposal
/// WAL append, the votes' WAL appends and the decide synchronously, so every
/// durability effect's outcome is the `propose` result.
fn sql_core(storage: SqliteStorage) -> SqlCore {
    let valset = common::valset(1);
    HostCore::new(
        common::chain_id(),
        common::key(0),
        Address(0),
        QuorumProfile::Majority,
        common::params(0, QuorumProfile::Majority),
        Height::INITIAL,
        valset.clone(),
        SqlApp { valset },
        storage,
        MemGossip::default(),
        MemTimers::default(),
    )
}

fn flaky_core(
    name: &str,
    failures: u32,
    transient: bool,
) -> (std::path::PathBuf, FlakyCore, CallCount, MemGossip) {
    let path = temp_db(name);
    let valset = common::valset(1);
    let (storage, calls) = FlakyStorage::new(open_storage(&path), failures, transient);
    let gossip = MemGossip::default();
    let core = HostCore::new(
        common::chain_id(),
        common::key(0),
        Address(0),
        QuorumProfile::Majority,
        common::params(0, QuorumProfile::Majority),
        Height::INITIAL,
        valset.clone(),
        SqlApp { valset },
        storage,
        gossip.clone(),
        MemTimers::default(),
    );
    (path, core, calls, gossip)
}

fn decided_outputs(outputs: Vec<HostOutput>) -> Vec<u64> {
    outputs
        .into_iter()
        .filter_map(|o| match o {
            HostOutput::Decided { height } => Some(height.0),
            _ => None,
        })
        .collect()
}

// Should: retry a WAL append that hits BUSY while another connection holds
// the write lock longer than the connection's busy_timeout, and decide the
// height once the lock clears.
// Should not: return the contention to the shell, which aborts the process.
// Impact: thor, 2026-10-01 — the proposer's preflight held the lock > 15 s,
// the vote WAL append failed after one 5 s busy wait, the process aborted,
// and the same height reproduced it 24 times; the mesh was pinned at 82752.
#[test]
fn wal_append_survives_a_lock_held_past_one_busy_wait() {
    let (path, storage) = contended_db_with_busy_timeout("wal-busy", 200);
    let mut core = sql_core(storage);
    core.start_height(Height::INITIAL, false).unwrap();
    core.take_outputs();

    // 200 ms busy wait + 100/200/400/800 ms backoffs: the lock outlives the
    // first four attempts and clears before the budget does.
    let holder = hold_write_lock(&path, Duration::from_millis(1500));
    let block = build_block(Height::INITIAL, Round::new(0), 0, None);
    core.propose(Height::INITIAL, Round::new(0), block)
        .expect("contention that clears within the budget is retried, not returned");
    holder.join().unwrap();

    assert_eq!(decided_outputs(core.take_outputs()), vec![1]);
    drop(core);
    assert_eq!(
        decided_heights(&path).len(),
        1,
        "the height decided exactly once"
    );
    let _ = std::fs::remove_file(&path);
}

// Should: give up after the retry budget when the contention never clears,
// and return it as a storage error.
// Should not: retry forever on the single-threaded consensus shell, or
// report the height decided.
#[test]
fn wal_append_retries_are_bounded() {
    let (path, mut core, calls, _gossip) = flaky_core("wal-bounded", 20, true);
    core.start_height(Height::INITIAL, false).unwrap();
    core.take_outputs();

    let block = build_block(Height::INITIAL, Round::new(0), 0, None);
    let err = core
        .propose(Height::INITIAL, Round::new(0), block)
        .expect_err("contention that outlives the budget is returned");
    assert!(
        matches!(err, HostError::Storage(StoreError::ApplyTransient(_))),
        "unexpected error class: {err:?}"
    );
    assert_eq!(calls.get(), 7, "one attempt plus six retries, then fatal");
    assert!(decided_outputs(core.take_outputs()).is_empty());
    drop(core);
    assert!(decided_heights(&path).is_empty());
    let _ = std::fs::remove_file(&path);
}

// Should: stop executing effects for the input once one effect has failed —
// no vote is signed or published past a failed WAL append, and no decide runs.
// Should not: leave the database advanced, or gossip carrying votes for a
// height the engine will replay after the restart.
// Impact: before the short-circuit, the engine macro kept driving the input
// after the first failed append: thor signed and published votes whose WAL
// entries never landed, then aborted — exactly the equivocation window the
// WAL exists to close.
#[test]
fn no_effect_runs_after_a_parked_error() {
    let (path, mut core, calls, gossip) = flaky_core("wal-fail-fast", 1, false);
    core.start_height(Height::INITIAL, false).unwrap();
    core.take_outputs();
    gossip.take_outbox();

    let block = build_block(Height::INITIAL, Round::new(0), 0, None);
    let err = core
        .propose(Height::INITIAL, Round::new(0), block)
        .expect_err("a permanent append failure must reach the caller");
    assert!(
        matches!(err, HostError::Storage(StoreError::Apply(_))),
        "unexpected error class: {err:?}"
    );

    assert_eq!(
        calls.get(),
        1,
        "the failed append was the last effect to run"
    );
    let votes = gossip
        .take_outbox()
        .into_iter()
        .filter(|m| {
            matches!(
                m,
                WireConsensusMsg::Vote(_) | WireConsensusMsg::LivenessVote(_)
            )
        })
        .count();
    assert_eq!(votes, 0, "no vote is published past a failed WAL append");
    assert!(decided_outputs(core.take_outputs()).is_empty());
    drop(core);
    assert!(decided_heights(&path).is_empty());
    let _ = std::fs::remove_file(&path);
}

// Should: start a height with a WAL reset while a second connection holds
// the write lock for longer than one busy wait.
// Should not: make the height start — the sync-jump path — fatal on contention.
#[test]
fn height_start_retries_wal_reset_under_contention() {
    let (path, storage) = contended_db_with_busy_timeout("wal-reset-busy", 200);
    let mut core = sql_core(storage);

    let holder = hold_write_lock(&path, Duration::from_millis(1200));
    core.start_height(Height::INITIAL, true)
        .expect("the WAL reset waits out the lock within the budget");
    holder.join().unwrap();

    drop(core);
    let _ = std::fs::remove_file(&path);
}

/// WAL sequence numbers persisted for `height`, straight from the DB file.
fn wal_seqs(path: &std::path::Path, height: u64) -> Vec<i64> {
    let conn = rusqlite::Connection::open(path).unwrap();
    let mut stmt = conn
        .prepare("SELECT seq FROM consensus_wal WHERE height = ? ORDER BY seq")
        .unwrap();
    let rows = stmt.query_map([height as i64], |row| row.get(0)).unwrap();
    rows.collect::<Result<_, _>>().unwrap()
}

// Should: continue the WAL sequence past the rows appended while StartHeight
// replayed the votes buffered for the resumed height, so the first live
// append after an on-demand resume lands on a fresh (height, seq) key and the
// height decides.
// Should not: reset the counter over rows the replay already persisted.
// Impact: thor, 2026-10-01, on v2026.10.3 — lagging a chain running 24
// heights a minute, it held the next height's votes before deciding the
// current one; every resume replayed them, the counter snapped back to 0,
// the next vote's append hit the UNIQUE key and the process aborted — 17
// times in 22 minutes.
#[test]
fn resume_counts_the_appends_made_while_replaying_buffered_votes() {
    let path = temp_db("wal-resume-seq");
    let mut core = sql_core(open_storage(&path)).on_demand();
    core.start_height(Height::INITIAL, false).unwrap();
    let block1 = build_block(Height::INITIAL, Round::new(0), 0, None);
    core.propose(Height::INITIAL, Round::new(0), block1.clone())
        .unwrap();
    assert_eq!(decided_outputs(core.take_outputs()), vec![1]);
    assert_eq!(
        core.paused_at(),
        Some(Height(2)),
        "on-demand: paused before 2"
    );

    // A vote for the pending height arrives while the engine still sits at
    // height 1: the engine buffers it (no WAL row yet).
    let block2 = build_block(Height(2), Round::new(0), 0, Some(block1.block_hash));
    let vote = HopNetContext.new_prevote(
        Height(2),
        Round::new(0),
        NilOrVal::Val(block2.block_hash),
        Address(0),
    );
    let signed = SignedVote::new(
        vote.clone(),
        sign_vote(&common::chain_id(), &common::key(0), &vote),
    );
    core.on_wire(WireConsensusMsg::Vote((&signed).into()))
        .unwrap();
    assert!(
        wal_seqs(&path, 2).is_empty(),
        "a buffered vote is not yet persisted"
    );

    // Resume: StartHeight(2) replays the buffered vote, which appends seq 0.
    core.resume_height().unwrap();
    assert_eq!(
        wal_seqs(&path, 2),
        vec![0],
        "the replayed vote was persisted"
    );

    // The live append that follows must take seq 1, not collide on seq 0.
    core.propose(Height(2), Round::new(0), block2)
        .expect("the live append after a resume continues past the replayed rows");
    assert_eq!(decided_outputs(core.take_outputs()), vec![2]);
    drop(core);
    assert_eq!(decided_heights(&path).len(), 2);
    let _ = std::fs::remove_file(&path);
}
