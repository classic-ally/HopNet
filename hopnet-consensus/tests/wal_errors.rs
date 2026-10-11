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
use malachitebft_core_types::{Context, NilOrVal, Round, Timeout};

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

/// The `NeedValue` requests in `outputs`, as `(height, round)`.
fn need_values(outputs: &[HostOutput]) -> Vec<(u64, u32)> {
    outputs
        .iter()
        .filter_map(|o| match o {
            HostOutput::NeedValue { height, round } => Some((height.0, round.as_u32()?)),
            _ => None,
        })
        .collect()
}

// Impact: 2026.10.14 crossing — a regenesis seal takes ~100 s to build, the
// validators were restarted mid build, and the replay dropped every value
// request on the assumption it had supplied the value. Nothing was in the
// WAL, so the restarted proposer never proposed in its round and the two
// validators waited on each other for over half an hour.
// Should: ask a restarted proposer again for the value of the round it ends
// in when the replay held no proposal of its own, and decide once given.
#[test]
fn a_restarted_proposer_is_asked_again_for_a_value_it_never_proposed() {
    let path = temp_db("wal-owed-value");
    let mut core = sql_core(open_storage(&path));
    core.start_height(Height::INITIAL, false).unwrap();
    assert_eq!(need_values(&core.take_outputs()), vec![(1, 0)]);

    // The build outlives round 0: nil prevote and precommit, round 1 begins
    // and asks for a value again — which is still being built at the crash.
    core.on_timeout(Timeout::propose(Round::new(0))).unwrap();
    core.on_timeout(Timeout::precommit(Round::new(0))).unwrap();
    assert_eq!(need_values(&core.take_outputs()), vec![(1, 1)]);
    drop(core);

    let mut core = sql_core(open_storage(&path));
    core.start_height(Height::INITIAL, false).unwrap();
    assert_eq!(
        need_values(&core.take_outputs()),
        vec![(1, 1)],
        "the replay supplied no value for round 1, so it is asked for again"
    );
    let block = build_block(Height::INITIAL, Round::new(1), 0, None);
    core.propose(Height::INITIAL, Round::new(1), block).unwrap();
    assert_eq!(decided_outputs(core.take_outputs()), vec![1]);
    drop(core);
    let _ = std::fs::remove_file(&path);
}

/// Node 1 of a two-validator Majority set: the proposer of height 1 round 0,
/// which needs its peer's votes to decide.
fn second_of_two(storage: SqliteStorage) -> SqlCore {
    let valset = common::valset(2);
    HostCore::new(
        common::chain_id(),
        common::key(1),
        Address(1),
        QuorumProfile::Majority,
        common::params(1, QuorumProfile::Majority),
        Height::INITIAL,
        valset.clone(),
        SqlApp { valset },
        storage,
        MemGossip::default(),
        MemTimers::default(),
    )
}

// Impact: re-asking a proposer that DID propose would have the shell build
// and ship a second, different block for the same (height, round).
// Should not: ask a restarted proposer for a value when its replay supplied
// its own proposal for the round.
#[test]
fn a_restarted_proposer_is_not_asked_for_a_value_it_already_proposed() {
    let path = temp_db("wal-replayed-value");
    let mut core = second_of_two(open_storage(&path));
    core.start_height(Height::INITIAL, false).unwrap();
    assert_eq!(need_values(&core.take_outputs()), vec![(1, 0)]);
    let block = build_block(Height::INITIAL, Round::new(0), 1, None);
    core.propose(Height::INITIAL, Round::new(0), block).unwrap();
    assert!(
        decided_outputs(core.take_outputs()).is_empty(),
        "one of two votes"
    );
    drop(core);

    let mut core = second_of_two(open_storage(&path));
    core.start_height(Height::INITIAL, false).unwrap();
    assert!(
        need_values(&core.take_outputs()).is_empty(),
        "the replayed proposal is the round's value"
    );
    drop(core);
    let _ = std::fs::remove_file(&path);
}

// Impact: proposals were gossiped before the engine appended them to the
// WAL; a crash in between shipped a block the restarted node knew nothing
// of, and it then built a second one for the same (height, round).
// Should not: ship a proposal whose WAL append failed.
#[test]
fn a_proposal_is_not_gossiped_unless_persisted() {
    let (path, mut core, _calls, gossip) = flaky_core("wal-propose-order", 1, false);
    core.start_height(Height::INITIAL, false).unwrap();
    core.take_outputs();
    gossip.take_outbox();

    let block = build_block(Height::INITIAL, Round::new(0), 0, None);
    core.propose(Height::INITIAL, Round::new(0), block)
        .expect_err("a permanent append failure reaches the caller");
    let shipped = gossip
        .take_outbox()
        .into_iter()
        .filter(|m| matches!(m, WireConsensusMsg::ProposedValue(_)))
        .count();
    assert_eq!(shipped, 0, "an unpersisted proposal never leaves the node");
    drop(core);
    let _ = std::fs::remove_file(&path);
}

// Impact: a liveness certificate whose round failed to decode returned a
// codec error out of `on_wire`; the shell treats any step error as fatal
// and aborts, so one bad peer message crashed the node.
// Should: drop an undecodable polka or round certificate and keep running.
#[test]
fn an_undecodable_liveness_certificate_is_dropped() {
    let path = temp_db("wal-bad-liveness");
    let mut core = sql_core(open_storage(&path));
    core.start_height(Height::INITIAL, false).unwrap();
    core.take_outputs();

    let polka = hopnet_consensus::codec::WirePolkaCertificate {
        height: 1,
        round: -7,
        value_id: hopnet_consensus::types::Blake3Hash::from_bytes([1u8; 32]),
        signatures: Vec::new(),
    };
    core.on_wire(WireConsensusMsg::LivenessPolka(polka))
        .expect("an undecodable polka certificate is dropped, not fatal");
    let skip = hopnet_consensus::codec::WireRoundCertificate {
        height: 1,
        round: -7,
        cert_type: hopnet_consensus::codec::WireRoundCertificateType::Skip,
        signatures: Vec::new(),
    };
    core.on_wire(WireConsensusMsg::LivenessSkipRound(skip))
        .expect("an undecodable round certificate is dropped, not fatal");

    let block = build_block(Height::INITIAL, Round::new(0), 0, None);
    core.propose(Height::INITIAL, Round::new(0), block).unwrap();
    assert_eq!(decided_outputs(core.take_outputs()), vec![1]);
    drop(core);
    let _ = std::fs::remove_file(&path);
}
