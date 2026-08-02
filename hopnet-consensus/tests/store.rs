//! SqliteStorage contract tests: durability across reopen, WAL ordering,
//! decide atomicity, torn-entry tolerance — plus an end-to-end host run over
//! real SQLite files including a mid-height crash + WAL replay.

mod common;

use std::collections::BTreeMap;
use std::path::PathBuf;

use malachitebft_core_types::Round;

use common::{build_block, decided_heights, open_storage, temp_db, SqlApp};
use hopnet_consensus::codec::{WireConsensusMsg, WireTimeoutKind, WireWalEntry};
use hopnet_consensus::config::QuorumProfile;
use hopnet_consensus::context::{Address, Height};
use hopnet_consensus::host::{HostCore, HostOutput};
use hopnet_consensus::sim::{MemGossip, MemTimers};
use hopnet_consensus::store::{SqliteStorage, StoreError};
use hopnet_consensus::traits::Storage;
use hopnet_consensus::types::Blake3Hash;

fn wal_entry(round: i64) -> WireWalEntry {
    WireWalEntry::Timeout {
        kind: WireTimeoutKind::Propose,
        round,
    }
}

type SqlCore = HostCore<SqlApp, SqliteStorage, MemGossip, MemTimers>;

fn build_core(
    node_id: i32,
    n: i32,
    path: &PathBuf,
    profile: QuorumProfile,
    gossip: MemGossip,
) -> SqlCore {
    let valset = common::valset(n);
    HostCore::new(
        common::chain_id(),
        common::key(node_id),
        Address(node_id),
        profile,
        common::params(node_id, profile),
        Height::INITIAL,
        valset.clone(),
        SqlApp { valset },
        open_storage(path),
        gossip,
        MemTimers::default(),
    )
}

// ---------------------------------------------------------------------------
// Contract tests

// Should: return WAL entries in seq order, persisted across a reopen.
// Should not: reorder, drop, or duplicate entries.
// Impact: replay order is the crash-recovery correctness contract — a
// reordered WAL replays a different protocol history.
#[test]
fn wal_persists_in_seq_order_across_reopen() {
    let path = temp_db("wal-order");
    {
        let mut s = open_storage(&path);
        // Insert out of arrival order within a height is impossible via the
        // host (seq is monotonic), but the PK + ORDER BY must still hold.
        s.wal_append(Height(3), 0, &wal_entry(0)).unwrap();
        s.wal_append(Height(3), 1, &wal_entry(1)).unwrap();
        s.wal_append(Height(3), 2, &wal_entry(2)).unwrap();
        s.wal_append(Height(4), 0, &wal_entry(9)).unwrap(); // other height
    }
    let mut s = open_storage(&path);
    let entries = s.wal_fetch(Height(3)).unwrap();
    assert_eq!(entries.len(), 3);
    for (i, e) in entries.iter().enumerate() {
        match e {
            WireWalEntry::Timeout { round, .. } => assert_eq!(*round, i as i64),
            other => panic!("unexpected entry {other:?}"),
        }
    }
    assert_eq!(s.wal_fetch(Height(4)).unwrap().len(), 1);

    s.wal_reset().unwrap();
    assert!(s.wal_fetch(Height(3)).unwrap().is_empty());
    assert!(s.wal_fetch(Height(4)).unwrap().is_empty());
    let _ = std::fs::remove_file(&path);
}

// Should: persist and read back WAL entries at heights above i64::MAX
// (stored as negative INTEGERs by the bit-cast mapping), across a reopen.
// Impact: RFC-019 S0 full-range contract — no height value may be
// unrepresentable or silently wrapped at the SQLite boundary.
#[test]
fn wal_roundtrips_extreme_heights_across_reopen() {
    let path = temp_db("wal-extreme-heights");
    for h in [i64::MAX as u64, i64::MAX as u64 + 1, u64::MAX] {
        {
            let mut s = open_storage(&path);
            s.wal_append(Height(h), 0, &wal_entry(7)).unwrap();
        }
        let mut s = open_storage(&path);
        assert_eq!(s.wal_fetch(Height(h)).unwrap().len(), 1);
        s.wal_reset().unwrap();
    }
    let _ = std::fs::remove_file(&path);
}

// Should: roll the ENTIRE decide back when the closure fails — app write,
// decided rows, WAL truncation, meta.
// Should not: leave any partial state.
// Impact: crash consistency between app state and consensus history; the
// one-transaction invariant the whole storage design exists for.
#[test]
fn decide_atomically_rolls_back_everything_on_error() {
    let path = temp_db("decide-atomic");
    let mut s = open_storage(&path);
    s.wal_append(Height(1), 0, &wal_entry(0)).unwrap();

    let block = build_block(Height::INITIAL, Round::new(0), 0, None);
    let cert = hopnet_consensus::codec::WireCommitCertificate {
        height: 1,
        round: 0,
        value_id: block.block_hash,
        signatures: Vec::new(),
    };

    let result: Result<(), StoreError> = s.decide_atomically(|tx| {
        tx.execute(
            "INSERT INTO applied (height, hash) VALUES (?, ?)",
            rusqlite::params![1i64, block.block_hash],
        )
        .unwrap();
        SqliteStorage::<hopnet_consensus::store::OwnedConn>::store_decided_tx(tx, &block, &cert)?;
        SqliteStorage::<hopnet_consensus::store::OwnedConn>::truncate_wal_tx(tx, Height(1))?;
        SqliteStorage::<hopnet_consensus::store::OwnedConn>::set_last_decided_tx(tx, Height(1))?;
        Err(StoreError::Apply("injected failure".into()))
    });
    assert!(result.is_err());

    // Nothing committed: WAL intact, no decided rows, no applied rows, no meta.
    assert_eq!(s.wal_fetch(Height(1)).unwrap().len(), 1);
    assert_eq!(s.last_decided().unwrap(), None);
    let conn = rusqlite::Connection::open(&path).unwrap();
    let applied: i64 = conn
        .query_row("SELECT COUNT(*) FROM applied", [], |r| r.get(0))
        .unwrap();
    let decided: i64 = conn
        .query_row("SELECT COUNT(*) FROM decided_blocks", [], |r| r.get(0))
        .unwrap();
    assert_eq!((applied, decided), (0, 0));
    let _ = std::fs::remove_file(&path);
}

// Should: drop a torn (undecodable) FINAL WAL entry and recover the prefix.
// Should not: silently skip a corrupt entry in the MIDDLE (that is data
// corruption, not a torn tail — it must error).
// Impact: a crash between bytes hitting the page cache and fsync must not
// brick recovery; mid-stream corruption must not silently alter history.
#[test]
fn torn_final_wal_entry_dropped_mid_corruption_errors() {
    let path = temp_db("torn");
    {
        let mut s = open_storage(&path);
        s.wal_append(Height(2), 0, &wal_entry(0)).unwrap();
        s.wal_append(Height(2), 1, &wal_entry(1)).unwrap();
        s.wal_append(Height(2), 2, &wal_entry(2)).unwrap();
    }
    // Corrupt the FINAL entry.
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute(
        "UPDATE consensus_wal SET entry = X'DEADBEEF' WHERE height = 2 AND seq = 2",
        [],
    )
    .unwrap();
    drop(conn);

    let mut s = open_storage(&path);
    let entries = s.wal_fetch(Height(2)).unwrap();
    assert_eq!(entries.len(), 2, "torn tail dropped, prefix recovered");

    // Corrupt a MIDDLE entry: fetch must error, not skip.
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute(
        "UPDATE consensus_wal SET entry = X'DEADBEEF' WHERE height = 2 AND seq = 0",
        [],
    )
    .unwrap();
    drop(conn);
    let mut s = open_storage(&path);
    assert!(matches!(s.wal_fetch(Height(2)), Err(StoreError::Codec(_))));
    let _ = std::fs::remove_file(&path);
}

// Should: serve only the contiguous decided prefix of the requested range.
// Should not: return past a gap (a gap means we don't actually have that
// history — serving beyond it would hand a syncing peer a hole).
// Impact: the decided-value sync server's correctness.
#[test]
fn decided_range_stops_at_gap() {
    let path = temp_db("range");
    let mut s = open_storage(&path);
    for h in [1u64, 2, 4] {
        let block = build_block(Height(h), Round::new(0), 0, None);
        let cert = hopnet_consensus::codec::WireCommitCertificate {
            height: h,
            round: 0,
            value_id: block.block_hash,
            signatures: Vec::new(),
        };
        s.decide_atomically(|tx| {
            SqliteStorage::<hopnet_consensus::store::OwnedConn>::store_decided_tx(
                tx, &block, &cert,
            )?;
            SqliteStorage::<hopnet_consensus::store::OwnedConn>::set_last_decided_tx(
                tx,
                Height(h),
            )?;
            Ok(())
        })
        .unwrap();
    }
    let got = s.decided_range(Height(1), Height(4)).unwrap();
    assert_eq!(got.len(), 2, "stops before the gap at height 3");
    assert_eq!(got[0].0.data.height, 1);
    assert_eq!(got[1].0.data.height, 2);
    assert!(s.decided_range(Height(3), Height(4)).unwrap().is_empty());
    let _ = std::fs::remove_file(&path);
}

// ---------------------------------------------------------------------------
// End-to-end over real SQLite

/// Drain a node: fulfil NeedValue (up to `max_height` — a single-node mesh
/// decides synchronously, so an uncapped loop chains decide→propose→decide
/// forever), collect Decided, and append outbound msgs to `sent_log`.
fn settle(
    core: &mut SqlCore,
    gossip: &MemGossip,
    sent_log: &mut Vec<WireConsensusMsg>,
    max_height: u64,
) -> Vec<u64> {
    let mut decided = Vec::new();
    loop {
        let outs = core.take_outputs();
        if outs.is_empty() {
            break;
        }
        for out in outs {
            match out {
                HostOutput::NeedValue { height, round } if height.0 <= max_height => {
                    let block = build_block(height, round, core.address().0, None);
                    core.propose(height, round, block).unwrap();
                }
                HostOutput::NeedValue { .. } => {}
                HostOutput::Decided { height } => decided.push(height.0),
                _ => {}
            }
        }
    }
    sent_log.extend(gossip.take_outbox());
    decided
}

// Should: decide contiguous heights end-to-end through real SQLite storage
// (WAL appended + truncated, decided rows + certs + meta committed with the
// app write in one transaction), single-node Majority mesh.
// Should not: leave stale WAL rows after each decide.
// Impact: the production Storage impl driven by the production host loop.
#[test]
fn single_node_decides_heights_over_sqlite() {
    let path = temp_db("single-node");
    let gossip = MemGossip::default();
    let mut core = build_core(0, 1, &path, QuorumProfile::Majority, gossip.clone());
    let mut log = Vec::new();

    core.start_height(Height::INITIAL, false).unwrap();
    let mut all_decided = Vec::new();
    // Single-node majority (quorum 1): each propose decides synchronously and
    // queues the next height's NeedValue, so one settle loop runs the chain.
    while all_decided.len() < 5 {
        let decided = settle(&mut core, &gossip, &mut log, 5);
        assert!(!decided.is_empty(), "single node must make progress");
        all_decided.extend(decided);
    }
    drop(core);

    let rows = decided_heights(&path);
    assert!(rows.len() >= 5);
    for (i, (h, _)) in rows.iter().take(5).enumerate() {
        assert_eq!(*h, (i + 1) as i64);
    }
    // App writes landed atomically with the decides.
    let conn = rusqlite::Connection::open(&path).unwrap();
    let applied: i64 = conn
        .query_row("SELECT COUNT(*) FROM applied", [], |r| r.get(0))
        .unwrap();
    assert!(applied >= 5);
    // WAL truncated up to the last decided height.
    let wal_rows: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM consensus_wal WHERE height <= (SELECT MAX(height) FROM decided_blocks)",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(wal_rows, 0);
    let _ = std::fs::remove_file(&path);
}

// Should: recover from a mid-height crash via SQLite WAL replay WITHOUT
// equivocating — the restarted node re-publishes only what it already signed,
// and consensus completes on the original value.
// Should not: sign a second value for a (height, round, type) it voted in
// before the crash, or corrupt the decided history.
// Impact: risk #1 (post-restart equivocation) exercised over the REAL storage
// path, not the in-memory fake.
#[test]
fn two_node_crash_midheight_replays_wal_without_equivocation() {
    let path0 = temp_db("crash-n0");
    let path1 = temp_db("crash-n1");
    let g0 = MemGossip::default();
    let g1 = MemGossip::default();
    let mut n0 = build_core(0, 2, &path0, QuorumProfile::Majority, g0.clone());
    let mut n1 = build_core(1, 2, &path1, QuorumProfile::Majority, g1.clone());
    let mut log0: Vec<WireConsensusMsg> = Vec::new();
    let mut log1: Vec<WireConsensusMsg> = Vec::new();

    // Height 1 proposer is validators[(1+0) % 2] = node 1.
    n0.start_height(Height::INITIAL, false).unwrap();
    n1.start_height(Height::INITIAL, false).unwrap();
    settle(&mut n0, &g0, &mut log0, 1);
    settle(&mut n1, &g1, &mut log1, 1);

    // Deliver ONLY the proposal + node 1's prevote to node 0, so node 0 signs
    // its own prevote/precommit (WAL entries) but cannot complete the height
    // (node 1 never sees node 0's votes before the "crash").
    for msg in log1.clone() {
        n0.on_wire(msg).unwrap();
    }
    settle(&mut n0, &g0, &mut log0, 1);
    let pre_crash_votes: Vec<_> = log0
        .iter()
        .filter_map(|m| match m {
            WireConsensusMsg::Vote(v) => Some(v.clone()),
            _ => None,
        })
        .collect();
    assert!(
        !pre_crash_votes.is_empty(),
        "node 0 must have signed votes before the crash"
    );

    // CRASH node 0 mid-height: drop the core (in-process state gone), reopen
    // the same DB file, replay the WAL.
    drop(n0);
    let mut n0 = build_core(0, 2, &path0, QuorumProfile::Majority, g0.clone());
    n0.start_height(Height::INITIAL, false).unwrap();
    let mut post_log0: Vec<WireConsensusMsg> = Vec::new();
    settle(&mut n0, &g0, &mut post_log0, 1);

    // Redeliver node 1's messages (rebroadcast) and pump to completion.
    for msg in log1.clone() {
        n0.on_wire(msg).unwrap();
    }
    settle(&mut n0, &g0, &mut post_log0, 1);
    for _ in 0..4 {
        for msg in std::mem::take(&mut post_log0) {
            n1.on_wire(msg).unwrap();
        }
        settle(&mut n1, &g1, &mut log1, 1);
        for msg in std::mem::take(&mut log1) {
            n0.on_wire(msg).unwrap();
        }
        settle(&mut n0, &g0, &mut post_log0, 1);
    }

    // No equivocation: every vote node 0 ever published (pre + post crash)
    // has a unique value per (height, round, type).
    let mut signed: BTreeMap<(u64, i64, bool), Option<Blake3Hash>> = BTreeMap::new();
    let all_votes =
        pre_crash_votes
            .iter()
            .cloned()
            .chain(log0.iter().chain(post_log0.iter()).filter_map(|m| match m {
                WireConsensusMsg::Vote(v) | WireConsensusMsg::LivenessVote(v) => Some(v.clone()),
                _ => None,
            }));
    for v in all_votes {
        let key = (
            v.height,
            v.round,
            matches!(v.typ, hopnet_consensus::codec::WireVoteType::Precommit),
        );
        if let Some(prev) = signed.get(&key) {
            assert_eq!(*prev, v.value, "EQUIVOCATION after WAL replay at {key:?}");
        } else {
            signed.insert(key, v.value);
        }
    }

    // Both nodes decided height 1 on the SAME block.
    let d0 = decided_heights(&path0);
    let d1 = decided_heights(&path1);
    assert!(!d0.is_empty() && !d1.is_empty(), "both nodes must decide");
    assert_eq!(d0[0], d1[0], "divergence at height 1");
    let _ = std::fs::remove_file(&path0);
    let _ = std::fs::remove_file(&path1);
}

// Should: hopnet_consensus_policy roundtrip — defaults when empty, per-key
// resolution on partial seeds, INSERT OR REPLACE overwrite semantics.
// Impact: genesis-seeded policy is how orchestrator membership tests run
// at seconds-scale (the genesis path is the test path).
#[test]
fn consensus_policy_rows_roundtrip() {
    use hopnet_consensus::membership::ConsensusPolicy;
    use std::time::Duration;

    let conn = rusqlite::Connection::open_in_memory().unwrap();
    hopnet_consensus::store::install_schema(&conn).unwrap();

    assert_eq!(
        hopnet_consensus::store::read_policy(&conn).unwrap(),
        ConsensusPolicy::default()
    );

    hopnet_consensus::store::apply_policy_rows(
        &conn,
        &[
            ("probe_base".to_string(), "2".to_string()),
            ("grace".to_string(), "1".to_string()),
            ("s_full".to_string(), "6".to_string()),
        ],
    )
    .unwrap();
    let p = hopnet_consensus::store::read_policy(&conn).unwrap();
    assert_eq!(p.probe_base, Duration::from_secs(2));
    assert_eq!(p.grace, Duration::from_secs(1));
    assert_eq!(p.s_full, Duration::from_secs(6));
    assert_eq!(p.p_prove, Duration::from_secs(1800)); // unseeded -> default

    // Overwrite (INSERT OR REPLACE).
    hopnet_consensus::store::apply_policy_rows(
        &conn,
        &[("probe_base".to_string(), "4".to_string())],
    )
    .unwrap();
    let p = hopnet_consensus::store::read_policy(&conn).unwrap();
    assert_eq!(p.probe_base, Duration::from_secs(4));
    assert_eq!(p.grace, Duration::from_secs(1)); // untouched
}
