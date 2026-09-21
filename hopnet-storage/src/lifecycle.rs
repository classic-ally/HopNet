//! RFC-STORAGE-003 block lifecycle (S1): the `(confirmed, target)` handoff
//! pair, the two consensus transactions that move it, and the transition
//! record that gives declared heights their meaning.
//!
//! - `placement_height` is `confirmed`: the obligation epoch, NULL until the
//!   first ConfirmPlacement.
//! - `desired_placement_height` is `target`: NOT NULL, stamped at insert,
//!   moved only by DeclarePlacementTarget.
//! - The transition record memoizes the derived storage view at every height
//!   it changed, so "the view at height h" is one indexed read.
//!
//! Apply-side validation is per ENTRY and never fails the block: an invalid
//! entry is skipped (counted, logged), the rest apply. The host's dispatch
//! aborts the whole block on a handler error, so only an undecodable payload
//! may error — need is recorded, never refused.
//!
//! Pure/sync: no runtime, no seams. The host calls these inside its one
//! consensus apply transaction.

use std::collections::{BTreeMap, HashMap};

use hopnet_common::height::{height_from_db, height_to_db};
use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};

use crate::placement::{self, MetricsRow};
use crate::store::db_err;
use crate::traits::StorageView;
use crate::types::BlobId;
use crate::StorageError;

/// Consensus function name of the declare transaction.
pub const DECLARE_TX_FN: &str = "declare_placement_target";
/// Consensus function name of the confirm transaction.
pub const CONFIRM_TX_FN: &str = "confirm_placement";

/// One blob's re-goal: compare-and-swap `desired_placement_height` from
/// `from` to `to`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlacementTarget {
    pub blob_id: BlobId,
    pub from: u64,
    pub to: u64,
}

/// DeclarePlacementTarget payload: batched, proposer-originated.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeclarePlacementTarget {
    pub targets: Vec<PlacementTarget>,
}

/// One blob's confirmation: stamp `placement_height = height` once every
/// responsible node under `view@height` has attested its class.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlacementConfirmation {
    pub blob_id: BlobId,
    pub height: u64,
}

/// ConfirmPlacement payload: batched, proposable by anyone — the proofs are
/// the attestation rows already in consensus.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfirmPlacement {
    pub confirmations: Vec<PlacementConfirmation>,
}

/// The placement inputs of one storage view, canonically ordered: exactly
/// what `placement::select_nodes_for_blob` + `assign_fragment_classes`
/// consume, nothing else (liveness sets, watermark, tiers stay out — they
/// do not move bytes).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ViewSnapshot {
    /// Member node ids, ascending.
    pub members: Vec<i32>,
    /// Quantized placement weight per member.
    pub weights: BTreeMap<i32, u64>,
    /// Members' metrics rows, ascending by node id (the >30-member
    /// selection stage scores these).
    pub metrics: Vec<MetricsRow>,
}

impl From<&StorageView> for ViewSnapshot {
    fn from(view: &StorageView) -> Self {
        let mut members: Vec<i32> = view.members.iter().map(|p| p.node_id).collect();
        members.sort_unstable();
        members.dedup();
        let weights = members
            .iter()
            .map(|n| (*n, view.weights.get(n).copied().unwrap_or(1)))
            .collect();
        let mut metrics: Vec<MetricsRow> = view
            .metrics
            .iter()
            .filter(|m| members.binary_search(&m.node_id).is_ok())
            .cloned()
            .collect();
        metrics.sort_by_key(|m| m.node_id);
        Self {
            members,
            weights,
            metrics,
        }
    }
}

impl ViewSnapshot {
    /// Canonical bytes: the struct is fully ordered, so bincode of it is a
    /// deterministic function of the view.
    pub fn encode(&self) -> Vec<u8> {
        bincode::serde::encode_to_vec(self, bincode::config::standard())
            .expect("ViewSnapshot is plain data")
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        bincode::serde::decode_from_slice(bytes, bincode::config::standard())
            .map(|(v, _)| v)
            .map_err(|e| StorageError::Host(format!("view snapshot decode: {e}")))
    }

    /// Class → responsible node for one blob under this view, the same
    /// recipe the engine runs (`engine::assign_for_blob`): seeded selection
    /// then balanced capped rendezvous. Empty when the view has no members.
    pub fn assignment(&self, blob_id: &BlobId) -> Vec<i32> {
        let seed = placement::placement_seed(blob_id);
        let selected: Vec<i32> =
            placement::select_nodes_for_blob(self.members.clone(), self.metrics.clone(), &seed);
        let weights: HashMap<i32, u64> = self.weights.iter().map(|(k, v)| (*k, *v)).collect();
        placement::assign_fragment_classes(
            &seed,
            &selected,
            &weights,
            crate::rs::TOTAL_FRAGMENTS_PER_CHUNK as u32,
        )
    }
}

// ---------------------------------------------------------------------------
// The transition record

/// Append `snapshot` at `height` iff it differs from the latest recorded
/// view (or the record is empty). Returns whether a row was written.
/// Idempotent at one height (re-apply of the same block rewrites the same
/// bytes).
pub fn record_transition(
    db_tx: &rusqlite::Transaction,
    height: u64,
    snapshot: &ViewSnapshot,
) -> Result<bool, StorageError> {
    let latest: Option<(i64, Vec<u8>)> = db_tx
        .query_row(
            "SELECT height, snapshot FROM storage_view_transitions
             ORDER BY height DESC LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(db_err("read latest view transition"))?;
    let bytes = snapshot.encode();
    if let Some((latest_height, latest_bytes)) = latest {
        // The latest row is at-or-above this height only on a re-apply of
        // the same block (validation dry-run, then execute): same bytes,
        // nothing to do.
        if latest_bytes == bytes || height_from_db(latest_height) > height {
            return Ok(false);
        }
    }
    db_tx
        .execute(
            "INSERT OR REPLACE INTO storage_view_transitions (height, snapshot) VALUES (?, ?)",
            params![height_to_db(height), bytes],
        )
        .map_err(db_err("insert view transition"))?;
    Ok(true)
}

/// Height of the latest recorded transition (T in the staleness predicate
/// `desired < T`). `None` before the first block after the cutover.
pub fn latest_transition_height(conn: &rusqlite::Connection) -> Result<Option<u64>, StorageError> {
    conn.query_row(
        "SELECT MAX(height) FROM storage_view_transitions",
        [],
        |row| row.get::<_, Option<i64>>(0),
    )
    .map(|h| h.map(height_from_db))
    .map_err(db_err("read latest transition height"))
}

/// Whether the storage view changed in `(from, to]` — declare's "legitimate
/// need" check, one indexed comparison.
pub fn transition_in(
    conn: &rusqlite::Connection,
    from_exclusive: u64,
    to_inclusive: u64,
) -> Result<bool, StorageError> {
    conn.query_row(
        "SELECT EXISTS (SELECT 1 FROM storage_view_transitions
                        WHERE height > ? AND height <= ?)",
        params![height_to_db(from_exclusive), height_to_db(to_inclusive)],
        |row| row.get(0),
    )
    .map_err(db_err("probe transition range"))
}

/// The storage view in force at `height`: the latest transition at or
/// below it (the view is constant between transitions). `None` when the
/// record does not reach back that far.
pub fn snapshot_at(
    conn: &rusqlite::Connection,
    height: u64,
) -> Result<Option<ViewSnapshot>, StorageError> {
    let bytes: Option<Vec<u8>> = conn
        .query_row(
            "SELECT snapshot FROM storage_view_transitions
             WHERE height <= ? ORDER BY height DESC LIMIT 1",
            params![height_to_db(height)],
            |row| row.get(0),
        )
        .optional()
        .map_err(db_err("read view snapshot"))?;
    bytes.map(|b| ViewSnapshot::decode(&b)).transpose()
}

// ---------------------------------------------------------------------------
// DeclarePlacementTarget apply

/// Per-entry tally of one declare apply.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DeclareOutcome {
    pub applied: usize,
    pub skipped: usize,
}

/// Apply a declare batch under the deciding block's height. Per entry:
/// the blob exists; `from` equals the current goal (CAS, so a stale
/// declaration racing a newer one rejects cleanly); `from < to <
/// deciding_height` (a decided height — never the block being applied, so
/// a same-block transition is never referenced); a transition exists in
/// `(from, to]`. Effect: `desired_placement_height = to`. Obligations and
/// protection are untouched (they lapse only at confirm).
pub fn apply_declare(
    db_tx: &rusqlite::Transaction,
    payload: &DeclarePlacementTarget,
    deciding_height: u64,
) -> Result<DeclareOutcome, StorageError> {
    let mut outcome = DeclareOutcome::default();
    let mut read = db_tx
        .prepare_cached("SELECT desired_placement_height FROM data_blocks WHERE id = ?")
        .map_err(db_err("prepare goal read"))?;
    let mut write = db_tx
        .prepare_cached(
            "UPDATE data_blocks SET desired_placement_height = ?
             WHERE id = ? AND desired_placement_height = ?",
        )
        .map_err(db_err("prepare goal write"))?;

    for target in &payload.targets {
        let desired: Option<i64> = read
            .query_row(params![target.blob_id], |row| row.get(0))
            .optional()
            .map_err(db_err("read goal"))?;
        let reason = match desired {
            None => Some("unknown blob"),
            Some(d) if height_from_db(d) != target.from => Some("goal moved (CAS)"),
            _ if target.to <= target.from => Some("to <= from"),
            _ if target.to >= deciding_height => Some("to not below the deciding height"),
            _ if !transition_in(db_tx, target.from, target.to)? => Some("no transition in range"),
            _ => None,
        };
        if let Some(reason) = reason {
            tracing::debug!(
                blob = %target.blob_id, from = target.from, to = target.to,
                "declare skipped: {reason}"
            );
            outcome.skipped += 1;
            continue;
        }
        let n = write
            .execute(params![
                height_to_db(target.to),
                target.blob_id,
                height_to_db(target.from)
            ])
            .map_err(db_err("write goal"))?;
        if n == 1 {
            outcome.applied += 1;
        } else {
            outcome.skipped += 1;
        }
    }
    Ok(outcome)
}

// ---------------------------------------------------------------------------
// ConfirmPlacement apply

/// Per-entry tally of one confirm apply.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ConfirmOutcome {
    pub applied: usize,
    pub skipped: usize,
}

/// Apply a confirm batch. Per entry: the blob exists; `height` equals the
/// current goal (confirming exactly the declared goal); the view at
/// `height` is on record; every fragment's responsible node under that
/// view has an attested inventory row for it. Effect: `placement_height =
/// height` — old holders' obligations lapse. Recency of the attestation is
/// S5's addition. An already-confirmed goal is a no-op.
pub fn apply_confirm(
    db_tx: &rusqlite::Transaction,
    payload: &ConfirmPlacement,
) -> Result<ConfirmOutcome, StorageError> {
    let mut outcome = ConfirmOutcome::default();
    let mut read = db_tx
        .prepare_cached(
            "SELECT desired_placement_height, placement_height FROM data_blocks WHERE id = ?",
        )
        .map_err(db_err("prepare pair read"))?;
    let mut fragments = db_tx
        .prepare_cached(
            "SELECT local_index, fragment_hash FROM fragment_hashes WHERE data_block_id = ?",
        )
        .map_err(db_err("prepare fragment layout read"))?;
    let mut attested = db_tx
        .prepare_cached(
            "SELECT EXISTS (SELECT 1 FROM fragment_inventory
                            WHERE fragment_hash = ? AND node_id = ?)",
        )
        .map_err(db_err("prepare attestation probe"))?;
    let mut write = db_tx
        .prepare_cached(
            "UPDATE data_blocks SET placement_height = ?
             WHERE id = ? AND desired_placement_height = ?",
        )
        .map_err(db_err("prepare confirm write"))?;

    for c in &payload.confirmations {
        let pair: Option<(i64, Option<i64>)> = read
            .query_row(params![c.blob_id], |row| Ok((row.get(0)?, row.get(1)?)))
            .optional()
            .map_err(db_err("read pair"))?;
        let reason = match pair {
            None => Some("unknown blob"),
            Some((d, _)) if height_from_db(d) != c.height => Some("not the declared goal"),
            Some((_, Some(p))) if height_from_db(p) == c.height => Some("already confirmed"),
            _ => None,
        };
        let reason = match reason {
            Some(r) => Some(r),
            None => match snapshot_at(db_tx, c.height)? {
                None => Some("view at goal not on record"),
                Some(snapshot) => {
                    let assignment = snapshot.assignment(&c.blob_id);
                    let mut complete = true;
                    let rows = fragments
                        .query_map(params![c.blob_id], |row| {
                            Ok((row.get::<_, u32>(0)?, row.get::<_, Vec<u8>>(1)?))
                        })
                        .map_err(db_err("read fragment layout"))?;
                    for row in rows {
                        let (local_index, hash) = row.map_err(db_err("read fragment row"))?;
                        let Some(node) = assignment.get(local_index as usize) else {
                            complete = false;
                            break;
                        };
                        let has: bool = attested
                            .query_row(params![hash, node], |row| row.get(0))
                            .map_err(db_err("probe attestation"))?;
                        if !has {
                            complete = false;
                            break;
                        }
                    }
                    if complete {
                        None
                    } else {
                        Some("evidence incomplete")
                    }
                }
            },
        };
        if let Some(reason) = reason {
            tracing::debug!(blob = %c.blob_id, height = c.height, "confirm skipped: {reason}");
            outcome.skipped += 1;
            continue;
        }
        let n = write
            .execute(params![
                height_to_db(c.height),
                c.blob_id,
                height_to_db(c.height)
            ])
            .map_err(db_err("write confirm"))?;
        if n == 1 {
            outcome.applied += 1;
        } else {
            outcome.skipped += 1;
        }
    }
    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::BlobId;
    use std::str::FromStr;

    /// The S1 schema subset these functions touch (chain step 0002 shape).
    fn test_conn() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE data_blocks (
                id TEXT PRIMARY KEY, modified_at TEXT, file_hash BLOB,
                fragment_count INTEGER, added_bytes INTEGER,
                placement_height INTEGER, file_size INTEGER,
                desired_placement_height INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE fragment_hashes (
                data_block_id TEXT, chunk_number INTEGER, local_index INTEGER,
                fragment_id TEXT, fragment_hash BLOB, chunk_type INTEGER,
                stored_locally INTEGER
            );
            CREATE TABLE fragment_inventory (
                fragment_hash BLOB NOT NULL, node_id INTEGER NOT NULL,
                self_verified_height INTEGER,
                PRIMARY KEY (fragment_hash, node_id)
            );
            CREATE TABLE storage_view_transitions (
                height INTEGER PRIMARY KEY, snapshot BLOB NOT NULL
            );",
        )
        .unwrap();
        conn
    }

    fn blob(n: u8) -> BlobId {
        BlobId::from_str(&format!("01890a5d-ac96-774b-b9aa-9f8b24f0c9{n:02x}")).unwrap()
    }

    fn insert_blob(conn: &rusqlite::Connection, id: &BlobId, desired: u64, placed: Option<u64>) {
        conn.execute(
            "INSERT INTO data_blocks (id, file_hash, fragment_count, added_bytes, placement_height, file_size, desired_placement_height)
             VALUES (?, X'00', 3, 0, ?, 10, ?)",
            params![id, placed.map(height_to_db), height_to_db(desired)],
        )
        .unwrap();
        for i in 0..3u32 {
            conn.execute(
                "INSERT INTO fragment_hashes VALUES (?, 0, ?, 'f', ?, 0, 0)",
                params![id, i, vec![i as u8; 32]],
            )
            .unwrap();
        }
    }

    fn view(members: &[i32]) -> ViewSnapshot {
        ViewSnapshot {
            members: members.to_vec(),
            weights: members.iter().map(|m| (*m, 1)).collect(),
            metrics: Vec::new(),
        }
    }

    fn goal(conn: &rusqlite::Connection, id: &BlobId) -> (u64, Option<u64>) {
        conn.query_row(
            "SELECT desired_placement_height, placement_height FROM data_blocks WHERE id = ?",
            params![id],
            |row| {
                Ok((
                    height_from_db(row.get(0)?),
                    row.get::<_, Option<i64>>(1)?.map(height_from_db),
                ))
            },
        )
        .unwrap()
    }

    fn declare(id: &BlobId, from: u64, to: u64) -> DeclarePlacementTarget {
        DeclarePlacementTarget {
            targets: vec![PlacementTarget {
                blob_id: id.clone(),
                from,
                to,
            }],
        }
    }

    fn confirm(id: &BlobId, height: u64) -> ConfirmPlacement {
        ConfirmPlacement {
            confirmations: vec![PlacementConfirmation {
                blob_id: id.clone(),
                height,
            }],
        }
    }

    // Should: round-trip the canonical bytes and order members, weights and
    // metrics by node id regardless of the view's own order.
    // Impact: every node must produce byte-identical snapshot rows — the
    // record is replicated, divergence-checked state.
    #[test]
    fn snapshot_is_canonical_and_round_trips() {
        let mut metrics = Vec::new();
        for n in [3, 1, 2] {
            metrics.push(MetricsRow {
                node_id: n,
                trust_factor: 1.0,
                availability_score: 0.5,
                throughput_score: 0.5,
                latency_score: 0.5,
                stability_score: 0.5,
                storage_multiplier: 1.0,
            });
        }
        let mk = |order: &[i32]| StorageView {
            height: 7,
            members: order
                .iter()
                .map(|n| hopnet_comms::PeerRef {
                    node_id: *n,
                    pubkey: [0; 32],
                })
                .collect(),
            tiers: Default::default(),
            weights: [(1, 4), (2, 8), (3, 16), (9, 2)].into_iter().collect(),
            watermark: 1,
            online: vec![],
            metrics: metrics.clone(),
        };
        let a = ViewSnapshot::from(&mk(&[3, 1, 2]));
        let b = ViewSnapshot::from(&mk(&[1, 2, 3]));
        assert_eq!(a, b);
        assert_eq!(a.members, vec![1, 2, 3]);
        assert!(!a.weights.contains_key(&9), "non-members carry no weight");
        assert_eq!(ViewSnapshot::decode(&a.encode()).unwrap(), a);
    }

    // Should: write the first row unconditionally, skip an identical view,
    // and append when the view changes.
    #[test]
    fn record_transition_appends_only_on_change() {
        let mut conn = test_conn();
        let tx = conn.transaction().unwrap();
        assert!(record_transition(&tx, 5, &view(&[1, 2])).unwrap());
        assert!(!record_transition(&tx, 6, &view(&[1, 2])).unwrap());
        assert!(record_transition(&tx, 9, &view(&[1, 2, 3])).unwrap());
        assert_eq!(latest_transition_height(&tx).unwrap(), Some(9));
        assert!(transition_in(&tx, 5, 9).unwrap());
        assert!(!transition_in(&tx, 9, 20).unwrap());
        assert_eq!(snapshot_at(&tx, 4).unwrap(), None);
        assert_eq!(snapshot_at(&tx, 7).unwrap(), Some(view(&[1, 2])));
        assert_eq!(snapshot_at(&tx, 9).unwrap(), Some(view(&[1, 2, 3])));
    }

    // Should: move the goal forward when the CAS matches, a transition lies
    // in range, and `to` is a decided height.
    // Should not: touch the goal on a stale `from`, a non-advancing `to`, a
    // `to` at or above the deciding height, or a range with no transition —
    // and never fail the batch over them.
    #[test]
    fn declare_validates_each_entry_and_skips() {
        let mut conn = test_conn();
        let tx = conn.transaction().unwrap();
        let b = blob(1);
        insert_blob(&tx, &b, 3, None);
        record_transition(&tx, 5, &view(&[1, 2])).unwrap();

        let ok = apply_declare(&tx, &declare(&b, 3, 6), 10).unwrap();
        assert_eq!(
            ok,
            DeclareOutcome {
                applied: 1,
                skipped: 0
            }
        );
        assert_eq!(goal(&tx, &b).0, 6);

        let stale = apply_declare(&tx, &declare(&b, 3, 8), 10).unwrap(); // CAS
        let backwards = apply_declare(&tx, &declare(&b, 6, 6), 10).unwrap();
        let tip = apply_declare(&tx, &declare(&b, 6, 10), 10).unwrap();
        let quiet = apply_declare(&tx, &declare(&b, 6, 8), 10).unwrap(); // no transition in (6, 8]
        let unknown = apply_declare(&tx, &declare(&blob(2), 0, 6), 10).unwrap();
        for o in [stale, backwards, tip, quiet, unknown] {
            assert_eq!(
                o,
                DeclareOutcome {
                    applied: 0,
                    skipped: 1
                }
            );
        }
        assert_eq!(goal(&tx, &b).0, 6);
    }

    // Should: let a later declaration supersede an in-flight goal under the
    // same rules (a supersede is just another declare).
    #[test]
    fn declare_supersedes_in_flight_goal() {
        let mut conn = test_conn();
        let tx = conn.transaction().unwrap();
        let b = blob(1);
        insert_blob(&tx, &b, 0, None);
        record_transition(&tx, 5, &view(&[1, 2])).unwrap();
        record_transition(&tx, 8, &view(&[1])).unwrap();
        assert_eq!(
            apply_declare(&tx, &declare(&b, 0, 5), 20).unwrap().applied,
            1
        );
        assert_eq!(
            apply_declare(&tx, &declare(&b, 5, 8), 20).unwrap().applied,
            1
        );
        assert_eq!(goal(&tx, &b), (8, None));
    }

    // Should: stamp placement_height only when every class's responsible
    // node under the view at the goal has an attestation row.
    // Should not: confirm a height other than the goal, a goal whose view is
    // not on record, or a blob with a missing attestation.
    // Impact: ConfirmPlacement is the sole writer of placement_height after
    // insert; lapsing obligations on incomplete evidence would let eviction
    // reclaim the only copies.
    #[test]
    fn confirm_requires_complete_attested_evidence() {
        let mut conn = test_conn();
        let tx = conn.transaction().unwrap();
        let b = blob(1);
        insert_blob(&tx, &b, 6, None);

        // Goal's view not on record yet.
        assert_eq!(apply_confirm(&tx, &confirm(&b, 6)).unwrap().skipped, 1);

        record_transition(&tx, 5, &view(&[1, 2, 3])).unwrap();
        let assignment = snapshot_at(&tx, 6).unwrap().unwrap().assignment(&b);

        // Wrong height.
        assert_eq!(apply_confirm(&tx, &confirm(&b, 5)).unwrap().skipped, 1);
        // No attestations at all.
        assert_eq!(apply_confirm(&tx, &confirm(&b, 6)).unwrap().skipped, 1);

        // Attest two of three classes on their responsible nodes.
        for i in 0..2u32 {
            tx.execute(
                "INSERT INTO fragment_inventory (fragment_hash, node_id) VALUES (?, ?)",
                params![vec![i as u8; 32], assignment[i as usize]],
            )
            .unwrap();
        }
        assert_eq!(apply_confirm(&tx, &confirm(&b, 6)).unwrap().skipped, 1);
        assert_eq!(goal(&tx, &b), (6, None));

        // Third class attested on the WRONG node: still incomplete.
        let wrong = (1..=3).find(|n| *n != assignment[2]).unwrap();
        tx.execute(
            "INSERT INTO fragment_inventory (fragment_hash, node_id) VALUES (?, ?)",
            params![vec![2u8; 32], wrong],
        )
        .unwrap();
        assert_eq!(apply_confirm(&tx, &confirm(&b, 6)).unwrap().skipped, 1);

        tx.execute(
            "INSERT INTO fragment_inventory (fragment_hash, node_id) VALUES (?, ?)",
            params![vec![2u8; 32], assignment[2]],
        )
        .unwrap();
        assert_eq!(
            apply_confirm(&tx, &confirm(&b, 6)).unwrap(),
            ConfirmOutcome {
                applied: 1,
                skipped: 0
            }
        );
        assert_eq!(goal(&tx, &b), (6, Some(6)));

        // Re-confirming the confirmed goal is a no-op.
        assert_eq!(apply_confirm(&tx, &confirm(&b, 6)).unwrap().skipped, 1);
    }

    // Should: reuse the engine's placement recipe — the class map from a
    // snapshot equals assign_fragment_classes over the same members.
    // Impact: a confirm validated against a different assignment than the
    // one bytes were pushed under would never find its evidence.
    #[test]
    fn assignment_matches_engine_recipe() {
        let v = view(&[1, 2, 3, 4]);
        let b = blob(7);
        let seed = placement::placement_seed(&b);
        let expected = placement::assign_fragment_classes(
            &seed,
            &v.members,
            &v.weights.iter().map(|(k, w)| (*k, *w)).collect(),
            crate::rs::TOTAL_FRAGMENTS_PER_CHUNK as u32,
        );
        assert_eq!(v.assignment(&b), expected);
        assert_eq!(expected.len(), crate::rs::TOTAL_FRAGMENTS_PER_CHUNK);
    }
}
