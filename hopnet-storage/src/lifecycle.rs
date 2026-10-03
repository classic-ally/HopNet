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

use crate::placement;
use crate::store::db_err;
use crate::traits::StorageView;
use crate::types::BlobId;
use crate::StorageError;

/// Consensus function name of the declare transaction.
pub const DECLARE_TX_FN: &str = "declare_placement_target";
/// Consensus function name of the confirm transaction.
pub const CONFIRM_TX_FN: &str = "confirm_placement";

/// Confirmation evidence must have been disk-verified within this many
/// heights of the deciding block (S5); surplus release demands the same
/// recency of another member's copy. Sized for the slowest member, not
/// the typical one: an HDD node's sweep takes ~50 minutes, runs on a
/// 30-minute cron, and must survive one failed sweep, at drain traffic
/// (~45 heights a minute, the 2026-09-27 rehearsal). 8192 heights is
/// ~3 hours at that rate and ~10 at the ~14 a minute of a quiet mesh.
/// The first value, 1024, aged a healthy sweep's rows out in 23 minutes
/// under drain and left the 2026-10-02 mesh with no confirmable blob.
/// A consensus rule (`evidence_complete` runs in the confirm apply): it
/// changes only at an epoch crossing.
pub const ATTESTATION_RECENCY_HEIGHTS: u64 = 8192;

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
/// consume, nothing else. Members and their quantized weights move bytes;
/// liveness sets, watermark and tiers do not, and raw metric scores never
/// enter — the 16-level weight bucket is the finest thing selection reads,
/// so a metrics submission that stays inside its bucket leaves the
/// snapshot's bytes, and therefore the transition record, untouched
/// (the model's view: a member set and a constant weight map).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ViewSnapshot {
    /// Member node ids, ascending.
    pub members: Vec<i32>,
    /// Quantized placement weight per member.
    pub weights: BTreeMap<i32, u64>,
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
        Self { members, weights }
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

    /// Class → responsible node for one blob under this view — THE placement
    /// recipe (every consumer, engine included, reaches it through here):
    /// seeded selection over the members by weight, then balanced capped
    /// rendezvous. Empty when the view has no members.
    pub fn assignment(&self, blob_id: &BlobId) -> Vec<i32> {
        let seed = placement::placement_seed(blob_id);
        let weights: HashMap<i32, u64> = self.weights.iter().map(|(k, v)| (*k, *v)).collect();
        let selected: Vec<i32> =
            placement::select_nodes_for_blob(self.members.clone(), &weights, &seed);
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

/// The epochs protecting one blob's copies (RFC-STORAGE-003 S2): the
/// confirmed epoch's assignment and every declared-but-unconfirmed epoch's
/// — the distinct views on record in `(placement_height, desired]` plus the
/// view in force at `desired` (the current target, always in flight while
/// the pair differs). `Ok(None)` when the record cannot answer: a confirmed
/// height below the record's first row, or a target no row reaches. The
/// caller treats that as protected — never evict on an unanswerable
/// question.
pub fn epochs_for_blob(
    conn: &rusqlite::Connection,
    blob_id: &BlobId,
    placement_height: Option<u64>,
    desired: u64,
) -> Result<Option<crate::protection::ProtectionEpochs>, StorageError> {
    let Some(confirmed_height) = placement_height else {
        // Never confirmed: the predicate protects everything; no reads.
        return Ok(Some(crate::protection::ProtectionEpochs::default()));
    };
    let Some(confirmed) = snapshot_at(conn, confirmed_height)? else {
        return Ok(None);
    };
    let mut in_flight = Vec::new();
    if desired != confirmed_height {
        let Some(target) = snapshot_at(conn, desired)? else {
            return Ok(None);
        };
        let mut stmt = conn
            .prepare_cached(
                "SELECT DISTINCT snapshot FROM storage_view_transitions
                 WHERE height > ? AND height <= ? ORDER BY height",
            )
            .map_err(db_err("prepare in-flight snapshots"))?;
        let rows = stmt
            .query_map(
                params![height_to_db(confirmed_height), height_to_db(desired)],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .map_err(db_err("read in-flight snapshots"))?;
        let mut seen: Vec<ViewSnapshot> = Vec::new();
        for row in rows {
            let snapshot = ViewSnapshot::decode(&row.map_err(db_err("read in-flight row"))?)?;
            if !seen.contains(&snapshot) {
                seen.push(snapshot);
            }
        }
        if !seen.contains(&target) {
            seen.push(target);
        }
        in_flight = seen.iter().map(|s| s.assignment(blob_id)).collect();
    }
    Ok(Some(crate::protection::ProtectionEpochs {
        confirmed: Some(confirmed.assignment(blob_id)),
        in_flight,
    }))
}

/// `(placement_height, desired_placement_height)` for a batch of blobs;
/// unknown ids are absent from the map.
pub fn placement_pairs(
    conn: &rusqlite::Connection,
    blob_ids: &[BlobId],
) -> Result<HashMap<BlobId, (Option<u64>, u64)>, StorageError> {
    let mut out = HashMap::new();
    for chunk in blob_ids.chunks(500) {
        let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        let query = format!(
            "SELECT id, placement_height, desired_placement_height
             FROM data_blocks WHERE id IN ({placeholders})"
        );
        let mut stmt = conn.prepare(&query).map_err(db_err("prepare pairs"))?;
        let rows = stmt
            .query_map(rusqlite::params_from_iter(chunk.iter()), |row| {
                Ok((
                    row.get::<_, BlobId>(0)?,
                    row.get::<_, Option<i64>>(1)?.map(height_from_db),
                    height_from_db(row.get::<_, i64>(2)?),
                ))
            })
            .map_err(db_err("read pairs"))?;
        for row in rows {
            let (id, placed, desired) = row.map_err(db_err("read pair row"))?;
            out.insert(id, (placed, desired));
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// DeclarePlacementTarget apply

/// Per-entry tally of one declare apply.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DeclareOutcome {
    pub applied: usize,
    pub skipped: usize,
    /// The blobs whose goal moved — pull duties derive from these at
    /// apply (the host kicks its reconciler for each under execute).
    pub applied_ids: Vec<BlobId>,
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
            outcome.applied_ids.push(target.blob_id.clone());
        } else {
            outcome.skipped += 1;
        }
    }
    Ok(outcome)
}

// ---------------------------------------------------------------------------
// Reads the reconciler consumes (S3)

/// The blob's goal for the pull side: its pair and the assignment under
/// the view in force at `desired`. `None` for an unknown blob or a goal
/// the record does not reach.
pub fn pull_target(
    conn: &rusqlite::Connection,
    blob_id: &BlobId,
) -> Result<Option<crate::traits::PullTarget>, StorageError> {
    let pair: Option<(Option<i64>, i64)> = conn
        .query_row(
            "SELECT placement_height, desired_placement_height FROM data_blocks WHERE id = ?",
            params![blob_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(db_err("read pull target pair"))?;
    let Some((placed, desired)) = pair else {
        return Ok(None);
    };
    let desired = height_from_db(desired);
    let Some(snapshot) = snapshot_at(conn, desired)? else {
        return Ok(None);
    };
    Ok(Some(crate::traits::PullTarget {
        placement_height: placed.map(height_from_db),
        desired,
        assignment: snapshot.assignment(blob_id),
    }))
}

/// In-flight blobs — goal not yet confirmed — oldest goal first, bounded.
/// The FIRST page only: one transition gives every in-flight blob the
/// same goal height, so repeated calls return the same lowest ids for as
/// long as those blobs wait on confirmation. Anything that must reach the
/// whole set walks it with [`in_flight_page_after`] (the pull planner);
/// this stays for the operator route's compatibility paths and the pane.
pub fn in_flight_blobs(
    conn: &rusqlite::Connection,
    limit: usize,
) -> Result<Vec<BlobId>, StorageError> {
    let mut stmt = conn
        .prepare_cached(
            "SELECT id FROM data_blocks
             WHERE placement_height IS NULL OR placement_height != desired_placement_height
             ORDER BY desired_placement_height ASC, id ASC
             LIMIT ?",
        )
        .map_err(db_err("prepare in-flight read"))?;
    let ids = stmt
        .query_map(params![limit as i64], |row| row.get(0))
        .map_err(db_err("read in-flight blobs"))?
        .collect::<Result<Vec<BlobId>, _>>()
        .map_err(db_err("read in-flight row"))?;
    Ok(ids)
}

const IN_FLIGHT_PAGE_SQL: &str = "SELECT desired_placement_height, id FROM data_blocks
     WHERE (placement_height IS NULL OR placement_height != desired_placement_height)
       AND id > ?
     ORDER BY id ASC
     LIMIT ?";

/// One page of the in-flight set in primary-key (`id`) order, strictly
/// after `after` (a keyset cursor; `None` starts from the beginning).
/// Returns each blob with its goal height. An empty or short page means
/// the walk reached the end. Paged by the PK alone so every page is an
/// index range scan: ordering by `(desired_placement_height, id)` has no
/// covering index and sorted the whole same-height set (a temp B-tree)
/// for every page. Callers that need an order (the pull planner's risk
/// order) sort what they collect. Each call is its own short read, so a
/// walk over the whole set never holds one long reader (which would pin
/// the WAL).
pub fn in_flight_page_after(
    conn: &rusqlite::Connection,
    after: Option<&BlobId>,
    limit: usize,
) -> Result<Vec<(u64, BlobId)>, StorageError> {
    let map_row = |row: &rusqlite::Row| -> rusqlite::Result<(u64, BlobId)> {
        Ok((height_from_db(row.get(0)?), row.get(1)?))
    };
    // Without a cursor, start below every real key: ids are non-empty text.
    let id: &dyn rusqlite::ToSql = match after {
        Some(id) => id,
        None => &"",
    };
    let mut stmt = conn
        .prepare_cached(IN_FLIGHT_PAGE_SQL)
        .map_err(db_err("prepare in-flight page"))?;
    let rows = stmt
        .query_map(params![id, limit as i64], map_row)
        .map_err(db_err("read in-flight page"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(db_err("read in-flight page row"))?;
    Ok(rows)
}

/// The staleness pass's page (S4): blobs whose goal predates the latest
/// transition `t`, oldest goal first, as declare targets to `t`. One
/// indexed predicate; the set consumes itself as pages apply.
pub fn stale_page(
    conn: &rusqlite::Connection,
    t: u64,
    limit: usize,
) -> Result<Vec<PlacementTarget>, StorageError> {
    let mut stmt = conn
        .prepare_cached(
            "SELECT id, desired_placement_height FROM data_blocks
             WHERE desired_placement_height < ?
             ORDER BY desired_placement_height ASC, id ASC
             LIMIT ?",
        )
        .map_err(db_err("prepare stale page"))?;
    let rows = stmt
        .query_map(params![height_to_db(t), limit as i64], |row| {
            Ok(PlacementTarget {
                blob_id: row.get(0)?,
                from: height_from_db(row.get(1)?),
                to: t,
            })
        })
        .map_err(db_err("read stale page"))?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(db_err("read stale row"))
}

/// A random sample of in-flight blobs (S4 fulfillment): recurrence over a
/// draining set makes the sample comprehensive; the order is node-local
/// (a proposal input, never consensus state).
pub fn in_flight_sample(
    conn: &rusqlite::Connection,
    n: usize,
) -> Result<Vec<BlobId>, StorageError> {
    let mut stmt = conn
        .prepare_cached(
            "SELECT id FROM data_blocks
             WHERE placement_height IS NULL OR placement_height != desired_placement_height
             ORDER BY RANDOM()
             LIMIT ?",
        )
        .map_err(db_err("prepare in-flight sample"))?;
    let ids = stmt
        .query_map(params![n as i64], |row| row.get(0))
        .map_err(db_err("read in-flight sample"))?
        .collect::<Result<Vec<BlobId>, _>>()
        .map_err(db_err("read in-flight sample row"))?;
    Ok(ids)
}

/// Whether every fragment's responsible node under `assignment` has an
/// attested inventory row — the evidence ConfirmPlacement validates.
/// Shared by the apply and by the fulfillment read (`confirm_ready`).
fn evidence_complete(
    conn: &rusqlite::Connection,
    blob_id: &BlobId,
    assignment: &[i32],
    tip: u64,
) -> Result<bool, StorageError> {
    // Recency (S5): the row must have been disk-verified within the
    // window of the deciding height, and not be suspect. A row that was
    // never disk-verified (verified_height NULL) is belief, not evidence.
    let floor = tip.saturating_sub(ATTESTATION_RECENCY_HEIGHTS);
    let mut fragments = conn
        .prepare_cached(
            "SELECT local_index, fragment_hash FROM fragment_hashes WHERE data_block_id = ?",
        )
        .map_err(db_err("prepare fragment layout read"))?;
    let mut attested = conn
        .prepare_cached(
            "SELECT EXISTS (SELECT 1 FROM fragment_inventory
                            WHERE fragment_hash = ? AND node_id = ?
                              AND suspect = 0
                              AND verified_height IS NOT NULL
                              AND verified_height >= ?)",
        )
        .map_err(db_err("prepare attestation probe"))?;
    let rows = fragments
        .query_map(params![blob_id], |row| {
            Ok((row.get::<_, u32>(0)?, row.get::<_, Vec<u8>>(1)?))
        })
        .map_err(db_err("read fragment layout"))?;
    let mut any = false;
    for row in rows {
        any = true;
        let (local_index, hash) = row.map_err(db_err("read fragment row"))?;
        let Some(node) = assignment.get(local_index as usize) else {
            return Ok(false);
        };
        let has: bool = attested
            .query_row(params![hash, node, height_to_db(floor)], |row| row.get(0))
            .map_err(db_err("probe attestation"))?;
        if !has {
            return Ok(false);
        }
    }
    Ok(any)
}

/// The fulfillment read: `Some(desired)` when the blob is in flight and
/// its confirmation evidence is complete — a ConfirmPlacement for it would
/// apply. `None` when quiescent, unknown, unanswerable, or still owed.
pub fn confirm_ready(
    conn: &rusqlite::Connection,
    blob_id: &BlobId,
    tip: u64,
) -> Result<Option<u64>, StorageError> {
    let Some(target) = pull_target(conn, blob_id)? else {
        return Ok(None);
    };
    if target.placement_height == Some(target.desired) {
        return Ok(None);
    }
    Ok(evidence_complete(conn, blob_id, &target.assignment, tip)?.then_some(target.desired))
}

/// The fulfillment pass's read (S4): a random sample of `n` in-flight
/// blobs, reduced to the confirmation entries whose evidence is complete
/// right now, plus how many were sampled (the density the adaptive sample
/// steers by). One snapshot decode per distinct goal height — after a
/// transition nearly every in-flight blob shares one goal, so the balloon
/// costs one decode, not thousands.
pub fn ready_confirmations(
    conn: &rusqlite::Connection,
    n: usize,
    tip: u64,
) -> Result<(Vec<PlacementConfirmation>, usize), StorageError> {
    let sample = in_flight_sample(conn, n)?;
    let sampled = sample.len();
    let mut pair = conn
        .prepare_cached(
            "SELECT placement_height, desired_placement_height FROM data_blocks WHERE id = ?",
        )
        .map_err(db_err("prepare pair read"))?;
    let mut snapshots: HashMap<u64, Option<ViewSnapshot>> = HashMap::new();
    let mut ready = Vec::new();
    for blob_id in sample {
        let goal: Option<(Option<i64>, i64)> = pair
            .query_row(params![blob_id], |row| Ok((row.get(0)?, row.get(1)?)))
            .optional()
            .map_err(db_err("read pull target pair"))?;
        let Some((placed, desired)) = goal else {
            continue;
        };
        let desired = height_from_db(desired);
        if placed.map(height_from_db) == Some(desired) {
            continue;
        }
        let snapshot = match snapshots.entry(desired) {
            std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
            std::collections::hash_map::Entry::Vacant(e) => e.insert(snapshot_at(conn, desired)?),
        };
        let Some(snapshot) = snapshot.as_ref() else {
            continue;
        };
        if evidence_complete(conn, &blob_id, &snapshot.assignment(&blob_id), tip)? {
            ready.push(PlacementConfirmation {
                blob_id,
                height: desired,
            });
        }
    }
    Ok((ready, sampled))
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
    deciding_height: u64,
) -> Result<ConfirmOutcome, StorageError> {
    let mut outcome = ConfirmOutcome::default();
    let mut read = db_tx
        .prepare_cached(
            "SELECT desired_placement_height, placement_height FROM data_blocks WHERE id = ?",
        )
        .map_err(db_err("prepare pair read"))?;
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
                    if evidence_complete(
                        db_tx,
                        &c.blob_id,
                        &snapshot.assignment(&c.blob_id),
                        deciding_height,
                    )? {
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
    use crate::placement::MetricsRow;
    use crate::types::BlobId;
    use hopnet_common::Blake3Hash;
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
                verified_height INTEGER, provenance INTEGER,
                suspect INTEGER NOT NULL DEFAULT 0,
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
        }
    }

    /// A host-shaped view: members with their raw metrics rows, weights
    /// derived the way `membership::derive_view` derives them.
    fn host_view(rows: &[MetricsRow]) -> StorageView {
        StorageView {
            height: 7,
            members: rows
                .iter()
                .map(|m| hopnet_comms::PeerRef {
                    node_id: m.node_id,
                    pubkey: [0; 32],
                })
                .collect(),
            tiers: Default::default(),
            weights: rows
                .iter()
                .map(|m| (m.node_id, placement::quantized_weight(m)))
                .collect(),
            watermark: 1,
            online: vec![],
            absence: Default::default(),
            grid_step_secs: 0,
        }
    }

    fn row(node_id: i32, availability: f64) -> MetricsRow {
        MetricsRow {
            node_id,
            trust_factor: 1.0,
            availability_score: availability,
            throughput_score: 0.5,
            latency_score: 0.5,
            stability_score: 0.5,
            storage_multiplier: 1.0,
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

    // Should: round-trip the canonical bytes and order members and weights
    // by node id regardless of the view's own order.
    // Should not: carry anything but members and weights — no metrics row,
    // no liveness, no watermark.
    // Impact: every node must produce byte-identical snapshot rows — the
    // record is replicated, divergence-checked state.
    #[test]
    fn snapshot_is_canonical_and_round_trips() {
        let mk = |order: &[i32]| StorageView {
            height: 7,
            members: order
                .iter()
                .map(|n| hopnet_comms::PeerRef {
                    node_id: *n,
                    pubkey: [0; 32],
                })
                .collect(),
            tiers: [(1, 3600)].into_iter().collect(),
            weights: [(1, 4), (2, 8), (3, 16), (9, 2)].into_iter().collect(),
            watermark: 1,
            online: vec![1],
            absence: Default::default(),
            grid_step_secs: 0,
        };
        let a = ViewSnapshot::from(&mk(&[3, 1, 2]));
        let b = ViewSnapshot::from(&mk(&[1, 2, 3]));
        assert_eq!(a, b);
        assert_eq!(a.members, vec![1, 2, 3]);
        assert!(!a.weights.contains_key(&9), "non-members carry no weight");
        assert_eq!(ViewSnapshot::decode(&a.encode()).unwrap(), a);
        let mut bare = view(&[1, 2, 3]);
        bare.weights = [(1, 4), (2, 8), (3, 16)].into_iter().collect();
        assert_eq!(
            a.encode(),
            bare.encode(),
            "the bytes are a function of members and weights alone"
        );
    }

    // Should: leave the transition record untouched when a metrics
    // submission moves a member's raw scores inside its weight bucket.
    // Should: append a transition when the scores cross a bucket edge.
    // Impact: the 500-blob cutover rehearsal (2026-09-27) never converged
    // because every submit_metrics commit was a transition that re-declared
    // every blob; the snapshot must key on what moves bytes, quantized.
    #[test]
    fn metrics_only_change_records_no_transition() {
        let mut conn = test_conn();
        let tx = conn.transaction().unwrap();
        let before = host_view(&[row(1, 0.50), row(2, 0.50), row(3, 0.50)]);
        let noisier = host_view(&[row(1, 0.55), row(2, 0.50), row(3, 0.50)]);
        let bucket_up = host_view(&[row(1, 0.60), row(2, 0.50), row(3, 0.50)]);
        assert_eq!(before.weights[&1], 8);
        assert_eq!(noisier.weights[&1], 8, "0.55 stays in bucket 8");
        assert_eq!(bucket_up.weights[&1], 9, "0.60 crosses into bucket 9");

        assert!(record_transition(&tx, 5, &ViewSnapshot::from(&before)).unwrap());
        assert!(
            !record_transition(&tx, 6, &ViewSnapshot::from(&noisier)).unwrap(),
            "same buckets, same bytes: no transition"
        );
        assert_eq!(latest_transition_height(&tx).unwrap(), Some(5));
        assert!(
            record_transition(&tx, 9, &ViewSnapshot::from(&bucket_up)).unwrap(),
            "a weight bucket moved: the view changed"
        );
        assert_eq!(latest_transition_height(&tx).unwrap(), Some(9));
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
                skipped: 0,
                applied_ids: vec![b.clone()],
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
                    skipped: 1,
                    applied_ids: vec![],
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
        assert_eq!(apply_confirm(&tx, &confirm(&b, 6), 10).unwrap().skipped, 1);

        record_transition(&tx, 5, &view(&[1, 2, 3])).unwrap();
        let assignment = snapshot_at(&tx, 6).unwrap().unwrap().assignment(&b);

        // Wrong height.
        assert_eq!(apply_confirm(&tx, &confirm(&b, 5), 10).unwrap().skipped, 1);
        // No attestations at all.
        assert_eq!(apply_confirm(&tx, &confirm(&b, 6), 10).unwrap().skipped, 1);

        // Attest two of three classes on their responsible nodes.
        for i in 0..2u32 {
            tx.execute(
                "INSERT INTO fragment_inventory (fragment_hash, node_id, verified_height) VALUES (?, ?, 6)",
                params![vec![i as u8; 32], assignment[i as usize]],
            )
            .unwrap();
        }
        assert_eq!(apply_confirm(&tx, &confirm(&b, 6), 10).unwrap().skipped, 1);
        assert_eq!(goal(&tx, &b), (6, None));

        // Third class attested on the WRONG node: still incomplete.
        let wrong = (1..=3).find(|n| *n != assignment[2]).unwrap();
        tx.execute(
            "INSERT INTO fragment_inventory (fragment_hash, node_id, verified_height) VALUES (?, ?, 6)",
            params![vec![2u8; 32], wrong],
        )
        .unwrap();
        assert_eq!(apply_confirm(&tx, &confirm(&b, 6), 10).unwrap().skipped, 1);

        tx.execute(
            "INSERT INTO fragment_inventory (fragment_hash, node_id, verified_height) VALUES (?, ?, 6)",
            params![vec![2u8; 32], assignment[2]],
        )
        .unwrap();
        assert_eq!(
            apply_confirm(&tx, &confirm(&b, 6), 10).unwrap(),
            ConfirmOutcome {
                applied: 1,
                skipped: 0,
            }
        );
        assert_eq!(goal(&tx, &b), (6, Some(6)));

        // Re-confirming the confirmed goal is a no-op.
        assert_eq!(apply_confirm(&tx, &confirm(&b, 6), 10).unwrap().skipped, 1);
    }

    // Should: return, from a sample of the in-flight set, exactly the blobs
    // whose evidence is complete at the tip — as entries a ConfirmPlacement
    // would apply — and report how many were sampled.
    // Should not: sample a quiescent blob, or list one with a stale or
    // missing attestation.
    // Impact: this is the fulfillment pass's read; the ratio ready/sampled
    // steers the adaptive sample, so both numbers must be honest.
    #[test]
    fn ready_confirmations_lists_only_complete_evidence() {
        let mut conn = test_conn();
        let tx = conn.transaction().unwrap();
        record_transition(&tx, 5, &view(&[1, 2, 3])).unwrap();

        // a: fully attested at the goal's assignment. b: one class short.
        // c: quiescent (confirmed at its goal) — never sampled.
        let a = blob(1);
        let b = blob(2);
        let c = blob(3);
        insert_blob(&tx, &a, 6, None);
        insert_blob(&tx, &c, 6, Some(6));
        tx.execute(
            "INSERT INTO data_blocks (id, file_hash, fragment_count, added_bytes, placement_height, file_size, desired_placement_height)
             VALUES (?, X'00', 3, 0, NULL, 10, ?)",
            params![b, height_to_db(6)],
        )
        .unwrap();
        for i in 0..3u32 {
            tx.execute(
                "INSERT INTO fragment_hashes VALUES (?, 0, ?, 'f', ?, 0, 0)",
                params![b, i, vec![0x10 + i as u8; 32]],
            )
            .unwrap();
        }
        let snapshot = snapshot_at(&tx, 6).unwrap().unwrap();
        let (assign_a, assign_b) = (snapshot.assignment(&a), snapshot.assignment(&b));
        for i in 0..3u32 {
            tx.execute(
                "INSERT INTO fragment_inventory (fragment_hash, node_id, verified_height) VALUES (?, ?, 6)",
                params![vec![i as u8; 32], assign_a[i as usize]],
            )
            .unwrap();
        }
        for i in 0..2u32 {
            tx.execute(
                "INSERT INTO fragment_inventory (fragment_hash, node_id, verified_height) VALUES (?, ?, 6)",
                params![vec![0x10 + i as u8; 32], assign_b[i as usize]],
            )
            .unwrap();
        }

        let (ready, sampled) = ready_confirmations(&tx, 10, 10).unwrap();
        assert_eq!(sampled, 2, "the quiescent blob is not in flight");
        assert_eq!(
            ready,
            vec![PlacementConfirmation {
                blob_id: a.clone(),
                height: 6,
            }]
        );

        // The entries apply as-is.
        assert_eq!(
            apply_confirm(
                &tx,
                &ConfirmPlacement {
                    confirmations: ready
                },
                10
            )
            .unwrap()
            .applied,
            1
        );
        assert_eq!(goal(&tx, &a), (6, Some(6)));

        // Evidence past the recency window is not evidence.
        let far = 6 + ATTESTATION_RECENCY_HEIGHTS + 1;
        let (ready, sampled) = ready_confirmations(&tx, 10, far).unwrap();
        assert_eq!((ready.len(), sampled), (0, 1));
    }

    // Should: resolve a blob's protection epochs from the record — the
    // confirmed view, every distinct in-flight view in (confirmed, desired]
    // and the view at desired — and answer None when the record cannot.
    // Should not: read anything for a never-confirmed blob (protected
    // outright) or list in-flight epochs for a quiescent one.
    #[test]
    fn epochs_for_blob_reads_confirmed_and_in_flight_views() {
        let mut conn = test_conn();
        let tx = conn.transaction().unwrap();
        let b = blob(1);
        record_transition(&tx, 5, &view(&[1, 2])).unwrap();
        record_transition(&tx, 8, &view(&[1, 2, 3])).unwrap();
        record_transition(&tx, 12, &view(&[1, 3])).unwrap();

        // Never confirmed: everything protected, no epochs listed.
        let birth = epochs_for_blob(&tx, &b, None, 20).unwrap().unwrap();
        assert_eq!(birth, crate::protection::ProtectionEpochs::default());

        // Quiescent at 6: the view in force is the row at 5.
        let quiet = epochs_for_blob(&tx, &b, Some(6), 6).unwrap().unwrap();
        assert_eq!(quiet.confirmed, Some(view(&[1, 2]).assignment(&b)));
        assert!(quiet.in_flight.is_empty());

        // In flight 6 → 15: rows 8 and 12 are in range; the target view at
        // 15 is row 12 (deduplicated).
        let moving = epochs_for_blob(&tx, &b, Some(6), 15).unwrap().unwrap();
        assert_eq!(
            moving.in_flight,
            vec![
                view(&[1, 2, 3]).assignment(&b),
                view(&[1, 3]).assignment(&b)
            ]
        );

        // Confirmed below the record's first row: unanswerable.
        assert_eq!(epochs_for_blob(&tx, &b, Some(3), 15).unwrap(), None);
    }

    // Should: list in-flight blobs oldest goal first and stop listing a blob
    // once confirmed; resolve a pull target only when the record reaches
    // the goal; report confirm-readiness exactly when every responsible has
    // attested and the blob is still in flight.
    // Impact: these three reads ARE the reconciler's work-list, its duty
    // derivation, and the fulfillment floor.
    #[test]
    fn reconciler_reads_follow_the_pair_and_the_record() {
        let mut conn = test_conn();
        let tx = conn.transaction().unwrap();
        let a = blob(1);
        let b = blob(2);
        let c = blob(3);
        insert_blob(&tx, &a, 9, None); // in flight, newest goal
        insert_blob(&tx, &b, 6, Some(6)); // quiescent
        insert_blob(&tx, &c, 7, Some(2)); // in flight, older goal

        assert_eq!(
            in_flight_blobs(&tx, 10).unwrap(),
            vec![c.clone(), a.clone()]
        );
        assert_eq!(in_flight_blobs(&tx, 1).unwrap(), vec![c.clone()]);

        // No record yet: no target, not ready.
        assert_eq!(pull_target(&tx, &a).unwrap(), None);
        assert_eq!(confirm_ready(&tx, &a, 10).unwrap(), None);

        record_transition(&tx, 5, &view(&[1, 2, 3])).unwrap();
        let target = pull_target(&tx, &a).unwrap().unwrap();
        assert_eq!((target.placement_height, target.desired), (None, 9));
        assert_eq!(target.assignment, view(&[1, 2, 3]).assignment(&a));
        assert_eq!(pull_target(&tx, &blob(9)).unwrap(), None, "unknown blob");

        // Quiescent blob: never confirm-ready.
        assert_eq!(confirm_ready(&tx, &b, 10).unwrap(), None);

        // Attest every class of `a` on its responsible: ready at 9.
        for (i, node) in target.assignment.iter().take(3).enumerate() {
            tx.execute(
                "INSERT INTO fragment_inventory (fragment_hash, node_id, verified_height) VALUES (?, ?, 6)",
                params![vec![i as u8; 32], node],
            )
            .unwrap();
        }
        assert_eq!(confirm_ready(&tx, &a, 10).unwrap(), Some(9));
        assert_eq!(apply_confirm(&tx, &confirm(&a, 9), 10).unwrap().applied, 1);
        assert_eq!(
            confirm_ready(&tx, &a, 10).unwrap(),
            None,
            "confirmed: quiescent"
        );
        assert_eq!(in_flight_blobs(&tx, 10).unwrap(), vec![c.clone()]);
    }

    // Impact: production 2026-10-03 — every in-flight blob shared one goal
    // height, so the tick's LIMIT-64 kick returned the same lowest ids
    // forever and the other ~68.8k blobs were never pulled.
    // Should: return disjoint pages that together cover every in-flight
    // blob exactly once, in id order, with each blob's goal height.
    // Should not: return a quiescent blob, or anything after the last page.
    #[test]
    fn in_flight_page_after_walks_every_blob_once() {
        let mut conn = test_conn();
        let tx = conn.transaction().unwrap();
        for n in 1..=7u8 {
            insert_blob(&tx, &blob(n), 9, None);
        }
        insert_blob(&tx, &blob(8), 4, Some(2)); // older goal, in flight
        insert_blob(&tx, &blob(9), 9, Some(9)); // quiescent

        let mut seen = Vec::new();
        let mut cursor: Option<BlobId> = None;
        loop {
            let page = in_flight_page_after(&tx, cursor.as_ref(), 3).unwrap();
            if page.is_empty() {
                break;
            }
            cursor = page.last().map(|(_, id)| id.clone());
            seen.extend(page);
        }

        let mut expected: Vec<(u64, BlobId)> = (1..=7u8).map(|n| (9, blob(n))).collect();
        expected.push((4, blob(8)));
        assert_eq!(seen, expected);
    }

    // Impact: ordering pages by (goal, id) had no covering index, so every
    // 512-blob page sorted the whole same-height in-flight set (~68.8k
    // rows in production) in a temp B-tree: the walk was quadratic.
    // Should: page by the primary-key index.
    // Should not: build a temp B-tree to order a page.
    #[test]
    fn in_flight_page_uses_the_primary_key_without_a_sort() {
        let conn = test_conn();
        let plan: Vec<String> = conn
            .prepare(&format!("EXPLAIN QUERY PLAN {IN_FLIGHT_PAGE_SQL}"))
            .unwrap()
            .query_map(params!["", 10], |r| r.get::<_, String>(3))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        let plan = plan.join(" | ");
        assert!(!plan.contains("TEMP B-TREE"), "{plan}");
        assert!(plan.contains("sqlite_autoindex_data_blocks"), "{plan}");
    }

    // Should: page the blobs whose goal predates T, oldest goal first, as
    // declares to T; sample in-flight blobs without ever returning a
    // quiescent one.
    // Impact: the staleness page is the propose hook's whole payload —
    // a wrong `from` fails the CAS, a wrong `to` fails the range check.
    #[test]
    fn stale_page_and_sample_select_by_the_pair() {
        let mut conn = test_conn();
        let tx = conn.transaction().unwrap();
        let a = blob(1);
        let b = blob(2);
        let c = blob(3);
        insert_blob(&tx, &a, 9, Some(9)); // quiescent, goal 9
        insert_blob(&tx, &b, 4, Some(4)); // quiescent, goal 4: stale below T=8
        insert_blob(&tx, &c, 2, None); // in flight, goal 2: stale too

        let page = stale_page(&tx, 8, 10).unwrap();
        assert_eq!(
            page,
            vec![
                PlacementTarget {
                    blob_id: c.clone(),
                    from: 2,
                    to: 8
                },
                PlacementTarget {
                    blob_id: b.clone(),
                    from: 4,
                    to: 8
                },
            ]
        );
        assert_eq!(stale_page(&tx, 8, 1).unwrap().len(), 1);
        assert!(stale_page(&tx, 2, 10).unwrap().is_empty());

        let sample = in_flight_sample(&tx, 10).unwrap();
        assert_eq!(sample, vec![c.clone()], "only in-flight blobs are sampled");
    }

    // Should: stamp verified_height / provenance on the attested rows,
    // mark suspect rows, clear suspect on a later present attestation,
    // ignore hashes with no row, and apply idempotently.
    // Should not: let a stale or suspect row count as confirmation
    // evidence, nor one that was never disk-verified.
    // Impact: this is what makes belief honest — confirm validation and
    // read routing trust verified_height, never the self-check's flag.
    #[test]
    fn attestation_stamps_rows_and_evidence_needs_recent_verified_rows() {
        let mut conn = test_conn();
        let tx = conn.transaction().unwrap();
        let b = blob(1);
        insert_blob(&tx, &b, 6, None);
        record_transition(&tx, 5, &view(&[1, 2, 3])).unwrap();
        let assignment = snapshot_at(&tx, 6).unwrap().unwrap().assignment(&b);

        // Belief rows without verification: not evidence.
        for i in 0..3u32 {
            tx.execute(
                "INSERT INTO fragment_inventory (fragment_hash, node_id) VALUES (?, ?)",
                params![vec![i as u8; 32], assignment[i as usize]],
            )
            .unwrap();
        }
        assert_eq!(
            confirm_ready(&tx, &b, 10).unwrap(),
            None,
            "never disk-verified"
        );

        // Each responsible attests its own class.
        for i in 0..3u32 {
            let stamped = crate::store::apply_attestation(
                &tx,
                assignment[i as usize],
                8,
                10,
                &[Blake3Hash::from_bytes([i as u8; 32])],
                &[],
            )
            .unwrap();
            assert_eq!(stamped, 1);
        }
        assert_eq!(confirm_ready(&tx, &b, 10).unwrap(), Some(6));
        // Unknown hash: nothing stamped, nothing created.
        assert_eq!(
            crate::store::apply_attestation(&tx, 1, 8, 10, &[Blake3Hash::from_bytes([9; 32])], &[])
                .unwrap(),
            0
        );
        let rows: i64 = tx
            .query_row("SELECT COUNT(*) FROM fragment_inventory", [], |r| r.get(0))
            .unwrap();
        assert_eq!(rows, 3);

        // Too old for the window: not evidence.
        let far = 8 + ATTESTATION_RECENCY_HEIGHTS + 1;
        assert_eq!(confirm_ready(&tx, &b, far).unwrap(), None);

        // Suspect on one class: evidence gone; a fresh present attestation
        // clears it.
        let victim = Blake3Hash::from_bytes([0; 32]);
        crate::store::apply_attestation(&tx, assignment[0], 9, 10, &[], &[victim]).unwrap();
        assert_eq!(confirm_ready(&tx, &b, 10).unwrap(), None);
        crate::store::apply_attestation(&tx, assignment[0], 9, 10, &[victim], &[]).unwrap();
        assert_eq!(confirm_ready(&tx, &b, 10).unwrap(), Some(6));
        let (verified, provenance, suspect): (i64, i64, i64) = tx
            .query_row(
                "SELECT verified_height, provenance, suspect FROM fragment_inventory
                 WHERE fragment_hash = ? AND node_id = ?",
                params![victim, assignment[0]],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!((verified, provenance, suspect), (9, 0, 0));
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
