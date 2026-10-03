//! The pull planner's pure core and its read pass (RFC-STORAGE-003 S3).
//!
//! The policy tick used to wake the reconciler for the first 64 in-flight
//! blobs by `(goal, id)`. One transition gives every in-flight blob the
//! same goal, so that page never changed while its blobs waited on
//! confirmation, and the rest of the set was never pulled (production,
//! 2026-10-03: ~68.8k blobs, desktop at zero fetches). The planner walks
//! the WHOLE in-flight set instead, keeps only blobs this node owes a
//! class of, and orders them at-risk first by the resilience pane's own
//! worst-case rule, so pulls drain the pane's tolerance-0 bucket first.
//!
//! Read-only and paged: every page is its own short read, so a pass never
//! holds a long reader or the write lock.

use crate::lifecycle::{in_flight_page_after, snapshot_at, ViewSnapshot};
use crate::store::db_err;
use crate::types::BlobId;
use crate::StorageError;
use hopnet_common::Blake3Hash;
use rusqlite::params;
use std::collections::{BTreeSet, HashMap};

/// Blobs read per planning page (one short read transaction each).
pub const PLAN_PAGE_SIZE: usize = 512;

/// One blob this node owes classes of, with its risk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanItem {
    pub blob_id: BlobId,
    /// Worst-case member fault tolerance (`resilience_level_rows`): how
    /// many of the largest member holders can be lost with K fragments
    /// left; -1 when fewer than K survive on members today.
    pub tolerance: i32,
    /// Classes assigned to this node under the goal and not on its disk.
    pub owed: usize,
}

/// The resilience pane's per-block worst case: drop the largest member
/// holders first and count how many can go while at least `k` fragments
/// remain. `holders` is fragments held per member node (any order). Fewer
/// than `k` in total is -1, matching the pane's "unrecoverable" bucket.
pub fn worst_case_tolerance(holders: &[usize], k: usize) -> i32 {
    let mut sorted: Vec<usize> = holders.iter().copied().filter(|n| *n > 0).collect();
    sorted.sort_unstable_by(|a, b| b.cmp(a));
    let total: usize = sorted.iter().sum();
    if total < k || sorted.is_empty() {
        return -1;
    }
    let mut removed = 0usize;
    let mut level = -1i32;
    // Rank r (1-based) is still standing after removing the r-1 largest.
    for (rank0, on_node) in sorted.iter().enumerate() {
        if total - removed >= k {
            level = rank0 as i32;
        }
        removed += on_node;
    }
    level
}

/// At-risk first: lower tolerance, then more owed classes, then id (a
/// stable order, so a restart replans the same way).
pub fn sort_plan(items: &mut [PlanItem]) {
    items.sort_by(|a, b| {
        a.tolerance
            .cmp(&b.tolerance)
            .then(b.owed.cmp(&a.owed))
            .then_with(|| a.blob_id.to_string().cmp(&b.blob_id.to_string()))
    });
}

/// One manifest row as the planner needs it.
struct ClassRow {
    local_index: usize,
    hash: Blake3Hash,
    stored_locally: bool,
    original: bool,
}

/// The planner's read of one blob, or `None` when this node owes nothing
/// (or the record does not reach the goal yet).
pub fn plan_blob(
    conn: &rusqlite::Connection,
    blob_id: &BlobId,
    desired: u64,
    me: i32,
    members: &BTreeSet<i32>,
    snapshots: &mut HashMap<u64, Option<ViewSnapshot>>,
) -> Result<Option<PlanItem>, StorageError> {
    let snapshot = match snapshots.get(&desired) {
        Some(s) => s.clone(),
        None => {
            let s = snapshot_at(conn, desired)?;
            snapshots.insert(desired, s.clone());
            s
        }
    };
    let Some(snapshot) = snapshot else {
        return Ok(None);
    };
    let assignment = snapshot.assignment(blob_id);

    let mut stmt = conn
        .prepare_cached(
            "SELECT local_index, fragment_hash, COALESCE(stored_locally, 0), chunk_type
             FROM fragment_hashes WHERE data_block_id = ?",
        )
        .map_err(db_err("prepare planner manifest"))?;
    let rows: Vec<ClassRow> = stmt
        .query_map(params![blob_id], |row| {
            Ok(ClassRow {
                local_index: row.get::<_, i64>(0)? as usize,
                hash: row.get(1)?,
                stored_locally: row.get::<_, i64>(2)? != 0,
                original: row.get::<_, i64>(3)? == 0,
            })
        })
        .map_err(db_err("read planner manifest"))?
        .collect::<Result<_, _>>()
        .map_err(db_err("read planner manifest row"))?;

    let owed = rows
        .iter()
        .filter(|r| !r.stored_locally && assignment.get(r.local_index).copied() == Some(me))
        .count();
    if owed == 0 {
        return Ok(None);
    }

    let k = rows.iter().filter(|r| r.original).count();
    let mut holders: HashMap<i32, usize> = HashMap::new();
    let mut probe = conn
        .prepare_cached("SELECT node_id FROM fragment_inventory WHERE fragment_hash = ?")
        .map_err(db_err("prepare planner holders"))?;
    for row in &rows {
        let nodes = probe
            .query_map(params![row.hash], |r| r.get::<_, i32>(0))
            .map_err(db_err("read planner holders"))?;
        for node in nodes {
            let node = node.map_err(db_err("read planner holder"))?;
            if members.contains(&node) {
                *holders.entry(node).or_default() += 1;
            }
        }
    }
    let counts: Vec<usize> = holders.into_values().collect();
    Ok(Some(PlanItem {
        blob_id: blob_id.clone(),
        tolerance: worst_case_tolerance(&counts, k),
        owed,
    }))
}

/// One planned page: the owed items, where the next page starts (`None`
/// at the end of the set), and how many in-flight blobs the page held.
#[derive(Debug, Default)]
pub struct PlannedPage {
    pub items: Vec<PlanItem>,
    pub next: Option<(u64, BlobId)>,
    pub scanned: usize,
}

/// A whole planning pass over the in-flight set, page by page. `read_page`
/// runs one page in its own short read (the host passes a closure that
/// checks out a connection per call). Returns the sorted plan and how many
/// in-flight blobs were scanned.
pub fn plan_pass<F>(mut read_page: F) -> Result<(Vec<PlanItem>, usize), StorageError>
where
    F: FnMut(Option<(u64, BlobId)>) -> Result<PlannedPage, StorageError>,
{
    let mut plan = Vec::new();
    let mut scanned = 0usize;
    let mut cursor: Option<(u64, BlobId)> = None;
    loop {
        let page = read_page(cursor.take())?;
        scanned += page.scanned;
        plan.extend(page.items);
        match page.next {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }
    sort_plan(&mut plan);
    Ok((plan, scanned))
}

/// One planning page on `conn`: the in-flight blobs after `after`, planned.
pub fn plan_page(
    conn: &rusqlite::Connection,
    after: Option<(u64, &BlobId)>,
    me: i32,
    members: &BTreeSet<i32>,
    snapshots: &mut HashMap<u64, Option<ViewSnapshot>>,
) -> Result<PlannedPage, StorageError> {
    let page = in_flight_page_after(conn, after, PLAN_PAGE_SIZE)?;
    let scanned = page.len();
    let next = if scanned < PLAN_PAGE_SIZE {
        None
    } else {
        page.last().cloned()
    };
    let mut items = Vec::new();
    for (desired, blob_id) in &page {
        if let Some(item) = plan_blob(conn, blob_id, *desired, me, members, snapshots)? {
            items.push(item);
        }
    }
    Ok(PlannedPage {
        items,
        next,
        scanned,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lifecycle::record_transition;
    use std::str::FromStr;

    fn blob(n: u8) -> BlobId {
        BlobId::from_str(&format!("01890a5d-ac96-774b-b9aa-9f8b24f0c9{n:02x}")).unwrap()
    }

    fn view(members: &[i32]) -> ViewSnapshot {
        ViewSnapshot {
            members: members.to_vec(),
            weights: members.iter().map(|m| (*m, 1)).collect(),
        }
    }

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
                self_verified_height INTEGER, verified_height INTEGER,
                provenance INTEGER, suspect INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (fragment_hash, node_id)
            );
            CREATE TABLE storage_view_transitions (
                height INTEGER PRIMARY KEY, snapshot BLOB NOT NULL
            );",
        )
        .unwrap();
        conn
    }

    /// A blob of `classes` single-chunk classes (the first `k` originals),
    /// with `local` marking which this node already holds, and
    /// `held_by[class]` the nodes with an inventory row for it.
    fn insert_blob(
        conn: &rusqlite::Connection,
        id: &BlobId,
        k: usize,
        classes: usize,
        local: &[usize],
        held_by: &[&[i32]],
    ) {
        conn.execute(
            "INSERT INTO data_blocks (id, file_hash, fragment_count, added_bytes, placement_height, file_size, desired_placement_height)
             VALUES (?, X'00', ?, 0, NULL, 10, 9)",
            params![id, classes as i64],
        )
        .unwrap();
        for i in 0..classes {
            let hash = Blake3Hash::new(blake3::hash(format!("{id}-{i}").as_bytes()));
            conn.execute(
                "INSERT INTO fragment_hashes VALUES (?, 0, ?, 'f', ?, ?, ?)",
                params![
                    id,
                    i as i64,
                    hash,
                    if i < k { 0 } else { 1 },
                    local.contains(&i) as i64
                ],
            )
            .unwrap();
            for node in held_by.get(i).copied().unwrap_or(&[]) {
                conn.execute(
                    "INSERT INTO fragment_inventory (fragment_hash, node_id) VALUES (?, ?)",
                    params![hash, node],
                )
                .unwrap();
            }
        }
    }

    // Impact: the planner's order is meant to drain the pane's
    // tolerance-0 bucket first; a different rule here would prioritize
    // blobs the pane does not show as at risk.
    // Should: drop the largest holders first and count how many can go
    // with k fragments left; report -1 when fewer than k exist.
    #[test]
    fn tolerance_matches_the_resilience_panes_worst_case() {
        // 10 + 10 + 10 with k = 10: any two can go.
        assert_eq!(worst_case_tolerance(&[10, 10, 10], 10), 2);
        // 25 on one node, 5 elsewhere, k = 10: losing the big one is fatal.
        assert_eq!(worst_case_tolerance(&[25, 5], 10), 0);
        // 15 + 10 + 5, k = 10: drop 15 -> 15 left; drop 10 too -> 5 left.
        assert_eq!(worst_case_tolerance(&[5, 15, 10], 10), 1);
        assert_eq!(worst_case_tolerance(&[4, 4], 10), -1);
        assert_eq!(worst_case_tolerance(&[], 10), -1);
    }

    // Should: order at-risk blobs first, then the ones owing more classes,
    // then by id.
    #[test]
    fn plan_orders_at_risk_before_healthy() {
        let item = |n: u8, tolerance: i32, owed: usize| PlanItem {
            blob_id: blob(n),
            tolerance,
            owed,
        };
        let mut plan = vec![
            item(1, 2, 1),
            item(2, 0, 1),
            item(3, 1, 4),
            item(4, 0, 3),
            item(5, -1, 1),
        ];
        sort_plan(&mut plan);
        let order: Vec<BlobId> = plan.into_iter().map(|i| i.blob_id).collect();
        assert_eq!(order, vec![blob(5), blob(4), blob(2), blob(3), blob(1)]);
    }

    // Impact: the starved kick re-checked blobs that owed nothing; the
    // planner exists to hand the worker only real work.
    // Should: plan only blobs with a class assigned to this node that is
    // not on its disk, counting those classes.
    // Should not: plan a blob whose assigned classes are all held here.
    #[test]
    fn plan_skips_blobs_this_node_owes_nothing() {
        let mut conn = test_conn();
        {
            let tx = conn.transaction().unwrap();
            record_transition(&tx, 5, &view(&[1, 2, 3])).unwrap();
            tx.commit().unwrap();
        }
        let members: BTreeSet<i32> = [1, 2, 3].into();
        let mut snapshots = HashMap::new();
        let owes = blob(1);
        let done = blob(2);
        insert_blob(&conn, &owes, 2, 6, &[], &[]);
        let mine: Vec<usize> = view(&[1, 2, 3])
            .assignment(&done)
            .iter()
            .enumerate()
            .filter(|(_, n)| **n == 1)
            .map(|(i, _)| i)
            .collect();
        insert_blob(&conn, &done, 2, 6, &mine, &[]);

        let expected_owed = view(&[1, 2, 3])
            .assignment(&owes)
            .iter()
            .take(6)
            .filter(|n| **n == 1)
            .count();
        assert!(expected_owed > 0, "fixture: node 1 must own a class");
        let item = plan_blob(&conn, &owes, 9, 1, &members, &mut snapshots).unwrap();
        assert_eq!(item.map(|i| i.owed), Some(expected_owed));
        assert_eq!(
            plan_blob(&conn, &done, 9, 1, &members, &mut snapshots).unwrap(),
            None
        );
    }

    // Should: walk every in-flight page and return the owed blobs sorted
    // at-risk first.
    // Should not: count a non-member's rows toward a blob's tolerance.
    #[test]
    fn plan_pass_covers_the_set_and_ranks_by_member_holdings() {
        let mut conn = test_conn();
        {
            let tx = conn.transaction().unwrap();
            record_transition(&tx, 5, &view(&[1])).unwrap();
            tx.commit().unwrap();
        }
        let members: BTreeSet<i32> = [1, 2, 3].into();
        // Every class assigned to node 1 (sole view member) and none held.
        // Blob 1: classes spread over 2 and 3 -> tolerance 1.
        // Blob 2: all on node 2, plus a non-member 9 -> tolerance 0.
        let spread: Vec<&[i32]> = vec![&[2], &[3], &[2], &[3]];
        let lumped: Vec<&[i32]> = vec![&[2, 9], &[2, 9], &[2, 9], &[2, 9]];
        insert_blob(&conn, &blob(1), 2, 4, &[], &spread);
        insert_blob(&conn, &blob(2), 2, 4, &[], &lumped);

        let mut snapshots = HashMap::new();
        let (plan, scanned) = plan_pass(|after| {
            plan_page(
                &conn,
                after.as_ref().map(|(h, id)| (*h, id)),
                1,
                &members,
                &mut snapshots,
            )
        })
        .unwrap();
        assert_eq!(scanned, 2);
        let got: Vec<(BlobId, i32)> = plan.into_iter().map(|i| (i.blob_id, i.tolerance)).collect();
        assert_eq!(got, vec![(blob(2), 0), (blob(1), 1)]);
    }
}
