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

/// The owed blobs the planner knows of, across bounded passes, in global
/// at-risk-first order. Each pass reads one slice of the in-flight set
/// (by id) and `absorb`s it: the slice's old entries are replaced by its
/// fresh scores, so a blob re-scored or no longer owed is updated, and an
/// at-risk blob from any slice reaches the head as soon as its slice is
/// read. The feeder `pop`s from the head while passes continue. ~70k
/// entries fit easily in memory.
#[derive(Debug, Default)]
pub struct PriorityBook {
    /// blob id (as its stored text) → (tolerance, owed, id).
    entries: std::collections::BTreeMap<String, (i32, usize, BlobId)>,
    /// (tolerance, more owed first, id text) — the feed order.
    order: BTreeSet<(i32, std::cmp::Reverse<usize>, String)>,
}

impl PriorityBook {
    /// Replace everything known about the slice of ids in `(after, through]`
    /// (`after` None = from the start, `through` None = to the end) with
    /// `items`, the slice's freshly scored owed blobs.
    pub fn absorb(
        &mut self,
        after: Option<&BlobId>,
        through: Option<&BlobId>,
        items: Vec<PlanItem>,
    ) {
        use std::ops::Bound;
        let lo = after.map_or(Bound::Unbounded, |id| Bound::Excluded(id.to_string()));
        let hi = through.map_or(Bound::Unbounded, |id| Bound::Included(id.to_string()));
        let stale: Vec<String> = self
            .entries
            .range((lo, hi))
            .map(|(k, _)| k.clone())
            .collect();
        for key in stale {
            self.remove(&key);
        }
        for item in items {
            let key = item.blob_id.to_string();
            self.remove(&key);
            self.order
                .insert((item.tolerance, std::cmp::Reverse(item.owed), key.clone()));
            self.entries
                .insert(key, (item.tolerance, item.owed, item.blob_id));
        }
    }

    fn remove(&mut self, key: &str) {
        if let Some((tolerance, owed, _)) = self.entries.remove(key) {
            self.order
                .remove(&(tolerance, std::cmp::Reverse(owed), key.to_string()));
        }
    }

    /// Take the most at-risk blob (it returns on its slice's next read if
    /// still owed).
    pub fn pop(&mut self) -> Option<BlobId> {
        let (_, _, key) = self.order.pop_first()?;
        self.entries.remove(&key).map(|(_, _, id)| id)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Owed blobs at member fault tolerance 0 or below.
    pub fn at_risk(&self) -> usize {
        self.order.iter().take_while(|(t, _, _)| *t <= 0).count()
    }
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
    pub next: Option<BlobId>,
    pub scanned: usize,
}

/// What one (bounded) pass produced.
#[derive(Debug, Default)]
pub struct PlannedPass {
    /// Owed blobs, sorted at-risk first.
    pub plan: Vec<PlanItem>,
    /// In-flight blobs this pass read.
    pub scanned: usize,
    /// Where the next pass resumes; `None` once the walk reached the end
    /// (the next pass starts over).
    pub resume: Option<BlobId>,
}

/// One planning pass over the in-flight set, page by page from `start`,
/// stopping once `budget` blobs have been read: per-pass DB work is
/// bounded, and the next pass resumes where this one stopped. `read_page`
/// runs one page in its own short read (the host passes a closure that
/// checks out a connection per call).
pub fn plan_pass<F>(
    start: Option<BlobId>,
    budget: usize,
    mut read_page: F,
) -> Result<PlannedPass, StorageError>
where
    F: FnMut(Option<BlobId>) -> Result<PlannedPage, StorageError>,
{
    let mut pass = PlannedPass::default();
    let mut cursor = start;
    loop {
        let page = read_page(cursor.take())?;
        pass.scanned += page.scanned;
        pass.plan.extend(page.items);
        match page.next {
            Some(c) if pass.scanned < budget => cursor = Some(c),
            next => {
                pass.resume = next;
                break;
            }
        }
    }
    sort_plan(&mut pass.plan);
    Ok(pass)
}

/// One planning page on `conn`: the in-flight blobs after `after`, planned.
/// Blobs `skip` names (parked on unreachable sources) cost no lookups.
pub fn plan_page(
    conn: &rusqlite::Connection,
    after: Option<&BlobId>,
    me: i32,
    members: &BTreeSet<i32>,
    snapshots: &mut HashMap<u64, Option<ViewSnapshot>>,
    skip: &dyn Fn(&BlobId) -> bool,
) -> Result<PlannedPage, StorageError> {
    let page = in_flight_page_after(conn, after, PLAN_PAGE_SIZE)?;
    let scanned = page.len();
    let next = if scanned < PLAN_PAGE_SIZE {
        None
    } else {
        page.last().map(|(_, id)| id.clone())
    };
    let mut items = Vec::new();
    for (desired, blob_id) in &page {
        if skip(blob_id) {
            continue;
        }
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
        let pass = plan_pass(None, usize::MAX, |after| {
            plan_page(&conn, after.as_ref(), 1, &members, &mut snapshots, &|_| {
                false
            })
        })
        .unwrap();
        assert_eq!(pass.scanned, 2);
        assert_eq!(pass.resume, None);
        let got: Vec<(BlobId, i32)> = pass
            .plan
            .into_iter()
            .map(|i| (i.blob_id, i.tolerance))
            .collect();
        assert_eq!(got, vec![(blob(2), 0), (blob(1), 1)]);
    }

    // Impact: review of #96 — each pass read every in-flight blob's
    // manifest and ~30 inventory rows (~2M random reads at 68.8k blobs),
    // back to back, even when most owed blobs were parked.
    // Should: stop a pass once its budget of blobs is read and hand back
    // where to resume; resume there next pass.
    // Should not: look up a blob the skip predicate names (parked).
    #[test]
    fn a_pass_is_bounded_resumable_and_skips_parked_blobs() {
        let pages: Vec<Vec<u8>> = vec![vec![1, 2], vec![3, 4], vec![5]];
        let page = |after: Option<BlobId>| -> Result<PlannedPage, StorageError> {
            let i = match after {
                None => 0,
                Some(id) => {
                    pages
                        .iter()
                        .position(|p| blob(*p.last().unwrap()) == id)
                        .unwrap()
                        + 1
                }
            };
            let ids = &pages[i];
            Ok(PlannedPage {
                items: ids
                    .iter()
                    .map(|n| PlanItem {
                        blob_id: blob(*n),
                        tolerance: 1,
                        owed: 1,
                    })
                    .collect(),
                next: (i + 1 < pages.len()).then(|| blob(*ids.last().unwrap())),
                scanned: ids.len(),
            })
        };
        let first = plan_pass(None, 2, page).unwrap();
        assert_eq!(first.scanned, 2);
        assert_eq!(first.resume, Some(blob(2)));
        let second = plan_pass(first.resume, 2, page).unwrap();
        assert_eq!(
            second
                .plan
                .iter()
                .map(|i| i.blob_id.clone())
                .collect::<Vec<_>>(),
            vec![blob(3), blob(4)]
        );
        let last = plan_pass(second.resume, 2, page).unwrap();
        assert_eq!(last.resume, None, "end of the set: start over");

        // Skip: a parked blob costs no plan_blob lookups.
        let mut conn = test_conn();
        {
            let tx = conn.transaction().unwrap();
            record_transition(&tx, 5, &view(&[1])).unwrap();
            tx.commit().unwrap();
        }
        insert_blob(&conn, &blob(1), 2, 4, &[], &[]);
        insert_blob(&conn, &blob(2), 2, 4, &[], &[]);
        let members: BTreeSet<i32> = [1].into();
        let mut snapshots = HashMap::new();
        let parked = blob(1);
        let got = plan_page(&conn, None, 1, &members, &mut snapshots, &|id| {
            *id == parked
        })
        .unwrap();
        assert_eq!(got.scanned, 2);
        assert_eq!(
            got.items.into_iter().map(|i| i.blob_id).collect::<Vec<_>>(),
            vec![blob(2)]
        );
    }

    // Impact: re-review of #96 — with bounded passes, at-risk-first held
    // only within each 16k slice, so an at-risk blob in a later slice waited
    // behind healthy blobs of the slice being fed.
    // Should: feed the most at-risk blob across every slice read so far.
    // Should: re-score a blob when its slice is read again, and drop one
    // its slice no longer lists as owed.
    // Should not: touch entries outside the slice being absorbed.
    #[test]
    fn the_priority_book_orders_globally_across_slices() {
        let item = |n: u8, tolerance: i32, owed: usize| PlanItem {
            blob_id: blob(n),
            tolerance,
            owed,
        };
        let mut book = PriorityBook::default();
        // Slice 1 (.. through blob 3): healthy blobs.
        book.absorb(None, Some(&blob(3)), vec![item(1, 2, 1), item(2, 2, 3)]);
        // Slice 2 (blob 3 .. end): one at risk.
        book.absorb(Some(&blob(3)), None, vec![item(5, 0, 1), item(6, 1, 1)]);
        assert_eq!(book.len(), 4);
        assert_eq!(book.at_risk(), 1);
        assert_eq!(book.pop(), Some(blob(5)), "at risk, from the later slice");

        // Slice 1 re-read: blob 1 now at risk, blob 2 no longer owed.
        book.absorb(None, Some(&blob(3)), vec![item(1, -1, 2)]);
        assert_eq!(book.pop(), Some(blob(1)));
        assert_eq!(book.pop(), Some(blob(6)), "slice 2 untouched");
        assert_eq!(book.pop(), None);
    }
}
