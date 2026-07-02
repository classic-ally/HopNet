//! Tier-1 automatic startup reconciliation (spec §Recovery): after an
//! unclean shutdown, recount `blobs.ref_count` from the authoritative JOIN
//! and repair drift. Row-level only — startup repair NEVER deletes blob
//! files (orphan deletion is fsck's one destructive repair, Tier 2).
//!
//! Split for Phase 6: [`recount_diff`] is the pure read half (fsck's
//! default read-only report); [`repair_refcounts`] wraps it in the write
//! transaction that applies the repairs.

use chrono::Utc;
use std::collections::HashMap;

use crate::error::Result;
use crate::ids::{ContentHash, LibraryId};
use crate::store::StateStore;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct RefcountRepairReport {
    /// Rows whose stored count disagreed with the recount.
    pub updated: u64,
    /// Rows with a zero recount — deleted (the file, if any, becomes fsck's
    /// benign orphan class).
    pub deleted: u64,
    /// Referenced (library, hash) pairs with no `blobs` row — inserted
    /// (ext/size from a referencing resource row; file existence deliberately
    /// unchecked — a missing file is fsck's loud byte-loss class).
    pub inserted: u64,
}

impl RefcountRepairReport {
    pub fn drift(&self) -> u64 {
        self.updated + self.deleted + self.inserted
    }
}

/// One stored-vs-recounted disagreement on a `blobs` row.
#[derive(Debug, Clone, serde::Serialize)]
pub struct RefcountDrift {
    pub library_id: LibraryId,
    pub content_hash: ContentHash,
    #[serde(flatten)]
    pub kind: DriftKind,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DriftKind {
    /// Stored count differs from the recount — repaired by UPDATE.
    Count { stored: i64, recount: i64 },
    /// Row exists but no resource references the hash — repaired by DELETE
    /// (the file, if any, becomes fsck's benign orphan class).
    OrphanRow { stored: i64 },
    /// Resources reference the hash but no row exists — repaired by INSERT.
    MissingRow {
        recount: i64,
        ext: String,
        size_bytes: i64,
    },
}

/// Recount refcounts from the authoritative JOIN and diff against stored
/// `blobs` rows — pure read, no writes. Counts rows by `content_hash`
/// regardless of `written_at`: a superseded-pending row (reopened re-edit)
/// still references its old blob until the write-time swap.
///
/// Run inside [`repair_refcounts`]'s transaction, or on a read-only
/// connection for fsck's report-only mode (the two SELECTs run
/// back-to-back on one connection; a concurrent writer between them can
/// produce a transient false drift, which fsck's live-daemon banner
/// already disclaims).
pub(crate) async fn recount_diff(conn: &mut sqlx::SqliteConnection) -> Result<Vec<RefcountDrift>> {
    let recounts: Vec<(LibraryId, ContentHash, i64, String, i64)> = sqlx::query_as(
        "SELECT p.library_id, r.content_hash, COUNT(*), MIN(r.ext), MIN(r.size_bytes) \
         FROM photo_resources r \
         JOIN photos p ON p.photo_id = r.photo_id \
         WHERE r.content_hash IS NOT NULL AND p.library_id IS NOT NULL \
         GROUP BY p.library_id, r.content_hash",
    )
    .fetch_all(&mut *conn)
    .await?;
    let stored: Vec<(LibraryId, ContentHash, i64)> =
        sqlx::query_as("SELECT library_id, content_hash, ref_count FROM blobs")
            .fetch_all(&mut *conn)
            .await?;

    let recount_map: HashMap<(LibraryId, ContentHash), (i64, String, i64)> = recounts
        .into_iter()
        .map(|(lib, hash, n, ext, size)| ((lib, hash), (n, ext, size)))
        .collect();

    let mut drifts = Vec::new();
    let mut seen: std::collections::HashSet<(LibraryId, ContentHash)> = Default::default();
    for (lib, hash, stored_count) in stored {
        seen.insert((lib.clone(), hash.clone()));
        match recount_map.get(&(lib.clone(), hash.clone())) {
            Some((recount, ..)) if *recount == stored_count => {}
            Some((recount, ..)) => drifts.push(RefcountDrift {
                library_id: lib,
                content_hash: hash,
                kind: DriftKind::Count {
                    stored: stored_count,
                    recount: *recount,
                },
            }),
            None => drifts.push(RefcountDrift {
                library_id: lib,
                content_hash: hash,
                kind: DriftKind::OrphanRow {
                    stored: stored_count,
                },
            }),
        }
    }
    for ((lib, hash), (recount, ext, size)) in &recount_map {
        if seen.contains(&(lib.clone(), hash.clone())) {
            continue;
        }
        drifts.push(RefcountDrift {
            library_id: lib.clone(),
            content_hash: hash.clone(),
            kind: DriftKind::MissingRow {
                recount: *recount,
                ext: ext.clone(),
                size_bytes: *size,
            },
        });
    }
    Ok(drifts)
}

/// Recount, diff, repair, and log — one transaction. Logs ONE
/// `refcount_repaired` event when drift was found (a per-drift event could
/// flood after a bad crash); silent when clean.
pub async fn repair_refcounts(store: &StateStore) -> Result<RefcountRepairReport> {
    let mut tx = store.pool().begin().await?;

    let drifts = recount_diff(&mut tx).await?;

    let mut report = RefcountRepairReport::default();
    let mut samples: Vec<serde_json::Value> = Vec::new();
    let sample = |samples: &mut Vec<serde_json::Value>, v: serde_json::Value| {
        if samples.len() < 50 {
            samples.push(v);
        }
    };

    for drift in &drifts {
        let (lib, hash) = (&drift.library_id, &drift.content_hash);
        match &drift.kind {
            DriftKind::Count { stored, recount } => {
                sqlx::query(
                    "UPDATE blobs SET ref_count = ? WHERE library_id = ? AND content_hash = ?",
                )
                .bind(recount)
                .bind(lib)
                .bind(hash)
                .execute(&mut *tx)
                .await?;
                report.updated += 1;
                sample(
                    &mut samples,
                    serde_json::json!({
                        "kind": "count", "library": lib.to_string(), "hash": hash.to_string(),
                        "stored": stored, "recount": recount,
                    }),
                );
            }
            DriftKind::OrphanRow { stored } => {
                sqlx::query("DELETE FROM blobs WHERE library_id = ? AND content_hash = ?")
                    .bind(lib)
                    .bind(hash)
                    .execute(&mut *tx)
                    .await?;
                report.deleted += 1;
                sample(
                    &mut samples,
                    serde_json::json!({
                        "kind": "orphan_row", "library": lib.to_string(), "hash": hash.to_string(),
                        "stored": stored,
                    }),
                );
            }
            DriftKind::MissingRow {
                recount,
                ext,
                size_bytes,
            } => {
                sqlx::query(
                    "INSERT INTO blobs (library_id, content_hash, ext, size_bytes, ref_count, written_at) \
                     VALUES (?, ?, ?, ?, ?, ?)",
                )
                .bind(lib)
                .bind(hash)
                .bind(ext)
                .bind(size_bytes)
                .bind(recount)
                .bind(Utc::now())
                .execute(&mut *tx)
                .await?;
                report.inserted += 1;
                sample(
                    &mut samples,
                    serde_json::json!({
                        "kind": "missing_row", "library": lib.to_string(), "hash": hash.to_string(),
                        "recount": recount,
                    }),
                );
            }
        }
    }

    if report.drift() > 0 {
        crate::store::log::append(
            &mut *tx,
            "refcount_repaired",
            None,
            Some(serde_json::json!({
                "updated": report.updated,
                "deleted": report.deleted,
                "inserted": report.inserted,
                "samples": samples,
            })),
        )
        .await?;
    }
    tx.commit().await?;
    Ok(report)
}
