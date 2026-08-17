//! Photos GC reference provider (RFC-011 Phase 1).
//!
//! Declares which data blocks (blobs) the photos projection still
//! references, so orphan cleanup never collects a referenced blob.
//! Registered cross-crate via the hopnet-projection inventory registry.
//!
//! Two reference surfaces (photos.md:397-475):
//!
//! 1. **`photo_resources.data_block_id`** — any resource (original, edited,
//!    paired_video, thumbnails, etc.) of any photo, active or soft-deleted
//!    within the 30-day tombstone window. `photo_resources` rows are only
//!    hard-deleted after the parent photo's tombstone expires, so this
//!    check naturally covers both active and recently-deleted photos.
//!
//! 2. **`photo_operations.prior_data_block_id` / `new_data_block_id`** —
//!    edit-history retention: superseded versions pinned for the 30-day
//!    undo window. Operation rows live indefinitely for audit, but their
//!    blob references age out — the retention filter is what keeps the
//!    cleanup sweep from leaking storage proportional to total edit
//!    history.
//!
//! ## Retention filter — two implementations, one policy
//!
//! The window is enforced in BOTH code paths:
//!
//! - **Per-row check** (Rust, `references_data_block`): binds a
//!   `CustomUUID::retention_cutoff(EDIT_HISTORY_RETENTION_DAYS)` parameter
//!   against the operation-log `id` (UUIDv7 string comparison is
//!   chronological because the timestamp occupies the high bits of the
//!   canonical hyphenated form).
//! - **Bulk subquery** (SQL, `referenced_data_blocks_subquery`): filters
//!   via the `uuid_extract_timestamp(id)` SQL function (registered on
//!   every pooled connection at `src/db/shared.rs::register_custom_functions`)
//!   against `julianday('now','-30 days')` millisecond conversion.
//!
//! The two must agree to within clock skew. A boundary disagreement means
//! the subquery says "expired" (candidate) while the verify says "pinned"
//! (veto) — and one vetoed block fails the **entire cleanup batch**
//! (`src/storage_host/db_apply.rs` returns `Err` on the first referenced
//! block). Fails closed and self-heals next pass. Wall-clock inside
//! consensus validation is impure (the verify runs in both validate and
//! execute), but precedent exists (`has_active_takeout_tx` compares
//! `expires_at > CURRENT_TIMESTAMP` in the same path), both fail closed
//! (never premature-delete), and the mutation itself (DELETE by explicit
//! id list) is deterministic — a genuine validator disagreement requires
//! an operation row within clock-skew of exactly 30 days old at validation
//! time, which retries cleanly next pass.

use hopnet_common::CustomUUID;
use hopnet_projection::{DataBlockReferenceProvider, DatabaseError};

use crate::db::EDIT_HISTORY_RETENTION_DAYS;

pub struct PhotosReferenceProvider;

impl DataBlockReferenceProvider for PhotosReferenceProvider {
    fn name(&self) -> &'static str {
        "photos"
    }

    fn references_data_block(
        &self,
        db_tx: &rusqlite::Transaction,
        data_block_id: &str,
    ) -> Result<bool, DatabaseError> {
        // 1. Any resource of any photo, active or soft-deleted within the
        //    tombstone window (photo_resources rows outlive their parent's
        //    tombstone by design — see photos.md:367-396).
        let in_resources: bool = db_tx
            .query_row(
                "SELECT COUNT(*) > 0 FROM photo_resources WHERE data_block_id = ?",
                rusqlite::params![data_block_id],
                |row| row.get(0),
            )
            .map_err(|_| DatabaseError::RecallError)?;
        if in_resources {
            return Ok(true);
        }

        // 2. Edit-history retention: superseded versions referenced by
        //    content_edit operations within the 30-day window. UUIDv7
        //    encodes timestamp in the high bits, so lexicographic string
        //    ordering == chronological ordering — the cutoff is itself a
        //    UUIDv7, and `id > cutoff` selects rows minted after it.
        let cutoff = CustomUUID::retention_cutoff(EDIT_HISTORY_RETENTION_DAYS).to_string();
        let in_history: bool = db_tx
            .query_row(
                "SELECT COUNT(*) > 0 FROM photo_operations
                 WHERE (prior_data_block_id = ? OR new_data_block_id = ?)
                   AND id > ?",
                rusqlite::params![data_block_id, data_block_id, cutoff],
                |row| row.get(0),
            )
            .map_err(|_| DatabaseError::RecallError)?;

        Ok(in_history)
    }

    fn referenced_data_blocks_subquery(&self) -> &'static str {
        // Must return rows with a column named `data_block_id` (the host's
        // bulk-candidate query aliases by that name — see
        // hopnet-projection/src/lib.rs:190-193). The first SELECT names
        // the column; UNION keeps it.
        //
        // The retention filter on the operation-log arms uses
        // `uuid_extract_timestamp` (registered on every pooled connection)
        // with julianday math. This is the SQL-side implementation of the
        // same policy the Rust verify path enforces via
        // `CustomUUID::retention_cutoff` — keep them bit-comparable.
        //
        // Over-exclusion (omitting the filter) would leak every superseded
        // prior-version blob permanently, because operation rows live
        // indefinitely and the bulk subquery excludes candidates from
        // orphan cleanup. Under-exclusion merely wastes a consensus round
        // (the per-row verify vetoes). So the filter MUST be here.
        "
        SELECT data_block_id FROM photo_resources
        UNION
        SELECT prior_data_block_id FROM photo_operations
         WHERE prior_data_block_id IS NOT NULL
           AND uuid_extract_timestamp(id) > CAST((julianday('now','-30 days') - 2440587.5) * 86400000 AS INTEGER)
        UNION
        SELECT new_data_block_id FROM photo_operations
         WHERE new_data_block_id IS NOT NULL
           AND uuid_extract_timestamp(id) > CAST((julianday('now','-30 days') - 2440587.5) * 86400000 AS INTEGER)
        "
    }
}

inventory::submit! { &PhotosReferenceProvider as &dyn DataBlockReferenceProvider }

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    /// Build a fixture DB with the photos schema + the `uuid_extract_timestamp`
    /// SQL function registered (mirrors what every pooled connection gets
    /// via `src/db/shared.rs::register_custom_functions`).
    fn fixture() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        conn.execute_batch(
            "CREATE TABLE users (user_id INTEGER PRIMARY KEY, username TEXT);
             CREATE TABLE nodes (node_id INTEGER PRIMARY KEY);",
        )
        .unwrap();
        hopnet_storage::store::CHAIN.install(&conn).unwrap();
        crate::db::CHAIN.install(&conn).unwrap();
        hopnet_common::db_impl::register_uuid_extract_timestamp(&conn).unwrap();
        conn.execute("INSERT INTO users (user_id, username) VALUES (1, 'a')", [])
            .unwrap();
        conn
    }

    /// Mint a UUIDv7 `ago` before now, as a hyphenated string.
    fn uuid_v7_ago(ago: chrono::Duration) -> String {
        CustomUUID::cutoff_before(ago).to_string()
    }

    /// Insert a photo + a `photo_resources` row referencing `data_block_id`.
    fn insert_photo_with_resource(conn: &Connection, photo_id: &str, data_block_id: &str) {
        conn.execute(
            "INSERT INTO data_blocks (id, file_hash, fragment_count, added_bytes, file_size)
             VALUES (?1, x'00', 1, 0, 10)",
            rusqlite::params![data_block_id],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO photos (id, uploaded_by, encrypted_metadata, metadata_nonce)
             VALUES (?1, 1, x'00', x'00')",
            rusqlite::params![photo_id],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO photo_resources (photo_id, resource_type, data_block_id)
             VALUES (?1, 0, ?2)",
            rusqlite::params![photo_id, data_block_id],
        )
        .unwrap();
    }

    /// Insert a `photo_operations` row with a given `id` (UUIDv7 — controls
    /// the apparent timestamp) referencing `prior` and/or `new` data blocks.
    /// The `prior`/`new` columns are soft pointers — no FK, no data_blocks
    /// row needed (the operation log outlives the blobs it points at).
    fn insert_op(conn: &Connection, op_id: &str, prior: Option<&str>, new: Option<&str>) {
        conn.execute(
            "INSERT INTO photo_operations
               (id, photo_id, operation_type, prior_data_block_id, new_data_block_id, performed_by)
             VALUES (?1, 'ph1', 1, ?2, ?3, 1)",
            rusqlite::params![op_id, prior, new],
        )
        .unwrap();
    }

    /// Get a tx for the per-row reference check.
    fn tx<'a>(conn: &'a mut Connection) -> rusqlite::Transaction<'a> {
        conn.transaction().unwrap()
    }

    // ------------------------------------------------------------------
    // 1. photo_resources surface — the simple, unretentioned case.
    // ------------------------------------------------------------------

    /// A blob referenced by an active photo's resource is always pinned,
    /// regardless of photo age. There is no "photo_resources age out" —
    /// rows are only hard-deleted after the parent photo's tombstone
    /// expires, which removes the row (and thus the reference) entirely.
    #[test]
    fn active_photo_resource_pinned() {
        let mut conn = fixture();
        insert_photo_with_resource(&conn, "ph1", "blob_active");

        let t = tx(&mut conn);
        let provider = PhotosReferenceProvider;
        assert!(
            provider.references_data_block(&t, "blob_active").unwrap(),
            "active photo resource must be pinned"
        );
        assert!(
            !provider
                .references_data_block(&t, "blob_nonexistent")
                .unwrap(),
            "unreferenced blob must not be pinned"
        );
    }

    /// Soft-deleted photo's resources remain pinned for the tombstone
    /// window (photos.md:375). The reference provider doesn't compute
    /// tombstone expiry — that's enforced by the cleanup job that deletes
    /// `photo_resources` rows when the tombstone expires. While the row
    /// exists, the blob is pinned.
    #[test]
    fn soft_deleted_photo_resource_still_pinned() {
        let mut conn = fixture();
        insert_photo_with_resource(&conn, "ph1", "blob_tomb");
        // Tombstone the photo (resources row stays).
        conn.execute(
            "UPDATE photos SET deleted_at = '2025-01-01T00:00:00Z', deleted_by = 1
             WHERE id = 'ph1'",
            [],
        )
        .unwrap();

        let t = tx(&mut conn);
        let provider = PhotosReferenceProvider;
        assert!(
            provider.references_data_block(&t, "blob_tomb").unwrap(),
            "soft-deleted photo's resources stay pinned until the row is deleted"
        );
    }

    // ------------------------------------------------------------------
    // 2. photo_operations surface — the retention-filtered case.
    // ------------------------------------------------------------------

    /// A content_edit op referencing a superseded blob (prior_data_block_id)
    /// pins it for EDIT_HISTORY_RETENTION_DAYS. After the window, the
    /// blob becomes collectable.
    #[test]
    fn recent_content_edit_pins_prior_blob() {
        let mut conn = fixture();
        insert_photo_with_resource(&conn, "ph1", "blob_current");
        // Op minted 5 days ago — inside the 30-day window.
        insert_op(
            &conn,
            &uuid_v7_ago(chrono::Duration::days(5)),
            Some("blob_superseded"),
            Some("blob_current"),
        );

        let t = tx(&mut conn);
        let provider = PhotosReferenceProvider;
        assert!(
            provider
                .references_data_block(&t, "blob_superseded")
                .unwrap(),
            "recently-superseded blob (5d old) must be pinned by op log"
        );
    }

    /// A content_edit op older than the retention window does NOT pin its
    /// prior/new blobs — they're collectable. This is the leak-direction
    /// test: omitting the retention filter would pin forever.
    #[test]
    fn aged_content_edit_releases_blob() {
        let mut conn = fixture();
        insert_photo_with_resource(&conn, "ph1", "blob_current");
        // Op minted 45 days ago — outside the 30-day window.
        insert_op(
            &conn,
            &uuid_v7_ago(chrono::Duration::days(45)),
            Some("blob_superseded_aged"),
            Some("blob_current"),
        );

        let t = tx(&mut conn);
        let provider = PhotosReferenceProvider;
        assert!(
            !provider
                .references_data_block(&t, "blob_superseded_aged")
                .unwrap(),
            "superseded blob referenced only by an op older than 30d must be collectable"
        );
        // The current blob is still pinned by photo_resources — so it's
        // still referenced even though the op aged out.
        assert!(
            provider.references_data_block(&t, "blob_current").unwrap(),
            "current blob still pinned by photo_resources when op ages out"
        );
    }

    /// `new_data_block_id` is symmetric to `prior_data_block_id` — same
    /// retention window. Aged-out new-block references release.
    #[test]
    fn aged_new_data_block_releases() {
        let mut conn = fixture();
        // photo_resources references blob_v1 (current); the op's
        // new_data_block_id was blob_v2, which has since been deleted
        // from photo_resources (replaced by a newer edit). The op is old.
        insert_photo_with_resource(&conn, "ph1", "blob_v1");
        insert_op(
            &conn,
            &uuid_v7_ago(chrono::Duration::days(45)),
            None,
            Some("blob_v2_ghost"),
        );

        let t = tx(&mut conn);
        let provider = PhotosReferenceProvider;
        assert!(
            !provider.references_data_block(&t, "blob_v2_ghost").unwrap(),
            "new_data_block_id aged past retention must release"
        );
    }

    /// An operation with both prior and new referencing the same data_block
    /// (degenerate but possible) is pinned if the op is within the window.
    #[test]
    fn op_with_same_prior_and_new_pinned_within_window() {
        let mut conn = fixture();
        insert_photo_with_resource(&conn, "ph1", "blob_elsewhere");
        insert_op(
            &conn,
            &uuid_v7_ago(chrono::Duration::days(10)),
            Some("blob_self"),
            Some("blob_self"),
        );

        let t = tx(&mut conn);
        let provider = PhotosReferenceProvider;
        assert!(
            provider.references_data_block(&t, "blob_self").unwrap(),
            "op within window referencing the same block as prior+new must pin"
        );
    }

    // ------------------------------------------------------------------
    // 3. Bulk subquery — the SQL-side implementation.
    // ------------------------------------------------------------------
    //
    // The subquery is the candidate-identification step (fragments.rs:19-23).
    // A blob NOT in the subquery's result is NEVER considered for cleanup,
    // so the verify path can never rescue an over-excluded blob. Test the
    // negative direction hard: an aged-out blob that the verify would
    // release MUST also be absent from the subquery, and a recent blob
    // that the verify would pin MUST also be present in the subquery.

    /// The subquery's result must include every blob the per-row check
    /// would pin, for both surfaces. Drives the "no over-exclusion"
    /// invariant from the other direction.
    #[test]
    fn subquery_includes_everything_verify_would_pin() {
        let conn = fixture();
        insert_photo_with_resource(&conn, "ph1", "blob_resource");
        insert_op(
            &conn,
            &uuid_v7_ago(chrono::Duration::days(5)),
            Some("blob_recent_op_prior"),
            Some("blob_recent_op_new"),
        );

        let subquery = PhotosReferenceProvider.referenced_data_blocks_subquery();
        let in_subquery = |blob_id: &str| -> bool {
            conn.query_row(
                &format!("SELECT COUNT(*) > 0 FROM ({subquery}) WHERE data_block_id = ?"),
                rusqlite::params![blob_id],
                |r| r.get(0),
            )
            .unwrap()
        };

        assert!(in_subquery("blob_resource"), "resource blob in subquery");
        assert!(
            in_subquery("blob_recent_op_prior"),
            "recent op prior blob in subquery"
        );
        assert!(
            in_subquery("blob_recent_op_new"),
            "recent op new blob in subquery"
        );
    }

    /// Aged-out op references must NOT appear in the subquery — otherwise
    /// the bulk candidate sweep would exclude them forever (over-exclusion
    /// leak). This is the test that would fail if the retention filter
    /// were omitted from the subquery.
    #[test]
    fn subquery_excludes_aged_op_references() {
        let conn = fixture();
        insert_photo_with_resource(&conn, "ph1", "blob_current");
        insert_op(
            &conn,
            &uuid_v7_ago(chrono::Duration::days(45)),
            Some("blob_aged_prior"),
            Some("blob_aged_new"),
        );

        let subquery = PhotosReferenceProvider.referenced_data_blocks_subquery();
        let in_subquery = |blob_id: &str| -> bool {
            conn.query_row(
                &format!("SELECT COUNT(*) > 0 FROM ({subquery}) WHERE data_block_id = ?"),
                rusqlite::params![blob_id],
                |r| r.get(0),
            )
            .unwrap()
        };

        assert!(
            !in_subquery("blob_aged_prior"),
            "aged prior blob must NOT be in subquery (over-exclusion leak)"
        );
        assert!(
            !in_subquery("blob_aged_new"),
            "aged new blob must NOT be in subquery (over-exclusion leak)"
        );
        // The current resource is in the subquery (resources arm has no
        // retention filter — rows are deleted, not aged).
        assert!(in_subquery("blob_current"), "current resource in subquery");
    }

    /// The two implementations must agree on the boundary: a blob pinned
    /// by the per-row check must appear in the subquery, and vice versa.
    /// This is the invariant that, if broken, fails entire cleanup
    /// batches (subquery says expired → candidate → verify says pinned →
    /// batch fails closed).
    ///
    /// We test agreement across the recent/aged split for both surfaces.
    #[test]
    fn rust_and_sql_implementations_agree() {
        let mut conn = fixture();
        insert_photo_with_resource(&conn, "ph1", "blob_resource");
        insert_op(
            &conn,
            &uuid_v7_ago(chrono::Duration::days(5)),
            Some("blob_recent_prior"),
            Some("blob_recent_new"),
        );
        insert_op(
            &conn,
            &uuid_v7_ago(chrono::Duration::days(45)),
            Some("blob_aged_prior"),
            Some("blob_aged_new"),
        );

        // Collect SQL subquery results first (immutable borrow), then run
        // the per-row Rust checks (mutable borrow) — the two borrow kinds
        // can't coexist on the same connection.
        let subquery = PhotosReferenceProvider.referenced_data_blocks_subquery();
        let blob_ids = [
            "blob_resource",
            "blob_recent_prior",
            "blob_recent_new",
            "blob_aged_prior",
            "blob_aged_new",
            "blob_nonexistent",
        ];
        let sql_pins: Vec<(&str, bool)> = blob_ids
            .iter()
            .map(|&blob_id| {
                let pinned: bool = conn
                    .query_row(
                        &format!("SELECT COUNT(*) > 0 FROM ({subquery}) WHERE data_block_id = ?"),
                        rusqlite::params![blob_id],
                        |r| r.get(0),
                    )
                    .unwrap();
                (blob_id, pinned)
            })
            .collect();

        let provider = PhotosReferenceProvider;
        for (blob_id, sql_pinned) in sql_pins {
            let t = tx(&mut conn);
            let rust_pinned = provider.references_data_block(&t, blob_id).unwrap();
            t.rollback().unwrap();
            assert_eq!(
                rust_pinned, sql_pinned,
                "Rust verify and SQL subquery disagree on {blob_id}: \
                 rust={rust_pinned}, sql={sql_pinned}"
            );
        }
    }
}
