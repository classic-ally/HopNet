//! Periodic tombstone cleanup job (RFC-011 Phase 1).
//!
//! Scans for soft-deleted photos whose 30-day recovery window has elapsed
//! (wall-clock host-side), batches them into explicit-ID consensus
//! transactions, and submits them via the `TxGateway` seam with
//! `TxSigner::Node`. The handler deterministically re-checks the window
//! using the `scan_cutoff` from the payload — no wall-clock in consensus
//! apply.
//!
//! Pattern matches takeout's `run_takeout_maintenance` (hopnet-takeout/
//! src/jobs.rs): scan locally, submit to consensus per batch, tolerate
//! idempotent duplicate submissions (every node runs this cron).

use crate::envelopes::PhotoCleanupExpiredPayload;
use hopnet_projection::host::{HostCapabilities, TxSigner, TxSpec};

/// Maximum photo_ids per consensus transaction. Matches the orphan-cleanup
/// batch size (src/storage_host/jobs.rs:23).
const CLEANUP_BATCH_SIZE: usize = 50;

/// Scan for expired tombstones and submit a consensus cleanup transaction
/// per batch. Called by the host's apalis cron wrapper.
///
/// Early-returns gracefully if the node hasn't bootstrapped yet (no
/// `node_id`) or if the scan finds nothing to clean up.
pub async fn run_photo_tombstone_cleanup(caps: &HostCapabilities) -> Result<(), String> {
    if caps.node_id().is_none() {
        tracing::warn!("photo_tombstone_cleanup: node not initialised, skipping");
        return Ok(()); // Not an error — matches takeout pattern.
    }

    // Scan on a tight scope — the Connection and Statement must be
    // dropped before the first `.await` (they're !Send).
    let expired_ids: Vec<hopnet_common::CustomUUID> = {
        let conn = caps.db_pool.get().map_err(|e| format!("db pool: {e}"))?;
        let mut stmt = conn
            .prepare(
                "SELECT id FROM photos
                 WHERE deleted_at IS NOT NULL
                   AND datetime(deleted_at, '+30 days') < datetime('now')",
            )
            .map_err(|e| format!("prepare scan query: {e}"))?;
        stmt.query_map([], |row| row.get::<_, hopnet_common::CustomUUID>(0))
            .map_err(|e| format!("scan query: {e}"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("collect scan results: {e}"))?
    };

    if expired_ids.is_empty() {
        return Ok(());
    }

    tracing::info!(
        "photo_tombstone_cleanup: {} expired tombstones found",
        expired_ids.len(),
    );

    // Scan cutoff — recorded once so all batches in this run share the
    // same determinism boundary. On replay, this is payload data, not
    // a per-validator clock.
    let scan_cutoff = chrono::Utc::now().to_rfc3339();

    let mut txs = Vec::with_capacity(expired_ids.len().div_ceil(CLEANUP_BATCH_SIZE));
    for chunk in expired_ids.chunks(CLEANUP_BATCH_SIZE) {
        let payload = bincode::serde::encode_to_vec(
            &PhotoCleanupExpiredPayload {
                photo_ids: chunk.to_vec(),
                scan_cutoff: scan_cutoff.clone(),
            },
            bincode::config::standard(),
        )
        .map_err(|e| format!("encode cleanup payload: {e}"))?;

        txs.push(TxSpec {
            function: "photo_cleanup_expired",
            payload,
            signer: TxSigner::Node,
        });
    }

    let results = caps.txs.submit_batch(txs).await;
    let mut failures = 0usize;
    for (i, r) in results.into_iter().enumerate() {
        if let Err(e) = r {
            tracing::error!("photo_tombstone_cleanup: batch {} failed: {:?}", i, e,);
            failures += 1;
        }
    }

    if failures > 0 {
        Err(format!("{failures} batch submission(s) failed"))
    } else {
        Ok(())
    }
}
