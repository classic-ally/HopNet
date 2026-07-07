//! Import DB shim (RFC-015 Stage D5b): the import-owned SQL moved to
//! `hopnet_takeout::db::imports`; re-exported here so host call sites and
//! tests are unchanged. The validator-storage aggregation below stays
//! HOST-side (it needs the host's validator set + metrics machinery) and
//! reaches the takeout core through `TakeoutHooks::available_storage_bytes`.

pub use hopnet_takeout::db::imports::*;

use crate::db::DatabaseError;

/// Sum the most recently reported available storage across the active
/// validator set at `height`. Used by the import upload quota check (via
/// the takeout hooks) to reject over-quota archives before consensus
/// submission.
///
/// Returns the **raw sum** in bytes — callers apply the RS expansion
/// multiplier (×3) and `STORAGE_SAFETY_MARGIN_BYTES` at the comparison site
/// per spec § 3.3.
///
/// Bootstrap fallback: a fresh mesh has no `metrics` rows until the cron
/// fires (~10 min). When aggregation finds zero, we fall back to the owner
/// node's filesystem capacity scaled by the validator count. Conservative
/// — assumes peers have similar capacity to self. Logs a warning so
/// operators can see the bootstrap path was hit.
pub async fn get_total_validator_storage_available(
    state: &crate::AppState,
    height: i32,
) -> Result<u64, DatabaseError> {
    let db_lock = state.db_pool.get().map_err(|_| DatabaseError::LockError)?;
    let validators = crate::db::consensus::get_validators_with_conn(&db_lock, height)?;
    if validators.is_empty() {
        return Ok(0);
    }

    let metrics_bytes: u64 = {
        let placeholders = validators.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let query = format!(
            "WITH latest AS (
                 SELECT to_node, storage_total_gb, storage_used_gb,
                        ROW_NUMBER() OVER (PARTITION BY to_node ORDER BY start_time DESC) AS rn
                 FROM metrics
                 WHERE to_node IN ({}) AND storage_total_gb IS NOT NULL
             )
             SELECT COALESCE(SUM(storage_total_gb - storage_used_gb), 0)
             FROM latest WHERE rn = 1",
            placeholders
        );
        let params: Vec<Box<dyn rusqlite::ToSql>> = validators
            .iter()
            .map(|v| Box::new(v.node_id) as Box<dyn rusqlite::ToSql>)
            .collect();
        let refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|p| p.as_ref()).collect();
        let total_gb: i64 = db_lock
            .query_row(&query, refs.as_slice(), |row| row.get(0))
            .map_err(|_| DatabaseError::RecallError)?;
        (total_gb.max(0) as u64) * 1024 * 1024 * 1024
    };
    if metrics_bytes > 0 {
        return Ok(metrics_bytes);
    }

    let validator_count = validators.len() as u64;
    drop(db_lock);
    tracing::warn!(
        "No validator storage metrics yet (height={}); bootstrapping quota from owner filesystem × {} validators",
        height,
        validator_count
    );
    match crate::metrics::routes::calculate_storage_usage(&state.fragments_dir).await {
        Ok(s) => {
            let per_node = (s.total_gb.saturating_sub(s.used_gb) as u64) * 1024 * 1024 * 1024;
            Ok(per_node.saturating_mul(validator_count))
        }
        Err(e) => {
            tracing::error!("Bootstrap fs storage calc failed: {:?}", e);
            Ok(0)
        }
    }
}
