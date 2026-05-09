use apalis::prelude::*;
use apalis_cron::CronContext;
use serde::{Deserialize, Serialize};
use chrono::Utc;
use std::sync::Arc;
use crate::AppState;
use crate::db::{CustomUUID, takeout::TakeoutStatusPayload};
use crate::consensus::Transaction;
use hopnet_common::TakeoutStatus;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TakeoutMaintenanceJob;

/// Handler for takeout maintenance (runs every 4-6 hours with randomization)
/// Handles expiration checking and orphaned cleanup as a safety net for edge cases
pub async fn handle_takeout_maintenance(
    _job: TakeoutMaintenanceJob,
    _ctx: CronContext<Utc>,
    data: Data<AppState>,
) -> Result<(), Error> {
    let app_state = &*data;

    tracing::info!("Starting takeout maintenance job");

    // Get user ID for consensus submissions
    let user_id = match app_state.get_user_id() {
        Ok(id) => id,
        Err(_) => {
            tracing::warn!("User ID not initialized, skipping takeout maintenance");
            return Ok(());
        }
    };

    // Step 1: Find and mark expired takeouts (network-wide, not just this node's)
    let expired_takeouts = match crate::db::takeout::get_expired_takeouts_needing_status_update(
        app_state.db_pool.get()
    ) {
        Ok(takeouts) => takeouts,
        Err(e) => {
            tracing::error!("Failed to get expired takeouts: {:?}", e);
            return Err(Error::Failed(Arc::new(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to get expired takeouts: {:?}", e)
            )))));
        }
    };

    if expired_takeouts.is_empty() {
        tracing::debug!("No expired takeouts found needing status update");
    } else {
        tracing::info!("Found {} expired takeouts needing status update", expired_takeouts.len());

        // Batch all expiration updates into a single consensus submission
        let mut transactions = Vec::new();

        for takeout_id in &expired_takeouts {
            let status_payload = TakeoutStatusPayload {
                takeout_id: takeout_id.clone(),
                new_status: TakeoutStatus::Expired,
            };

            let encoded_payload = match bincode::serde::encode_to_vec(&status_payload, bincode::config::standard()) {
                Ok(data) => data,
                Err(e) => {
                    tracing::error!("Failed to encode expired status payload for takeout {}: {:?}", takeout_id, e);
                    continue; // Skip this one but continue with others
                }
            };

            transactions.push(crate::consensus::functions::create_signed_transaction(
                app_state,
                "update_takeout_status".to_string(),
                encoded_payload,
            ).map_err(|_| Error::Failed(Arc::new(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Failed to sign transaction"
            )))))?);
        }

        if transactions.is_empty() {
            tracing::warn!("No valid transactions to submit for expiration updates");
        } else {
            tracing::info!("Submitting {} expiration updates to consensus in single batch", transactions.len());

            // Submit all expiration updates in one consensus call
            // This triggers cleanup on owner nodes for all expired takeouts
            let results = app_state.consensus_queue.submit_batch(transactions).await;
            let failures: Vec<_> = results.iter().filter(|r| r.is_err()).collect();
            if failures.is_empty() {
                tracing::info!("Successfully marked {} takeouts as expired via consensus", expired_takeouts.len());
            } else {
                tracing::error!("Failed to submit expiration updates to consensus: {} failures", failures.len());
                return Err(Error::Failed(Arc::new(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to submit expiration updates: {} failures", failures.len())
                )))));
            }
        }
    }

    // TODO: Step 2: Handle stuck processing (takeouts that should have processed but didn't)
    // TODO: Step 3: Clean up orphaned files (files without corresponding database entries)

    tracing::info!("Takeout maintenance job completed");
    Ok(())
}

#[derive(Debug)]
pub enum TakeoutMaintenanceError {
    Database(crate::db::DatabaseError),
    Consensus(String),
}

impl From<crate::db::DatabaseError> for TakeoutMaintenanceError {
    fn from(e: crate::db::DatabaseError) -> Self {
        TakeoutMaintenanceError::Database(e)
    }
}

impl std::fmt::Display for TakeoutMaintenanceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TakeoutMaintenanceError::Database(e) => write!(f, "Database error: {:?}", e),
            TakeoutMaintenanceError::Consensus(e) => write!(f, "Consensus error: {}", e),
        }
    }
}

impl std::error::Error for TakeoutMaintenanceError {}

// ============================================================================
// Owner-restart import resume
// ============================================================================
//
// Imports can be stranded at status `Importing` if the owner process dies
// between the `Pending → Importing` flip and the terminal `Completed`. The
// bg task's session clone dies with the process; persistent session storage
// is intentionally deferred, so resume routes through the next user
// authentication event. Three session-establishment paths converge through
// `maybe_resume_for_user`: macOS keychain auto-load, POST /login, and
// device-token middleware lazy bootstrap.

use crate::db::import_paths;

/// Owner-startup scan. Reads `imports` rows in `Pending`/`Importing` status
/// owned by this node, filters to those whose extraction is far enough along
/// to resume (status == Importing AND `import_paths_{id}` table exists with
/// at least one Pending row), and stashes (user_id → import_id) into the
/// resume registry. Imports stuck mid-extraction are not auto-recoverable in
/// v1 — user re-uploads.
///
/// Returns the count of registered imports for log output.
pub async fn scan_at_startup(state: &AppState) -> Result<usize, crate::db::DatabaseError> {
    let self_node = match state.get_node_id() {
        Ok(n) => n,
        Err(_) => {
            tracing::debug!("scan_at_startup: node_id unset, skipping");
            return Ok(0);
        }
    };
    let conn = state
        .db_pool
        .get()
        .map_err(|_| crate::db::DatabaseError::LockError)?;

    let mut stmt = conn
        .prepare(
            "SELECT id, user_id, status FROM imports
             WHERE owner_node_id = ? AND status IN (0, 1)",
        )
        .map_err(|_| crate::db::DatabaseError::RecallError)?;
    let rows = stmt
        .query_map([self_node], |row| {
            let id: CustomUUID = row.get(0)?;
            let user_id: i32 = row.get(1)?;
            let status: hopnet_common::ImportStatus = row.get(2)?;
            Ok((id, user_id, status))
        })
        .map_err(|_| crate::db::DatabaseError::RecallError)?;

    let mut to_register: Vec<(i32, CustomUUID)> = Vec::new();
    for r in rows {
        let (import_id, user_id, status) =
            r.map_err(|_| crate::db::DatabaseError::RecallError)?;
        // Only `Importing` is resumable in v1. `Pending` means extraction
        // never reached the Importing flip; user must re-upload.
        if status != hopnet_common::ImportStatus::Importing {
            tracing::warn!(
                "Import {} for user {} stranded at {:?}; not resumable in v1",
                import_id, user_id, status
            );
            continue;
        }
        let counts = match import_paths::count_paths_by_status(&conn, &import_id) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!("count_paths_by_status failed for {}: {:?}", import_id, e);
                continue;
            }
        };
        if counts.total == 0 || counts.pending == 0 {
            tracing::warn!(
                "Import {} has no pending rows ({} total / {} pending); skipping resume",
                import_id, counts.total, counts.pending
            );
            continue;
        }
        to_register.push((user_id, import_id));
    }
    drop(stmt);
    drop(conn);

    let count = to_register.len();
    if count > 0 {
        let mut registry = state.takeout_runtime.resume_registry.lock().await;
        for (user_id, import_id) in to_register {
            registry.insert(user_id, import_id);
        }
    }
    Ok(count)
}

/// Hook fired immediately after a session insert. If the user has a stranded
/// import in the registry, drains it and spawns `run_creation_phase` to
/// finish the work. No-op when registry has no entry. Idempotent on repeat
/// auth events because the entry is removed atomically before the spawn.
pub async fn maybe_resume_for_user(state: AppState, user_id: i32) {
    let import_id: CustomUUID = {
        let mut registry = state.takeout_runtime.resume_registry.lock().await;
        match registry.remove(&user_id) {
            Some(id) => id,
            None => return,
        }
    };

    tracing::info!("Resuming stranded import {} for user {}", import_id, user_id);
    let staging = crate::takeout::import::staging_dir(&state, &import_id);

    let state_for_task = state.clone();
    let import_id_for_task = import_id.clone();
    tokio::spawn(async move {
        if let Err(e) = crate::takeout::import::run_creation_phase(
            &state_for_task,
            &import_id_for_task,
            user_id,
            &staging,
        )
        .await
        {
            tracing::error!(
                "Resume of import {} for user {} failed: {:?}",
                import_id_for_task, user_id, e
            );
            // On failure, re-stash so the next auth event tries again.
            state_for_task
                .takeout_runtime
                .resume_registry
                .lock()
                .await
                .insert(user_id, import_id_for_task);
        }
    });
}