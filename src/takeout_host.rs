//! Host adapter for the takeout service (RFC-015 Stage D5b).
//!
//! `TakeoutHost` implements [`hopnet_takeout::TakeoutHooks`] — the domains
//! that stay host-side (users onboarding tx, storage metrics, projection
//! sizing SQL) — and `takeout_state()` assembles a
//! [`hopnet_takeout::TakeoutState`] on the fly (cheap Arc clones; the
//! mutable runtime — resume registry + barriers — is the single instance on
//! AppState, so every construction shares it). Sessions and consensus
//! submission reuse the same `DriveHost` seam impls behind
//! `drive_host::drive_state` — they are hopnet-projection traits now.

use std::sync::Arc;

use hopnet_projection::host::BoxFuture;
use hopnet_takeout::{TakeoutHooks, TakeoutState};

use crate::AppState;

pub struct TakeoutHost {
    app_state: AppState,
}

impl TakeoutHooks for TakeoutHost {
    fn import_completed(&self, user_id: i32) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(async move {
            // Onboarding bits — additive only (clear=NONE preserves any other
            // bits the user has accumulated on other devices).
            crate::users::helpers::submit_onboarding_update(
                &self.app_state,
                user_id,
                hopnet_common::OnboardingFlags::IMPORT_OFFERED
                    | hopnet_common::OnboardingFlags::IMPORT_COMPLETED,
                hopnet_common::OnboardingFlags::NONE,
            )
            .await
        })
    }

    fn available_storage_bytes(&self) -> BoxFuture<'_, Result<u64, String>> {
        Box::pin(async move {
            // Height read + validator aggregation exactly as the pre-split
            // quota check did (src/takeout/import.rs::check_quota).
            let height = {
                let conn = self
                    .app_state
                    .db_pool
                    .get()
                    .map_err(|_| "db pool".to_string())?;
                let tx = conn
                    .unchecked_transaction()
                    .map_err(|_| "tx open".to_string())?;
                crate::db::consensus::get_current_consensus_height(&tx)
                    .map_err(|e| format!("consensus height: {:?}", e))?
            };

            crate::db::imports::get_total_validator_storage_available(&self.app_state, height)
                .await
                .map_err(|e| format!("validator storage: {:?}", e))
        })
    }

    fn node_available_storage_bytes(&self) -> BoxFuture<'_, Result<Option<u64>, String>> {
        Box::pin(async move {
            let node_id = self
                .app_state
                .get_node_id()
                .map_err(|_| "node id unavailable".to_string())?;
            crate::db::takeout::get_node_available_storage(
                self.app_state.db_pool.get(),
                &self.app_state,
                node_id,
            )
            .await
            .map_err(|e| format!("node storage: {:?}", e))
        })
    }

    fn user_data_size_bytes(&self, user_id: i32) -> BoxFuture<'_, Result<u64, String>> {
        Box::pin(async move {
            crate::db::takeout::calculate_user_data_size(self.app_state.db_pool.get(), user_id)
                .map_err(|e| format!("user data size: {:?}", e))
        })
    }
}

/// Build the takeout service state over this host. Cheap — Arc clones plus
/// one `DriveHost` + one `TakeoutHost` allocation; callers may construct it
/// on the fly (the work scheduler and auth hooks do).
pub fn takeout_state(app_state: &AppState) -> TakeoutState {
    let drive = crate::drive_host::drive_state(app_state);
    let exporters: Vec<Arc<dyn hopnet_projection::ProjectionExporter>> =
        vec![hopnet_drive::exporter::drive_exporter(drive.clone())];
    TakeoutState {
        db_pool: app_state.db_pool.clone(),
        fragments_dir: app_state.fragments_dir.clone(),
        node_id: app_state.node_id.clone(),
        sessions: drive.sessions.clone(),
        txs: drive.txs.clone(),
        exporters: exporters.into(),
        runtime: app_state.takeout_runtime.clone(),
        hooks: Arc::new(TakeoutHost {
            app_state: app_state.clone(),
        }),
    }
}

// The takeout barrier registration stays a HOST shim pointing into the
// crate-owned TakeoutRuntime (the barrier HTTP test routes are host-side).
inventory::submit! {
    &crate::barriers::BarrierRegistration {
        subsystem: "takeout",
        accessor: |state: &crate::AppState| &state.takeout_runtime.barriers,
        names: hopnet_takeout::barriers::ALL_BARRIER_NAMES,
    }
}

/// apalis wrapper for the crate's maintenance job — cron wiring stays in
/// main.rs, error mapping onto apalis' type stays here.
pub async fn handle_takeout_maintenance(
    _job: TakeoutMaintenanceJob,
    _ctx: apalis_cron::CronContext<chrono::Utc>,
    data: apalis::prelude::Data<AppState>,
) -> Result<(), apalis::prelude::Error> {
    hopnet_takeout::jobs::run_takeout_maintenance(&takeout_state(&data)).await.map_err(|msg| {
        apalis::prelude::Error::Failed(std::sync::Arc::new(Box::new(std::io::Error::other(msg))))
    })
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct TakeoutMaintenanceJob;
