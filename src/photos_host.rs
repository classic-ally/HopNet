//! Photos projection — host-side wiring (RFC-011 Phase 1).
//!
//! Thin apalis wrapper for the crate's tombstone cleanup job. Every node
//! runs this cron independently with randomized offsets to desynchronize
//! scans. Pattern matches takeout_host.rs.

use std::str::FromStr;

use apalis::prelude::WorkerFactoryFn;
use rand::RngExt;

/// Cron schedule: randomized daily scan (the 30-day recovery window makes
/// more frequent scanning wasteful; randomized seconds/minutes/hours
/// desynchronize nodes).
pub fn spawn_tombstone_cleanup_worker(app_state: crate::AppState) {
    let random_second = rand::rng().random_range(5..55);
    let random_minute = rand::rng().random_range(0..60);
    let random_hour = rand::rng().random_range(0..24);
    let cron_expr = format!("{} {} {} * * *", random_second, random_minute, random_hour);
    let schedule = apalis_cron::Schedule::from_str(&cron_expr).unwrap();
    let stream = apalis_cron::CronStream::new(schedule);

    let worker = apalis::prelude::WorkerBuilder::new("photo-tombstone-cleanup")
        .data(app_state)
        .backend(stream)
        .build_fn(handle_photo_tombstone_cleanup);

    tokio::spawn(async move {
        worker.run().await;
    });
}

pub async fn handle_photo_tombstone_cleanup(
    _job: PhotoTombstoneCleanupJob,
    _ctx: apalis_cron::CronContext<chrono::Utc>,
    data: apalis::prelude::Data<crate::AppState>,
) -> Result<(), apalis::prelude::Error> {
    let caps = crate::capabilities::build_capabilities(&data);
    hopnet_photos::jobs::run_photo_tombstone_cleanup(&caps)
        .await
        .map_err(|msg| {
            apalis::prelude::Error::Failed(std::sync::Arc::new(Box::new(
                std::io::Error::other(msg),
            )))
        })
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct PhotoTombstoneCleanupJob;
