use std::collections::BTreeMap;

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::params;

use super::fixtures::FixtureContext;
use crate::schema::FunctionResult;

mod consensus;
mod debug;
mod devices;
mod documentprovider;
mod fileprovider;
mod files;
mod fragments;
mod helpers;
mod inventory;
mod metrics;
mod nodes;
mod resilience;
mod setup;
mod shares;
mod takeout;
mod users;

pub fn capture_all(
    pool: &Pool<SqliteConnectionManager>,
    ctx: &FixtureContext,
) -> BTreeMap<String, FunctionResult> {
    let mut results = BTreeMap::new();

    // Phase 1: Capture functions that read raw metric data (fixed timestamps, deterministic)
    metrics::capture(pool, &mut results);
    debug::capture(pool, &mut results);

    // Phase 2: Time-shift metrics into the live datetime('now') window.
    // Shifts all metric timestamps forward by (now - base_time) seconds so that
    // SQL expressions like datetime('now', '-7 days') include them.
    if let Some(base_time) = ctx.metrics_base_time {
        let now = chrono::Utc::now();
        let shift_seconds = (now - base_time).num_seconds();
        let conn = pool.get().expect("Failed to get connection for time-shift");
        conn.execute(
            "UPDATE metrics SET start_time = datetime(start_time, '+' || ? || ' seconds')",
            params![shift_seconds],
        )
        .expect("Failed to time-shift metrics");
    }

    // Phase 3: Capture time-windowed functions (metrics are now "recent")
    metrics::capture_time_windowed(pool, &mut results);
    fragments::capture_time_windowed(pool, &mut results);

    // Phase 4: All other captures (not time-sensitive)
    consensus::capture(pool, ctx, &mut results);
    resilience::capture(pool, &mut results);
    files::capture(pool, ctx, &mut results);
    inventory::capture(pool, ctx, &mut results);
    fileprovider::capture(pool, ctx, &mut results);
    documentprovider::capture(pool, ctx, &mut results);
    nodes::capture(pool, &mut results);
    users::capture(pool, &mut results);
    shares::capture(pool, ctx, &mut results);
    devices::capture(pool, ctx, &mut results);
    takeout::capture(pool, &mut results);
    setup::capture(pool, &mut results);

    results
}
