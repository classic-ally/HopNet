use std::collections::BTreeMap;

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;

use super::helpers::wrap;
use crate::schema::FunctionResult;

pub fn capture(
    pool: &Pool<SqliteConnectionManager>,
    results: &mut BTreeMap<String, FunctionResult>,
) {
    use hopnet::db::resilience;

    const NAMES: [&str; 3] = [
        "db::resilience::resilience_level_rows",
        "db::resilience::get_node_storage_baselines",
        "db::resilience::generate_fault_tolerance_curve",
    ];
    let fail_all = |results: &mut BTreeMap<String, FunctionResult>, error_variant: String| {
        for name in NAMES {
            results.insert(
                name.into(),
                FunctionResult::Error {
                    error_variant: error_variant.clone(),
                },
            );
        }
    };

    // One checkout for every entry: the block/node counts borrow a
    // connection, and the capture pool is max_size(1).
    let conn = match pool.get() {
        Ok(conn) => conn,
        Err(e) => return fail_all(results, format!("{:?}", e)),
    };
    let counts = match resilience::BlockNodeCounts::build(&conn) {
        Ok(counts) => counts,
        Err(e) => return fail_all(results, format!("{:?}", e)),
    };

    // Replaces the old compute_network_resilience_stats capture. Member ids
    // come from the storage view rather than a metrics.available subquery, so
    // this exercises the durable predicate. Still deterministic given the DB:
    // the availability grid is anchored to the newest replicated
    // metrics.start_time, never to wall clock.
    //
    // Deliberately NOT capturing unplaced_age_buckets — its cutoffs are
    // derived from Utc::now(), so it would diff on every run and tell you
    // nothing about a commit.
    let members = hopnet::storage_host::substrate_host::storage_view_with_conn(&conn)
        .map(|v| v.members.iter().map(|p| p.node_id).collect::<Vec<_>>())
        .unwrap_or_default();
    results.insert(
        NAMES[0].into(),
        wrap(|| resilience::resilience_level_rows(&counts, &members)),
    );

    results.insert(
        NAMES[1].into(),
        wrap(|| resilience::get_node_storage_baselines(&counts)),
    );

    // generate_fault_tolerance_curve takes baselines + threshold, not a DB connection
    results.insert(
        NAMES[2].into(),
        wrap(|| {
            resilience::get_node_storage_baselines(&counts)
                .map(|baselines| resilience::generate_fault_tolerance_curve(baselines, 0.5))
        }),
    );
}
