use std::collections::BTreeMap;

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;

use super::helpers::wrap;
use crate::schema::FunctionResult;

pub fn capture(
    pool: &Pool<SqliteConnectionManager>,
    results: &mut BTreeMap<String, FunctionResult>,
) {
    use hopnet::db::metrics;

    results.insert(
        "db::metrics::get_metric".into(),
        wrap(|| metrics::get_metric(pool.get())),
    );

    results.insert(
        "db::metrics::get_nodes_to_measure(exclude=0)".into(),
        wrap(|| metrics::get_nodes_to_measure(pool.get(), 0)),
    );
}

/// Capture time-windowed metric functions. Called AFTER metrics have been time-shifted.
pub fn capture_time_windowed(
    pool: &Pool<SqliteConnectionManager>,
    results: &mut BTreeMap<String, FunctionResult>,
) {
    use hopnet::db::metrics;

    results.insert(
        "db::metrics::get_all_node_metrics(height=5)".into(),
        wrap(|| metrics::get_all_node_metrics(pool.get(), 5)),
    );
}
