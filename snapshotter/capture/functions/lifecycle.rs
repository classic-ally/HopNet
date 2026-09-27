use std::collections::BTreeMap;

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use serde::Serialize;

use crate::schema::FunctionResult;

fn storage_wrap<T: Serialize>(r: Result<T, hopnet_storage::StorageError>) -> FunctionResult {
    match r {
        Ok(value) => FunctionResult::Ok {
            value: serde_json::to_value(&value).unwrap(),
        },
        Err(e) => FunctionResult::Error {
            error_variant: format!("{:?}", e),
        },
    }
}

/// RFC-STORAGE-003 lifecycle reads (hopnet_storage::lifecycle). The
/// fixture never runs the block-apply hook, so the transition record is
/// empty here: these pin the read shapes (None / false / None) and diff
/// loudly if a fixture change starts populating the record.
pub fn capture(
    pool: &Pool<SqliteConnectionManager>,
    results: &mut BTreeMap<String, FunctionResult>,
) {
    use hopnet_storage::lifecycle;

    let conn = match pool.get() {
        Ok(c) => c,
        Err(e) => {
            results.insert(
                "storage::lifecycle::latest_transition_height".into(),
                FunctionResult::Error {
                    error_variant: format!("{:?}", e),
                },
            );
            return;
        }
    };

    results.insert(
        "storage::lifecycle::latest_transition_height".into(),
        storage_wrap(lifecycle::latest_transition_height(&conn)),
    );
    results.insert(
        "storage::lifecycle::transition_in(0,5)".into(),
        storage_wrap(lifecycle::transition_in(&conn, 0, 5)),
    );
    results.insert(
        "storage::lifecycle::snapshot_at(5)".into(),
        storage_wrap(lifecycle::snapshot_at(&conn, 5)),
    );

    // RFC-STORAGE-003 S7 pane reads (hopnet_storage::observe). Height-based,
    // so deterministic at a fixed tip — unlike unplaced-by-age, which keys
    // on the wall clock and is deliberately not captured.
    use hopnet_storage::observe;
    results.insert(
        "storage::observe::lifecycle_counts(t=5)".into(),
        storage_wrap(observe::lifecycle_counts(&conn, Some(5))),
    );
    results.insert(
        "storage::observe::in_flight_age_buckets(tip=100)".into(),
        storage_wrap(observe::in_flight_age_buckets(&conn, 100)),
    );
    results.insert(
        "storage::observe::verification_by_node(tip=100,window=1024)".into(),
        storage_wrap(observe::verification_by_node(&conn, 100, 1024)),
    );
}
