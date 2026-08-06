use std::collections::BTreeMap;

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;

use crate::schema::FunctionResult;

pub fn capture(
    pool: &Pool<SqliteConnectionManager>,
    results: &mut BTreeMap<String, FunctionResult>,
) {
    // NodeStateReport serializes directly (sections/tables are ordered
    // Vecs, hashes render as hex) — no proxy needed.
    results.insert("db::snapshot::state_manifest".into(), {
        match hopnet::db::snapshot::compute_node_state(pool.get()) {
            Ok(report) => FunctionResult::Ok {
                value: serde_json::to_value(report).unwrap(),
            },
            Err(e) => FunctionResult::Error {
                error_variant: format!("{:?}", e),
            },
        }
    });
}
