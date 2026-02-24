use std::collections::BTreeMap;

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use serde::Serialize;

use crate::schema::FunctionResult;

/// Capture time-windowed fragment functions. Called AFTER metrics have been time-shifted.
pub fn capture_time_windowed(pool: &Pool<SqliteConnectionManager>, results: &mut BTreeMap<String, FunctionResult>) {
    use hopnet::db::fragments;

    // get_node_availability_classification uses datetime('now', '-N days')
    for node_id in 0..3i32 {
        let key = format!("db::fragments::get_node_availability_classification(node={},days=30)", node_id);
        results.insert(key, {
            match fragments::get_node_availability_classification(pool.get(), node_id, 30) {
                Ok((score, class)) => {
                    #[derive(Serialize)]
                    struct ClassificationProxy {
                        score: f64,
                        class: String,
                    }
                    FunctionResult::Ok {
                        value: serde_json::to_value(ClassificationProxy {
                            score,
                            class: format!("{:?}", class),
                        }).unwrap(),
                    }
                }
                Err(e) => FunctionResult::Error { error_variant: format!("{:?}", e) },
            }
        });
    }
}
