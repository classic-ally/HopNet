use std::collections::BTreeMap;

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;

use crate::schema::FunctionResult;
use super::helpers::wrap;

pub fn capture(pool: &Pool<SqliteConnectionManager>, results: &mut BTreeMap<String, FunctionResult>) {
    use hopnet::db::resilience;

    results.insert("db::resilience::compute_network_resilience_stats".into(), {
        match resilience::compute_network_resilience_stats(pool.get()) {
            Ok(mut stats) => {
                stats.computation_time_ms = 0;
                FunctionResult::Ok {
                    value: serde_json::to_value(&stats).unwrap(),
                }
            }
            Err(e) => FunctionResult::Error {
                error_variant: format!("{:?}", e),
            },
        }
    });

    results.insert("db::resilience::get_node_storage_baselines".into(), wrap(|| {
        resilience::get_node_storage_baselines(pool.get())
    }));

    // generate_fault_tolerance_curve takes baselines + threshold, not a DB connection
    results.insert("db::resilience::generate_fault_tolerance_curve".into(), {
        match resilience::get_node_storage_baselines(pool.get()) {
            Ok(baselines) => {
                let curve = resilience::generate_fault_tolerance_curve(baselines, 0.5);
                FunctionResult::Ok {
                    value: serde_json::to_value(&curve).unwrap(),
                }
            }
            Err(e) => FunctionResult::Error {
                error_variant: format!("{:?}", e),
            },
        }
    });
}
