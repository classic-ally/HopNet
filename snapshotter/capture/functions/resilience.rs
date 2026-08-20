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

    // Replaces the old compute_network_resilience_stats capture. Member ids
    // come from the storage view rather than a metrics.available subquery, so
    // this exercises the durable predicate. Still deterministic given the DB:
    // the availability grid is anchored to the newest replicated
    // metrics.start_time, never to wall clock.
    //
    // Deliberately NOT capturing unplaced_age_buckets — its cutoffs are
    // derived from Utc::now(), so it would diff on every run and tell you
    // nothing about a commit.
    results.insert("db::resilience::resilience_level_rows".into(), {
        match pool.get() {
            Ok(conn) => {
                let members = hopnet::storage_host::substrate_host::storage_view_with_conn(&conn)
                    .map(|v| v.members.iter().map(|p| p.node_id).collect::<Vec<_>>())
                    .unwrap_or_default();
                match resilience::resilience_level_rows(&conn, &members) {
                    Ok(levels) => FunctionResult::Ok {
                        value: serde_json::to_value(&levels).unwrap(),
                    },
                    Err(e) => FunctionResult::Error {
                        error_variant: format!("{:?}", e),
                    },
                }
            }
            Err(e) => FunctionResult::Error {
                error_variant: format!("{:?}", e),
            },
        }
    });

    // One checkout for both entries: get_node_storage_baselines borrows a
    // connection now, and the capture pool is max_size(1).
    match pool.get() {
        Ok(conn) => {
            results.insert(
                "db::resilience::get_node_storage_baselines".into(),
                wrap(|| resilience::get_node_storage_baselines(&conn)),
            );

            // generate_fault_tolerance_curve takes baselines + threshold, not a DB connection
            results.insert("db::resilience::generate_fault_tolerance_curve".into(), {
                match resilience::get_node_storage_baselines(&conn) {
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
        Err(e) => {
            for name in [
                "db::resilience::get_node_storage_baselines",
                "db::resilience::generate_fault_tolerance_curve",
            ] {
                results.insert(
                    name.into(),
                    FunctionResult::Error {
                        error_variant: format!("{:?}", e),
                    },
                );
            }
        }
    }
}
