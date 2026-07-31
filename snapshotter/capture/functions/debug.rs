use std::collections::BTreeMap;

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use serde::Serialize;

use crate::schema::FunctionResult;

pub fn capture(
    pool: &Pool<SqliteConnectionManager>,
    results: &mut BTreeMap<String, FunctionResult>,
) {
    use hopnet::db::debug;

    // StateSnapshot doesn't implement Serialize, use proxy
    results.insert("db::debug::compute_state_snapshot".into(), {
        match debug::compute_state_snapshot(pool.get()) {
            Ok(snapshot) => {
                #[derive(Serialize)]
                struct TableHashProxy {
                    name: String,
                    hash: String,
                    row_count: usize,
                }
                #[derive(Serialize)]
                struct StateSnapshotProxy {
                    consensus_height: u64,
                    committed_view: u64,
                    tables: Vec<TableHashProxy>,
                }
                let mut tables: Vec<TableHashProxy> = snapshot
                    .table_hashes
                    .into_iter()
                    .map(|(name, info)| TableHashProxy {
                        name,
                        hash: info.hash.to_hex(),
                        row_count: info.row_count,
                    })
                    .collect();
                tables.sort_by(|a, b| a.name.cmp(&b.name));
                let proxy = StateSnapshotProxy {
                    consensus_height: snapshot.consensus_height,
                    committed_view: snapshot.committed_view,
                    tables,
                };
                FunctionResult::Ok {
                    value: serde_json::to_value(proxy).unwrap(),
                }
            }
            Err(e) => FunctionResult::Error {
                error_variant: format!("{:?}", e),
            },
        }
    });
}
