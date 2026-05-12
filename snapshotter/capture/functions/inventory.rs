use std::collections::BTreeMap;

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;

use super::super::fixtures::FixtureContext;
use super::helpers::wrap;
use crate::schema::FunctionResult;

pub fn capture(
    pool: &Pool<SqliteConnectionManager>,
    ctx: &FixtureContext,
    results: &mut BTreeMap<String, FunctionResult>,
) {
    use hopnet::db::inventory;

    for node_id in 0..3i32 {
        let key = format!(
            "db::inventory::compute_inventory_differential(node={})",
            node_id
        );
        results.insert(key, {
            match inventory::compute_inventory_differential(pool.get(), node_id) {
                Ok(mut diff) => {
                    // Sort for determinism — EXCEPT queries have non-deterministic ordering
                    diff.fragments_added
                        .sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
                    diff.fragments_removed
                        .sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
                    FunctionResult::Ok {
                        value: serde_json::to_value(&diff).unwrap(),
                    }
                }
                Err(e) => FunctionResult::Error {
                    error_variant: format!("{:?}", e),
                },
            }
        });
    }

    if !ctx.fragment_hashes.is_empty() {
        results.insert(
            "db::inventory::batch_query_fragment_inventory".into(),
            wrap(|| {
                inventory::batch_query_fragment_inventory(pool.get(), &ctx.fragment_hashes, Some(3))
            }),
        );
    }
}
