use std::collections::BTreeMap;

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;

use crate::schema::FunctionResult;
use super::helpers::wrap;

pub fn capture(pool: &Pool<SqliteConnectionManager>, results: &mut BTreeMap<String, FunctionResult>) {
    use hopnet::db::nodes;

    results.insert("db::nodes::get_nodes".into(), wrap(|| {
        nodes::get_nodes(pool.get())
    }));

    results.insert("db::nodes::get_next_node_id".into(), wrap(|| {
        nodes::get_next_node_id(pool.get())
    }));

    results.insert("db::nodes::node_exists(node=0)".into(), wrap(|| {
        nodes::node_exists(pool.get(), 0)
    }));

    results.insert("db::nodes::node_exists(node=99)".into(), wrap(|| {
        nodes::node_exists(pool.get(), 99)
    }));

    results.insert("db::nodes::get_all_nodes_as_connection_info(exclude=0)".into(), wrap(|| {
        nodes::get_all_nodes_as_connection_info(pool.get(), 0)
    }));
}
