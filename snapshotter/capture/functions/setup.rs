use std::collections::BTreeMap;

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;

use crate::schema::FunctionResult;

pub fn capture(
    pool: &Pool<SqliteConnectionManager>,
    results: &mut BTreeMap<String, FunctionResult>,
) {
    use hopnet::db::setup;

    results.insert("db::setup::get_initial_setup".into(), {
        match setup::get_initial_setup(pool.get()) {
            Ok(status) => FunctionResult::Ok {
                value: serde_json::to_value(status.as_u16()).unwrap(),
            },
            Err(e) => FunctionResult::Error {
                error_variant: format!("{:?}", e),
            },
        }
    });
}
