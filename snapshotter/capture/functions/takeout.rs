use std::collections::BTreeMap;

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;

use super::helpers::wrap;
use crate::schema::FunctionResult;

pub fn capture(
    pool: &Pool<SqliteConnectionManager>,
    results: &mut BTreeMap<String, FunctionResult>,
) {
    use hopnet::db::takeout;

    results.insert(
        "db::takeout::has_active_takeout(user=0)".into(),
        wrap(|| takeout::has_active_takeout(pool.get(), Some(0))),
    );

    results.insert(
        "db::takeout::has_active_takeout(user=1)".into(),
        wrap(|| takeout::has_active_takeout(pool.get(), Some(1))),
    );

    results.insert(
        "db::takeout::calculate_user_data_size(user=0)".into(),
        wrap(|| takeout::calculate_user_data_size(pool.get(), 0)),
    );

    results.insert(
        "db::takeout::get_takeouts_by_user(user=0)".into(),
        wrap(|| takeout::get_takeouts_by_user(pool.get(), 0)),
    );

    results.insert(
        "db::takeout::get_expired_takeouts_needing_status_update".into(),
        wrap(|| takeout::get_expired_takeouts_needing_status_update(pool.get())),
    );
}
