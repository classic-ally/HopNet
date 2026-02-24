use std::collections::BTreeMap;

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;

use crate::schema::FunctionResult;
use super::helpers::wrap;

pub fn capture(pool: &Pool<SqliteConnectionManager>, results: &mut BTreeMap<String, FunctionResult>) {
    use hopnet::db::users;

    results.insert("db::users::get_users".into(), wrap(|| {
        users::get_users(pool.get())
    }));

    results.insert("db::users::get_user_by_username(alice)".into(), wrap(|| {
        users::get_user_by_username(pool.get(), "alice".to_string())
    }));

    results.insert("db::users::get_user_by_username(nonexistent)".into(), wrap(|| {
        users::get_user_by_username(pool.get(), "nonexistent".to_string())
    }));

    results.insert("db::users::get_user_by_userid(user=0)".into(), wrap(|| {
        users::get_user_by_userid(pool.get(), 0)
    }));

    results.insert("db::users::get_user_by_userid(user=1)".into(), wrap(|| {
        users::get_user_by_userid(pool.get(), 1)
    }));
}
