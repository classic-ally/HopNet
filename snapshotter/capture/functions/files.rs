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
    use hopnet::db::files;

    let siv_key = ctx.siv_key.as_ref().unwrap();
    let siv_nonce = ctx.siv_nonce.as_ref().unwrap();

    // get_files for root path, user 0
    let encrypted_root = {
        use aes_siv::{
            Aes256SivAead,
            aead::{Aead, KeyInit},
        };
        let cipher = Aes256SivAead::new(siv_key);
        let enc = cipher.encrypt(siv_nonce, b"root".as_slice()).unwrap();
        format!("/{}", hex::encode(enc))
    };

    results.insert(
        "db::files::get_files(user=0,path=/root)".into(),
        wrap(|| {
            files::get_files(pool.get(), encrypted_root.clone(), 0, siv_key, siv_nonce).map(
                |mut items| {
                    items.sort_by(|a, b| a.path.cmp(&b.path));
                    items
                },
            )
        }),
    );

    results.insert(
        "db::files::get_local_fragment_count".into(),
        wrap(|| files::get_local_fragment_count(pool.get())),
    );

    // get_file_access for first data block, user 0
    if let Some(db_id) = ctx.data_block_ids.first() {
        let conn = pool.get().unwrap();
        results.insert(
            "db::files::get_file_access(user=0)".into(),
            wrap(|| files::get_file_access(&conn, db_id, 0)),
        );
    }
}
