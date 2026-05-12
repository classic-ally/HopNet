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
    use hopnet::db::documentprovider;

    let conn = pool.get().unwrap();
    let siv_key = ctx.siv_key.as_ref().unwrap();
    let siv_nonce = ctx.siv_nonce.as_ref().unwrap();
    let encrypted_root = ctx.encrypted_root.as_ref().unwrap();

    // get_item — DocumentProviderItem implements Serialize
    if let Some(file_id) = ctx.file_ids.first() {
        results.insert(
            "db::documentprovider::get_item".into(),
            wrap(|| documentprovider::get_item(&conn, file_id, 0, siv_key, siv_nonce)),
        );
    }

    // get_download_metadata — returns (String, InodeType) tuple
    if let Some(file_id) = ctx.file_ids.first() {
        results.insert(
            "db::documentprovider::get_download_metadata".into(),
            wrap(|| documentprovider::get_download_metadata(&conn, file_id, 0)),
        );
    }

    // get_path_by_inode_id
    if let Some(file_id) = ctx.file_ids.first() {
        results.insert(
            "db::documentprovider::get_path_by_inode_id".into(),
            wrap(|| documentprovider::get_path_by_inode_id(&conn, file_id, 0)),
        );
    }

    // get_children — Vec<DocumentProviderItem> implements Serialize
    if let Some(root_id) = &ctx.root_folder_id {
        results.insert(
            "db::documentprovider::get_children(user=0,path=/root)".into(),
            wrap(|| {
                documentprovider::get_children(
                    &conn,
                    0,
                    encrypted_root,
                    siv_key,
                    siv_nonce,
                    Some(root_id.clone()),
                )
            }),
        );
    }
}
