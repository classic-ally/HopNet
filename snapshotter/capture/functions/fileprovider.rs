use std::collections::BTreeMap;

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use serde::Serialize;

use crate::schema::FunctionResult;
use super::helpers::wrap;
use super::super::fixtures::FixtureContext;

pub fn capture(pool: &Pool<SqliteConnectionManager>, ctx: &FixtureContext, results: &mut BTreeMap<String, FunctionResult>) {
    use hopnet::db::fileprovider;

    let siv_key = ctx.siv_key.as_ref().unwrap();
    let siv_nonce = ctx.siv_nonce.as_ref().unwrap();
    let encrypted_root = ctx.encrypted_root.as_ref().unwrap();

    // get_folder_contents — returns FileProviderEnumerateResult (not Serialize)
    {
        let parent_pattern = format!("{}/%", encrypted_root);
        results.insert("db::fileprovider::get_folder_contents(user=0,path=/root)".into(), {
            match fileprovider::get_folder_contents(pool.get(), 0, &parent_pattern, siv_key, siv_nonce, None, 100) {
                Ok(result) => {
                    #[derive(Serialize)]
                    struct ItemProxy {
                        identifier: String,
                        item_type: String,
                        filename: String,
                        parent_item_identifier: String,
                        file_size: Option<u64>,
                        modification_height: Option<i32>,
                    }
                    #[derive(Serialize)]
                    struct EnumerateProxy {
                        items: Vec<ItemProxy>,
                        current_consensus_height: i32,
                        deleted_identifiers: Option<Vec<String>>,
                    }
                    let mut items: Vec<ItemProxy> = result.items.into_iter().map(|item| ItemProxy {
                        identifier: item.identifier,
                        item_type: format!("{:?}", item.item_type),
                        filename: item.filename,
                        parent_item_identifier: item.parent_item_identifier,
                        file_size: item.file_size,
                        modification_height: item.modification_height,
                    }).collect();
                    items.sort_by(|a, b| a.identifier.cmp(&b.identifier).then(a.modification_height.cmp(&b.modification_height)));
                    let proxy = EnumerateProxy {
                        items,
                        current_consensus_height: result.current_consensus_height,
                        deleted_identifiers: result.deleted_identifiers,
                    };
                    FunctionResult::Ok {
                        value: serde_json::to_value(proxy).unwrap(),
                    }
                }
                Err(e) => FunctionResult::Error { error_variant: format!("{:?}", e) },
            }
        });
    }

    // get_folder_changes_since_height
    {
        results.insert("db::fileprovider::get_folder_changes_since_height(user=0,height=0)".into(), {
            match fileprovider::get_folder_changes_since_height(pool.get(), 0, encrypted_root, 0, siv_key, siv_nonce) {
                Ok(result) => {
                    #[derive(Serialize)]
                    struct ItemProxy {
                        identifier: String,
                        item_type: String,
                        filename: String,
                        parent_item_identifier: String,
                        file_size: Option<u64>,
                        modification_height: Option<i32>,
                    }
                    #[derive(Serialize)]
                    struct EnumerateProxy {
                        items: Vec<ItemProxy>,
                        current_consensus_height: i32,
                        deleted_identifiers: Option<Vec<String>>,
                    }
                    let mut items: Vec<ItemProxy> = result.items.into_iter().map(|item| ItemProxy {
                        identifier: item.identifier,
                        item_type: format!("{:?}", item.item_type),
                        filename: item.filename,
                        parent_item_identifier: item.parent_item_identifier,
                        file_size: item.file_size,
                        modification_height: item.modification_height,
                    }).collect();
                    items.sort_by(|a, b| a.identifier.cmp(&b.identifier).then(a.modification_height.cmp(&b.modification_height)));
                    let proxy = EnumerateProxy {
                        items,
                        current_consensus_height: result.current_consensus_height,
                        deleted_identifiers: result.deleted_identifiers,
                    };
                    FunctionResult::Ok {
                        value: serde_json::to_value(proxy).unwrap(),
                    }
                }
                Err(e) => FunctionResult::Error { error_variant: format!("{:?}", e) },
            }
        });
    }

    // get_item_metadata_by_inode_id — returns tuple (not directly serializable as named fields)
    if let Some(file_id) = ctx.file_ids.first() {
        results.insert("db::fileprovider::get_item_metadata_by_inode_id".into(), {
            match fileprovider::get_item_metadata_by_inode_id(pool.get(), file_id.clone(), 0) {
                Ok((path, inode_type, file_size, created, modified, height)) => {
                    #[derive(Serialize)]
                    struct MetadataProxy {
                        path: String,
                        inode_type: String,
                        file_size: Option<u64>,
                        created_at: String,
                        modified_at: Option<String>,
                        modification_height: Option<i32>,
                    }
                    FunctionResult::Ok {
                        value: serde_json::to_value(MetadataProxy {
                            path,
                            inode_type: format!("{:?}", inode_type),
                            file_size,
                            created_at: format!("{:?}", created),
                            modified_at: modified.map(|m| format!("{:?}", m)),
                            modification_height: height,
                        }).unwrap(),
                    }
                }
                Err(e) => FunctionResult::Error { error_variant: format!("{:?}", e) },
            }
        });
    }

    // get_file_path_by_data_id
    if let Some(data_id) = ctx.data_block_ids.first() {
        results.insert("db::fileprovider::get_file_path_by_data_id".into(), wrap(|| {
            fileprovider::get_file_path_by_data_id(pool.get(), data_id.clone(), 0)
        }));
    }

    // get_inode_id_by_path
    results.insert("db::fileprovider::get_inode_id_by_path(path=/root)".into(), wrap(|| {
        fileprovider::get_inode_id_by_path(pool.get(), encrypted_root, 0)
    }));

    // is_folder_empty
    results.insert("db::fileprovider::is_folder_empty(path=/root)".into(), wrap(|| {
        fileprovider::is_folder_empty(pool.get(), encrypted_root, 0)
    }));
}
