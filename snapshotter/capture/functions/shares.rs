use std::collections::BTreeMap;

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use serde::Serialize;

use super::super::fixtures::FixtureContext;
use super::helpers::wrap;
use crate::schema::FunctionResult;

pub fn capture(
    pool: &Pool<SqliteConnectionManager>,
    ctx: &FixtureContext,
    results: &mut BTreeMap<String, FunctionResult>,
) {
    use hopnet::db::shares;

    // IncomingShareRow doesn't implement Serialize
    results.insert("db::shares::get_incoming_shares_for_user(user=1)".into(), {
        match shares::get_incoming_shares_for_user(pool.get(), 1) {
            Ok(shares_list) => {
                #[derive(Serialize)]
                struct ShareProxy {
                    id: String,
                    data_block_id: String,
                    sender_id: i32,
                    recipient_id: i32,
                    display_name: String,
                }
                let proxies: Vec<ShareProxy> = shares_list
                    .into_iter()
                    .map(|(row, display_name)| ShareProxy {
                        id: row.id.to_string(),
                        data_block_id: row.data_block_id.to_string(),
                        sender_id: row.sender_id,
                        recipient_id: row.recipient_id,
                        display_name,
                    })
                    .collect();
                FunctionResult::Ok {
                    value: serde_json::to_value(&proxies).unwrap(),
                }
            }
            Err(e) => FunctionResult::Error {
                error_variant: format!("{:?}", e),
            },
        }
    });

    results.insert(
        "db::shares::get_incoming_share_count(user=1)".into(),
        wrap(|| shares::get_incoming_share_count(pool.get(), 1)),
    );

    results.insert(
        "db::shares::get_incoming_share_count(user=0)".into(),
        wrap(|| shares::get_incoming_share_count(pool.get(), 0)),
    );

    // ShareMember doesn't implement Serialize
    if let Some(db_id) = ctx.data_block_ids.first() {
        results.insert("db::shares::get_share_details".into(), {
            match shares::get_share_details(pool.get(), db_id) {
                Ok(members) => {
                    #[derive(Serialize)]
                    struct MemberProxy {
                        user_id: i32,
                        username: String,
                    }
                    let proxies: Vec<MemberProxy> = members
                        .into_iter()
                        .map(|m| MemberProxy {
                            user_id: m.user_id,
                            username: m.username,
                        })
                        .collect();
                    FunctionResult::Ok {
                        value: serde_json::to_value(&proxies).unwrap(),
                    }
                }
                Err(e) => FunctionResult::Error {
                    error_variant: format!("{:?}", e),
                },
            }
        });
    }
}
