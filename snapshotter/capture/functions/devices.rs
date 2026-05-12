use std::collections::BTreeMap;

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use serde::Serialize;

use super::super::fixtures::FixtureContext;
use crate::schema::FunctionResult;

pub fn capture(
    pool: &Pool<SqliteConnectionManager>,
    ctx: &FixtureContext,
    results: &mut BTreeMap<String, FunctionResult>,
) {
    use hopnet::db::devices;

    let conn = pool.get().unwrap();

    // DeviceTokenRecord doesn't implement Serialize
    if let Some(device_id) = ctx.device_ids.first() {
        results.insert("db::devices::get_device_by_id".into(), {
            match devices::get_device_by_id(&conn, device_id) {
                Ok(Some(record)) => {
                    #[derive(Serialize)]
                    struct DeviceProxy {
                        id: String,
                        user_id: i32,
                        api_key_hash: String,
                    }
                    FunctionResult::Ok {
                        value: serde_json::to_value(DeviceProxy {
                            id: record.id.to_string(),
                            user_id: record.user_id,
                            api_key_hash: record.api_key_hash.to_hex(),
                        })
                        .unwrap(),
                    }
                }
                Ok(None) => FunctionResult::Ok {
                    value: serde_json::Value::Null,
                },
                Err(e) => FunctionResult::Error {
                    error_variant: format!("{:?}", e),
                },
            }
        });
    }

    // DeviceListRecord doesn't implement Serialize
    for user_id in [0, 1] {
        let key = format!("db::devices::get_devices_for_user(user={})", user_id);
        results.insert(key, {
            match devices::get_devices_for_user(&conn, user_id) {
                Ok(records) => {
                    #[derive(Serialize)]
                    struct DeviceListProxy {
                        id: String,
                        encrypted_device_name: String,
                    }
                    let proxies: Vec<DeviceListProxy> = records
                        .into_iter()
                        .map(|r| DeviceListProxy {
                            id: r.id.to_string(),
                            encrypted_device_name: r.encrypted_device_name,
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
