use hopnet::db::DatabaseError;
use serde::Serialize;

use crate::schema::FunctionResult;

/// Generic wrapper that calls a DB function and converts the result to FunctionResult.
/// The return type must implement Serialize.
pub fn wrap<T: Serialize, F: FnOnce() -> Result<T, DatabaseError>>(f: F) -> FunctionResult {
    match f() {
        Ok(value) => match serde_json::to_value(&value) {
            Ok(json) => FunctionResult::Ok { value: json },
            Err(e) => FunctionResult::Error {
                error_variant: format!("SerializationError: {}", e),
            },
        },
        Err(e) => FunctionResult::Error {
            error_variant: format!("{:?}", e),
        },
    }
}
