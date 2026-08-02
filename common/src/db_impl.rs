// Database-specific implementations for common types
// Only included by the main binary, not the FileProvider

use super::{CustomUUID, ImportPathStatus, ImportStatus, InodeType, TakeoutStatus};
use crate::users::OnboardingFlags;
use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef};
use std::str::FromStr;
use uuid::Uuid;

impl ToSql for InodeType {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        let v: i32 = match self {
            InodeType::File => 0,
            InodeType::Folder => 1,
        };
        Ok(ToSqlOutput::from(v))
    }
}

impl FromSql for InodeType {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Integer(i) => match i as i32 {
                0 => Ok(InodeType::File),
                1 => Ok(InodeType::Folder),
                _ => Err(FromSqlError::InvalidType),
            },
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

impl ToSql for TakeoutStatus {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        let v: i32 = match self {
            TakeoutStatus::Pending => 0,
            TakeoutStatus::Materializing => 1,
            TakeoutStatus::Ready => 2,
            TakeoutStatus::Expired => 3,
            TakeoutStatus::Cancelled => 4,
        };
        Ok(ToSqlOutput::from(v))
    }
}

impl FromSql for TakeoutStatus {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Integer(i) => match i as i32 {
                0 => Ok(TakeoutStatus::Pending),
                1 => Ok(TakeoutStatus::Materializing),
                2 => Ok(TakeoutStatus::Ready),
                3 => Ok(TakeoutStatus::Expired),
                4 => Ok(TakeoutStatus::Cancelled),
                _ => Err(FromSqlError::InvalidType),
            },
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

impl ToSql for ImportStatus {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        let v: i32 = match self {
            ImportStatus::Pending => 0,
            ImportStatus::Importing => 1,
            ImportStatus::Completed => 2,
            ImportStatus::Failed => 3,
        };
        Ok(ToSqlOutput::from(v))
    }
}

impl FromSql for ImportStatus {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Integer(i) => match i as i32 {
                0 => Ok(ImportStatus::Pending),
                1 => Ok(ImportStatus::Importing),
                2 => Ok(ImportStatus::Completed),
                3 => Ok(ImportStatus::Failed),
                _ => Err(FromSqlError::InvalidType),
            },
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

impl ToSql for ImportPathStatus {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        let v: i32 = match self {
            ImportPathStatus::Pending => 0,
            ImportPathStatus::Imported => 1,
            ImportPathStatus::Skipped => 2,
            ImportPathStatus::Failed => 3,
        };
        Ok(ToSqlOutput::from(v))
    }
}

impl FromSql for ImportPathStatus {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Integer(i) => match i as i32 {
                0 => Ok(ImportPathStatus::Pending),
                1 => Ok(ImportPathStatus::Imported),
                2 => Ok(ImportPathStatus::Skipped),
                3 => Ok(ImportPathStatus::Failed),
                _ => Err(FromSqlError::InvalidType),
            },
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

/// Database implementations for CustomUUID
impl ToSql for CustomUUID {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        let insert_string = self.to_string();
        Ok(ToSqlOutput::from(insert_string))
    }
}

impl FromSql for CustomUUID {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Text(str) => {
                match std::str::from_utf8(str) {
                    Ok(utf_value) => {
                        match Uuid::parse_str(utf_value) {
                            Ok(_) => {
                                // Use from_str to construct CustomUUID properly
                                CustomUUID::from_str(utf_value)
                                    .map_err(|_| FromSqlError::InvalidType)
                            }
                            Err(_) => Err(FromSqlError::InvalidType),
                        }
                    }
                    Err(_) => Err(FromSqlError::InvalidType),
                }
            }
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

impl ToSql for OnboardingFlags {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self.0 as i64))
    }
}

impl FromSql for OnboardingFlags {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Integer(i) => Ok(OnboardingFlags(i as u32)),
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

/// Register the `uuid_extract_timestamp(uuid_text) → INTEGER` SQL function
/// on a connection. NULL-safe (NULL in → NULL out). Parses the first 12
/// hex digits of a UUIDv7 (the 48-bit millisecond timestamp) and returns
/// epoch milliseconds. Returns 0 for malformed/too-short input.
///
/// Used by retention-aware queries (e.g. the photos reference provider's
/// bulk subquery) to filter rows by age without round-tripping through
/// Rust. The host registers this on every pooled connection via
/// `on_acquire`; tests that build their own connections must call this
/// directly.
pub fn register_uuid_extract_timestamp(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
    conn.create_scalar_function(
        "uuid_extract_timestamp",
        1,
        rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let uuid_str: Option<String> = ctx.get(0)?;
            match uuid_str {
                None => Ok(None),
                Some(s) => {
                    let hex_only: String = s.replace('-', "");
                    if hex_only.len() < 12 {
                        return Ok(Some(0i64));
                    }
                    match i64::from_str_radix(&hex_only[..12], 16) {
                        Ok(millis) => Ok(Some(millis)),
                        Err(_) => Ok(Some(0i64)),
                    }
                }
            }
        },
    )
}
