// Database-specific implementations for common types
// Only included by the main binary, not the FileProvider

use rusqlite::types::{ToSql, ToSqlOutput, FromSql, FromSqlResult, ValueRef, FromSqlError};
use super::{InodeType, TakeoutStatus, CustomUUID};
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
                            },
                            Err(_) => Err(FromSqlError::InvalidType)
                        }
                    }
                    Err(_) => Err(FromSqlError::InvalidType)
                }
            }
            _ => Err(FromSqlError::InvalidType),
        }
    }
}
