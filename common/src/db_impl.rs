// Database-specific implementations for common types
// Only included by the main binary, not the FileProvider

use duckdb::types::{ToSql, ToSqlOutput, FromSql, FromSqlResult, ValueRef, FromSqlError, EnumType};
use duckdb::arrow::array::StringArray;
use super::{InodeType, TakeoutStatus, CustomUUID};
use uuid::Uuid;

/// Helper function to extract string value from DuckDB enum
fn extract_enum_string(enum_type: EnumType<'_>, row_idx: usize) -> Result<String, FromSqlError> {
    // Get the string values array
    let dict_values = match enum_type {
        EnumType::UInt8(dict_array) => dict_array.values(),
        EnumType::UInt16(dict_array) => dict_array.values(),
        EnumType::UInt32(dict_array) => dict_array.values(),
    }
    .as_any()
    .downcast_ref::<StringArray>()
    .ok_or(FromSqlError::InvalidType)?;
    
    // Get the dictionary key for this row
    let dict_key = match enum_type {
        EnumType::UInt8(dict_array) => dict_array.key(row_idx),
        EnumType::UInt16(dict_array) => dict_array.key(row_idx),
        EnumType::UInt32(dict_array) => dict_array.key(row_idx),
    }
    .ok_or(FromSqlError::InvalidType)?;
    
    // Get the actual string value
    Ok(dict_values.value(dict_key).to_string())
}

impl ToSql for InodeType {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>, duckdb::Error> {
        let phase_str = match self {
            InodeType::File => "file",
            InodeType::Folder => "folder",
        };
        return Ok(phase_str.into())
    }
}

impl FromSql for InodeType {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        if let ValueRef::Enum(enum_type, row_idx) = value {
            let enum_value = extract_enum_string(enum_type, row_idx)?;
            match enum_value.as_str() {
                "file" => Ok(InodeType::File),
                "folder" => Ok(InodeType::Folder),
                _ => Err(FromSqlError::InvalidType),
            }
        } else {
            Err(FromSqlError::InvalidType)
        }
    }
}

impl ToSql for TakeoutStatus {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>, duckdb::Error> {
        let status_str = match self {
            TakeoutStatus::Pending => "pending",
            TakeoutStatus::Materializing => "materializing",
            TakeoutStatus::Ready => "ready",
            TakeoutStatus::Expired => "expired",
            TakeoutStatus::Cancelled => "cancelled",
        };
        return Ok(status_str.into())
    }
}

impl FromSql for TakeoutStatus {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        if let ValueRef::Enum(enum_type, row_idx) = value {
            let enum_value = extract_enum_string(enum_type, row_idx)?;
            match enum_value.as_str() {
                "pending" => Ok(TakeoutStatus::Pending),
                "materializing" => Ok(TakeoutStatus::Materializing),
                "ready" => Ok(TakeoutStatus::Ready),
                "expired" => Ok(TakeoutStatus::Expired),
                "cancelled" => Ok(TakeoutStatus::Cancelled),
                _ => Err(FromSqlError::InvalidType),
            }
        } else {
            Err(FromSqlError::InvalidType)
        }
    }
}

/// Database implementations for CustomUUID
impl ToSql for CustomUUID {
    fn to_sql(&self) -> duckdb::Result<ToSqlOutput<'_>> {
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