// Database-specific implementations for common types
// Only included by the main binary, not the FileProvider

use duckdb::types::{ToSql, ToSqlOutput, FromSql, FromSqlResult, ValueRef, FromSqlError, EnumType};
use duckdb::arrow::array::StringArray;
use super::InodeType;

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