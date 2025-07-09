#[derive(Debug)]
pub enum DatabaseError {
    LockError,
    InsertError,
    RecordError,
    RecallError,
    ProcessingError,
    InvalidPayload
}

use crate::db::{Blake3Hash, PrivKey, User};
use std::{collections::HashMap, ops::Deref};
pub struct MyNode {
    pub node_id: i32,
    pub privkey: PrivKey,
}
use chrono::{DateTime, Utc};
use either::Either;
use serde::{Serialize, Deserialize};
use uuid::{Timestamp, Uuid};
use duckdb::types::{ToSql, ToSqlOutput, FromSql, FromSqlResult, ValueRef, FromSqlError, EnumType};
use duckdb::arrow::array::StringArray;

/// Helper function to extract string value from DuckDB enum
pub fn extract_enum_string(enum_type: EnumType<'_>, row_idx: usize) -> Result<String, FromSqlError> {
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

#[derive(Serialize)]
pub struct CustomUUID(Uuid);

impl CustomUUID{
    pub fn new(timestamp: Option<&Timestamp>) -> CustomUUID {
        match timestamp {
            Some(timestamp) => {return CustomUUID(Uuid::new_v7(*timestamp))},
            None => {return CustomUUID(Uuid::now_v7())}
        }
    }
}

impl Deref for CustomUUID {
    type Target = Uuid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

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
                            Ok(data) => Ok(CustomUUID(data)),
                            Err(_) => Err(duckdb::types::FromSqlError::InvalidType)
                        }
                    }
                    Err(_) => Err(FromSqlError::InvalidType)
                }
            }
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

#[derive(Serialize)]
pub struct CustomDateTime(DateTime<Utc>);

impl Deref for CustomDateTime {
    type Target = DateTime<Utc>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ToSql for CustomDateTime {
    fn to_sql(&self) -> duckdb::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self.to_rfc3339()))
    }
}

#[derive(Serialize)]
pub enum InodeType {
    File,
    Folder
}

#[derive(Serialize, Debug, PartialEq, Clone)]
pub enum ChunkType {
    Original,
    Recovery
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

impl ToSql for ChunkType {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>, duckdb::Error> {
        let chunk_str = match self {
            ChunkType::Original => "original",
            ChunkType::Recovery => "recovery",
        };
        return Ok(chunk_str.into())
    }
}

impl FromSql for ChunkType {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        if let ValueRef::Enum(enum_type, row_idx) = value {
            let enum_value = extract_enum_string(enum_type, row_idx)?;
            match enum_value.as_str() {
                "original" => Ok(ChunkType::Original),
                "recovery" => Ok(ChunkType::Recovery),
                _ => Err(FromSqlError::InvalidType),
            }
        } else {
            Err(FromSqlError::InvalidType)
        }
    }
}

#[derive(Serialize)]
pub struct Inode {
    // the owner for this specific node:
    pub owner: Either<i32, User>,
    // path is split by /
    // each segment encrypted with AES-SIV with the owner's key
    // this way, we can compute all files in a folder quickly whilst maintaining OK privacy
    pub path: String,
    // it is either a folder or file
    pub inode_type: InodeType,
    // if file, point to datablock
    // if folder, None
    pub data_id: Option<Either<CustomUUID, DataRecord>>
}

#[derive(Serialize)]
pub struct DataRecord {
    // PK for this datablock
    // referenced by inoderecord
    // distinct from hash to allow file update without needing to update inode
    // also encodes creation time due to uuidv7
    pub id: CustomUUID,
    // map of { user_id -> encrypted_file_key }
    // on share:
    // 1. through DH key exchange, x25519 privkey of original owner + x25519 pubkey of sharee
    // 2. we add record to database encrypted with this
    // 3. slow lookup of list of access keys only needed when materializing file
    pub access_list: AccessList,
    pub modified_at: Option<CustomDateTime>,

    pub data: Data,
}

#[derive(Serialize, Debug, PartialEq)]
pub struct Data {
    // data hash for integrity
    pub hash: Blake3Hash,
    // list of fragment hashes
    pub fragments: Vec<DataBlockRepresentation>,
    pub added_bytes: u8,
}

#[derive(Serialize, Deserialize)]
pub struct AccessList {
    // string probably will change
    pub keys: HashMap<i32, String>
}

impl ToSql for AccessList {
    fn to_sql(&self) -> duckdb::Result<ToSqlOutput<'_>> {
        let encoded = bincode::serde::encode_to_vec(self, bincode::config::standard())
            .map_err(|_| duckdb::Error::ToSqlConversionFailure(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Failed to encode AccessList"))))?;
        Ok(ToSqlOutput::from(encoded))
    }
}

impl FromSql for AccessList {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Blob(blob) => {
                let (decoded, _): (AccessList, usize) = bincode::serde::decode_from_slice(blob, bincode::config::standard())
                    .map_err(|_| FromSqlError::InvalidType)?;
                Ok(decoded)
            }
            _ => Err(FromSqlError::InvalidType),
        }
    }
}


// not using Either for DataBlockRepresentation
// possible for other cases in future?
// direct fetch from API of other nodes?
#[derive(Serialize, Debug, PartialEq)]
pub enum DataBlockRepresentation {
    Hash(Blake3Hash, ChunkType),
    Data(Vec<u8>, ChunkType)
}