//! Blake3Hash — the canonical 32-byte content-hash wrapper shared by the main
//! crate and hopnet-storage. Moved verbatim from the main crate's types.rs
//! during the storage-substrate extraction (Stage A).

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A wrapper around blake3::Hash that implements bincode's Encode and Decode traits
#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub struct Blake3Hash(pub blake3::Hash);

impl Blake3Hash {
    /// Create a new Blake3Hash from a blake3::Hash
    pub fn new(hash: blake3::Hash) -> Self {
        Self(hash)
    }

    /// Get the inner blake3::Hash
    pub fn inner(&self) -> &blake3::Hash {
        &self.0
    }

    /// Convert into the inner blake3::Hash
    pub fn into_inner(self) -> blake3::Hash {
        self.0
    }

    /// Create from bytes
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(blake3::Hash::from(bytes))
    }

    /// Get as bytes
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    /// Convert to hex string
    pub fn to_hex(&self) -> String {
        self.0.to_hex().to_string()
    }
}

impl From<blake3::Hash> for Blake3Hash {
    fn from(hash: blake3::Hash) -> Self {
        Self(hash)
    }
}

impl From<Blake3Hash> for blake3::Hash {
    fn from(wrapper: Blake3Hash) -> Self {
        wrapper.0
    }
}

impl std::fmt::Display for Blake3Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Serialize for Blake3Hash {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.to_hex())
        } else {
            serializer.serialize_bytes(self.0.as_bytes())
        }
    }
}

impl<'de> Deserialize<'de> for Blake3Hash {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::{Error, Visitor};
        use std::fmt;

        struct Blake3HashVisitor;

        impl<'de> Visitor<'de> for Blake3HashVisitor {
            type Value = Blake3Hash;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a hex string or binary data representing a Blake3 hash")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                let bytes = hex::decode(value).map_err(E::custom)?;
                self.visit_bytes(&bytes)
            }

            fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
            where
                E: Error,
            {
                if value.len() != 32 {
                    return Err(E::custom("Blake3 hash must be exactly 32 bytes"));
                }
                let mut array = [0u8; 32];
                array.copy_from_slice(value);
                Ok(Blake3Hash::from_bytes(array))
            }
        }

        if deserializer.is_human_readable() {
            deserializer.deserialize_str(Blake3HashVisitor)
        } else {
            deserializer.deserialize_bytes(Blake3HashVisitor)
        }
    }
}

// Simple wrapper that converts to/from Vec<u8> for bincode compatibility
impl bincode::Encode for Blake3Hash {
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> Result<(), bincode::error::EncodeError> {
        // Convert to Vec<u8> and encode that
        let bytes: Vec<u8> = self.as_bytes().to_vec();
        bytes.encode(encoder)
    }
}

impl<Context> bincode::Decode<Context> for Blake3Hash {
    fn decode<D: bincode::de::Decoder>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        // Decode as Vec<u8> and convert to [u8; 32]
        let bytes: Vec<u8> = Vec::decode(decoder)?;
        if bytes.len() != 32 {
            return Err(bincode::error::DecodeError::Other(
                "Blake3 hash must be exactly 32 bytes",
            ));
        }
        let mut array = [0u8; 32];
        array.copy_from_slice(&bytes);
        Ok(Blake3Hash::from_bytes(array))
    }
}

impl<'de, Context> bincode::BorrowDecode<'de, Context> for Blake3Hash {
    fn borrow_decode<D: bincode::de::BorrowDecoder<'de>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        // Borrow decode as a slice and convert to [u8; 32]
        let bytes: &[u8] = bincode::BorrowDecode::borrow_decode(decoder)?;
        if bytes.len() != 32 {
            return Err(bincode::error::DecodeError::Other(
                "Blake3 hash must be exactly 32 bytes",
            ));
        }
        let mut array = [0u8; 32];
        array.copy_from_slice(bytes);
        Ok(Blake3Hash::from_bytes(array))
    }
}

#[cfg(feature = "database")]
mod sql_impls {
    use super::Blake3Hash;
    use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef};

    impl FromSql for Blake3Hash {
        fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
            match value {
                ValueRef::Blob(bytes) => {
                    if bytes.len() != 32 {
                        return Err(FromSqlError::Other(
                            format!(
                                "Blake3Hash must be exactly 32 bytes, got {} bytes",
                                bytes.len()
                            )
                            .into(),
                        ));
                    }
                    let mut array = [0u8; 32];
                    array.copy_from_slice(bytes);
                    Ok(Blake3Hash::from_bytes(array))
                }
                _ => Err(FromSqlError::InvalidType),
            }
        }
    }

    impl ToSql for Blake3Hash {
        fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
            Ok(ToSqlOutput::from(self.as_bytes()))
        }
    }
}
