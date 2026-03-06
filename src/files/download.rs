use crate::db::{DatabaseError, files};
use crate::files::functions;
use axum::http::StatusCode;

/// Inclusive byte range for partial content requests
pub struct ByteRange {
    pub start: u64,
    pub end: u64,
}

/// Result of file download preparation (range-aware)
pub struct FileDownloadInfo {
    pub stream: std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<bytes::Bytes, functions::FileError>> + Send>>,
    pub file_size: u64,
    pub is_partial: bool,
    pub range: Option<ByteRange>,
}

#[derive(Debug)]
pub enum FileReconstructionError {
    NotFound,
    Forbidden,
    DatabaseError(DatabaseError),
    ReassemblyError(functions::FileError),
    KeyDecryptionError,
    InternalError,
    RangeNotSatisfiable(u64),
}

impl From<DatabaseError> for FileReconstructionError {
    fn from(error: DatabaseError) -> Self {
        match error {
            DatabaseError::RecallError => FileReconstructionError::NotFound,
            _ => FileReconstructionError::DatabaseError(error),
        }
    }
}

impl From<functions::FileError> for FileReconstructionError {
    fn from(error: functions::FileError) -> Self {
        FileReconstructionError::ReassemblyError(error)
    }
}

impl From<FileReconstructionError> for StatusCode {
    fn from(error: FileReconstructionError) -> Self {
        match error {
            FileReconstructionError::NotFound => StatusCode::NOT_FOUND,
            FileReconstructionError::Forbidden => StatusCode::FORBIDDEN,
            FileReconstructionError::DatabaseError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            FileReconstructionError::ReassemblyError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            FileReconstructionError::KeyDecryptionError => StatusCode::FORBIDDEN,
            FileReconstructionError::InternalError => StatusCode::INTERNAL_SERVER_ERROR,
            FileReconstructionError::RangeNotSatisfiable(_) => StatusCode::RANGE_NOT_SATISFIABLE,
        }
    }
}

enum PreparedFile {
    Empty,
    Ready {
        file_data: functions::FileReassemblyData,
        file_size: u64,
        placement_height: Option<i32>,
    },
}

/// Shared preparation: DB lookup, empty-file check, key decryption
async fn prepare_file_data(
    app_state: &crate::AppState,
    encrypted_path: &str,
    user_id: i32,
) -> Result<PreparedFile, FileReconstructionError> {
    let file_access_data = files::get_file_fragments(app_state.db_pool.get(), encrypted_path.to_string(), user_id)?;
    let file_size = file_access_data.file_size;

    let file_data = match file_access_data.file_reassembly_data {
        None => {
            tracing::debug!("Empty file: {}", encrypted_path);
            return Ok(PreparedFile::Empty);
        }
        Some(data) => data,
    };

    let placement_height = file_data.placement_height;

    let mut file_data = file_data;
    if let Some(file_access_entry) = file_access_data.file_access_entry {
        let session = app_state.get_session(user_id).await
            .map_err(|_| FileReconstructionError::InternalError)?;
        let user_x25519_privkey = crate::auth::derive_x25519_privkey_from_user(&session.user_keys.private_key);

        match crate::auth::decrypt_wrapped_file_key(&file_access_entry, &user_x25519_privkey) {
            Ok(per_file_key) => { file_data.per_file_key = Some(per_file_key); }
            Err(e) => {
                tracing::error!("Failed to decrypt file key for {}: {:?}", encrypted_path, e);
                return Err(FileReconstructionError::KeyDecryptionError);
            }
        }
    } else {
        tracing::warn!("User {} does not have access to file {}", user_id, encrypted_path);
        return Err(FileReconstructionError::Forbidden);
    }

    Ok(PreparedFile::Ready { file_data, file_size, placement_height })
}

fn empty_stream() -> std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<bytes::Bytes, functions::FileError>> + Send>> {
    Box::pin(async_stream::try_stream! {
        if false { yield bytes::Bytes::new(); }
    })
}

/// Full-file streaming reconstruction (no range support)
/// Used by takeout materialization and document provider
pub async fn reconstruct_file_stream(
    app_state: &crate::AppState,
    encrypted_path: String,
    user_id: i32,
    fragments_dir: &str,
) -> Result<std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<bytes::Bytes, functions::FileError>> + Send>>, FileReconstructionError> {
    match prepare_file_data(app_state, &encrypted_path, user_id).await? {
        PreparedFile::Empty => Ok(empty_stream()),
        PreparedFile::Ready { file_data, placement_height, .. } => {
            let stream = functions::reconstruct_file_chunked(
                fragments_dir.to_string(),
                file_data,
                Some(app_state.clone()),
                placement_height,
                None,
            );
            tracing::debug!("Starting streaming reconstruction for file {}", encrypted_path);
            Ok(Box::pin(stream))
        }
    }
}

/// Range-aware file reconstruction for the download route
/// Returns FileDownloadInfo with stream, file_size, and range metadata for building HTTP responses
pub async fn reconstruct_file_range(
    app_state: &crate::AppState,
    encrypted_path: String,
    user_id: i32,
    fragments_dir: &str,
    requested_range: Option<(u64, Option<u64>)>,
) -> Result<FileDownloadInfo, FileReconstructionError> {
    match prepare_file_data(app_state, &encrypted_path, user_id).await? {
        PreparedFile::Empty => {
            Ok(FileDownloadInfo {
                stream: empty_stream(),
                file_size: 0,
                is_partial: false,
                range: None,
            })
        }
        PreparedFile::Ready { file_data, file_size, placement_height } => {
            let resolved_range = match requested_range {
                Some((start, end_opt)) => {
                    if start >= file_size {
                        return Err(FileReconstructionError::RangeNotSatisfiable(file_size));
                    }
                    let end = end_opt.unwrap_or(file_size - 1).min(file_size - 1);
                    Some(ByteRange { start, end })
                }
                None => None,
            };

            let range_tuple = resolved_range.as_ref().map(|r| (r.start, r.end));
            let is_partial = resolved_range.is_some();

            let stream = functions::reconstruct_file_chunked(
                fragments_dir.to_string(),
                file_data,
                Some(app_state.clone()),
                placement_height,
                range_tuple,
            );

            tracing::debug!("Starting {} reconstruction for file {}",
                if is_partial { "partial" } else { "full" }, encrypted_path);

            Ok(FileDownloadInfo {
                stream: Box::pin(stream),
                file_size,
                is_partial,
                range: resolved_range,
            })
        }
    }
}
