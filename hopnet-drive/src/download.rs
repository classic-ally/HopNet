//! File download preparation and reconstruction streams (RFC-015 Stage D4).
//!
//! Moved from the host's `files::download`; reconstruction now flows
//! through the `BlobStreamer` seam (the host's api::get over its substrate
//! seams) and sessions through `SessionAccess`.

use crate::db::files;
use crate::error::FileError;
use crate::host::DriveState;
use axum::http::StatusCode;
use hopnet_projection::DatabaseError;

/// Inclusive byte range for partial content requests
pub struct ByteRange {
    pub start: u64,
    pub end: u64,
}

/// Result of file download preparation (range-aware)
pub struct FileDownloadInfo {
    pub stream: std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = Result<bytes::Bytes, FileError>> + Send>,
    >,
    pub file_size: u64,
    pub is_partial: bool,
    pub range: Option<ByteRange>,
}

#[derive(Debug)]
pub enum FileReconstructionError {
    NotFound,
    Forbidden,
    DatabaseError(DatabaseError),
    ReassemblyError(FileError),
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

impl From<FileError> for FileReconstructionError {
    fn from(error: FileError) -> Self {
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
        manifest: hopnet_storage::store::BlobManifest,
        per_blob_key: chacha20poly1305::Key,
        file_size: u64,
    },
}

/// Shared preparation: DB lookup, empty-file check, key decryption. The
/// session unwrap stays projection-side (user identity, session cache);
/// reconstruction itself is the substrate's api::get.
async fn prepare_file_data(
    state: &DriveState,
    encrypted_path: &str,
    user_id: i32,
) -> Result<PreparedFile, FileReconstructionError> {
    let file_access_data =
        files::get_file_fragments(state.db_pool.get(), encrypted_path.to_string(), user_id)?;
    let file_size = file_access_data.file_size;

    let manifest = match file_access_data.manifest {
        None => {
            tracing::debug!("Empty file: {}", encrypted_path);
            return Ok(PreparedFile::Empty);
        }
        Some(manifest) => manifest,
    };

    let Some(file_access_entry) = file_access_data.file_access_entry else {
        tracing::warn!(
            "User {} does not have access to file {}",
            user_id,
            encrypted_path
        );
        return Err(FileReconstructionError::Forbidden);
    };

    let session = state
        .sessions
        .user_session(user_id)
        .await
        .map_err(|_| FileReconstructionError::InternalError)?;

    let per_blob_key = match hopnet_storage::crypto::unwrap_blob_key(
        &file_access_entry,
        &hopnet_storage::crypto::StaticRecipient(session.x25519_privkey),
    ) {
        Ok(key) => key,
        Err(e) => {
            tracing::error!("Failed to decrypt file key for {}: {:?}", encrypted_path, e);
            return Err(FileReconstructionError::KeyDecryptionError);
        }
    };

    Ok(PreparedFile::Ready {
        manifest,
        per_blob_key,
        file_size,
    })
}

/// Substrate reconstruction stream with errors mapped onto the projection's
/// FileError (callers' HTTP mapping unchanged).
fn reconstruct_stream(
    state: &DriveState,
    manifest: hopnet_storage::store::BlobManifest,
    per_blob_key: chacha20poly1305::Key,
    range: Option<(u64, u64)>,
) -> impl tokio_stream::Stream<Item = Result<bytes::Bytes, FileError>> {
    use tokio_stream::StreamExt;
    state
        .blobs
        .stream(manifest, Some(per_blob_key), range)
        .map(|item| item.map_err(FileError::from))
}

fn empty_stream()
-> std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<bytes::Bytes, FileError>> + Send>> {
    Box::pin(async_stream::try_stream! {
        if false { yield bytes::Bytes::new(); }
    })
}

/// Full-file streaming reconstruction (no range support)
/// Used by takeout materialization and document provider
pub async fn reconstruct_file_stream(
    state: &DriveState,
    encrypted_path: String,
    user_id: i32,
) -> Result<
    std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<bytes::Bytes, FileError>> + Send>>,
    FileReconstructionError,
> {
    match prepare_file_data(state, &encrypted_path, user_id).await? {
        PreparedFile::Empty => Ok(empty_stream()),
        PreparedFile::Ready {
            manifest,
            per_blob_key,
            ..
        } => {
            let stream = reconstruct_stream(state, manifest, per_blob_key, None);
            tracing::debug!(
                "Starting streaming reconstruction for file {}",
                encrypted_path
            );
            Ok(Box::pin(stream))
        }
    }
}

/// Range-aware file reconstruction for the download route
/// Returns FileDownloadInfo with stream, file_size, and range metadata for building HTTP responses
pub async fn reconstruct_file_range(
    state: &DriveState,
    encrypted_path: String,
    user_id: i32,
    requested_range: Option<(u64, Option<u64>)>,
) -> Result<FileDownloadInfo, FileReconstructionError> {
    match prepare_file_data(state, &encrypted_path, user_id).await? {
        PreparedFile::Empty => Ok(FileDownloadInfo {
            stream: empty_stream(),
            file_size: 0,
            is_partial: false,
            range: None,
        }),
        PreparedFile::Ready {
            manifest,
            per_blob_key,
            file_size,
        } => {
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

            let stream = reconstruct_stream(state, manifest, per_blob_key, range_tuple);

            tracing::debug!(
                "Starting {} reconstruction for file {}",
                if is_partial { "partial" } else { "full" },
                encrypted_path
            );

            Ok(FileDownloadInfo {
                stream: Box::pin(stream),
                file_size,
                is_partial,
                range: resolved_range,
            })
        }
    }
}
