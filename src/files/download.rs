use crate::db::{DatabaseError, files};
use crate::files::functions;
use axum::http::StatusCode;

#[derive(Debug)]
pub enum FileReconstructionError {
    NotFound,
    Forbidden,
    DatabaseError(DatabaseError),
    ReassemblyError(functions::FileError),
    KeyDecryptionError,
    InternalError,
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
        }
    }
}

/// Reconstruct a file with proper key decryption and access control
/// Returns a stream of file chunks for memory-efficient downloads
/// This is the shared logic used by both download routes and takeout materialization
pub async fn reconstruct_file_for_user(
    app_state: &crate::AppState,
    encrypted_path: String,
    user_id: i32,
    fragments_dir: &str,
) -> Result<std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<bytes::Bytes, functions::FileError>> + Send>>, FileReconstructionError> {
    // Get file access data from database
    let file_access_data = files::get_file_fragments(app_state.db_pool.get(), encrypted_path.clone(), user_id)?;

    // Handle empty files (no fragments, no encryption)
    let file_data = match file_access_data.file_reassembly_data {
        None => {
            // Empty file case - return empty stream
            tracing::debug!("Downloading empty file: {}", encrypted_path);
            return Ok(Box::pin(async_stream::try_stream! {
                // Type hint for empty stream (never executed)
                if false {
                    yield bytes::Bytes::new();
                }
            }));
        }
        Some(data) => data,
    };

    // Extract placement_height before moving file_data
    let placement_height = file_data.placement_height;

    // Decrypt the per-file key if user has access
    let mut file_data = file_data;
    if let Some(file_access_entry) = file_access_data.file_access_entry {
        // Get user's private key from app_state
        let user_private_key = match app_state.user_keys.get() {
            Some(user_keys) => &user_keys.private_key,
            None => {
                tracing::error!("No user keys available in app_state");
                return Err(FileReconstructionError::InternalError);
            }
        };
        
        // Derive user's X25519 private key from app_state private key
        let user_x25519_privkey = crate::auth::derive_x25519_privkey_from_user(user_private_key);
        
        // Decrypt the wrapped per-file key
        match crate::auth::decrypt_wrapped_file_key(&file_access_entry, &user_x25519_privkey) {
            Ok(per_file_key) => {
                file_data.per_file_key = Some(per_file_key);
            }
            Err(e) => {
                tracing::error!("Failed to decrypt file key for {}: {:?}", encrypted_path, e);
                return Err(FileReconstructionError::KeyDecryptionError);
            }
        }
    } else {
        // User doesn't have access to this file
        tracing::warn!("User {} does not have access to file {}", user_id, encrypted_path);
        return Err(FileReconstructionError::Forbidden);
    }
    
    // Return streaming reconstruction (yields 40MB chunks as they're reconstructed)
    let stream = functions::reconstruct_file_chunked(
        fragments_dir.to_string(),
        file_data,
        Some(app_state.clone()),
        placement_height
    );

    tracing::debug!("Starting streaming reconstruction for file {}", encrypted_path);
    Ok(Box::pin(stream))
}