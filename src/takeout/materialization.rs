use crate::db::{CustomUUID, takeout::MaterializationStatus};
use crate::files::download::{reconstruct_file_for_user, FileReconstructionError};

/// Materialize a single file by reconstructing from fragments and writing to staging
/// Returns (file_id, status, optional_error_message) for database update
pub async fn materialize_single_file(
    app_state: &crate::AppState,
    takeout_id: &CustomUUID,
    file_id: CustomUUID,
    encrypted_path: String,
    data_id: CustomUUID,
    fragments_dir: &str,
    user_id: i32,
) -> (CustomUUID, MaterializationStatus, Option<String>) {
    let staging_dir = format!("{}/takeouts/{}/staging/files", fragments_dir, takeout_id.simple());

    // Get SIV key and nonce from session store
    let session = match app_state.get_session(user_id).await {
        Ok(s) => s,
        Err(_) => return (file_id, MaterializationStatus::Failed, Some("Failed to get session keys".to_string())),
    };

    // Decrypt the path segments
    let decrypted_path = match crate::files::functions::decrypt_path(encrypted_path.clone(), &session.siv_key, &session.siv_nonce) {
        Ok(path) => path,
        Err(e) => {
            tracing::error!("Failed to decrypt file path {}: {:?}", encrypted_path, e);
            return (file_id, MaterializationStatus::Failed, Some("Path decryption failed".to_string()));
        }
    };

    tracing::debug!("Materializing file: {} -> {}", encrypted_path, decrypted_path);

    // Use shared file reconstruction logic
    // Get streaming reconstruction (memory-efficient for large files)
    let mut stream = match reconstruct_file_for_user(
        app_state,
        encrypted_path.clone(),
        user_id,
        fragments_dir,
    ).await {
        Ok(stream) => stream,
        Err(e) => {
            tracing::error!("Failed to reconstruct file {} (data_id: {}): {:?}", encrypted_path, data_id, e);
            let error_msg = match e {
                FileReconstructionError::NotFound => "File not found",
                FileReconstructionError::Forbidden => "Access denied",
                FileReconstructionError::KeyDecryptionError => "Key decryption failed",
                _ => "File reconstruction failed",
            };
            return (file_id, MaterializationStatus::Failed, Some(error_msg.to_string()));
        }
    };

    // Write file to staging directory (chunk-by-chunk streaming)
    let full_staging_path = format!("{}/{}", staging_dir, decrypted_path.trim_start_matches('/'));

    // Ensure parent directory exists
    if let Some(parent) = std::path::Path::new(&full_staging_path).parent() {
        if let Err(e) = tokio::fs::create_dir_all(parent).await {
            tracing::error!("Failed to create parent directory for {}: {:?}", full_staging_path, e);
            return (file_id, MaterializationStatus::Failed, Some(format!("Parent directory creation failed: {}", e)));
        }
    }

    // Open file for writing
    let mut file = match tokio::fs::File::create(&full_staging_path).await {
        Ok(f) => f,
        Err(e) => {
            tracing::error!("Failed to create file {}: {:?}", full_staging_path, e);
            return (file_id, MaterializationStatus::Failed, Some(format!("File creation failed: {}", e)));
        }
    };

    // Write chunks as they arrive from the stream
    use tokio::io::AsyncWriteExt;
    use tokio_stream::StreamExt;

    let mut total_bytes = 0;
    while let Some(chunk_result) = stream.next().await {
        let chunk = match chunk_result {
            Ok(chunk) => chunk,
            Err(e) => {
                tracing::error!("Stream error while reconstructing {}: {:?}", full_staging_path, e);
                return (file_id, MaterializationStatus::Failed, Some("Stream reconstruction error".to_string()));
            }
        };

        if let Err(e) = file.write_all(&chunk).await {
            tracing::error!("Failed to write chunk to {}: {:?}", full_staging_path, e);
            return (file_id, MaterializationStatus::Failed, Some(format!("Chunk write failed: {}", e)));
        }

        total_bytes += chunk.len();
    }

    // Ensure all data is flushed to disk
    if let Err(e) = file.sync_all().await {
        tracing::error!("Failed to sync file {}: {:?}", full_staging_path, e);
        return (file_id, MaterializationStatus::Failed, Some(format!("File sync failed: {}", e)));
    }

    tracing::debug!("Materialized file: {} ({} bytes)", full_staging_path, total_bytes);
    (file_id, MaterializationStatus::Success, None)
}