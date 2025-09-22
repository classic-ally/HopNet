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
) -> (CustomUUID, MaterializationStatus, Option<String>) {
    let staging_dir = format!("{}/takeouts/{}/staging/files", fragments_dir, takeout_id.simple());

    // Get SIV key and nonce for path decryption
    let (siv_key, siv_nonce) = match (app_state.get_siv_key(), app_state.get_siv_nonce()) {
        (Ok(key), Ok(nonce)) => (key, nonce),
        _ => return (file_id, MaterializationStatus::Failed, Some("Failed to get encryption keys".to_string())),
    };

    // Decrypt the path segments
    let decrypted_path = match crate::files::functions::decrypt_path(encrypted_path.clone(), siv_key, siv_nonce) {
        Ok(path) => path,
        Err(e) => {
            tracing::error!("Failed to decrypt file path {}: {:?}", encrypted_path, e);
            return (file_id, MaterializationStatus::Failed, Some("Path decryption failed".to_string()));
        }
    };

    tracing::debug!("Materializing file: {} -> {}", encrypted_path, decrypted_path);

    // Get user ID for reconstruction
    let user_id = match app_state.get_user_id() {
        Ok(id) => id,
        Err(_) => return (file_id, MaterializationStatus::Failed, Some("Failed to get user ID".to_string())),
    };

    // Use shared file reconstruction logic
    let file_content = match reconstruct_file_for_user(
        app_state,
        encrypted_path.clone(),
        user_id,
        fragments_dir,
    ).await {
        Ok(content) => content,
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

    // Write file to staging directory
    let full_staging_path = format!("{}/{}", staging_dir, decrypted_path.trim_start_matches('/'));

    // Ensure parent directory exists
    if let Some(parent) = std::path::Path::new(&full_staging_path).parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            tracing::error!("Failed to create parent directory for {}: {:?}", full_staging_path, e);
            return (file_id, MaterializationStatus::Failed, Some(format!("Parent directory creation failed: {}", e)));
        }
    }

    // Write the file content
    match std::fs::write(&full_staging_path, &file_content) {
        Ok(_) => {
            tracing::debug!("Materialized file: {} ({} bytes)", full_staging_path, file_content.len());
            (file_id, MaterializationStatus::Success, None)
        }
        Err(e) => {
            tracing::error!("Failed to write file {}: {:?}", full_staging_path, e);
            (file_id, MaterializationStatus::Failed, Some(format!("File write failed: {}", e)))
        }
    }
}