//! Import side of the takeout/portability boundary. Owns the HTTP route
//! handlers, the upload workflow, and the per-failure error mapping.
//!
//! Reshaped at RFC-015 Stage D5b for manifest v2 + per-projection
//! translators: extraction stages files under `{staging}/{projection}/…`
//! and hash-verifies each entry against the manifest; the creation walk
//! dispatches each section to its registered `ProjectionExporter`
//! (`import_entry` per Pending row + `flush` at section end). A section
//! with NO registered translator has all its rows marked Skipped with
//! `error_code = "no_translator"` — reported, never failed.

use axum::Router;
use axum::extract::{DefaultBodyLimit, Extension, Multipart, State};
use axum::http::StatusCode;
use axum::response::Json;
use axum::routing::get;
use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use tokio_stream::StreamExt;
use tokio_util::io::StreamReader;

use hopnet_common::{
    Blake3Hash, CustomUUID, ImportPathCounts, ImportPathRow, ImportRecord, ImportStatus, InodeType,
};
use hopnet_projection::host::{TxSigner, TxSpec, UserSession};
use hopnet_projection::{DatabaseError, ExportEntry, ImportEntryError};

use crate::STORAGE_SAFETY_MARGIN_BYTES;
use crate::TakeoutState;
use crate::archive::{ImportArchiveError, read_manifest_from_archive};
use crate::db::import_paths;
use crate::db::imports::{self, ImportPayload, ImportStatusPayload};
use crate::manifest::{EntryKind, ManifestEntry, TakeoutManifest};

/// Router for `/takeout/import` and its children. Nested from
/// `routes::router()`.
pub(crate) fn import_routes() -> Router<TakeoutState> {
    Router::new()
        .route("/", get(get_current_import).post(post_initiate_import))
        .route("/paths", get(get_current_import_paths))
        .route("/status", get(get_current_import_status))
        .layer(DefaultBodyLimit::max(5_000_000_000))
}

/// POST /takeout/import — accept a multipart tar.gz, validate manifest,
/// run quota check, submit `create_import` consensus txn. Returns the
/// freshly created `ImportRecord` on success.
async fn post_initiate_import(
    State(state): State<TakeoutState>,
    Extension(user_id): Extension<i32>,
    multipart: Multipart,
) -> Result<(StatusCode, Json<ImportRecord>), StatusCode> {
    match process_upload(&state, user_id, multipart).await {
        Ok(record) => Ok((StatusCode::CREATED, Json(record))),
        Err(e) => {
            tracing::warn!("Import upload failed for user {}: {}", user_id, e);
            Err(e.status_code())
        }
    }
}

/// GET /takeout/import — return the user's current or most-recent import as a singleton.
async fn get_current_import(
    State(state): State<TakeoutState>,
    Extension(user_id): Extension<i32>,
) -> Result<Json<Option<ImportRecord>>, StatusCode> {
    let conn = state
        .db_pool
        .get()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    match imports::get_current_import_for_user(&conn, user_id) {
        Ok(record) => Ok(Json(record)),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

/// GET /takeout/import/paths — owner-node-local debug view of the per-import
/// path table. Returns 404 from any non-owner node since the table is local.
async fn get_current_import_paths(
    State(state): State<TakeoutState>,
    Extension(user_id): Extension<i32>,
) -> Result<Json<Vec<ImportPathRow>>, StatusCode> {
    let conn = state
        .db_pool
        .get()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let record = imports::get_current_import_for_user(&conn, user_id)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let self_node = state.node_id().ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
    if record.owner_node_id != self_node {
        return Err(StatusCode::NOT_FOUND);
    }

    let import_id = CustomUUID::from_str(&record.id.to_string())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let rows = import_paths::list_paths(&conn, &import_id)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(rows))
}

/// GET /takeout/import/status — owner-node-local aggregate counts for the
/// user's current import. Returns 404 from non-owner nodes (per-import path
/// table is owner-local). Frontend uses these counts to drive progress.
async fn get_current_import_status(
    State(state): State<TakeoutState>,
    Extension(user_id): Extension<i32>,
) -> Result<Json<ImportPathCounts>, StatusCode> {
    let conn = state
        .db_pool
        .get()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let record = imports::get_current_import_for_user(&conn, user_id)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let self_node = state.node_id().ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
    if record.owner_node_id != self_node {
        return Err(StatusCode::NOT_FOUND);
    }

    let import_id = CustomUUID::from_str(&record.id.to_string())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let counts = import_paths::count_paths_by_status(&conn, &import_id)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(counts))
}

/// Errors raised during import upload processing. Each variant maps to a
/// specific HTTP status code at the route boundary.
#[derive(Debug, thiserror::Error)]
pub enum ImportUploadError {
    #[error("user not eligible to start an import")]
    NotEligible,
    #[error("multipart body invalid: {0}")]
    BadMultipart(String),
    #[error("missing 'archive' multipart field")]
    MissingArchiveField,
    #[error("archive content invalid: {0}")]
    BadArchive(#[from] ImportArchiveError),
    #[error("quota exceeded: required {required} bytes, available {available} bytes")]
    QuotaExceeded { required: u64, available: u64 },
    #[error("manifest total_bytes overflow")]
    SizeOverflow,
    #[error("filesystem error: {0}")]
    Io(#[from] std::io::Error),
    #[error("database error: {0:?}")]
    Database(DatabaseError),
    #[error("consensus submission failed")]
    ConsensusFailed,
    #[error("internal error: {0}")]
    Internal(String),
}

impl ImportUploadError {
    pub fn status_code(&self) -> StatusCode {
        match self {
            ImportUploadError::NotEligible => StatusCode::TOO_MANY_REQUESTS,
            ImportUploadError::BadMultipart(_) | ImportUploadError::MissingArchiveField => {
                StatusCode::BAD_REQUEST
            }
            ImportUploadError::BadArchive(_) => StatusCode::BAD_REQUEST,
            ImportUploadError::QuotaExceeded { .. } => StatusCode::INSUFFICIENT_STORAGE,
            ImportUploadError::SizeOverflow => StatusCode::BAD_REQUEST,
            ImportUploadError::Io(_)
            | ImportUploadError::Database(_)
            | ImportUploadError::ConsensusFailed
            | ImportUploadError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl From<DatabaseError> for ImportUploadError {
    fn from(e: DatabaseError) -> Self {
        ImportUploadError::Database(e)
    }
}

/// Per-import staging path: `{fragments_dir}/imports/{import_id}/`.
pub fn staging_dir(state: &TakeoutState, import_id: &CustomUUID) -> PathBuf {
    PathBuf::from(&state.fragments_dir)
        .join("imports")
        .join(import_id.to_string())
}

/// Cleanup staging on any failure path. Logs but does not propagate cleanup
/// errors — leaving stray staging is preferable to masking the original
/// failure cause.
async fn cleanup_staging(staging: &std::path::Path) {
    if let Err(e) = tokio::fs::remove_dir_all(staging).await {
        if e.kind() != std::io::ErrorKind::NotFound {
            tracing::warn!("Failed to clean staging {}: {:?}", staging.display(), e);
        }
    }
}

/// Drive the full upload-side pipeline. The session lives in this function's
/// scope and moves into the spawned extraction task — no global pin map,
/// RAII handles release on either success or any error path.
pub async fn process_upload(
    state: &TakeoutState,
    user_id: i32,
    mut multipart: Multipart,
) -> Result<ImportRecord, ImportUploadError> {
    // 1. Route-level eligibility check (fast 429 before any disk work).
    {
        let conn = state
            .db_pool
            .get()
            .map_err(|_| ImportUploadError::Internal("db pool".into()))?;
        if !imports::is_import_eligible(&conn, user_id)? {
            return Err(ImportUploadError::NotEligible);
        }
    }

    // 2. Capture session locally — moved into the spawned extraction task
    //    below so logout mid-import doesn't break crypto. RAII drops when the
    //    task ends. Owner-process death still strands the import; the resume
    //    registry routes recovery through the next auth event.
    let session = state
        .sessions
        .user_session(user_id)
        .await
        .map_err(|_| ImportUploadError::Internal("session unavailable".into()))?;

    // 3. Identifiers + staging path.
    let import_id = CustomUUID::new(None);
    let staging = staging_dir(state, &import_id);
    tokio::fs::create_dir_all(&staging).await?;
    let upload_path = staging.join("upload.tar.gz");

    // 4. Stream the "archive" multipart field to disk.
    let stream_result = stream_archive_field(&mut multipart, &upload_path).await;
    if let Err(e) = stream_result {
        cleanup_staging(&staging).await;
        return Err(e);
    }

    // 5. Read manifest from staging in spawn_blocking (sync tar reads).
    let upload_path_for_blocking = upload_path.clone();
    let manifest_result =
        tokio::task::spawn_blocking(move || read_manifest_from_archive(&upload_path_for_blocking))
            .await
            .map_err(|e| ImportUploadError::Internal(format!("spawn_blocking join: {}", e)))?;
    let manifest: TakeoutManifest = match manifest_result {
        Ok(m) => m,
        Err(e) => {
            cleanup_staging(&staging).await;
            return Err(e.into());
        }
    };

    // 6. Quota check using network-wide validator storage aggregate.
    if let Err(e) = check_quota(state, &manifest).await {
        cleanup_staging(&staging).await;
        return Err(e);
    }

    // 7. Submit create_import consensus txn (user-signed, as before).
    let node_id = state
        .node_id()
        .ok_or_else(|| ImportUploadError::Internal("node id unavailable".into()))?;
    let payload = ImportPayload {
        import_id: import_id.clone(),
        user_id,
        owner_node_id: node_id,
        status: ImportStatus::Pending,
    };
    let encoded = bincode::serde::encode_to_vec(&payload, bincode::config::standard())
        .map_err(|_| ImportUploadError::Internal("payload encode".into()))?;

    if let Err(e) = state
        .txs
        .submit(TxSpec {
            function: "create_import",
            payload: encoded,
            signer: TxSigner::User(user_id),
        })
        .await
    {
        tracing::error!("Failed to submit create_import for {}: {:?}", import_id, e);
        cleanup_staging(&staging).await;
        return Err(ImportUploadError::ConsensusFailed);
    }

    tracing::info!(
        "Initiated import {} for user {} ({} files, {} bytes manifest)",
        import_id,
        user_id,
        manifest.total_files(),
        manifest.total_bytes().unwrap_or(u64::MAX)
    );

    let record = payload.to_record();

    // 8. Spawn extraction. Session moves into the task; the upload.tar.gz
    //    file in `staging` is consumed by the bg walk. Staging dir lives
    //    until the terminal sweep.
    let state_clone = state.clone();
    let manifest_clone = manifest.clone();
    let import_id_clone = import_id.clone();
    let staging_clone = staging.clone();
    tokio::spawn(async move {
        if let Err(e) = run_extraction(
            state_clone,
            session,
            import_id_clone.clone(),
            user_id,
            manifest_clone,
            staging_clone,
        )
        .await
        {
            tracing::error!("Extraction failed for import {}: {:?}", import_id_clone, e);
        }
    });

    Ok(record)
}

/// Pull the first multipart field, validate name == "archive", stream its
/// body to `upload_path` via `tokio_util::io::StreamReader` adaption.
async fn stream_archive_field(
    multipart: &mut Multipart,
    upload_path: &std::path::Path,
) -> Result<(), ImportUploadError> {
    let field = multipart
        .next_field()
        .await
        .map_err(|e| ImportUploadError::BadMultipart(e.to_string()))?
        .ok_or(ImportUploadError::MissingArchiveField)?;

    if field.name() != Some("archive") {
        return Err(ImportUploadError::MissingArchiveField);
    }

    let mut reader = StreamReader::new(field.map(|r| r.map_err(std::io::Error::other)));
    let mut file = tokio::fs::File::create(upload_path).await?;
    tokio::io::copy(&mut reader, &mut file).await?;
    file.sync_all().await?;
    Ok(())
}

/// Apply the quota formula:
/// `total_bytes × 3 + STORAGE_SAFETY_MARGIN_BYTES ≤ sum(validator_available)`.
/// The validator aggregation is host machinery reached through
/// `TakeoutHooks::available_storage_bytes`; the FORMULA stays here.
async fn check_quota(
    state: &TakeoutState,
    manifest: &TakeoutManifest,
) -> Result<(), ImportUploadError> {
    let available = state
        .hooks
        .available_storage_bytes()
        .await
        .map_err(ImportUploadError::Internal)?;

    let total_bytes = manifest
        .total_bytes()
        .ok_or(ImportUploadError::SizeOverflow)?;
    let rs_expanded = total_bytes
        .checked_mul(3)
        .ok_or(ImportUploadError::SizeOverflow)?;
    let required = rs_expanded
        .checked_add(STORAGE_SAFETY_MARGIN_BYTES)
        .ok_or(ImportUploadError::SizeOverflow)?;

    if required > available {
        return Err(ImportUploadError::QuotaExceeded {
            required,
            available,
        });
    }
    Ok(())
}

/// Errors raised inside the spawned extraction task. The bg task has no HTTP
/// boundary — these surface only through tracing.
#[derive(Debug, thiserror::Error)]
pub enum ImportExtractError {
    #[error("DB error: {0:?}")]
    Database(DatabaseError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("consensus submission failed")]
    ConsensusFailed,
    #[error("archive read failed: {0}")]
    ArchiveRead(String),
    #[error("internal: {0}")]
    Internal(String),
}

impl From<DatabaseError> for ImportExtractError {
    fn from(e: DatabaseError) -> Self {
        ImportExtractError::Database(e)
    }
}

/// Background extraction task. Owns the session through the entire walk.
/// Status flips to `Importing` first, the per-import path table is created
/// and seeded from the manifest (one row per entry of every section, with
/// its projection + sidecar metadata), then the staging tar is reopened and
/// walked entry-by-entry: each file is streamed into
/// `{staging}/{projection}/{path}` while a parallel blake3 hasher computes
/// `blake3(plaintext ∥ blob_id)` and compares it to the manifest entry's
/// expected `content_hash`. Mismatches mark the row failed; surviving rows
/// stay Pending for the creation walk.
async fn run_extraction(
    state: TakeoutState,
    session: UserSession,
    import_id: CustomUUID,
    user_id: i32,
    manifest: TakeoutManifest,
    staging: PathBuf,
) -> Result<(), ImportExtractError> {
    // Hold the session for the duration of the bg task — the projection
    // translators re-resolve their own sessions through the seam, but the
    // upload-scope guarantee (crypto material survives logout mid-import)
    // rides this clone exactly as before.
    let _session = session;

    // 1. Flip imports.status → Importing via consensus.
    submit_status_update(&state, user_id, &import_id, ImportStatus::Importing).await?;

    // 2. Create per-import paths table, seed Pending rows from manifest —
    //    every section, translator or not (skip decision happens at the
    //    creation walk so status counts always cover the full archive).
    {
        let mut conn = state
            .db_pool
            .get()
            .map_err(|_| ImportExtractError::Internal("db pool".into()))?;
        import_paths::create_import_paths_table(&conn, &import_id)?;
        let tx = conn
            .transaction()
            .map_err(|_| ImportExtractError::Internal("tx open".into()))?;
        for (projection, section) in &manifest.projections {
            for entry in &section.entries {
                let (path_type, size) = match entry.kind {
                    EntryKind::Folder => (InodeType::Folder, None),
                    EntryKind::File => (InodeType::File, Some(entry.size)),
                };
                let metadata = if entry.metadata.is_null() {
                    None
                } else {
                    Some(serde_json::to_string(&entry.metadata).unwrap_or_default())
                };
                import_paths::insert_path_pending(
                    &tx,
                    &import_id,
                    projection,
                    &entry.logical_path,
                    &path_type,
                    size,
                    entry.blob_id.as_ref(),
                    metadata.as_deref(),
                )?;
            }
        }
        hopnet_projection::dbstats::commit_timed(tx)
            .map_err(|_| ImportExtractError::Internal("tx commit".into()))?;
    }

    // 3. Walk archive entries on the blocking pool. tar+flate2 are sync; the
    //    inner closure also takes a fresh DB connection from the pool to mark
    //    failed rows as it goes — no result threading required.
    let upload_path = staging.join("upload.tar.gz");
    let staging_root = staging.clone();
    let pool = state.db_pool.clone();
    let manifest_for_blocking = manifest.clone();
    let import_id_for_blocking = import_id.clone();
    let extraction = tokio::task::spawn_blocking(move || {
        walk_archive_entries(
            pool,
            import_id_for_blocking,
            manifest_for_blocking,
            upload_path,
            staging_root,
        )
    })
    .await
    .map_err(|e| ImportExtractError::Internal(format!("spawn_blocking join: {}", e)))?;
    extraction?;

    tracing::info!(
        "Extraction complete for import {} (user {}); starting creation walk",
        import_id,
        user_id
    );

    // 4. Creation walk: per projection section → translator (or skip) →
    //    terminal flip.
    run_creation_phase(&state, &import_id, user_id, &staging).await?;

    Ok(())
}

/// Creation walk. Per projection section (deterministic name order): when a
/// translator is registered, Pending folder rows (depth-asc) then Pending
/// file rows are driven through `import_entry` one at a time (uniform
/// resume/progress per row) with `flush` at the section end — the
/// projection's batch boundary. Sections with NO registered translator have
/// ALL their Pending rows marked Skipped (`error_code = "no_translator"`) —
/// the import still completes. Terminal: submit
/// `update_import_status(Completed)`, fire the host's completion hook, and
/// remove the staging dir.
///
/// Idempotent — only walks rows with `status = Pending` so resumed imports
/// skip already-Imported work.
pub async fn run_creation_phase(
    state: &TakeoutState,
    import_id: &CustomUUID,
    user_id: i32,
    staging: &Path,
) -> Result<(), ImportExtractError> {
    // Test-only barrier: pauses here so orchestrator can stop the owner
    // mid-import (after extraction + status flip but before any file/folder
    // creation) and verify resume on re-auth. No-op in production.
    state
        .runtime
        .barriers
        .wait(crate::barriers::names::BEFORE_IMPORT_CREATION_WALK)
        .await;

    let projections = {
        let conn = state
            .db_pool
            .get()
            .map_err(|_| ImportExtractError::Internal("db pool".into()))?;
        import_paths::list_projections(&conn, import_id)?
    };

    let mut attempted_folders = 0usize;
    let mut attempted_files = 0usize;

    for projection in &projections {
        let Some(exporter) = state.exporter(projection).cloned() else {
            // Skip-unknown-sections contract: no translator → the whole
            // section is reported skipped, never failed.
            let skipped = {
                let mut conn = state
                    .db_pool
                    .get()
                    .map_err(|_| ImportExtractError::Internal("db pool".into()))?;
                let tx = conn
                    .transaction()
                    .map_err(|_| ImportExtractError::Internal("tx open".into()))?;
                let n = import_paths::mark_projection_skipped(
                    &tx,
                    import_id,
                    projection,
                    "no_translator",
                )?;
                hopnet_projection::dbstats::commit_timed(tx)
                    .map_err(|_| ImportExtractError::Internal("tx commit".into()))?;
                n
            };
            tracing::warn!(
                "Import {}: no translator registered for projection {:?}; {} entries skipped",
                import_id,
                projection,
                skipped
            );
            continue;
        };

        // 1. Folders depth-ascending so parents commit before children.
        let folder_rows = read_pending(state, import_id, projection, InodeType::Folder)?;
        attempted_folders += folder_rows.len();
        for row in &folder_rows {
            let entry = pending_to_entry(row);
            match exporter.import_entry(user_id, &entry, None).await {
                Ok(()) => mark_imported(state, import_id, projection, &row.path)?,
                Err(e) => {
                    let (code, message) = entry_error_parts(e);
                    tracing::warn!(
                        "import_entry (folder) {} failed for import {}: {} ({})",
                        row.path,
                        import_id,
                        code,
                        message
                    );
                    mark_failed(state, import_id, projection, &row.path, code, &message)?;
                }
            }
        }

        // 2. Files: each hands its already-extracted staging file to the
        //    translator. Sequential — translators await their own commit
        //    acks so backpressure is implicit.
        let file_rows = read_pending(state, import_id, projection, InodeType::File)?;
        attempted_files += file_rows.len();
        for row in &file_rows {
            let entry = pending_to_entry(row);
            let staged = staging.join(projection).join(&row.path);
            match exporter.import_entry(user_id, &entry, Some(&staged)).await {
                Ok(()) => mark_imported(state, import_id, projection, &row.path)?,
                Err(e) => {
                    let (code, message) = entry_error_parts(e);
                    tracing::warn!(
                        "import_entry (file) {} failed for import {}: {} ({})",
                        row.path,
                        import_id,
                        code,
                        message
                    );
                    mark_failed(state, import_id, projection, &row.path, code, &message)?;
                }
            }
        }

        // 3. Section-end batch boundary.
        if let Err(e) = exporter.flush(user_id).await {
            let (code, message) = entry_error_parts(e);
            tracing::error!(
                "flush({}) failed for import {}: {} ({})",
                projection,
                import_id,
                code,
                message
            );
        }
    }

    // Terminal flip — Importing → Completed.
    submit_status_update(state, user_id, import_id, ImportStatus::Completed).await?;

    // Onboarding bits via the host hook — best-effort: failure here doesn't
    // retract the import (host impl submits the same additive-flags tx as
    // before).
    if let Err(e) = state.hooks.import_completed(user_id).await {
        tracing::warn!(
            "onboarding flag update for user {} after import {} failed: {}",
            user_id,
            import_id,
            e
        );
    }

    // Remove staging dir; per-file extracted bytes are no longer needed
    // since fragments are committed network-wide. Stray staging is
    // preferable to retracting the terminal flip on cleanup error.
    if let Err(e) = tokio::fs::remove_dir_all(staging).await {
        if e.kind() != std::io::ErrorKind::NotFound {
            tracing::warn!("Staging cleanup for {} failed: {:?}", import_id, e);
        }
    }

    tracing::info!(
        "Import {} complete: {} folders + {} files attempted",
        import_id,
        attempted_folders,
        attempted_files
    );
    Ok(())
}

/// Rebuild the translator-facing entry from a work-table row (the sidecar
/// metadata was persisted at seeding, so a post-restart resume hands the
/// FULL entry to `import_entry` without the manifest in hand).
fn pending_to_entry(row: &import_paths::PendingPath) -> ExportEntry {
    let metadata = row
        .metadata
        .as_deref()
        .and_then(|m| serde_json::from_str(m).ok())
        .unwrap_or(serde_json::Value::Null);
    ExportEntry {
        logical_path: row.path.clone(),
        blob_id: row.source_data_block_id.clone(),
        size: row.size_bytes.unwrap_or(0),
        metadata,
        export_handle: None,
    }
}

/// Split an `ImportEntryError` into the row's structured (code, message).
/// Transient failures are recorded with a stable code too — automated
/// re-drive of transient rows is future work; nothing returns it today.
fn entry_error_parts(e: ImportEntryError) -> (&'static str, String) {
    match e {
        ImportEntryError::Permanent { code, message } => (code, message),
        ImportEntryError::Transient(message) => ("transient", message),
    }
}

fn read_pending(
    state: &TakeoutState,
    import_id: &CustomUUID,
    projection: &str,
    path_type: InodeType,
) -> Result<Vec<import_paths::PendingPath>, ImportExtractError> {
    let conn = state
        .db_pool
        .get()
        .map_err(|_| ImportExtractError::Internal("db pool".into()))?;
    Ok(import_paths::read_pending_paths(
        &conn, import_id, projection, path_type,
    )?)
}

/// Open a fresh transaction and stamp a row Imported.
fn mark_imported(
    state: &TakeoutState,
    import_id: &CustomUUID,
    projection: &str,
    path: &str,
) -> Result<(), ImportExtractError> {
    let mut conn = state
        .db_pool
        .get()
        .map_err(|_| ImportExtractError::Internal("db pool".into()))?;
    let tx = conn
        .transaction()
        .map_err(|_| ImportExtractError::Internal("tx open".into()))?;
    import_paths::mark_path_imported(&tx, import_id, projection, path)?;
    hopnet_projection::dbstats::commit_timed(tx)
        .map_err(|_| ImportExtractError::Internal("tx commit".into()))?;
    Ok(())
}

/// Open a fresh transaction and stamp a row Failed with structured error info.
fn mark_failed(
    state: &TakeoutState,
    import_id: &CustomUUID,
    projection: &str,
    path: &str,
    code: &str,
    message: &str,
) -> Result<(), ImportExtractError> {
    let mut conn = state
        .db_pool
        .get()
        .map_err(|_| ImportExtractError::Internal("db pool".into()))?;
    let tx = conn
        .transaction()
        .map_err(|_| ImportExtractError::Internal("tx open".into()))?;
    import_paths::mark_path_failed(&tx, import_id, projection, path, code, Some(message))?;
    hopnet_projection::dbstats::commit_timed(tx)
        .map_err(|_| ImportExtractError::Internal("tx commit".into()))?;
    Ok(())
}

/// Build + sign + submit an `update_import_status` consensus transaction for
/// `import_id`. USER-signed — exactly the signing path the pre-split code
/// used (`create_signed_user_transaction`), now through the gateway.
async fn submit_status_update(
    state: &TakeoutState,
    user_id: i32,
    import_id: &CustomUUID,
    new_status: ImportStatus,
) -> Result<(), ImportExtractError> {
    let payload = ImportStatusPayload {
        import_id: import_id.clone(),
        new_status,
    };
    let encoded = bincode::serde::encode_to_vec(&payload, bincode::config::standard())
        .map_err(|_| ImportExtractError::Internal("status payload encode".into()))?;
    state
        .txs
        .submit(TxSpec {
            function: "update_import_status",
            payload: encoded,
            signer: TxSigner::User(user_id),
        })
        .await
        .map_err(|e| {
            tracing::error!(
                "update_import_status submit failed for {}: {:?}",
                import_id,
                e
            );
            ImportExtractError::ConsensusFailed
        })?;
    Ok(())
}

/// Mark one path row failed from the sync walk (fresh conn + tx per event).
fn walk_mark_failed(
    pool: &r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
    import_id: &CustomUUID,
    projection: &str,
    path: &str,
    code: &str,
    message: &str,
) -> Result<(), ImportExtractError> {
    let mut conn = pool
        .get()
        .map_err(|_| ImportExtractError::Internal("db pool".into()))?;
    let tx = conn
        .transaction()
        .map_err(|_| ImportExtractError::Internal("tx open".into()))?;
    import_paths::mark_path_failed(&tx, import_id, projection, path, code, Some(message))?;
    hopnet_projection::dbstats::commit_timed(tx)
        .map_err(|_| ImportExtractError::Internal("tx commit".into()))?;
    Ok(())
}

/// Sync entry-by-entry walk of the staging tar.gz. The first entry is the
/// already-consumed manifest; we discard it and process the rest. Every
/// content entry lives under a `{projection}/` prefix; files are staged to
/// `{staging}/{projection}/{path}` after hash verification against the
/// manifest's `content_hash` + `blob_id`.
fn walk_archive_entries(
    pool: r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
    import_id: CustomUUID,
    manifest: TakeoutManifest,
    upload_path: PathBuf,
    staging_root: PathBuf,
) -> Result<(), ImportExtractError> {
    use flate2::read::GzDecoder;
    use std::fs::File;
    use tar::Archive;

    // (projection, logical_path) → manifest entry, for per-file verification.
    let manifest_files: HashMap<(&str, &str), &ManifestEntry> = manifest
        .projections
        .iter()
        .flat_map(|(projection, section)| {
            section
                .entries
                .iter()
                .filter(|e| e.kind == EntryKind::File)
                .map(move |e| ((projection.as_str(), e.logical_path.as_str()), e))
        })
        .collect();

    let file = File::open(&upload_path)?;
    let gz = GzDecoder::new(file);
    let mut archive = Archive::new(gz);
    let mut entries = archive
        .entries()
        .map_err(|e| ImportExtractError::ArchiveRead(format!("entries: {}", e)))?;

    // Discard the manifest entry (already validated at upload).
    if let Some(first) = entries.next() {
        let _ =
            first.map_err(|e| ImportExtractError::ArchiveRead(format!("first entry: {}", e)))?;
    }

    for entry_result in entries {
        let mut entry = match entry_result {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!("Skipping unreadable tar entry in {}: {}", import_id, e);
                continue;
            }
        };

        let path_in_archive = match entry.path() {
            Ok(p) => p.to_string_lossy().into_owned(),
            Err(e) => {
                tracing::warn!("Bad path in entry of {}: {}", import_id, e);
                continue;
            }
        };

        // Split `{projection}/{logical_path}`. Entries not under a manifest
        // section prefix can't be attributed — mark best-effort (the UPDATE
        // matches no seeded row; a warn is logged) and continue, matching
        // the v1 wrong-prefix handling.
        let (projection, user_path) = match path_in_archive.split_once('/') {
            Some((top, rest)) if !rest.is_empty() && manifest.projections.contains_key(top) => {
                (top.to_string(), rest.trim_end_matches('/').to_string())
            }
            _ => {
                walk_mark_failed(
                    &pool,
                    &import_id,
                    path_in_archive.split('/').next().unwrap_or(""),
                    &path_in_archive,
                    "wrong_prefix",
                    "entry not under a projection prefix",
                )?;
                continue;
            }
        };

        match entry.header().entry_type() {
            tar::EntryType::Directory => {
                // Folder rows already seeded as Pending; the creation walk
                // picks them up from the manifest-seeded table.
            }
            tar::EntryType::Regular => {
                let manifest_file =
                    match manifest_files.get(&(projection.as_str(), user_path.as_str())) {
                        Some(f) => *f,
                        None => {
                            walk_mark_failed(
                                &pool,
                                &import_id,
                                &projection,
                                &user_path,
                                "not_in_manifest",
                                "file entry not present in its manifest section",
                            )?;
                            continue;
                        }
                    };

                let staging_file = staging_root.join(&projection).join(&user_path);
                let outcome = extract_and_hash(&mut entry, &staging_file, manifest_file);
                match outcome {
                    Ok(()) => {
                        // Hash matched; row stays Pending for the creation walk.
                    }
                    Err(reason) => {
                        let _ = std::fs::remove_file(&staging_file);
                        walk_mark_failed(
                            &pool,
                            &import_id,
                            &projection,
                            &user_path,
                            reason.code(),
                            &reason.message(),
                        )?;
                    }
                }
            }
            other => {
                tracing::debug!(
                    "Skipping unsupported tar entry type {:?} in {}",
                    other,
                    import_id
                );
            }
        }
    }
    Ok(())
}

/// Per-file extraction failure category. Maps to `error_code` on the path row.
enum ExtractFailure {
    Io(std::io::Error),
    HashMismatch {
        expected: Blake3Hash,
        computed: Blake3Hash,
    },
    /// The manifest entry lacks the fields verification needs (blob_id /
    /// content_hash) — real exports always carry both for files.
    MissingManifestFields,
}

impl ExtractFailure {
    fn code(&self) -> &'static str {
        match self {
            ExtractFailure::Io(_) => "extract_io",
            ExtractFailure::HashMismatch { .. } => "hash_mismatch",
            ExtractFailure::MissingManifestFields => "missing_manifest_fields",
        }
    }
    fn message(&self) -> String {
        match self {
            ExtractFailure::Io(e) => format!("io: {}", e),
            ExtractFailure::HashMismatch { expected, computed } => {
                format!("expected {} got {}", expected, computed)
            }
            ExtractFailure::MissingManifestFields => {
                "file entry missing blob_id/content_hash".to_string()
            }
        }
    }
}

/// Stream `entry` into `staging_file` while computing
/// `blake3(plaintext ∥ blob_id.as_bytes())`. Compares against
/// `manifest_entry.content_hash` (formula unchanged from v1). 64 KiB read
/// buffer matches the upload-side chunking pattern.
fn extract_and_hash(
    entry: &mut dyn Read,
    staging_file: &Path,
    manifest_entry: &ManifestEntry,
) -> Result<(), ExtractFailure> {
    use std::io::Write;

    let (blob_id, expected_hash) = match (&manifest_entry.blob_id, manifest_entry.content_hash) {
        (Some(blob_id), Some(hash)) => (blob_id, hash),
        _ => return Err(ExtractFailure::MissingManifestFields),
    };

    if let Some(parent) = staging_file.parent() {
        std::fs::create_dir_all(parent).map_err(ExtractFailure::Io)?;
    }
    let mut out = std::fs::File::create(staging_file).map_err(ExtractFailure::Io)?;

    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = entry.read(&mut buf).map_err(ExtractFailure::Io)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        out.write_all(&buf[..n]).map_err(ExtractFailure::Io)?;
    }
    out.sync_all().map_err(ExtractFailure::Io)?;

    hasher.update(blob_id.as_bytes());
    let computed = Blake3Hash::new(hasher.finalize());
    if computed != expected_hash {
        return Err(ExtractFailure::HashMismatch {
            expected: expected_hash,
            computed,
        });
    }
    Ok(())
}
