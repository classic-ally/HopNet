//! Import side of the takeout/portability boundary. Owns the HTTP route
//! handlers, the upload workflow, and the per-failure error mapping.
//!
//! Phase 3.3 landed the multipart upload pipeline (manifest read + quota
//! check + `create_import`). Phase 3.4 extends `process_upload` to spawn a
//! background task that flips the import to `Importing`, seeds the per-import
//! path table from the manifest, and walks remaining tar entries to extract
//! and hash-verify each file. Phase 3.5 will pick up creation from the same
//! per-path table.

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

use crate::AppState;
use crate::auth::SessionEntry;
use crate::db::CustomUUID;
use crate::db::import_paths;
use crate::db::imports::{self, ImportPayload, ImportStatusPayload};
use crate::takeout::STORAGE_SAFETY_MARGIN_BYTES;
use crate::takeout::archive::{ImportArchiveError, read_manifest_from_archive};
use crate::takeout::manifest::{ARCHIVE_FILES_PREFIX, TakeoutManifest};
use crate::types::Blake3Hash;
use hopnet_common::{ImportPathCounts, ImportPathRow, ImportRecord, ImportStatus, InodeType};

/// Router for `/takeout/import` and its children. Mounted from
/// `takeout_routes()` via `.nest("/import", import::import_routes())`.
pub fn import_routes() -> Router<AppState> {
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
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
    multipart: Multipart,
) -> Result<(StatusCode, Json<ImportRecord>), StatusCode> {
    match process_upload(&app_state, user_id, multipart).await {
        Ok(record) => Ok((StatusCode::CREATED, Json(record))),
        Err(e) => {
            tracing::warn!("Import upload failed for user {}: {}", user_id, e);
            Err(e.status_code())
        }
    }
}

/// GET /takeout/import — return the user's current or most-recent import as a singleton.
async fn get_current_import(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
) -> Result<Json<Option<ImportRecord>>, StatusCode> {
    let conn = app_state
        .db_pool
        .get()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    match imports::get_current_import_for_user(&conn, user_id) {
        Ok(record) => Ok(Json(record)),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

/// GET /takeout/import/paths — owner-node-local debug view of the per-import
/// path table. Returns 404 from any non-owner node since the table is local;
/// 3.7 supersedes with an aggregate status route reachable from any node.
async fn get_current_import_paths(
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
) -> Result<Json<Vec<ImportPathRow>>, StatusCode> {
    let conn = app_state
        .db_pool
        .get()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let record = imports::get_current_import_for_user(&conn, user_id)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let self_node = app_state
        .get_node_id()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
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
    State(app_state): State<AppState>,
    Extension(user_id): Extension<i32>,
) -> Result<Json<ImportPathCounts>, StatusCode> {
    let conn = app_state
        .db_pool
        .get()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let record = imports::get_current_import_for_user(&conn, user_id)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let self_node = app_state
        .get_node_id()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
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
    Database(crate::db::DatabaseError),
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

impl From<crate::db::DatabaseError> for ImportUploadError {
    fn from(e: crate::db::DatabaseError) -> Self {
        ImportUploadError::Database(e)
    }
}

/// Per-import staging path: `{fragments_dir}/imports/{import_id_simple}/`.
pub(crate) fn staging_dir(state: &AppState, import_id: &CustomUUID) -> PathBuf {
    PathBuf::from(&state.fragments_dir)
        .join("imports")
        .join(import_id.to_string())
}

/// Cleanup staging on any failure path. Logs but does not propagate cleanup
/// errors — leaving stray staging is preferable to masking the original
/// failure cause.
async fn cleanup_staging(staging: &std::path::Path) {
    if let Err(e) = tokio::fs::remove_dir_all(staging).await
        && e.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!("Failed to clean staging {}: {:?}", staging.display(), e);
    }
}

/// Drive the full upload-side pipeline. The session clone lives in this
/// function's scope through return — no global pin map, RAII handles release
/// on either success or any error path. 3.4+ will extend the workflow with
/// extraction and per-file iteration; for 3.3 the function returns after
/// successful consensus submission of the `Pending` row.
pub async fn process_upload(
    state: &AppState,
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
    //    task ends. Owner-process death still strands the import; 3.7 lands
    //    persistent session re-establishment.
    let session = state
        .get_session(user_id)
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

    // 7. Submit create_import consensus txn.
    let node_id = state
        .get_node_id()
        .map_err(|_| ImportUploadError::Internal("node id unavailable".into()))?;
    let payload = ImportPayload {
        import_id: import_id.clone(),
        user_id,
        owner_node_id: node_id,
        status: ImportStatus::Pending,
    };
    let encoded = bincode::serde::encode_to_vec(&payload, bincode::config::standard())
        .map_err(|_| ImportUploadError::Internal("payload encode".into()))?;
    let transaction = crate::consensus::dispatch::create_signed_user_transaction(
        state,
        "create_import".to_string(),
        encoded,
        user_id,
    )
    .await
    .map_err(|_| ImportUploadError::ConsensusFailed)?;

    if let Err(e) = state.consensus_queue.submit(transaction).await {
        tracing::error!("Failed to submit create_import for {}: {:?}", import_id, e);
        cleanup_staging(&staging).await;
        return Err(ImportUploadError::ConsensusFailed);
    }

    tracing::info!(
        "Initiated import {} for user {} ({} files, {} bytes manifest)",
        import_id,
        user_id,
        manifest.total_files,
        manifest.total_bytes
    );

    let record = payload.to_record();

    // 8. Spawn extraction. Session moves into the task; the upload upload.tar.gz
    //    file in `staging` is consumed by the bg walk. Staging dir lives until
    //    3.7's terminal sweep (3.5 still needs the per-file extracted bytes).
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

/// Apply the spec § 3.3 quota formula:
/// `manifest.total_bytes × 3 + STORAGE_SAFETY_MARGIN_BYTES ≤ sum(validator_available)`.
async fn check_quota(
    state: &AppState,
    manifest: &TakeoutManifest,
) -> Result<(), ImportUploadError> {
    let height = {
        let conn = state
            .db_pool
            .get()
            .map_err(|_| ImportUploadError::Internal("db pool".into()))?;
        let tx = conn
            .unchecked_transaction()
            .map_err(|_| ImportUploadError::Internal("tx open".into()))?;
        crate::db::consensus::get_current_consensus_height(&tx)?
    };

    let available = imports::get_total_validator_storage_available(state, height).await?;

    let rs_expanded = manifest
        .total_bytes
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
pub(crate) enum ImportExtractError {
    #[error("DB error: {0:?}")]
    Database(crate::db::DatabaseError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("consensus submission failed")]
    ConsensusFailed,
    #[error("archive read failed: {0}")]
    ArchiveRead(String),
    #[error("internal: {0}")]
    Internal(String),
}

impl From<crate::db::DatabaseError> for ImportExtractError {
    fn from(e: crate::db::DatabaseError) -> Self {
        ImportExtractError::Database(e)
    }
}

/// Phase 3.4 background task. Owns the cloned session through the entire walk.
/// Status flips to `Importing` first, the per-import path table is created and
/// seeded from the manifest, then the staging tar is reopened and walked
/// entry-by-entry: each file is streamed into `{staging}/files/{user_path}`
/// while a parallel blake3 hasher computes `blake3(plaintext ∥ data_id)` and
/// compares it to the manifest entry's expected `file_hash`. Mismatches mark
/// the row failed; folders stay pending for 3.5 to materialize via consensus.
async fn run_extraction(
    state: AppState,
    session: SessionEntry,
    import_id: CustomUUID,
    user_id: i32,
    manifest: TakeoutManifest,
    staging: PathBuf,
) -> Result<(), ImportExtractError> {
    // Hold the session for the duration of the bg task — same scope guarantee
    // as Phase 3.3's handler-scoped clone.
    let _session = session;

    // 1. Flip imports.status → Importing via consensus.
    submit_status_update(&state, user_id, &import_id, ImportStatus::Importing).await?;

    // 2. Create per-import paths table, seed Pending rows from manifest.
    {
        let mut conn = state
            .db_pool
            .get()
            .map_err(|_| ImportExtractError::Internal("db pool".into()))?;
        import_paths::create_import_paths_table(&conn, &import_id)?;
        let tx = conn
            .transaction()
            .map_err(|_| ImportExtractError::Internal("tx open".into()))?;
        for folder in &manifest.folders {
            import_paths::insert_path_pending(
                &tx,
                &import_id,
                &folder.path,
                &InodeType::Folder,
                None,
                None,
            )?;
        }
        for file in &manifest.files {
            import_paths::insert_path_pending(
                &tx,
                &import_id,
                &file.path,
                &InodeType::File,
                Some(file.size),
                Some(&file.source_data_block_id),
            )?;
        }
        crate::db::shared::commit_timed(tx)
            .map_err(|_| ImportExtractError::Internal("tx commit".into()))?;
    }

    // 3. Walk archive entries on the blocking pool. tar+flate2 are sync; the
    //    inner closure also takes a fresh DB connection from the pool to mark
    //    failed rows as it goes — no result threading required.
    let upload_path = staging.join("upload.tar.gz");
    let files_root = staging.join("files");
    let pool = state.db_pool.clone();
    let manifest_for_blocking = manifest.clone();
    let import_id_for_blocking = import_id.clone();
    let extraction = tokio::task::spawn_blocking(move || {
        walk_archive_entries(
            pool,
            import_id_for_blocking,
            manifest_for_blocking,
            upload_path,
            files_root,
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

    // 4. Phase 3.5 creation walk: folders (depth-asc) → files → terminal flip.
    run_creation_phase(&state, &import_id, user_id, &staging).await?;

    Ok(())
}

/// Phase 3.5 creation walk. Reads Pending folder rows in depth-ascending
/// order, then Pending file rows, calling the existing `create_folder` /
/// `create_file_with_fragments` helpers. Successful rows flip to Imported;
/// per-call failures flip to Failed without aborting. Terminal: submit
/// `update_import_status(Completed)` and remove staging dir.
///
/// Sequential per spec § 3.5; Phase 5 swaps in JoinSet-based parallel-4 with
/// channel-coordinator fan-in. Idempotent — only walks rows with
/// `status = Pending` so resumed imports skip already-Imported work.
pub(crate) async fn run_creation_phase(
    state: &AppState,
    import_id: &CustomUUID,
    user_id: i32,
    staging: &Path,
) -> Result<(), ImportExtractError> {
    // Test-only barrier: pauses here so orchestrator can stop the owner
    // mid-import (after extraction + status flip but before any file/folder
    // creation) and verify resume on re-auth. No-op in production.
    state
        .takeout_runtime
        .barriers
        .wait(crate::takeout::barriers::names::BEFORE_IMPORT_CREATION_WALK)
        .await;

    // 1. Folders depth-ascending so parents commit before children.
    //    Prepend "/" — `encrypt_path` only encrypts inputs with at least one
    //    slash; a bare "alpha" would collapse to "/" (root placeholder) and
    //    silently misroute the inode.
    let folder_paths = read_pending_paths(state, import_id, InodeType::Folder)?;
    for path in &folder_paths {
        let absolute = format!("/{}", path.trim_start_matches('/'));
        match crate::files::helpers::create_folder(state, user_id, &absolute).await {
            Ok(()) => mark_imported(state, import_id, path)?,
            Err(status) => {
                tracing::warn!(
                    "create_folder {} failed for import {}: {}",
                    path,
                    import_id,
                    status
                );
                mark_failed(
                    state,
                    import_id,
                    path,
                    "create_folder",
                    &format!("status {}", status.as_u16()),
                )?;
            }
        }
    }

    // 2. Files: each opens its already-extracted staging file and streams it
    //    through the regular upload helper. Sequential — `submit_inodes`
    //    awaits commit-ack so backpressure is implicit.
    let files_root = staging.join("files");
    let file_paths = read_pending_paths(state, import_id, InodeType::File)?;
    for path in &file_paths {
        match create_one_file(state, user_id, &files_root, path).await {
            Ok(()) => mark_imported(state, import_id, path)?,
            Err((code, message)) => {
                tracing::warn!(
                    "create_file {} failed for import {}: {} ({})",
                    path,
                    import_id,
                    code,
                    message
                );
                mark_failed(state, import_id, path, code, &message)?;
            }
        }
    }

    // 3. Terminal flip — Importing → Completed.
    submit_status_update(state, user_id, import_id, ImportStatus::Completed).await?;

    // 3b. Onboarding bits — additive only (clear=NONE preserves any other
    //     bits the user has accumulated on other devices). Best-effort:
    //     failure here doesn't retract the import.
    if let Err(e) = crate::users::helpers::submit_onboarding_update(
        state,
        user_id,
        hopnet_common::OnboardingFlags::IMPORT_OFFERED
            | hopnet_common::OnboardingFlags::IMPORT_COMPLETED,
        hopnet_common::OnboardingFlags::NONE,
    )
    .await
    {
        tracing::warn!(
            "onboarding flag update for user {} after import {} failed: {}",
            user_id,
            import_id,
            e
        );
    }

    // 4. Remove staging dir; per-file extracted bytes are no longer needed
    //    since fragments are committed network-wide. Stray staging is
    //    preferable to retracting the terminal flip on cleanup error.
    if let Err(e) = tokio::fs::remove_dir_all(staging).await
        && e.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!("Staging cleanup for {} failed: {:?}", import_id, e);
    }

    tracing::info!(
        "Import {} complete: {} folders + {} files attempted",
        import_id,
        folder_paths.len(),
        file_paths.len()
    );
    Ok(())
}

/// Read paths from `import_paths_{id}` filtered by type and Pending status.
/// Folders ordered by depth (slash count) then path; files ordered by path.
fn read_pending_paths(
    state: &AppState,
    import_id: &CustomUUID,
    path_type: InodeType,
) -> Result<Vec<String>, ImportExtractError> {
    let conn = state
        .db_pool
        .get()
        .map_err(|_| ImportExtractError::Internal("db pool".into()))?;
    let table = crate::db::import_paths::table_name(import_id);
    let type_value = match path_type {
        InodeType::File => 0,
        InodeType::Folder => 1,
    };
    let query = match path_type {
        InodeType::Folder => format!(
            "SELECT path FROM {} WHERE type = ? AND status = 0
             ORDER BY (length(path) - length(replace(path, '/', ''))) ASC, path ASC",
            table
        ),
        InodeType::File => format!(
            "SELECT path FROM {} WHERE type = ? AND status = 0 ORDER BY path ASC",
            table
        ),
    };
    let mut stmt = conn
        .prepare(&query)
        .map_err(|e| ImportExtractError::Internal(format!("prepare {}: {}", table, e)))?;
    let rows = stmt
        .query_map([type_value], |row| row.get::<_, String>(0))
        .map_err(|e| ImportExtractError::Internal(format!("query {}: {}", table, e)))?;
    let mut out = Vec::new();
    for r in rows {
        out.push(r.map_err(|e| ImportExtractError::Internal(format!("row {}: {}", table, e)))?);
    }
    Ok(out)
}

/// Open the staging file for `path`, query its size, split parent/filename,
/// and call `create_file_with_fragments`. Returns `(error_code, message)` on
/// failure for use by `mark_path_failed`.
async fn create_one_file(
    state: &AppState,
    user_id: i32,
    files_root: &Path,
    path: &str,
) -> Result<(), (&'static str, String)> {
    let staging_file = files_root.join(path);
    let metadata = tokio::fs::metadata(&staging_file)
        .await
        .map_err(|e| ("staging_metadata", e.to_string()))?;
    let file_size = metadata.len() as usize;

    let source = tokio::fs::File::open(&staging_file)
        .await
        .map_err(|e| ("staging_open", e.to_string()))?;

    // Build absolute path then split into parent + filename. `encrypt_path`
    // requires at least one slash to encrypt segments; any bare relative
    // input collapses to "/". Top-level files end up with parent = "/" and
    // a filename — same shape `post_files` produces from `path: "/foo.txt"`.
    let absolute = format!("/{}", path.trim_start_matches('/'));
    let (parent, filename) = match absolute.rfind('/') {
        Some(0) => ("/".to_string(), absolute[1..].to_string()),
        Some(i) => (absolute[..i].to_string(), absolute[i + 1..].to_string()),
        None => unreachable!("absolute path always contains '/'"),
    };

    crate::files::helpers::create_file_with_fragments(
        state, user_id, &parent, &filename, source, file_size,
    )
    .await
    .map(|_data_block_id| ())
    .map_err(|status| ("create_file", format!("status {}", status.as_u16())))
}

/// Open a fresh transaction and stamp a row Imported.
fn mark_imported(
    state: &AppState,
    import_id: &CustomUUID,
    path: &str,
) -> Result<(), ImportExtractError> {
    let mut conn = state
        .db_pool
        .get()
        .map_err(|_| ImportExtractError::Internal("db pool".into()))?;
    let tx = conn
        .transaction()
        .map_err(|_| ImportExtractError::Internal("tx open".into()))?;
    crate::db::import_paths::mark_path_imported(&tx, import_id, path)?;
    crate::db::shared::commit_timed(tx)
        .map_err(|_| ImportExtractError::Internal("tx commit".into()))?;
    Ok(())
}

/// Open a fresh transaction and stamp a row Failed with structured error info.
fn mark_failed(
    state: &AppState,
    import_id: &CustomUUID,
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
    crate::db::import_paths::mark_path_failed(&tx, import_id, path, code, Some(message))?;
    crate::db::shared::commit_timed(tx)
        .map_err(|_| ImportExtractError::Internal("tx commit".into()))?;
    Ok(())
}

/// Build + sign + submit an `update_import_status` consensus transaction for
/// `import_id`. Authentication uses the user signing path the create_import
/// txn already used.
async fn submit_status_update(
    state: &AppState,
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
    let txn = crate::consensus::dispatch::create_signed_user_transaction(
        state,
        "update_import_status".to_string(),
        encoded,
        user_id,
    )
    .await
    .map_err(|_| ImportExtractError::ConsensusFailed)?;
    state.consensus_queue.submit(txn).await.map_err(|e| {
        tracing::error!(
            "update_import_status submit failed for {}: {:?}",
            import_id,
            e
        );
        ImportExtractError::ConsensusFailed
    })?;
    Ok(())
}

/// Sync entry-by-entry walk of the staging tar.gz. The first entry is the
/// already-consumed manifest; we discard it and process the rest.
fn walk_archive_entries(
    pool: r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
    import_id: CustomUUID,
    manifest: TakeoutManifest,
    upload_path: PathBuf,
    files_root: PathBuf,
) -> Result<(), ImportExtractError> {
    use flate2::read::GzDecoder;
    use std::fs::File;
    use tar::Archive;

    let manifest_files: HashMap<String, &crate::takeout::manifest::TakeoutManifestFile> =
        manifest.files.iter().map(|f| (f.path.clone(), f)).collect();

    let file = File::open(&upload_path)?;
    let gz = GzDecoder::new(file);
    let mut archive = Archive::new(gz);
    let mut entries = archive
        .entries()
        .map_err(|e| ImportExtractError::ArchiveRead(format!("entries: {}", e)))?;

    // Discard the manifest entry (already validated in 3.3).
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

        let user_path = match path_in_archive.strip_prefix(ARCHIVE_FILES_PREFIX) {
            Some(rest) => rest.trim_end_matches('/').to_string(),
            None => {
                let mut conn = pool
                    .get()
                    .map_err(|_| ImportExtractError::Internal("db pool".into()))?;
                let tx = conn
                    .transaction()
                    .map_err(|_| ImportExtractError::Internal("tx open".into()))?;
                import_paths::mark_path_failed(
                    &tx,
                    &import_id,
                    &path_in_archive,
                    "wrong_prefix",
                    Some("entry not under files/"),
                )?;
                crate::db::shared::commit_timed(tx)
                    .map_err(|_| ImportExtractError::Internal("tx commit".into()))?;
                continue;
            }
        };

        match entry.header().entry_type() {
            tar::EntryType::Directory => {
                // Folder rows already seeded as Pending; 3.5 picks up.
            }
            tar::EntryType::Regular => {
                let manifest_file = match manifest_files.get(&user_path) {
                    Some(f) => *f,
                    None => {
                        let mut conn = pool
                            .get()
                            .map_err(|_| ImportExtractError::Internal("db pool".into()))?;
                        let tx = conn
                            .transaction()
                            .map_err(|_| ImportExtractError::Internal("tx open".into()))?;
                        import_paths::mark_path_failed(
                            &tx,
                            &import_id,
                            &user_path,
                            "not_in_manifest",
                            Some("file entry not present in manifest.files"),
                        )?;
                        crate::db::shared::commit_timed(tx)
                            .map_err(|_| ImportExtractError::Internal("tx commit".into()))?;
                        continue;
                    }
                };

                let staging_file = files_root.join(&user_path);
                let outcome = extract_and_hash(&mut entry, &staging_file, manifest_file);
                match outcome {
                    Ok(()) => {
                        // Hash matched; row stays Pending for 3.5.
                    }
                    Err(reason) => {
                        let _ = std::fs::remove_file(&staging_file);
                        let mut conn = pool
                            .get()
                            .map_err(|_| ImportExtractError::Internal("db pool".into()))?;
                        let tx = conn
                            .transaction()
                            .map_err(|_| ImportExtractError::Internal("tx open".into()))?;
                        import_paths::mark_path_failed(
                            &tx,
                            &import_id,
                            &user_path,
                            reason.code(),
                            Some(&reason.message()),
                        )?;
                        crate::db::shared::commit_timed(tx)
                            .map_err(|_| ImportExtractError::Internal("tx commit".into()))?;
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
}

impl ExtractFailure {
    fn code(&self) -> &'static str {
        match self {
            ExtractFailure::Io(_) => "extract_io",
            ExtractFailure::HashMismatch { .. } => "hash_mismatch",
        }
    }
    fn message(&self) -> String {
        match self {
            ExtractFailure::Io(e) => format!("io: {}", e),
            ExtractFailure::HashMismatch { expected, computed } => {
                format!("expected {} got {}", expected, computed)
            }
        }
    }
}

/// Stream `entry` into `staging_file` while computing
/// `blake3(plaintext ∥ source_data_block_id.as_bytes())`. Compares against
/// `manifest_file.file_hash`. 64 KiB read buffer matches the upload-side
/// chunking pattern.
fn extract_and_hash(
    entry: &mut dyn Read,
    staging_file: &Path,
    manifest_file: &crate::takeout::manifest::TakeoutManifestFile,
) -> Result<(), ExtractFailure> {
    use std::io::Write;

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

    hasher.update(manifest_file.source_data_block_id.as_bytes());
    let computed = Blake3Hash::new(hasher.finalize());
    if computed != manifest_file.file_hash {
        return Err(ExtractFailure::HashMismatch {
            expected: manifest_file.file_hash,
            computed,
        });
    }
    Ok(())
}
