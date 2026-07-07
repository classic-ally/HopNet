//! Export (materialization) pipeline (RFC-015 Stage D5b).
//!
//! Reshaped from the host's `takeout::routes::execute_takeout_materialization`
//! + `takeout::materialization`: enumeration moved OUT of consensus apply
//! (decision 3) — this task asks each registered exporter to `enumerate()`
//! the user's state, populates the core work table, streams each entry's
//! content through `open()` into staging while CORE computes the manifest
//! hash (`blake3(plaintext ‖ blob_id)`, formula unchanged from v1), then
//! assembles the v2 archive and drives status transitions via the TxGateway
//! (node-signed, exactly what the pre-split code signed with).

use std::collections::BTreeMap;

use hopnet_common::{Blake3Hash, CustomUUID, TakeoutStatus};
use hopnet_projection::host::{TxSigner, TxSpec};
use hopnet_projection::{DatabaseError, ExportEntry};
use tokio_stream::StreamExt;

use crate::TakeoutState;
use crate::db::entries::{self, EntryRow, MaterializationStatus};
use crate::db::takeout::TakeoutStatusPayload;
use crate::manifest::{
    EntryKind, MANIFEST_VERSION, ManifestEntry, ProjectionSection, TakeoutManifest,
};

#[derive(Debug)]
pub enum TakeoutMaterializationError {
    Database(DatabaseError),
    Consensus(String),
    Serialization(String),
    Archive(std::io::Error),
}

impl std::fmt::Display for TakeoutMaterializationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TakeoutMaterializationError::Database(e) => write!(f, "Database error: {:?}", e),
            TakeoutMaterializationError::Consensus(e) => write!(f, "Consensus error: {}", e),
            TakeoutMaterializationError::Serialization(e) => {
                write!(f, "Serialization error: {}", e)
            }
            TakeoutMaterializationError::Archive(e) => write!(f, "Archive error: {}", e),
        }
    }
}

impl std::error::Error for TakeoutMaterializationError {}

/// Submit a node-signed `update_takeout_status` transaction — takeout status
/// transitions were node-signed pre-split (`create_signed_transaction`) and
/// stay node-signed through the gateway.
async fn submit_status(
    state: &TakeoutState,
    takeout_id: &CustomUUID,
    new_status: TakeoutStatus,
) -> Result<(), TakeoutMaterializationError> {
    let status_payload = TakeoutStatusPayload {
        takeout_id: takeout_id.clone(),
        new_status,
    };
    let encoded = bincode::serde::encode_to_vec(&status_payload, bincode::config::standard())
        .map_err(|e| {
            TakeoutMaterializationError::Serialization(format!("Failed to encode status: {:?}", e))
        })?;
    state
        .txs
        .submit(TxSpec {
            function: "update_takeout_status",
            payload: encoded,
            signer: TxSigner::Node,
        })
        .await
        .map_err(|e| {
            TakeoutMaterializationError::Consensus(format!("Failed to update status: {:?}", e))
        })
}

fn staging_root(state: &TakeoutState, takeout_id: &CustomUUID) -> String {
    format!(
        "{}/takeouts/{}/staging",
        state.fragments_dir,
        takeout_id.simple()
    )
}

/// Execute the complete takeout materialization process. Entry point for
/// the host's `takeout.materialize` work-scheduler arm and the manual
/// `POST /takeout/{id}/process` route.
pub async fn execute_takeout_materialization(
    state: &TakeoutState,
    takeout_id: &CustomUUID,
    user_id: i32,
) -> Result<(), TakeoutMaterializationError> {
    // Update status to materializing via consensus
    submit_status(state, takeout_id, TakeoutStatus::Materializing).await?;

    // Reserve a coordinator connection for the rest of the takeout pipeline.
    // Sequential phases (enumeration insert, folder materialize, per-file
    // status updates, manifest build) all use this conn, guaranteeing the
    // pipeline never fails mid-takeout due to pool contention from other
    // routes.
    let mut reserved_conn = state.db_pool.get().map_err(|e| {
        tracing::error!(
            "Failed to acquire reserved coordinator conn for takeout {}: {:?}",
            takeout_id,
            e
        );
        TakeoutMaterializationError::Database(DatabaseError::LockError)
    })?;

    // Phase 1: enumeration (decision 3 — moved here from consensus apply).
    // One work table spanning every registered projection; each exporter
    // streams its entries (drive reads in one SQL moment, preserving the old
    // in-apply snapshot consistency).
    entries::create_entries_table(&reserved_conn, takeout_id)
        .map_err(TakeoutMaterializationError::Database)?;

    for exporter in state.exporters.iter() {
        let projection = exporter.name();
        let mut stream = exporter.enumerate(user_id).await.map_err(|e| {
            tracing::error!(
                "Enumeration failed for projection {} on takeout {}: {:?}",
                projection,
                takeout_id,
                e
            );
            TakeoutMaterializationError::Consensus(format!(
                "enumerate({}) failed: {}",
                projection, e.0
            ))
        })?;

        // Drain the stream BEFORE opening the insert transaction — a
        // rusqlite Transaction is !Send and must never live across an await.
        let mut enumerated: Vec<ExportEntry> = Vec::new();
        while let Some(item) = stream.next().await {
            let entry = item.map_err(|e| {
                TakeoutMaterializationError::Consensus(format!(
                    "enumerate({}) stream failed: {}",
                    projection, e.0
                ))
            })?;
            enumerated.push(entry);
        }

        let count = enumerated.len();
        let tx = reserved_conn
            .transaction()
            .map_err(|_| TakeoutMaterializationError::Database(DatabaseError::LockError))?;
        for entry in &enumerated {
            // ExportEntry contract: `blob_id` is None for folders/containers
            // (entries with content always carry their blob).
            let kind = if entry.blob_id.is_some() {
                EntryKind::File
            } else {
                EntryKind::Folder
            };
            entries::insert_entry(&tx, takeout_id, projection, entry, kind)
                .map_err(TakeoutMaterializationError::Database)?;
        }
        hopnet_projection::dbstats::commit_timed(tx)
            .map_err(|_| TakeoutMaterializationError::Database(DatabaseError::ProcessingError))?;
        tracing::info!(
            "Enumerated {} entries for projection {} on takeout {}",
            count,
            projection,
            takeout_id
        );
    }

    // Phase 2: folder materialization — staging dirs per projection.
    let folder_result = materialize_folders(state, &mut reserved_conn, takeout_id)
        .map_err(TakeoutMaterializationError::Database)?;

    tracing::info!(
        "Folder materialization for takeout {} completed: {} succeeded, {} failed",
        takeout_id,
        folder_result.0,
        folder_result.1
    );

    // Phase 3: file materialization — per entry exporter.open() streamed to
    // staging while core computes the manifest hash.
    let file_result = materialize_all_files(state, &mut reserved_conn, takeout_id, user_id)
        .await
        .map_err(TakeoutMaterializationError::Database)?;

    tracing::info!(
        "Complete takeout materialization for {} finished: {} folders ({} failed), {} files ({} failed)",
        takeout_id,
        folder_result.0,
        folder_result.1,
        file_result.0,
        file_result.1
    );

    // Phase 4: archive assembly.
    tracing::info!("Starting archive creation for takeout {}", takeout_id);

    let success_rows = entries::list_success_entries(&reserved_conn, takeout_id)
        .map_err(TakeoutMaterializationError::Database)?;

    // Source username — displayed on the import side for diagnostic context.
    let source_username = match crate::db::takeout::get_username(&reserved_conn, user_id)
        .map_err(TakeoutMaterializationError::Database)?
    {
        Some(name) => name,
        None => {
            tracing::error!(
                "User {} not found when building manifest for takeout {}",
                user_id,
                takeout_id
            );
            return Err(TakeoutMaterializationError::Database(
                DatabaseError::RecallError,
            ));
        }
    };

    // Reserved conn is no longer needed; release the slot before archive I/O
    // and the final consensus submit.
    drop(reserved_conn);

    let manifest = build_manifest(state, takeout_id, source_username, &success_rows);
    let manifest_bytes = manifest.to_archive_bytes().map_err(|e| {
        TakeoutMaterializationError::Serialization(format!("manifest json: {:?}", e))
    })?;

    let staging = staging_root(state, takeout_id);
    let archive_entries: Vec<crate::archive::ArchiveEntry> = success_rows
        .iter()
        .map(|row| crate::archive::ArchiveEntry {
            staging_path: format!(
                "{}/{}/{}",
                staging,
                row.projection,
                row.path.trim_start_matches('/')
            ),
            archive_path: format!("{}/{}", row.projection, row.path.trim_start_matches('/')),
            is_directory: row.kind == EntryKind::Folder,
        })
        .collect();

    // Create archive path
    let archive_path = format!(
        "{}/takeouts/{}.tar.gz",
        state.fragments_dir,
        takeout_id.simple()
    );

    // Create the archive and clean up staging files
    let archive_size = crate::archive::create_archive(
        &manifest_bytes,
        archive_entries,
        &archive_path,
        true, // delete_source_files = true for cleanup
    )
    .map_err(TakeoutMaterializationError::Archive)?;

    tracing::info!(
        "Archive created successfully for takeout {}: {} bytes at {}",
        takeout_id,
        archive_size,
        archive_path
    );

    // Clean up the entire takeout directory (staging + uuid folder)
    let takeout_root = format!(
        "{}/takeouts/{}",
        state.fragments_dir,
        takeout_id.simple()
    );
    if let Err(e) = std::fs::remove_dir_all(&takeout_root) {
        tracing::warn!(
            "Failed to remove takeout directory {}: {:?}",
            takeout_root,
            e
        );
        // Continue anyway - archive is created
    } else {
        tracing::debug!("Cleaned up takeout directory: {}", takeout_root);
    }

    // Update status to Ready via consensus
    submit_status(state, takeout_id, TakeoutStatus::Ready).await?;

    tracing::info!("Takeout {} marked as ready for download", takeout_id);

    Ok(())
}

/// Materialize the folder structure into staging (`{staging}/{projection}/…`)
/// and mark rows Success/Failed. Paths arrive DECRYPTED from `enumerate()`
/// (decision 2), so no per-row crypto here.
fn materialize_folders(
    state: &TakeoutState,
    conn: &mut rusqlite::Connection,
    takeout_id: &CustomUUID,
) -> Result<(u32, u32), DatabaseError> {
    tracing::info!("Starting folder materialization for takeout {}", takeout_id);

    let staging = staging_root(state, takeout_id);
    std::fs::create_dir_all(&staging).map_err(|e| {
        tracing::error!("Failed to create staging directory {}: {:?}", staging, e);
        DatabaseError::ProcessingError
    })?;

    let folders = entries::list_pending_folders(conn, takeout_id)?;

    let tx = conn.transaction().map_err(|_| DatabaseError::LockError)?;

    let mut materialized_count = 0;
    let mut failed_count = 0;

    for row in &folders {
        let full_staging_path = format!(
            "{}/{}/{}",
            staging,
            row.projection,
            row.path.trim_start_matches('/')
        );
        match std::fs::create_dir_all(&full_staging_path) {
            Ok(_) => {
                tracing::debug!("Created directory: {}", full_staging_path);
                if let Err(e) = entries::update_entry_status(
                    &tx,
                    takeout_id,
                    &row.projection,
                    &row.path,
                    MaterializationStatus::Success,
                    None,
                    None,
                ) {
                    tracing::error!("Failed to update folder status: {:?}", e);
                    failed_count += 1;
                } else {
                    materialized_count += 1;
                }
            }
            Err(e) => {
                tracing::error!("Failed to create directory {}: {:?}", full_staging_path, e);
                let error_msg = format!("Directory creation failed: {}", e);
                let _ = entries::update_entry_status(
                    &tx,
                    takeout_id,
                    &row.projection,
                    &row.path,
                    MaterializationStatus::Failed,
                    Some(&error_msg),
                    None,
                );
                failed_count += 1;
            }
        }
    }

    hopnet_projection::dbstats::commit_timed(tx).map_err(|e| {
        tracing::error!("Failed to commit folder materialization: {:?}", e);
        DatabaseError::ProcessingError
    })?;

    tracing::info!(
        "Folder materialization completed: {} succeeded, {} failed",
        materialized_count,
        failed_count
    );

    Ok((materialized_count, failed_count))
}

/// Materialize all files for a takeout. Sequential per entry — each file is
/// `open()`ed from its projection's exporter, streamed to staging while the
/// manifest hash is computed, then its status committed individually on the
/// reserved conn for durability.
async fn materialize_all_files(
    state: &TakeoutState,
    reserved_conn: &mut rusqlite::Connection,
    takeout_id: &CustomUUID,
    user_id: i32,
) -> Result<(u32, u32), DatabaseError> {
    let pending = entries::list_pending_files(reserved_conn, takeout_id)?;
    let total = pending.len();
    if total == 0 {
        tracing::info!("No pending files to materialize for takeout {}", takeout_id);
        return Ok((0, 0));
    }

    tracing::info!(
        "Starting file materialization for takeout {}: {} files",
        takeout_id,
        total
    );

    let mut total_materialized = 0u32;
    let mut total_failed = 0u32;

    for row in pending {
        let (status, error_msg, manifest_hash) =
            materialize_single_file(state, takeout_id, &row, user_id).await;

        let tx = reserved_conn.transaction().map_err(|e| {
            tracing::error!(
                "Failed to open status-update tx for entry {}/{}: {:?}",
                row.projection,
                row.path,
                e
            );
            DatabaseError::ProcessingError
        })?;
        if let Err(e) = entries::update_entry_status(
            &tx,
            takeout_id,
            &row.projection,
            &row.path,
            status.clone(),
            error_msg.as_deref(),
            manifest_hash.as_ref(),
        ) {
            tracing::error!(
                "Failed to update status for entry {}/{}: {:?}",
                row.projection,
                row.path,
                e
            );
        } else if let Err(e) = hopnet_projection::dbstats::commit_timed(tx) {
            tracing::error!(
                "Failed to commit status update for entry {}/{}: {:?}",
                row.projection,
                row.path,
                e
            );
        }

        match status {
            MaterializationStatus::Success => total_materialized += 1,
            _ => total_failed += 1,
        }
    }

    tracing::info!(
        "File materialization completed for takeout {}: {} succeeded, {} failed",
        takeout_id,
        total_materialized,
        total_failed
    );

    Ok((total_materialized, total_failed))
}

/// Materialize a single file by streaming the exporter's reconstruction into
/// staging. Returns (status, optional_error_message, manifest_hash).
/// `manifest_hash` = blake3(plaintext ‖ blob_id), computed HERE at export
/// time — the DB's integrity hash is KEYED (RFC-014) and unverifiable by an
/// importer, so the manifest carries this export-computed value instead.
/// Stream integrity is enforced inside the exporter's reconstruction.
async fn materialize_single_file(
    state: &TakeoutState,
    takeout_id: &CustomUUID,
    row: &EntryRow,
    user_id: i32,
) -> (MaterializationStatus, Option<String>, Option<Blake3Hash>) {
    let Some(exporter) = state.exporter(&row.projection) else {
        // Unreachable in practice — rows only exist for registered
        // exporters — but a removed projection between enumerate and
        // materialize must not panic the pipeline.
        return (
            MaterializationStatus::Failed,
            Some(format!("no exporter registered for {}", row.projection)),
            None,
        );
    };

    let Some(blob_id) = row.blob_id.clone() else {
        return (
            MaterializationStatus::Failed,
            Some("entry has no blob id".to_string()),
            None,
        );
    };

    let entry: ExportEntry = row.to_export_entry();

    tracing::debug!("Materializing file: {}/{}", row.projection, row.path);

    let mut stream = match exporter.open(user_id, &entry).await {
        Ok(stream) => stream,
        Err(e) => {
            tracing::error!(
                "Failed to open entry {}/{} (blob_id: {}): {}",
                row.projection,
                row.path,
                blob_id,
                e.0
            );
            return (MaterializationStatus::Failed, Some(e.0), None);
        }
    };

    // Write file to staging directory (chunk-by-chunk streaming)
    let full_staging_path = format!(
        "{}/{}/{}",
        staging_root(state, takeout_id),
        row.projection,
        row.path.trim_start_matches('/')
    );

    // Ensure parent directory exists
    if let Some(parent) = std::path::Path::new(&full_staging_path).parent() {
        if let Err(e) = tokio::fs::create_dir_all(parent).await {
            tracing::error!(
                "Failed to create parent directory for {}: {:?}",
                full_staging_path,
                e
            );
            return (
                MaterializationStatus::Failed,
                Some(format!("Parent directory creation failed: {}", e)),
                None,
            );
        }
    }

    // Open file for writing
    let mut file = match tokio::fs::File::create(&full_staging_path).await {
        Ok(f) => f,
        Err(e) => {
            tracing::error!("Failed to create file {}: {:?}", full_staging_path, e);
            return (
                MaterializationStatus::Failed,
                Some(format!("File creation failed: {}", e)),
                None,
            );
        }
    };

    // Write chunks as they arrive from the stream, hashing plaintext in
    // parallel for the export manifest (import-side verification).
    use tokio::io::AsyncWriteExt;

    let mut hasher = blake3::Hasher::new();
    let mut total_bytes = 0;
    while let Some(chunk_result) = stream.next().await {
        let chunk = match chunk_result {
            Ok(chunk) => chunk,
            Err(e) => {
                tracing::error!(
                    "Stream error while reconstructing {}: {:?}",
                    full_staging_path,
                    e
                );
                return (
                    MaterializationStatus::Failed,
                    Some("Stream reconstruction error".to_string()),
                    None,
                );
            }
        };

        hasher.update(&chunk);

        if let Err(e) = file.write_all(&chunk).await {
            tracing::error!("Failed to write chunk to {}: {:?}", full_staging_path, e);
            return (
                MaterializationStatus::Failed,
                Some(format!("Chunk write failed: {}", e)),
                None,
            );
        }

        total_bytes += chunk.len();
    }

    // Ensure all data is flushed to disk
    if let Err(e) = file.sync_all().await {
        tracing::error!("Failed to sync file {}: {:?}", full_staging_path, e);
        return (
            MaterializationStatus::Failed,
            Some(format!("File sync failed: {}", e)),
            None,
        );
    }

    // Manifest hash: blake3(plaintext ‖ blob_id), the formula import-side
    // extraction recomputes. Export integrity itself is enforced INSIDE the
    // exporter's reconstruction stream (keyed whole-blob verify with the
    // per-blob key) — the DB's integrity hash is keyed (RFC-014) and not
    // recomputable here or by an importer.
    hasher.update(blob_id.as_bytes());
    let manifest_hash = Blake3Hash::new(hasher.finalize());

    tracing::debug!(
        "Materialized file: {} ({} bytes)",
        full_staging_path,
        total_bytes
    );
    (MaterializationStatus::Success, None, Some(manifest_hash))
}

/// Build the v2 manifest from the Success rows. Emits one section per
/// REGISTERED exporter (a projection that exported nothing gets an empty
/// section — the export record stays explicit), entries ordered folders
/// (depth-asc) then files (path-asc) per section.
fn build_manifest(
    state: &TakeoutState,
    takeout_id: &CustomUUID,
    source_username: String,
    success_rows: &[EntryRow],
) -> TakeoutManifest {
    let mut projections: BTreeMap<String, ProjectionSection> = state
        .exporters
        .iter()
        .map(|e| (e.name().to_string(), ProjectionSection::default()))
        .collect();

    for row in success_rows {
        let section = projections.entry(row.projection.clone()).or_default();
        match row.kind {
            EntryKind::Folder => section.total_folders += 1,
            EntryKind::File => {
                section.total_files += 1;
                section.total_bytes = section.total_bytes.saturating_add(row.size);
            }
        }
        section.entries.push(ManifestEntry {
            logical_path: row.path.trim_start_matches('/').to_string(),
            kind: row.kind,
            size: row.size,
            blob_id: row.blob_id.clone(),
            content_hash: row.manifest_hash,
            metadata: row.metadata.clone(),
        });
    }

    // UUIDv7 encodes creation time — no separate DB query needed for created_at.
    let created_at = takeout_id
        .extract_timestamp()
        .unwrap_or_else(chrono::Utc::now);

    TakeoutManifest {
        version: MANIFEST_VERSION,
        takeout_id: takeout_id.clone(),
        created_at,
        source_username,
        projections,
    }
}
