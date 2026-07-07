//! The drive's takeout translator (RFC-015 Stage D5b).
//!
//! Implements [`ProjectionExporter`] for the "drive" manifest section:
//! - `enumerate`: one-SQL-moment read of the user's inodes (folders + files
//!   joined to `data_blocks` for sizes) with SIV path decryption via the
//!   session seam — entries carry DECRYPTED logical paths, and the encrypted
//!   path rides `export_handle` so `open()` needs no re-resolve. Files with
//!   no content blob (`data_id NULL`) are not exported — matching the
//!   pre-split takeout, which never materialized them.
//! - `open`: the drive's reconstruction stream.
//! - `import_entry`: `create_folder` / `create_file_with_fragments` — one
//!   consensus submit per entry, identical to the pre-split creation walk.
//! - `flush`: documented no-op — batching consensus txs underneath the
//!   per-entry contract is a later optimization.

use std::sync::Arc;

use hopnet_projection::host::BoxFuture;
use hopnet_projection::{
    ExportByteStream, ExportEntry, ExportEntryStream, ExportError, ImportEntryError,
    ProjectionExporter,
};

use crate::download::reconstruct_file_stream;
use crate::host::DriveState;
use crate::paths::decrypt_path;

pub struct DriveExporter {
    state: DriveState,
}

impl DriveExporter {
    pub fn new(state: DriveState) -> Self {
        Self { state }
    }
}

/// Rows read in the single enumeration moment.
struct RawInode {
    encrypted_path: String,
    is_folder: bool,
    blob_id: Option<hopnet_common::CustomUUID>,
    size: u64,
}

impl ProjectionExporter for DriveExporter {
    fn name(&self) -> &'static str {
        "drive"
    }

    fn enumerate(&self, user_id: i32) -> BoxFuture<'_, Result<ExportEntryStream, ExportError>> {
        Box::pin(async move {
            let session = self
                .state
                .sessions
                .user_session(user_id)
                .await
                .map_err(|e| ExportError(format!("session unavailable: {:?}", e)))?;

            // ONE SQL read moment (preserves the old in-apply snapshot
            // consistency now that enumeration runs post-commit): both
            // queries on one connection, rows buffered before any yield.
            // Drive-scale listings are path strings only — buffering is
            // cheap; photos-scale projections should page instead.
            let raw: Vec<RawInode> = {
                let conn = self
                    .state
                    .db_pool
                    .get()
                    .map_err(|e| ExportError(format!("db pool: {}", e)))?;
                let mut stmt = conn
                    .prepare(
                        "SELECT i.path, i.type, i.data_id, db.file_size
                         FROM inodes i
                         LEFT JOIN data_blocks db ON i.data_id = db.id
                         WHERE i.owner_id = ?
                         ORDER BY i.path",
                    )
                    .map_err(|e| ExportError(format!("prepare: {}", e)))?;
                let rows = stmt
                    .query_map([user_id], |row| {
                        let encrypted_path: String = row.get(0)?;
                        let inode_type: hopnet_common::InodeType = row.get(1)?;
                        let blob_id: Option<hopnet_common::CustomUUID> = row.get(2)?;
                        let size: Option<i64> = row.get(3)?;
                        Ok(RawInode {
                            encrypted_path,
                            is_folder: inode_type == hopnet_common::InodeType::Folder,
                            blob_id,
                            size: size.unwrap_or(0) as u64,
                        })
                    })
                    .map_err(|e| ExportError(format!("query: {}", e)))?;
                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(|e| ExportError(format!("row: {}", e)))?
            };

            // Decrypt + translate. Files without a blob (empty files) are
            // not exportable — same exclusion the old pipeline applied via
            // `data_id IS NOT NULL`. Per-row decrypt failures surface as
            // stream errors (the export marks the takeout run failed loudly
            // rather than silently dropping user data).
            let entries: Vec<Result<ExportEntry, ExportError>> = raw
                .into_iter()
                .filter(|r| r.is_folder || r.blob_id.is_some())
                .map(|r| {
                    let decrypted = decrypt_path(
                        r.encrypted_path.clone(),
                        &session.siv_key,
                        &session.siv_nonce,
                    )
                    .map_err(|e| {
                        ExportError(format!(
                            "path decryption failed for {}: {:?}",
                            r.encrypted_path, e
                        ))
                    })?;
                    Ok(ExportEntry {
                        logical_path: decrypted.trim_start_matches('/').to_string(),
                        blob_id: if r.is_folder { None } else { r.blob_id },
                        size: if r.is_folder { 0 } else { r.size },
                        metadata: serde_json::json!({}),
                        export_handle: Some(r.encrypted_path),
                    })
                })
                .collect();

            let stream: ExportEntryStream = Box::pin(tokio_stream::iter(entries));
            Ok(stream)
        })
    }

    fn open(
        &self,
        user_id: i32,
        entry: &ExportEntry,
    ) -> BoxFuture<'_, Result<ExportByteStream, ExportError>> {
        let export_handle = entry.export_handle.clone();
        let logical_path = entry.logical_path.clone();
        Box::pin(async move {
            let encrypted_path = export_handle.ok_or_else(|| {
                ExportError(format!("entry {} has no export handle", logical_path))
            })?;

            let stream = reconstruct_file_stream(&self.state, encrypted_path, user_id)
                .await
                .map_err(|e| {
                    // Same coarse categories the old materializer logged.
                    let msg = match e {
                        crate::download::FileReconstructionError::NotFound => "File not found",
                        crate::download::FileReconstructionError::Forbidden => "Access denied",
                        crate::download::FileReconstructionError::KeyDecryptionError => {
                            "Key decryption failed"
                        }
                        _ => "File reconstruction failed",
                    };
                    ExportError(msg.to_string())
                })?;

            use tokio_stream::StreamExt;
            let stream: ExportByteStream = Box::pin(
                stream.map(|item| item.map_err(|e| std::io::Error::other(format!("{:?}", e)))),
            );
            Ok(stream)
        })
    }

    fn import_entry<'a>(
        &'a self,
        user_id: i32,
        entry: &'a ExportEntry,
        staged_content: Option<&'a std::path::Path>,
    ) -> BoxFuture<'a, Result<(), ImportEntryError>> {
        Box::pin(async move {
            // Prepend "/" — `encrypt_path` only encrypts inputs with at
            // least one slash; a bare "alpha" would collapse to "/" (root
            // placeholder) and silently misroute the inode.
            let absolute = format!("/{}", entry.logical_path.trim_start_matches('/'));

            match staged_content {
                None => {
                    // Folder/container entry.
                    crate::upload::create_folder(&self.state, user_id, &absolute)
                        .await
                        .map_err(|status| ImportEntryError::Permanent {
                            code: "create_folder",
                            message: format!("status {}", status.as_u16()),
                        })
                }
                Some(staged) => {
                    let metadata = tokio::fs::metadata(staged).await.map_err(|e| {
                        ImportEntryError::Permanent {
                            code: "staging_metadata",
                            message: e.to_string(),
                        }
                    })?;
                    let file_size = metadata.len() as usize;

                    let source = tokio::fs::File::open(staged).await.map_err(|e| {
                        ImportEntryError::Permanent {
                            code: "staging_open",
                            message: e.to_string(),
                        }
                    })?;

                    // Split into parent + filename. Top-level files end up
                    // with parent = "/" and a filename — same shape
                    // `post_files` produces from `path: "/foo.txt"`.
                    let (parent, filename) = match absolute.rfind('/') {
                        Some(0) => ("/".to_string(), absolute[1..].to_string()),
                        Some(i) => (absolute[..i].to_string(), absolute[i + 1..].to_string()),
                        None => unreachable!("absolute path always contains '/'"),
                    };

                    crate::upload::create_file_with_fragments(
                        &self.state,
                        user_id,
                        &parent,
                        &filename,
                        source,
                        file_size,
                    )
                    .await
                    .map(|_data_block_id| ())
                    .map_err(|status| ImportEntryError::Permanent {
                        code: "create_file",
                        message: format!("status {}", status.as_u16()),
                    })
                }
            }
        })
    }

    /// Documented no-op: `import_entry` submits one consensus transaction
    /// per entry (v1-identical behavior). Batching submissions underneath
    /// the per-entry contract — accumulate here, settle at flush — is a
    /// later optimization; the section-end call site already exists.
    fn flush(&self, _user_id: i32) -> BoxFuture<'_, Result<(), ImportEntryError>> {
        Box::pin(async move { Ok(()) })
    }
}

/// Convenience for host wiring: the drive's translator as a trait object.
pub fn drive_exporter(state: DriveState) -> Arc<dyn ProjectionExporter> {
    Arc::new(DriveExporter::new(state))
}
