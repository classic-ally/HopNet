//! `blobs` refcount bookkeeping. DB rows only — file I/O is Phase 2+.

use chrono::Utc;
use sqlx::sqlite::Sqlite;
use sqlx::Executor;

use crate::error::Result;
use crate::ids::{ContentHash, LibraryId};
use crate::model::BlobRecord;

use super::StateStore;

impl StateStore {
    pub async fn blob(&self, library_id: &LibraryId, hash: &ContentHash) -> Result<Option<BlobRecord>> {
        Ok(
            sqlx::query_as("SELECT * FROM blobs WHERE library_id = ? AND content_hash = ?")
                .bind(library_id)
                .bind(hash)
                .fetch_optional(self.pool())
                .await?,
        )
    }
}

/// Increment the refcount, creating the row at 1 if absent. On conflict the
/// first writer's `ext` wins (spec §blobs notes).
pub(crate) async fn upsert_increment<'e, E>(
    exec: E,
    library_id: &LibraryId,
    hash: &ContentHash,
    ext: &str,
    size_bytes: i64,
) -> Result<()>
where
    E: Executor<'e, Database = Sqlite>,
{
    sqlx::query(
        "INSERT INTO blobs (library_id, content_hash, ext, size_bytes, ref_count, written_at) \
         VALUES (?, ?, ?, ?, 1, ?) \
         ON CONFLICT (library_id, content_hash) \
         DO UPDATE SET ref_count = ref_count + 1",
    )
    .bind(library_id)
    .bind(hash)
    .bind(ext)
    .bind(size_bytes)
    .bind(Utc::now())
    .execute(exec)
    .await?;
    Ok(())
}

// NOTE: the refcount `decrement` (returning the new count so the write path
// can delete the file at 0) arrives with Phase 2's write path — its callers
// (re-edit supersede, hard delete, hard move) don't exist yet.
