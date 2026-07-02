//! `libraries` table access and scope resolution.

use sqlx::sqlite::Sqlite;
use sqlx::Executor;

use crate::descriptor::LibraryScope;
use crate::error::Result;
use crate::ids::LibraryId;
use crate::model::{LibraryConfig, ICLOUD_SHARED_LIBRARY_BINDING};

use super::StateStore;

impl StateStore {
    /// Insert a library row (CLI configuration path).
    pub async fn insert_library(&self, lib: &LibraryConfig) -> Result<()> {
        sqlx::query(
            "INSERT INTO libraries \
             (library_id, display_name, blob_root, sidecar_root_remote, scope_binding, retention_days, created_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&lib.library_id)
        .bind(&lib.display_name)
        .bind(&lib.blob_root)
        .bind(&lib.sidecar_root_remote)
        .bind(&lib.scope_binding)
        .bind(lib.retention_days)
        .bind(lib.created_at)
        .execute(self.pool())
        .await?;
        Ok(())
    }

    pub async fn library(&self, id: &LibraryId) -> Result<Option<LibraryConfig>> {
        Ok(sqlx::query_as("SELECT * FROM libraries WHERE library_id = ?")
            .bind(id)
            .fetch_optional(self.pool())
            .await?)
    }
}

/// Resolve a descriptor's scope to a configured `library_id`
/// (spec §Routing rule). `None` = unbound scope → the caller records the
/// photo with `library_id = NULL` and blocks ingest.
///
/// Personal = the single row with `scope_binding IS NULL` (MVP invariant);
/// Shared = the row bound to the fixed `icloud-shared-library` marker.
pub(crate) async fn resolve_scope<'e, E>(exec: E, scope: LibraryScope) -> Result<Option<LibraryId>>
where
    E: Executor<'e, Database = Sqlite>,
{
    let row: Option<(LibraryId,)> = match scope {
        LibraryScope::Personal => {
            sqlx::query_as("SELECT library_id FROM libraries WHERE scope_binding IS NULL LIMIT 1")
                .fetch_optional(exec)
                .await?
        }
        LibraryScope::Shared => {
            sqlx::query_as("SELECT library_id FROM libraries WHERE scope_binding = ?")
                .bind(ICLOUD_SHARED_LIBRARY_BINDING)
                .fetch_optional(exec)
                .await?
        }
    };
    Ok(row.map(|(id,)| id))
}
