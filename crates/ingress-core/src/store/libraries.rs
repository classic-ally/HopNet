//! `libraries` table access and scope resolution.

use sqlx::Executor;
use sqlx::sqlite::Sqlite;

use crate::descriptor::LibraryScope;
use crate::error::Result;
use crate::ids::LibraryId;
use crate::model::{ICLOUD_SHARED_LIBRARY_BINDING, LibraryConfig};

use super::StateStore;

impl StateStore {
    /// Insert a library row (CLI configuration path).
    pub async fn insert_library(&self, lib: &LibraryConfig) -> Result<()> {
        insert(self.pool(), lib).await
    }

    /// All configured libraries.
    pub async fn libraries(&self) -> Result<Vec<LibraryConfig>> {
        Ok(
            sqlx::query_as("SELECT * FROM libraries ORDER BY library_id")
                .fetch_all(self.pool())
                .await?,
        )
    }

    pub async fn library(&self, id: &LibraryId) -> Result<Option<LibraryConfig>> {
        Ok(
            sqlx::query_as("SELECT * FROM libraries WHERE library_id = ?")
                .bind(id)
                .fetch_optional(self.pool())
                .await?,
        )
    }

    /// The library bound to a PhotoKit scope, if configured (public wrapper
    /// over the routing rule used by the resolve engine).
    pub async fn library_for_scope(&self, scope: LibraryScope) -> Result<Option<LibraryConfig>> {
        match resolve_scope(self.pool(), scope).await? {
            Some(id) => self.library(&id).await,
            None => Ok(None),
        }
    }
}

/// Insert a library row on any executor (libconfig logs in the same tx).
pub(crate) async fn insert<'e, E>(exec: E, lib: &LibraryConfig) -> Result<()>
where
    E: Executor<'e, Database = Sqlite>,
{
    sqlx::query(
        "INSERT INTO libraries \
         (library_id, display_name, scope_binding, retention_days, created_at) \
         VALUES (?, ?, ?, ?, ?)",
    )
    .bind(&lib.library_id)
    .bind(&lib.display_name)
    .bind(&lib.scope_binding)
    .bind(lib.retention_days)
    .bind(lib.created_at)
    .execute(exec)
    .await?;
    Ok(())
}

/// Set or clear a library's PhotoKit scope binding. Returns false if the
/// library does not exist. A UNIQUE violation (scope already bound
/// elsewhere) surfaces as the sqlx error for the caller to translate.
pub(crate) async fn update_scope_binding<'e, E>(
    exec: E,
    id: &LibraryId,
    binding: Option<&str>,
) -> Result<bool>
where
    E: Executor<'e, Database = Sqlite>,
{
    Ok(
        sqlx::query("UPDATE libraries SET scope_binding = ? WHERE library_id = ?")
            .bind(binding)
            .bind(id)
            .execute(exec)
            .await?
            .rows_affected()
            > 0,
    )
}

/// Update a library's retention window. Returns false if the library does
/// not exist.
pub(crate) async fn update_retention<'e, E>(exec: E, id: &LibraryId, days: i64) -> Result<bool>
where
    E: Executor<'e, Database = Sqlite>,
{
    Ok(
        sqlx::query("UPDATE libraries SET retention_days = ? WHERE library_id = ?")
            .bind(days)
            .bind(id)
            .execute(exec)
            .await?
            .rows_affected()
            > 0,
    )
}

/// Update a library's display name. Returns false if the library does not
/// exist.
pub(crate) async fn update_display_name<'e, E>(exec: E, id: &LibraryId, name: &str) -> Result<bool>
where
    E: Executor<'e, Database = Sqlite>,
{
    Ok(
        sqlx::query("UPDATE libraries SET display_name = ? WHERE library_id = ?")
            .bind(name)
            .bind(id)
            .execute(exec)
            .await?
            .rows_affected()
            > 0,
    )
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
