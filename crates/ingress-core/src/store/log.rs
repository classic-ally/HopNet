//! `ingest_log` — the black-box recorder. Authoritative for nothing.

use chrono::{DateTime, Utc};
use sqlx::Executor;
use sqlx::sqlite::Sqlite;

use crate::error::Result;
use crate::ids::PhotoId;

use super::StateStore;

/// One ingest-log row.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct LogEvent {
    pub id: i64,
    pub at: DateTime<Utc>,
    pub event_type: String,
    pub photo_id: Option<PhotoId>,
    pub detail: Option<String>,
}

impl StateStore {
    /// Append an ingest-log event (public entry point for callers outside
    /// the store's own transactions, e.g. the FFI layer's `unknown_uti`).
    pub async fn append_log(
        &self,
        event_type: &str,
        photo_id: Option<&PhotoId>,
        detail: Option<serde_json::Value>,
    ) -> Result<()> {
        append(self.pool(), event_type, photo_id, detail).await
    }

    /// Events of one type, oldest first — CLI history and test assertions.
    pub async fn log_events(&self, event_type: &str) -> Result<Vec<LogEvent>> {
        Ok(
            sqlx::query_as("SELECT * FROM ingest_log WHERE event_type = ? ORDER BY id")
                .bind(event_type)
                .fetch_all(self.pool())
                .await?,
        )
    }
}

pub(crate) async fn append<'e, E>(
    exec: E,
    event_type: &str,
    photo_id: Option<&PhotoId>,
    detail: Option<serde_json::Value>,
) -> Result<()>
where
    E: Executor<'e, Database = Sqlite>,
{
    sqlx::query("INSERT INTO ingest_log (at, event_type, photo_id, detail) VALUES (?, ?, ?, ?)")
        .bind(Utc::now())
        .bind(event_type)
        .bind(photo_id)
        .bind(detail.map(|d| d.to_string()))
        .execute(exec)
        .await?;
    Ok(())
}
