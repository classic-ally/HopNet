pub mod dispatch_local;
pub mod query;
pub mod routes;

use std::collections::HashMap;
use std::sync::Arc;

use hopnet_photos_core::sidecar::SidecarDb;
use hopnet_storage::crypto::StaticRecipient;

fn sidecar_db_dir() -> std::path::PathBuf {
    let main_db = crate::db::shared::get_database_path();
    std::path::Path::new(&main_db)
        .parent()
        .expect("database path has no parent directory")
        .to_path_buf()
}

pub(crate) fn sidecar_db_path(user_id: i32) -> std::path::PathBuf {
    sidecar_db_dir().join(format!("photos_sidecar_{}.sqlite", user_id))
}

pub struct UserSidecarState {
    pub db: Arc<tokio::sync::Mutex<SidecarDb<StaticRecipient>>>,
    shutdown: tokio::sync::oneshot::Sender<()>,
    handle: tokio::task::JoinHandle<()>,
}

pub struct PhotosHost {
    states: tokio::sync::RwLock<HashMap<i32, UserSidecarState>>,
    lifecycle: tokio::sync::Mutex<()>,
}

impl PhotosHost {
    pub fn new() -> Self {
        Self {
            states: tokio::sync::RwLock::new(HashMap::new()),
            lifecycle: tokio::sync::Mutex::new(()),
        }
    }

    pub async fn is_enabled(&self, user_id: i32) -> bool {
        self.states.read().await.contains_key(&user_id)
    }

    pub async fn enable(
        &self,
        user_id: i32,
        recipient: StaticRecipient,
        db_pool: r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
    ) -> Result<(), String> {
        let _lifecycle = self.lifecycle.lock().await;
        let mut states = self.states.write().await;
        if states.contains_key(&user_id) {
            return Ok(());
        }
        let state = open_sidecar_state(user_id, recipient, db_pool)?;
        states.insert(user_id, state);
        Ok(())
    }

    pub async fn disable(&self, user_id: i32) {
        let _lifecycle = self.lifecycle.lock().await;
        let state = self.states.write().await.remove(&user_id);
        if let Some(state) = state {
            stop_sidecar_state(state).await;
        }
        let path = sidecar_db_path(user_id);
        if let Err(e) = std::fs::remove_file(&path) {
            if e.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(%user_id, "sidecar unlink on disable failed: {e}");
            }
        }
    }

    /// Stop the sync worker and drop the in-memory key, but preserve the
    /// on-disk sidecar file. Used by sign-out — next enable resumes from
    /// the persisted cursor without full re-hydration.
    pub async fn shutdown(&self, user_id: i32) {
        let _lifecycle = self.lifecycle.lock().await;
        let state = self.states.write().await.remove(&user_id);
        if let Some(state) = state {
            stop_sidecar_state(state).await;
        }
    }

    pub async fn get_db(
        &self,
        user_id: i32,
    ) -> Option<Arc<tokio::sync::Mutex<SidecarDb<StaticRecipient>>>> {
        self.states.read().await.get(&user_id).map(|s| s.db.clone())
    }

    pub async fn reinit(
        &self,
        user_id: i32,
        recipient: StaticRecipient,
        db_pool: r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
    ) -> Result<(), String> {
        let _lifecycle = self.lifecycle.lock().await;
        let state = self.states.write().await.remove(&user_id);
        if let Some(state) = state {
            stop_sidecar_state(state).await;
        }
        let path = sidecar_db_path(user_id);
        if path.exists() {
            std::fs::remove_file(&path).map_err(|e| format!("remove: {e}"))?;
        }
        let state = open_sidecar_state(user_id, recipient, db_pool)?;
        self.states.write().await.insert(user_id, state);
        Ok(())
    }
}

fn open_sidecar_state(
    user_id: i32,
    recipient: StaticRecipient,
    db_pool: r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
) -> Result<UserSidecarState, String> {
    let path = sidecar_db_path(user_id);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("mkdir: {e}"))?;
    }

    let db = SidecarDb::open(&path, recipient).map_err(|e| format!("sidecar open: {e}"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Err(e) = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)) {
            tracing::warn!(%user_id, "sidecar chmod 0600 failed: {e}");
        }
    }

    let db = Arc::new(tokio::sync::Mutex::new(db));
    let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(sync_worker(user_id, db.clone(), db_pool, shutdown_rx));
    Ok(UserSidecarState {
        db,
        shutdown,
        handle,
    })
}

async fn stop_sidecar_state(state: UserSidecarState) {
    let UserSidecarState {
        db: _,
        shutdown,
        handle,
    } = state;
    let _ = shutdown.send(());
    let _ = handle.await;
}

async fn sync_worker(
    user_id: i32,
    db: Arc<tokio::sync::Mutex<SidecarDb<StaticRecipient>>>,
    db_pool: r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
    mut shutdown: tokio::sync::oneshot::Receiver<()>,
) {
    // First sync: immediate — don't make the user wait 30 s after enable.
    if drain_sync(user_id, &db, &db_pool, &mut shutdown).await {
        return;
    }

    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval.tick().await;

    loop {
        tokio::select! {
            _ = &mut shutdown => return,
            _ = interval.tick() => {
                if do_sync(user_id, &db, &db_pool, &mut shutdown).await {
                    return;
                }
            }
        }
    }
}

/// Drain all backlogged changes in a loop (500 per batch). Called at
/// enable time so the gallery is fully populated before the first 30 s tick.
async fn drain_sync(
    user_id: i32,
    db: &Arc<tokio::sync::Mutex<SidecarDb<StaticRecipient>>>,
    db_pool: &r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
    shutdown: &mut tokio::sync::oneshot::Receiver<()>,
) -> bool {
    for _ in 0..200 {
        match shutdown.try_recv() {
            Ok(()) | Err(tokio::sync::oneshot::error::TryRecvError::Closed) => return true,
            Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {}
        }
        let cursor = {
            let db = db.clone();
            tokio::task::spawn_blocking(move || db.blocking_lock().cursor().unwrap_or(0))
                .await
                .unwrap_or(0)
        };
        match run_one_sync(user_id, db, db_pool, cursor).await {
            Ok(false) => return false, // batch was empty or sub-capacity — done
            Ok(true) => continue,      // batch was full — more backlog
            Err(()) => return false,   // error logged inside
        }
    }
    tracing::warn!(%user_id, "photos drain_sync hit 200-round cap");
    false
}

/// Single sync tick: read cursor, fetch batch, apply. Returns `Ok(true)`
/// if the batch was non-empty and may have more (truncated), `Ok(false)`
/// if it was empty or under the cap, `Err(())` on any error.
async fn do_sync(
    user_id: i32,
    db: &Arc<tokio::sync::Mutex<SidecarDb<StaticRecipient>>>,
    db_pool: &r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
    shutdown: &mut tokio::sync::oneshot::Receiver<()>,
) -> bool {
    let cursor = {
        let db = db.clone();
        tokio::task::spawn_blocking(move || db.blocking_lock().cursor().unwrap_or(0))
            .await
            .unwrap_or(0)
    };
    match run_one_sync(user_id, db, db_pool, cursor).await {
        Ok(true) => {
            // Batch was truncated — more backlog pending. Don't wait 30 s;
            // drain the remaining backlog in the same wakeup.
            return drain_sync(user_id, db, db_pool, shutdown).await;
        }
        _ => false,
    }
}

async fn run_one_sync(
    user_id: i32,
    db: &Arc<tokio::sync::Mutex<SidecarDb<StaticRecipient>>>,
    db_pool: &r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
    cursor: u64,
) -> Result<bool, ()> {
    let batch = match tokio::task::spawn_blocking({
        let db_pool = db_pool.clone();
        move || query::read_photo_changes(&db_pool, user_id, cursor)
    })
    .await
    {
        Ok(Ok(batch)) => batch,
        Ok(Err(e)) => {
            tracing::warn!(%user_id, cursor, "photos read_changes error: {e}");
            return Err(());
        }
        Err(e) => {
            tracing::warn!(%user_id, "photos read_changes join error: {e}");
            return Err(());
        }
    };

    if batch.changes.is_empty() {
        return Ok(false);
    }

    let truncated = batch.changes.len() >= query::SYNC_BATCH_LIMIT as usize;

    let db = db.clone();
    let result =
        tokio::task::spawn_blocking(move || db.blocking_lock().sync_from_batch(batch)).await;
    match result {
        Err(e) => {
            tracing::warn!(%user_id, "photos sync join error: {e}");
            Err(())
        }
        Ok(Err(e)) => {
            tracing::warn!(%user_id, "photos sync error, retrying: {e}");
            Err(())
        }
        Ok(Ok(())) => Ok(truncated),
    }
}
