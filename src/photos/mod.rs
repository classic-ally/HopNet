pub mod converge;
pub mod dispatch_local;
pub mod query;
pub mod routes;

use std::collections::HashMap;
use std::sync::Arc;

use hopnet_photos_core::sidecar::SidecarDb;
use hopnet_storage::crypto::StaticRecipient;

pub(crate) fn sidecar_db_path(user_id: i32) -> std::path::PathBuf {
    crate::paths::data_dir().join(format!("photos_sidecar_{}.sqlite", user_id))
}

pub struct UserSidecarState {
    pub db: Arc<tokio::sync::Mutex<SidecarDb<StaticRecipient>>>,
    shutdown: tokio::sync::oneshot::Sender<()>,
    handle: tokio::task::JoinHandle<()>,
    converge_shutdown: tokio::sync::oneshot::Sender<()>,
    converge_handle: tokio::task::JoinHandle<()>,
    converge_notify: Arc<tokio::sync::Notify>,
}

pub struct PhotosHost {
    states: tokio::sync::RwLock<HashMap<i32, UserSidecarState>>,
    lifecycle: tokio::sync::Mutex<()>,
}

impl Default for PhotosHost {
    fn default() -> Self {
        Self::new()
    }
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
        app_state: crate::AppState,
    ) -> Result<(), String> {
        let _lifecycle = self.lifecycle.lock().await;
        let mut states = self.states.write().await;
        if states.contains_key(&user_id) {
            return Ok(());
        }
        let state = open_sidecar_state(user_id, recipient, app_state)?;
        states.insert(user_id, state);
        Ok(())
    }

    /// Wake the user's convergence worker immediately (e.g. right after an
    /// invite, so pre-staging doesn't wait for the next 30 s tick).
    pub async fn poke_converge(&self, user_id: i32) {
        if let Some(state) = self.states.read().await.get(&user_id) {
            state.converge_notify.notify_one();
        }
    }

    pub async fn disable(&self, user_id: i32) {
        let _lifecycle = self.lifecycle.lock().await;
        let state = self.states.write().await.remove(&user_id);
        if let Some(state) = state {
            stop_sidecar_state(state).await;
        }
        let path = sidecar_db_path(user_id);
        if let Err(e) = std::fs::remove_file(&path)
            && e.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(%user_id, "sidecar unlink on disable failed: {e}");
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
        app_state: crate::AppState,
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
        let state = open_sidecar_state(user_id, recipient, app_state)?;
        self.states.write().await.insert(user_id, state);
        Ok(())
    }
}

fn open_sidecar_state(
    user_id: i32,
    recipient: StaticRecipient,
    app_state: crate::AppState,
) -> Result<UserSidecarState, String> {
    let path = sidecar_db_path(user_id);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("mkdir: {e}"))?;
    }

    // StaticRecipient is deliberately not Clone; duplicate via its inner
    // secret for the second worker.
    let converge_recipient = StaticRecipient(recipient.0.clone());
    let db = SidecarDb::open(&path, recipient).map_err(|e| format!("sidecar open: {e}"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Err(e) = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)) {
            tracing::warn!(%user_id, "sidecar chmod 0600 failed: {e}");
        }
    }

    let db_pool = app_state.db_pool.clone();
    let db = Arc::new(tokio::sync::Mutex::new(db));
    let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(sync_worker(user_id, db.clone(), db_pool, shutdown_rx));
    // The convergence worker shares the session-key lifetime with the sync
    // worker: both die on sign-out/disable via stop_sidecar_state.
    let converge_notify = Arc::new(tokio::sync::Notify::new());
    let (converge_shutdown, converge_shutdown_rx) = tokio::sync::oneshot::channel();
    let converge_handle = tokio::spawn(converge::converge_worker(
        user_id,
        converge_recipient,
        app_state,
        converge_notify.clone(),
        converge_shutdown_rx,
    ));
    Ok(UserSidecarState {
        db,
        shutdown,
        handle,
        converge_shutdown,
        converge_handle,
        converge_notify,
    })
}

async fn stop_sidecar_state(state: UserSidecarState) {
    let UserSidecarState {
        db: _,
        shutdown,
        handle,
        converge_shutdown,
        converge_handle,
        converge_notify: _,
    } = state;
    let _ = shutdown.send(());
    let _ = converge_shutdown.send(());
    let _ = handle.await;
    let _ = converge_handle.await;
}

async fn sync_worker(
    user_id: i32,
    db: Arc<tokio::sync::Mutex<SidecarDb<StaticRecipient>>>,
    db_pool: r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
    mut shutdown: tokio::sync::oneshot::Receiver<()>,
) {
    // First sync: immediate — don't make the user wait 30 s after enable.
    membership_sync(user_id, &db, &db_pool).await;
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
                membership_sync(user_id, &db, &db_pool).await;
                if do_sync(user_id, &db, &db_pool, &mut shutdown).await {
                    return;
                }
            }
        }
    }
}

/// Membership-diff pre-phase: rematerialize shared libraries whose view
/// changed. Joining a library (or receiving late grants after joining)
/// does NOT bump the photo_changes cursor — the photos didn't change, the
/// user's view did — so this phase reconciles the sidecar against the
/// membership set + per-user `photo_view_changes` signals instead:
/// new/stale library → paged backfill; departed library → local purge
/// (the client half of kick; the mesh read gate already closed).
async fn membership_sync(
    user_id: i32,
    db: &Arc<tokio::sync::Mutex<SidecarDb<StaticRecipient>>>,
    db_pool: &r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
) {
    let node_state = tokio::task::spawn_blocking({
        let db_pool = db_pool.clone();
        move || query::read_membership_state(&db_pool, user_id)
    })
    .await;
    let (memberships, signals) = match node_state {
        Ok(Ok(state)) => state,
        Ok(Err(e)) => {
            tracing::warn!(%user_id, "photos membership state read failed: {e}");
            return;
        }
        Err(e) => {
            tracing::warn!(%user_id, "photos membership state join error: {e}");
            return;
        }
    };

    let local_states = {
        let db = db.clone();
        match tokio::task::spawn_blocking(move || db.blocking_lock().library_states()).await {
            Ok(Ok(states)) => states,
            Ok(Err(e)) => {
                tracing::warn!(%user_id, "sidecar library states read failed: {e}");
                return;
            }
            Err(e) => {
                tracing::warn!(%user_id, "sidecar library states join error: {e}");
                return;
            }
        }
    };

    // Purge libraries the user no longer belongs to.
    for (lib, _) in &local_states {
        if !memberships.contains(lib) {
            let db = db.clone();
            let lib = lib.clone();
            let purged = tokio::task::spawn_blocking(move || {
                let result = db.blocking_lock().purge_library(&lib);
                (lib, result)
            })
            .await;
            match purged {
                Ok((lib, Ok(()))) => {
                    tracing::info!(%user_id, %lib, "photos sidecar purged departed library")
                }
                Ok((lib, Err(e))) => {
                    tracing::warn!(%user_id, %lib, "sidecar library purge failed: {e}")
                }
                Err(e) => tracing::warn!(%user_id, "sidecar purge join error: {e}"),
            }
        }
    }

    // Backfill new libraries and re-backfill on a moved view signal.
    for lib in &memberships {
        let stored = local_states.iter().find(|(l, _)| l == lib).map(|(_, h)| *h);
        let signal = signals
            .iter()
            .find(|(l, _)| l == lib)
            .map(|(_, h)| *h)
            .unwrap_or(0);
        if stored.is_some_and(|h| h >= signal) {
            continue;
        }
        let mut after: Option<hopnet_common::CustomUUID> = None;
        for _ in 0..200 {
            let page = tokio::task::spawn_blocking({
                let db_pool = db_pool.clone();
                let lib = lib.clone();
                let after = after.clone();
                move || query::read_library_backfill(&db_pool, user_id, &lib, after.as_ref())
            })
            .await;
            let (changes, last) = match page {
                Ok(Ok(page)) => page,
                Ok(Err(e)) => {
                    tracing::warn!(%user_id, %lib, "library backfill read failed: {e}");
                    return;
                }
                Err(e) => {
                    tracing::warn!(%user_id, "library backfill join error: {e}");
                    return;
                }
            };
            let page_len = changes.len();
            if page_len > 0 {
                let db = db.clone();
                let applied = tokio::task::spawn_blocking(move || {
                    db.blocking_lock().apply_backfill(&changes)
                })
                .await;
                match applied {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        tracing::warn!(%user_id, %lib, "library backfill apply failed: {e}");
                        return;
                    }
                    Err(e) => {
                        tracing::warn!(%user_id, "library backfill apply join error: {e}");
                        return;
                    }
                }
            }
            if page_len < query::SYNC_BATCH_LIMIT as usize {
                break;
            }
            after = last;
        }
        let db = db.clone();
        let lib_c = lib.clone();
        let stamped = tokio::task::spawn_blocking(move || {
            db.blocking_lock().set_library_state(&lib_c, signal)
        })
        .await;
        match stamped {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::warn!(%user_id, %lib, "library state stamp failed: {e}"),
            Err(e) => tracing::warn!(%user_id, "library state stamp join error: {e}"),
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
