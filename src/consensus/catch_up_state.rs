use tokio::sync::{watch, Mutex};
use crate::consensus::functions::CatchUpError;
use crate::consensus::routes::perform_catch_up;
use crate::db::consensus as db;
use crate::AppState;

#[derive(Clone, Debug)]
pub enum CatchUpOutcome {
    Pending,
    Completed(i32),
    Failed(String),
}

struct InFlightCatchUp {
    target_view: i32,
    outcome: watch::Receiver<CatchUpOutcome>,
}

pub struct CatchUpState {
    inner: Mutex<Option<InFlightCatchUp>>,
}

enum Action {
    Attach(watch::Receiver<CatchUpOutcome>),
    Initiate(watch::Sender<CatchUpOutcome>),
    AlreadyCaughtUp,
}

impl CatchUpState {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(None),
        }
    }

    /// Single public API for cross-view catch-up. The first caller for a given
    /// target view initiates catch-up; all subsequent callers attach to its
    /// outcome via a watch channel.
    pub async fn ensure_view(
        &self,
        target_view: i32,
        app_state: &AppState,
    ) -> Result<(), CatchUpError> {
        loop {
            let action = {
                let mut state = self.inner.lock().await;
                match &*state {
                    Some(inflight) => {
                        let current = inflight.outcome.borrow().clone();
                        match current {
                            CatchUpOutcome::Completed(v) if v >= target_view => {
                                Action::AlreadyCaughtUp
                            }
                            CatchUpOutcome::Pending if inflight.target_view >= target_view => {
                                Action::Attach(inflight.outcome.clone())
                            }
                            _ => {
                                // Stale, failed, or insufficient target — replace
                                let (tx, rx) = watch::channel(CatchUpOutcome::Pending);
                                *state = Some(InFlightCatchUp {
                                    target_view,
                                    outcome: rx,
                                });
                                Action::Initiate(tx)
                            }
                        }
                    }
                    None => {
                        let (tx, rx) = watch::channel(CatchUpOutcome::Pending);
                        *state = Some(InFlightCatchUp {
                            target_view,
                            outcome: rx,
                        });
                        Action::Initiate(tx)
                    }
                }
            }; // inner mutex dropped

            match action {
                Action::AlreadyCaughtUp => return Ok(()),
                Action::Attach(mut rx) => {
                    // Wait for the in-flight catch-up to resolve
                    let _ = rx.changed().await;
                    let outcome = rx.borrow().clone();
                    match outcome {
                        CatchUpOutcome::Completed(v) if v >= target_view => return Ok(()),
                        CatchUpOutcome::Failed(_) => continue, // retry — loop will initiate fresh
                        _ => continue,
                    }
                }
                Action::Initiate(tx) => {
                    // We're the initiator. Acquire consensus_lock and do the work.
                    let result = async {
                        let _guard = app_state.consensus_lock.lock().await;
                        let conn = app_state.db_pool.get()
                            .map_err(|_| CatchUpError::Database)?;
                        let (our_view, _) = db::get_consensus_progress(&conn)
                            .map_err(|_| CatchUpError::Database)?;
                        if target_view > our_view {
                            perform_catch_up(app_state, our_view, target_view, None).await?;
                        }
                        let (final_view, _) = db::get_consensus_progress(&conn)
                            .map_err(|_| CatchUpError::Database)?;
                        Ok::<i32, CatchUpError>(final_view)
                    }
                    .await;

                    match result {
                        Ok(v) => {
                            let _ = tx.send(CatchUpOutcome::Completed(v));
                            return Ok(());
                        }
                        Err(e) => {
                            let _ = tx.send(CatchUpOutcome::Failed(format!("{:?}", e)));
                            return Err(e);
                        }
                    }
                }
            }
        }
    }
}
