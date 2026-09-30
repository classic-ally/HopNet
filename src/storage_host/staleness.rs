//! The staleness pass (RFC-STORAGE-003 S4): a proposer-driven,
//! self-consuming drain.
//!
//! Selection is one indexed predicate — `desired_placement_height < T`,
//! T the latest storage-view transition — and every selected blob is
//! declared to T. Whether a blob actually moves is discovered later, by
//! fulfillment; re-goaling is not movement. The work-list consumes itself:
//! a declared blob leaves the set, so pagination needs no cursor.
//!
//! Two triggers: the propose hook — whoever assembles a block runs the
//! check and appends an owed page to its own proposal (origination and
//! inclusion collapse into one act) — and, for self-containment, the
//! grace rung: a node that has not observed the check for
//! `STALENESS_GRACE_SECS` submits a page directly through the consensus
//! queue. Per-blob CAS at apply dedups every race.

use std::sync::atomic::{AtomicI64, Ordering};

use crate::AppState;
use hopnet_storage::engine::policy::{DECLARE_PAGE_SIZE, STALENESS_GRACE_SECS};
use hopnet_storage::lifecycle::{self, DECLARE_TX_FN, DeclarePlacementTarget};

/// Unix seconds of the last observed `desired < T` check on this node: a
/// propose-hook run of our own, or anyone's declare page applying. Process
/// state (the tick statics precedent), never persisted — a restart simply
/// re-arms the grace timer.
static STALENESS_CHECKED_AT: AtomicI64 = AtomicI64::new(0);

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Record that the check occurred (hook ran here, or a page applied).
pub fn stamp_observed() {
    STALENESS_CHECKED_AT.store(now_unix(), Ordering::Relaxed);
}

/// Whether the grace rung is due: no observation within the grace window.
pub fn grace_due() -> bool {
    now_unix() - STALENESS_CHECKED_AT.load(Ordering::Relaxed) > STALENESS_GRACE_SECS
}

/// One owed declare page, or `None` when nothing is stale (or the record
/// has no transition yet). Stamps the observation either way.
pub fn owed_page(conn: &rusqlite::Connection) -> Result<Option<DeclarePlacementTarget>, String> {
    stamp_observed();
    let Some(t) = lifecycle::latest_transition_height(conn).map_err(|e| e.to_string())? else {
        return Ok(None);
    };
    let targets = lifecycle::stale_page(conn, t, DECLARE_PAGE_SIZE).map_err(|e| e.to_string())?;
    if targets.is_empty() {
        return Ok(None);
    }
    Ok(Some(DeclarePlacementTarget { targets }))
}

/// The propose hook: the block assembler's own signed declare page, if any
/// blob's goal predates the latest transition. Runs on the build
/// connection before `build_value`; `None` adds nothing to the proposal.
pub fn propose_hook(
    app_state: &AppState,
    conn: &rusqlite::Connection,
) -> Result<Option<crate::consensus::types::Transaction>, String> {
    let Some(page) = owed_page(conn)? else {
        return Ok(None);
    };
    let count = page.targets.len();
    let payload = bincode::serde::encode_to_vec(&page, bincode::config::standard())
        .map_err(|e| format!("declare page encode: {e}"))?;
    let tx = crate::consensus::dispatch::create_signed_transaction(
        app_state,
        DECLARE_TX_FN.to_string(),
        payload,
    )
    .map_err(|e| format!("declare page signing: {e:?}"))?;
    tracing::info!("staleness pass: proposing a declare page of {count} blobs");
    Ok(Some(tx))
}

/// The grace rung, run from the policy tick: submit one page directly when
/// the check has gone unobserved for the grace window. Returns how many
/// blobs were declared (0 when not due or nothing stale).
pub async fn grace_rung(app_state: &AppState) -> Result<usize, String> {
    if !grace_due() {
        return Ok(0);
    }
    let page = {
        let conn = app_state.db_pool.get().map_err(|e| format!("pool: {e}"))?;
        owed_page(&conn)?
    };
    let Some(page) = page else {
        return Ok(0);
    };
    let count = page.targets.len();
    let payload = bincode::serde::encode_to_vec(&page, bincode::config::standard())
        .map_err(|e| format!("declare page encode: {e}"))?;
    use hopnet_storage::traits::TxSubmitter;
    super::substrate_host::SubstrateHost::new(app_state.clone())
        .submit(DECLARE_TX_FN, payload)
        .await
        .map_err(|e| format!("declare page submit: {e:?}"))?;
    tracing::info!("staleness pass: grace rung declared {count} blobs (propose hook unobserved)");
    Ok(count)
}
