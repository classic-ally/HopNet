//! Rolling timings of the photo-ingress publish path, node side.
//!
//! The ingress daemon publishes each photo as one `POST
//! /api/photos/client/data-block/{id}` per resource and one `POST
//! /api/photos/client/transaction` (`photo_add`). Both handlers record where
//! their time went into a process-wide rolling window (last
//! [`WINDOW_CAP`] samples per step, none older than [`WINDOW_AGE`]), read
//! back at the owner-only `GET /api/debug/ingest/timings`.
//!
//! Upload steps come from [`hopnet_storage::api::PutTimings`]:
//! `upload_total` (the whole put, client body included, carrying the
//! bytes), `upload_permit_wait` (queued behind the process-wide put
//! permits), `upload_receive` (awaiting the client's body, carrying the
//! bytes so its rate is the effective upload rate), `upload_encode`,
//! `upload_ledger`, `upload_write` and `upload_process` (chunk-task wall
//! time; minus encode + ledger + write it is blocking-pool queueing).
//! `upload_failed` times failed puts.
//!
//! Transaction steps, keyed by kind so `photo_add` latency stands apart
//! from deletes, restores and edits: `tx_gate.<kind>` (the responsibility
//! check), `tx_sign.<kind>`, `tx_decide.<kind>` (enqueue to the consensus
//! decision, the submit_batch wait) and `tx_total.<kind>`; `tx_failed.<kind>`
//! times failed submits. `<kind>` is `photo_add` or `other`. Instrumentation only:
//! nothing here changes what either handler does.

use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use axum::{
    Extension, Json,
    extract::State,
    response::{IntoResponse, Response},
};
use hopnet_common::timing::{RollingSteps, WindowSummary, millis};
use hopnet_storage::api::PutTimings;

use crate::AppState;

/// Samples kept per step.
pub const WINDOW_CAP: usize = 512;
/// Samples older than this are dropped.
pub const WINDOW_AGE: Duration = Duration::from_secs(3600);

static TIMINGS: LazyLock<Mutex<RollingSteps>> =
    LazyLock::new(|| Mutex::new(RollingSteps::new(WINDOW_CAP, WINDOW_AGE)));

fn window() -> std::sync::MutexGuard<'static, RollingSteps> {
    TIMINGS.lock().unwrap_or_else(|p| p.into_inner())
}

/// Where a submitted transaction spent its time.
#[derive(Debug, Clone, Copy, Default)]
pub struct SubmitTimings {
    pub sign: Duration,
    /// Enqueue to decision: the consensus queue's submit_batch wait.
    pub decide: Duration,
}

/// Record one successful client upload.
pub fn record_upload(blob_id: &hopnet_storage::BlobId, t: &PutTimings) {
    tracing::debug!(
        %blob_id,
        bytes = t.bytes,
        total_ms = millis(t.total),
        permit_wait_ms = millis(t.permit_wait),
        receive_ms = millis(t.receive),
        encode_ms = millis(t.encode),
        ledger_ms = millis(t.ledger),
        write_ms = millis(t.write),
        process_ms = millis(t.process),
        "ingest upload timed"
    );
    record_upload_into(&mut window(), t, Instant::now());
}

fn record_upload_into(window: &mut RollingSteps, t: &PutTimings, now: Instant) {
    window.record("upload_total", t.total, t.bytes, now);
    window.record("upload_permit_wait", t.permit_wait, 0, now);
    window.record("upload_receive", t.receive, t.bytes, now);
    window.record("upload_encode", t.encode, 0, now);
    window.record("upload_ledger", t.ledger, 0, now);
    window.record("upload_write", t.write, 0, now);
    window.record("upload_process", t.process, 0, now);
}

/// Record one failed client upload (its phases are not reported on error).
pub fn record_upload_failure(blob_id: &hopnet_storage::BlobId, elapsed: Duration) {
    tracing::debug!(%blob_id, total_ms = millis(elapsed), "ingest upload failed");
    window().record("upload_failed", elapsed, 0, Instant::now());
}

/// Record one client transaction. `submit` is None when it was refused
/// before signing.
pub fn record_transaction(
    tx_type: &str,
    gate: Duration,
    submit: Option<SubmitTimings>,
    total: Duration,
    ok: bool,
) {
    tracing::debug!(
        tx_type,
        ok,
        gate_ms = millis(gate),
        sign_ms = submit.map(|s| millis(s.sign)),
        decide_ms = submit.map(|s| millis(s.decide)),
        total_ms = millis(total),
        "ingest transaction timed"
    );
    record_transaction_into(
        &mut window(),
        TxSteps::for_type(tx_type),
        gate,
        submit,
        total,
        ok,
        Instant::now(),
    );
}

/// One transaction kind's step names (the window keys are static).
struct TxSteps {
    gate: &'static str,
    sign: &'static str,
    decide: &'static str,
    total: &'static str,
    failed: &'static str,
}

impl TxSteps {
    fn for_type(tx_type: &str) -> &'static TxSteps {
        const PHOTO_ADD: TxSteps = TxSteps {
            gate: "tx_gate.photo_add",
            sign: "tx_sign.photo_add",
            decide: "tx_decide.photo_add",
            total: "tx_total.photo_add",
            failed: "tx_failed.photo_add",
        };
        const OTHER: TxSteps = TxSteps {
            gate: "tx_gate.other",
            sign: "tx_sign.other",
            decide: "tx_decide.other",
            total: "tx_total.other",
            failed: "tx_failed.other",
        };
        if tx_type == "photo_add" {
            &PHOTO_ADD
        } else {
            &OTHER
        }
    }
}

fn record_transaction_into(
    window: &mut RollingSteps,
    steps: &TxSteps,
    gate: Duration,
    submit: Option<SubmitTimings>,
    total: Duration,
    ok: bool,
    now: Instant,
) {
    window.record(steps.gate, gate, 0, now);
    if let Some(s) = submit {
        window.record(steps.sign, s.sign, 0, now);
        window.record(steps.decide, s.decide, 0, now);
    }
    let end = if ok { steps.total } else { steps.failed };
    window.record(end, total, 0, now);
}

/// The current window, summarized.
pub fn snapshot() -> WindowSummary {
    window().summary(Instant::now())
}

/// GET /debug/ingest/timings
pub async fn get_ingest_timings(
    State(app_state): State<AppState>,
    Extension(uid): Extension<i32>,
) -> Response {
    if let Err(status) = crate::auth::require_owner(&app_state, uid) {
        return status.into_response();
    }
    Json(snapshot()).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    // Should: file each phase of a put under its own upload step.
    // Should: carry the blob's bytes on the total and receive steps only.
    #[test]
    fn an_upload_lands_each_phase_under_its_own_step() {
        let mut window = RollingSteps::new(8, WINDOW_AGE);
        let t = PutTimings {
            permit_wait: ms(1),
            receive: ms(2),
            encode: ms(3),
            ledger: ms(4),
            write: ms(5),
            process: ms(13),
            total: ms(20),
            bytes: 4096,
        };
        record_upload_into(&mut window, &t, Instant::now());

        let steps = window.summary(Instant::now()).steps;
        let max = |step: &str| steps[step].max_ms;
        assert_eq!(max("upload_permit_wait"), 1);
        assert_eq!(max("upload_receive"), 2);
        assert_eq!(max("upload_encode"), 3);
        assert_eq!(max("upload_ledger"), 4);
        assert_eq!(max("upload_write"), 5);
        assert_eq!(max("upload_process"), 13);
        assert_eq!(max("upload_total"), 20);
        assert_eq!(steps["upload_total"].bytes, 4096);
        assert_eq!(steps["upload_receive"].bytes, 4096);
        assert_eq!(steps["upload_encode"].bytes, 0);
    }

    // Should: time a decided transaction's sign and decide steps and total.
    // Should not: count a failed submit as a decided one.
    #[test]
    fn a_failed_transaction_is_kept_apart_from_decided_ones() {
        let mut window = RollingSteps::new(8, WINDOW_AGE);
        let now = Instant::now();
        let submit = SubmitTimings {
            sign: ms(2),
            decide: ms(900),
        };
        let add = TxSteps::for_type("photo_add");
        record_transaction_into(&mut window, add, ms(1), Some(submit), ms(903), true, now);
        record_transaction_into(&mut window, add, ms(1), None, ms(5), false, now);

        let steps = window.summary(now).steps;
        assert_eq!(steps["tx_gate.photo_add"].count, 2);
        assert_eq!(steps["tx_decide.photo_add"].count, 1);
        assert_eq!(steps["tx_decide.photo_add"].max_ms, 900);
        assert_eq!(steps["tx_total.photo_add"].count, 1);
        assert_eq!(steps["tx_total.photo_add"].max_ms, 903);
        assert_eq!(steps["tx_failed.photo_add"].count, 1);
    }

    // Should: time photo_add apart from every other transaction kind.
    // Should not: let a slow delete or edit show up in photo_add's decide.
    #[test]
    fn photo_add_is_timed_apart_from_other_transaction_kinds() {
        let mut window = RollingSteps::new(8, WINDOW_AGE);
        let now = Instant::now();
        let fast = SubmitTimings {
            sign: ms(1),
            decide: ms(40),
        };
        let slow = SubmitTimings {
            sign: ms(1),
            decide: ms(9000),
        };
        let add = TxSteps::for_type("photo_add");
        record_transaction_into(&mut window, add, ms(1), Some(fast), ms(42), true, now);
        for kind in ["photo_delete", "photo_edit_content", "photo_restore"] {
            let steps = TxSteps::for_type(kind);
            record_transaction_into(&mut window, steps, ms(1), Some(slow), ms(9002), true, now);
        }

        let steps = window.summary(now).steps;
        assert_eq!(steps["tx_decide.photo_add"].count, 1);
        assert_eq!(steps["tx_decide.photo_add"].max_ms, 40);
        assert_eq!(steps["tx_decide.other"].count, 3);
        assert_eq!(steps["tx_total.other"].max_ms, 9002);
    }

    // Impact: the route sits behind auth_middleware, which admits any user
    // on the node; the handler's owner check is what keeps it owner-only.
    // Should: refuse a signed-in user who is not the owner with 403.
    // Should: answer the owner with the window summary.
    #[test]
    fn the_timings_route_answers_only_the_owner() {
        // Built outside any runtime: the test state blocks on its own.
        let app_state = crate::consensus::tests::create_test_app_state();
        app_state.user_id.set(7).unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(owner_only(app_state));
    }

    async fn owner_only(app_state: AppState) {
        let refused = get_ingest_timings(State(app_state.clone()), Extension(8)).await;
        assert_eq!(refused.status(), axum::http::StatusCode::FORBIDDEN);

        let answered = get_ingest_timings(State(app_state), Extension(7)).await;
        assert_eq!(answered.status(), axum::http::StatusCode::OK);
        let body = axum::body::to_bytes(answered.into_body(), usize::MAX)
            .await
            .unwrap();
        let summary: WindowSummary = serde_json::from_slice(&body).unwrap();
        assert_eq!(summary.window_secs, WINDOW_AGE.as_secs());
        assert_eq!(summary.max_samples_per_step, WINDOW_CAP);
    }
}
