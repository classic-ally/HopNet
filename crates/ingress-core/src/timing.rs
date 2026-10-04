//! Per-step publish timings: where each photo's publish spent its time.
//!
//! A publish task runs inside [`timed`], which scopes a task-local
//! [`PhotoTiming`]; the publisher calls [`record`] around each network step
//! (confirm probe, admission probe, membership fetch, each resource upload,
//! the `photo_add` submit). A wrapper publisher passes straight through, and
//! a call outside a scope (tombstone and edit propagation) records nothing.
//!
//! Each pass sums its photos into a [`TimingSummary`] and logs both the
//! summary and the raw samples as one `publish_pass` ingest-log event; the
//! CLI `status` view rebuilds a rolling window (the newest
//! [`WINDOW_PHOTOS`] photos of the last [`WINDOW`]) from those events, so
//! it works from a separate process. Instrumentation only — nothing here
//! feeds a publish decision.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use hopnet_common::timing::{StepStats, millis, summarize as summarize_samples};

/// The rolling window's age limit.
pub const WINDOW: Duration = Duration::from_secs(3600);
/// The rolling window's photo limit.
pub const WINDOW_PHOTOS: usize = 200;

/// One network step of a photo publish.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum PublishStep {
    /// The committed-state confirm probe.
    Probe,
    /// The free-space admission probe.
    Admission,
    /// The library membership fetch.
    Members,
    /// One resource upload (once per resource, with its bytes).
    Upload,
    /// A resource upload that failed (no bytes: how much streamed is unknown).
    UploadFailed,
    /// The `photo_add` submit, which waits for the consensus decision.
    Submit,
    /// A submit that failed (refused, timed out, or rejected).
    SubmitFailed,
    /// The whole publish call, client-side work between the steps included.
    Total,
}

impl PublishStep {
    pub fn as_str(self) -> &'static str {
        match self {
            PublishStep::Probe => "probe",
            PublishStep::Admission => "admission",
            PublishStep::Members => "members",
            PublishStep::Upload => "upload",
            PublishStep::UploadFailed => "upload_failed",
            PublishStep::Submit => "submit",
            PublishStep::SubmitFailed => "submit_failed",
            PublishStep::Total => "total",
        }
    }
}

/// One step sample: the step, its milliseconds and the bytes it carried.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StepSample(pub PublishStep, pub u64, pub u64);

/// One photo's publish, step by step.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct PhotoTiming {
    pub samples: Vec<StepSample>,
}

/// Per-step stats over a set of photos.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TimingSummary {
    pub photos: u64,
    /// Bytes uploaded across those photos (only upload steps carry bytes).
    pub bytes: u64,
    pub steps: BTreeMap<PublishStep, StepStats>,
}

tokio::task_local! {
    static CURRENT: RefCell<PhotoTiming>;
}

/// Record one step of the photo publish running in this task. A no-op
/// outside a [`timed`] scope.
pub fn record(step: PublishStep, elapsed: Duration, bytes: u64) {
    let _ = CURRENT.try_with(|timing| {
        timing
            .borrow_mut()
            .samples
            .push(StepSample(step, millis(elapsed), bytes));
    });
}

/// Run one photo publish with step recording on, and hand back what it
/// recorded (a [`PublishStep::Total`] sample last).
pub async fn timed<F: std::future::Future>(publish: F) -> (F::Output, PhotoTiming) {
    CURRENT
        .scope(RefCell::new(PhotoTiming::default()), async move {
            let started = Instant::now();
            let output = publish.await;
            record(PublishStep::Total, started.elapsed(), 0);
            (output, CURRENT.with(RefCell::take))
        })
        .await
}

/// Sum photos into per-step stats.
pub fn summarize<'a>(photos: impl IntoIterator<Item = &'a PhotoTiming>) -> TimingSummary {
    let mut by_step: BTreeMap<PublishStep, Vec<(u64, u64)>> = BTreeMap::new();
    let mut summary = TimingSummary::default();
    for photo in photos {
        summary.photos += 1;
        for StepSample(step, ms, bytes) in &photo.samples {
            summary.bytes = summary.bytes.saturating_add(*bytes);
            by_step.entry(*step).or_default().push((*ms, *bytes));
        }
    }
    summary.steps = by_step
        .into_iter()
        .map(|(step, samples)| (step, summarize_samples(samples)))
        .collect();
    summary
}

/// The `publish_pass` ingest-log event: the pass's counters, its timing
/// summary, and the raw samples the status window is rebuilt from.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PassLog<'a> {
    pub published: u64,
    pub already_published: u64,
    pub failed: u64,
    pub gave_up: u64,
    pub parked: bool,
    pub summary: TimingSummary,
    /// Borrowed when logging a pass, owned when reading one back.
    pub samples: std::borrow::Cow<'a, [PhotoTiming]>,
}

/// The status view's rolling window.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct TimingWindow {
    pub window_secs: u64,
    /// Passes the window's photos came from.
    pub passes: u64,
    pub summary: TimingSummary,
}

/// Fold `publish_pass` events (newest first) into the rolling window: the
/// newest [`WINDOW_PHOTOS`] photos. The caller has already applied the age
/// limit. An unreadable event is skipped, not fatal — the log is a
/// recorder, never authoritative. None when no photo was timed.
pub fn window<'a>(events_newest_first: impl IntoIterator<Item = &'a str>) -> Option<TimingWindow> {
    let mut photos: Vec<PhotoTiming> = Vec::new();
    let mut passes = 0u64;
    for detail in events_newest_first {
        if photos.len() >= WINDOW_PHOTOS {
            break;
        }
        let Ok(pass) = serde_json::from_str::<PassLog<'_>>(detail) else {
            continue;
        };
        if pass.samples.is_empty() {
            continue;
        }
        passes += 1;
        let room = WINDOW_PHOTOS - photos.len();
        // Samples are in join order, newest last.
        photos.extend(pass.samples.into_owned().into_iter().rev().take(room));
    }
    (!photos.is_empty()).then(|| TimingWindow {
        window_secs: WINDOW.as_secs(),
        passes,
        summary: summarize(&photos),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn photo(samples: &[(PublishStep, u64, u64)]) -> PhotoTiming {
        PhotoTiming {
            samples: samples
                .iter()
                .map(|&(step, ms, bytes)| StepSample(step, ms, bytes))
                .collect(),
        }
    }

    fn pass_json(samples: Vec<PhotoTiming>) -> String {
        serde_json::to_string(&PassLog {
            published: samples.len() as u64,
            already_published: 0,
            failed: 0,
            gave_up: 0,
            parked: false,
            summary: summarize(&samples),
            samples: samples.into(),
        })
        .unwrap()
    }

    // Should: record the steps a publish takes, in order, then its total.
    // Should not: record anything for a call made outside a timed scope.
    #[tokio::test]
    async fn timed_collects_the_steps_recorded_inside_it() {
        record(PublishStep::Submit, Duration::from_millis(5), 0);

        let (output, timing) = timed(async {
            record(PublishStep::Probe, Duration::from_millis(3), 0);
            record(PublishStep::Upload, Duration::from_millis(40), 1024);
            7
        })
        .await;

        assert_eq!(output, 7);
        let steps: Vec<_> = timing.samples.iter().map(|s| s.0).collect();
        assert_eq!(
            steps,
            [PublishStep::Probe, PublishStep::Upload, PublishStep::Total]
        );
        assert_eq!(timing.samples[1], StepSample(PublishStep::Upload, 40, 1024));
    }

    // Should: count every photo, sum uploaded bytes, and give each step its
    // own percentiles, with one upload sample per resource.
    #[test]
    fn summarize_counts_photos_bytes_and_per_step_samples() {
        let photos = [
            photo(&[
                (PublishStep::Upload, 100, 1000),
                (PublishStep::Upload, 300, 3000),
                (PublishStep::Submit, 50, 0),
            ]),
            photo(&[
                (PublishStep::Upload, 200, 2000),
                (PublishStep::Submit, 70, 0),
            ]),
        ];
        let summary = summarize(&photos);

        assert_eq!(summary.photos, 2);
        assert_eq!(summary.bytes, 6000);
        let upload = &summary.steps[&PublishStep::Upload];
        assert_eq!((upload.count, upload.p50_ms, upload.max_ms), (3, 200, 300));
        assert_eq!(summary.steps[&PublishStep::Submit].max_ms, 70);
        assert!(!summary.steps.contains_key(&PublishStep::Probe));
    }

    // Should: keep the newest photos of the newest passes, up to the cap.
    // Should not: let an unreadable or untimed event break the window.
    #[test]
    fn window_keeps_the_newest_photos_up_to_the_cap() {
        let slow = photo(&[(PublishStep::Submit, 9000, 0)]);
        let fast = photo(&[(PublishStep::Submit, 10, 0)]);
        let newest = pass_json(vec![fast.clone(); WINDOW_PHOTOS - 1]);
        let older = pass_json(vec![slow.clone(), fast]);
        let oldest = pass_json(vec![slow; 10]);
        let untimed = pass_json(Vec::new());

        let events = [
            newest.as_str(),
            "not json",
            untimed.as_str(),
            &older,
            &oldest,
        ];
        let window = window(events).unwrap();

        assert_eq!(window.passes, 2);
        assert_eq!(window.summary.photos, WINDOW_PHOTOS as u64);
        // The older pass's newest photo (fast) fills the last slot; its slow
        // one and the oldest pass fall outside the window.
        assert_eq!(window.summary.steps[&PublishStep::Submit].max_ms, 10);
        assert!(super::window(std::iter::empty()).is_none());
    }
}
