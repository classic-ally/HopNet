//! Step timing summaries shared by the node's ingest instrumentation and the
//! ingress daemon's publish instrumentation.
//!
//! [`summarize`] turns raw `(ms, bytes)` samples into count / p50 / p95 /
//! max (nearest-rank percentiles). [`RollingSteps`] keeps a bounded,
//! age-limited ring buffer of samples per named step and summarizes it on
//! demand. Instrumentation only — nothing here feeds a decision.

use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

/// One step's summary over a set of samples.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepStats {
    pub count: u64,
    pub p50_ms: u64,
    pub p95_ms: u64,
    pub max_ms: u64,
    /// Sum of all sample durations.
    pub total_ms: u64,
    /// Sum of the bytes the samples carried (0 for steps without a payload).
    pub bytes: u64,
    /// `bytes` over `total_ms`: the per-request rate, not wall-clock
    /// throughput (overlapping requests each count their own time).
    pub bytes_per_sec: u64,
}

/// Summarize `(ms, bytes)` samples. Empty input gives an all-zero summary.
pub fn summarize(samples: impl IntoIterator<Item = (u64, u64)>) -> StepStats {
    let mut durations = Vec::new();
    let mut bytes = 0u64;
    for (ms, b) in samples {
        durations.push(ms);
        bytes = bytes.saturating_add(b);
    }
    if durations.is_empty() {
        return StepStats::default();
    }
    durations.sort_unstable();
    let total_ms = durations
        .iter()
        .fold(0u64, |acc, ms| acc.saturating_add(*ms));
    StepStats {
        count: durations.len() as u64,
        p50_ms: nearest_rank(&durations, 50),
        p95_ms: nearest_rank(&durations, 95),
        max_ms: *durations.last().expect("non-empty"),
        total_ms,
        bytes,
        bytes_per_sec: bytes.saturating_mul(1000) / total_ms.max(1),
    }
}

/// Nearest-rank percentile over sorted, non-empty input.
fn nearest_rank(sorted: &[u64], pct: usize) -> u64 {
    let rank = (pct * sorted.len()).div_ceil(100).max(1);
    sorted[rank - 1]
}

/// Duration in whole milliseconds, saturating.
pub fn millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// A bounded rolling window of samples per named step: at most `cap`
/// samples per step, none older than `max_age`.
#[derive(Debug)]
pub struct RollingSteps {
    cap: usize,
    max_age: Duration,
    steps: BTreeMap<&'static str, VecDeque<(Instant, u64, u64)>>,
}

/// A [`RollingSteps`] snapshot, ready to serialize.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WindowSummary {
    pub window_secs: u64,
    pub max_samples_per_step: usize,
    pub steps: BTreeMap<String, StepStats>,
}

impl RollingSteps {
    pub fn new(cap: usize, max_age: Duration) -> Self {
        Self {
            cap: cap.max(1),
            max_age,
            steps: BTreeMap::new(),
        }
    }

    /// Record one sample taken at `now`.
    pub fn record(&mut self, step: &'static str, elapsed: Duration, bytes: u64, now: Instant) {
        let ring = self.steps.entry(step).or_default();
        if ring.len() == self.cap {
            ring.pop_front();
        }
        ring.push_back((now, millis(elapsed), bytes));
    }

    /// Drop samples older than the window, then summarize every step that
    /// still has any.
    pub fn summary(&mut self, now: Instant) -> WindowSummary {
        let max_age = self.max_age;
        for ring in self.steps.values_mut() {
            while ring
                .front()
                .is_some_and(|(at, _, _)| now.saturating_duration_since(*at) > max_age)
            {
                ring.pop_front();
            }
        }
        self.steps.retain(|_, ring| !ring.is_empty());
        WindowSummary {
            window_secs: max_age.as_secs(),
            max_samples_per_step: self.cap,
            steps: self
                .steps
                .iter()
                .map(|(step, ring)| {
                    (
                        (*step).to_string(),
                        summarize(ring.iter().map(|(_, ms, b)| (*ms, *b))),
                    )
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Should: report the nearest-rank median and 95th percentile, the max,
    // the totals and the per-request byte rate.
    #[test]
    fn summarize_reports_nearest_rank_percentiles_and_totals() {
        let stats = summarize((1..=100).map(|ms| (ms, 10)));
        assert_eq!(stats.count, 100);
        assert_eq!(stats.p50_ms, 50);
        assert_eq!(stats.p95_ms, 95);
        assert_eq!(stats.max_ms, 100);
        assert_eq!(stats.total_ms, 5050);
        assert_eq!(stats.bytes, 1000);
        assert_eq!(stats.bytes_per_sec, 1000 * 1000 / 5050);
    }

    // Should: read the only sample as every percentile.
    // Should not: divide by zero when every sample took under a millisecond.
    #[test]
    fn summarize_handles_one_sample_and_zero_durations() {
        let stats = summarize([(0, 4096)]);
        assert_eq!(
            (stats.count, stats.p50_ms, stats.p95_ms, stats.max_ms),
            (1, 0, 0, 0)
        );
        assert_eq!(stats.bytes_per_sec, 4096 * 1000);
        assert_eq!(summarize(std::iter::empty()), StepStats::default());
    }

    // Should: keep only the newest `cap` samples of a step.
    #[test]
    fn rolling_steps_evicts_the_oldest_sample_past_the_cap() {
        let now = Instant::now();
        let mut window = RollingSteps::new(3, Duration::from_secs(3600));
        for ms in [500, 1, 2, 3] {
            window.record("upload", Duration::from_millis(ms), 0, now);
        }
        let stats = &window.summary(now).steps["upload"];
        assert_eq!(stats.count, 3);
        assert_eq!(stats.max_ms, 3);
    }

    // Should: forget samples older than the window and drop a step left
    // with none.
    #[test]
    fn rolling_steps_forgets_samples_older_than_the_window() {
        let start = Instant::now();
        let mut window = RollingSteps::new(100, Duration::from_secs(60));
        window.record("submit", Duration::from_millis(900), 0, start);
        window.record("upload", Duration::from_millis(900), 0, start);
        let later = start + Duration::from_secs(61);
        window.record("upload", Duration::from_millis(7), 0, later);

        let summary = window.summary(later);
        assert!(!summary.steps.contains_key("submit"));
        assert_eq!(summary.steps["upload"].count, 1);
        assert_eq!(summary.steps["upload"].max_ms, 7);
        assert_eq!(summary.window_secs, 60);
    }
}
