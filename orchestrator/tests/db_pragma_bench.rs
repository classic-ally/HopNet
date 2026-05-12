use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::NodeInfo;
use crate::tests::files::{list_files, upload_file};
use crate::tests::{Check, TestResult, TestScenario, print_and_add_check};

/// Mirror of `DbStats` from /debug/db-stats. Keep field names in sync with
/// `src/db/debug.rs::DbStats`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DbStatsClient {
    page_count: i64,
    page_size: i64,
    db_bytes: i64,
    freelist_count: i64,
    wal_bytes: u64,
    journal_mode: String,
    synchronous: String,
    cache_size_raw: i64,
    cache_bytes: i64,
    mmap_size: i64,
    temp_store: String,
    busy_timeout_ms: i64,
    counters: CounterSnapshot,
    commit_latency_us: LatencySnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CounterSnapshot {
    txn_commits: u64,
    txn_rollbacks: u64,
    conn_acquires: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LatencySnapshot {
    count: u64,
    p50_us: u64,
    p90_us: u64,
    p99_us: u64,
    p999_us: u64,
    max_us: u64,
}

async fn fetch_db_stats(node: &NodeInfo) -> Result<DbStatsClient> {
    // Retry transport-level failures: macOS ephemeral port exhaustion
    // (EADDRNOTAVAIL) after sustained HTTP load takes ~15s of TIME_WAIT to drain.
    // HTTP-level errors (4xx/5xx) are not retried.
    let url = format!("http://{}:{}/debug/db-stats", node.ip_address, node.port);
    let mut last_err: Option<anyhow::Error> = None;
    for attempt in 0..6 {
        if attempt > 0 {
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
        let send_result = Client::new()
            .get(&url)
            .header("Authorization", format!("Bearer {}", node.jwt_token))
            .timeout(Duration::from_secs(10))
            .send()
            .await;
        match send_result {
            Ok(resp) => {
                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    anyhow::bail!("db-stats returned {}: {}", status, body);
                }
                return resp.json().await.context("decode db-stats");
            }
            Err(e) => {
                last_err = Some(anyhow::anyhow!("attempt {}: {}", attempt + 1, e));
            }
        }
    }
    Err(last_err
        .unwrap_or_else(|| anyhow::anyhow!("GET /debug/db-stats failed with no error captured")))
}

/// Per-phase summary written into the result JSON dump.
#[derive(Debug, Serialize)]
struct PhaseSummary {
    name: String,
    wall_secs: f64,
    ops_attempted: u64,
    ops_succeeded: u64,
    ops_per_sec: f64,
    delta_commits: u64,
    delta_rollbacks: u64,
    delta_db_bytes: i64,
    delta_wal_bytes: i64,
    commit_p50_us: u64,
    commit_p90_us: u64,
    commit_p99_us: u64,
    commit_p999_us: u64,
    commit_max_us: u64,
    commit_count_in_phase: u64,
}

#[derive(Debug, Serialize)]
struct BenchReport {
    timestamp_ms: u128,
    pragmas: PragmaSnapshot,
    baseline: DbStatsClient,
    phases: Vec<PhaseSummary>,
    final_stats: DbStatsClient,
}

#[derive(Debug, Serialize)]
struct PragmaSnapshot {
    journal_mode: String,
    synchronous: String,
    cache_bytes: i64,
    mmap_size: i64,
    temp_store: String,
    page_size: i64,
}

fn timestamp_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

fn diff_phase(
    name: &str,
    wall: Duration,
    attempted: u64,
    succeeded: u64,
    before: &DbStatsClient,
    after: &DbStatsClient,
) -> PhaseSummary {
    let wall_secs = wall.as_secs_f64();
    let count_in_phase = after
        .commit_latency_us
        .count
        .saturating_sub(before.commit_latency_us.count);
    PhaseSummary {
        name: name.to_string(),
        wall_secs,
        ops_attempted: attempted,
        ops_succeeded: succeeded,
        ops_per_sec: if wall_secs > 0.0 {
            succeeded as f64 / wall_secs
        } else {
            0.0
        },
        delta_commits: after
            .counters
            .txn_commits
            .saturating_sub(before.counters.txn_commits),
        delta_rollbacks: after
            .counters
            .txn_rollbacks
            .saturating_sub(before.counters.txn_rollbacks),
        delta_db_bytes: after.db_bytes - before.db_bytes,
        delta_wal_bytes: (after.wal_bytes as i64) - (before.wal_bytes as i64),
        // Histogram is cumulative: percentiles are over the whole run, not the phase.
        // For per-phase percentiles we'd need a reset endpoint; document as cumulative.
        commit_p50_us: after.commit_latency_us.p50_us,
        commit_p90_us: after.commit_latency_us.p90_us,
        commit_p99_us: after.commit_latency_us.p99_us,
        commit_p999_us: after.commit_latency_us.p999_us,
        commit_max_us: after.commit_latency_us.max_us,
        commit_count_in_phase: count_in_phase,
    }
}

fn print_phase(result: &mut TestResult, summary: &PhaseSummary) {
    print_and_add_check(
        result,
        Check {
            name: format!("Phase '{}' completed", summary.name),
            passed: summary.ops_succeeded > 0,
            detail: Some(format!(
                "{:.2}s, {}/{} ops ({:.1}/s), commits +{}, db +{}B, wal Δ{}B",
                summary.wall_secs,
                summary.ops_succeeded,
                summary.ops_attempted,
                summary.ops_per_sec,
                summary.delta_commits,
                summary.delta_db_bytes,
                summary.delta_wal_bytes,
            )),
        },
    );
}

fn make_payload(size: usize) -> Vec<u8> {
    let pat = b"db-bench-payload-";
    let mut v = Vec::with_capacity(size);
    while v.len() < size {
        let take = (size - v.len()).min(pat.len());
        v.extend_from_slice(&pat[..take]);
    }
    v
}

/// db-pragma-bench: sustained write + read + mixed workload against node 0,
/// polling /debug/db-stats between phases. Output written to
/// /tmp/db-pragma-bench-<ts>.json so a matrix runner can diff configurations.
pub struct DbPragmaBench;

impl TestScenario for DbPragmaBench {
    fn name(&self) -> &'static str {
        "db-pragma-bench"
    }

    fn description(&self) -> &'static str {
        "DB pragma benchmarking: write burst, read burst, sustained mixed. \
         Polls /debug/db-stats and writes a JSON report to /tmp/."
    }

    async fn run(&self, _mesh_id: u32, nodes: &[NodeInfo], flags: &[String]) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();

        if nodes.is_empty() {
            print_and_add_check(
                &mut result,
                Check {
                    name: "Need at least 1 node".to_string(),
                    passed: false,
                    detail: None,
                },
            );
            result.duration = start.elapsed();
            return Ok(result);
        }

        let node = &nodes[0];

        // Tunables via flags: write=N (total files), writers=N, readers=N, read=N, mixed_secs=N, payload_bytes=N
        let mut write_count = 200usize;
        let mut writers = 4usize;
        let mut readers = 8usize;
        let mut read_count = 200usize;
        let mut mixed_secs = 30u64;
        let mut payload_bytes = 1024usize;
        for f in flags {
            if let Some(v) = f.strip_prefix("write=")
                && let Ok(n) = v.parse() {
                    write_count = n;
                }
            if let Some(v) = f.strip_prefix("writers=")
                && let Ok(n) = v.parse() {
                    writers = n;
                }
            if let Some(v) = f.strip_prefix("readers=")
                && let Ok(n) = v.parse() {
                    readers = n;
                }
            if let Some(v) = f.strip_prefix("read=")
                && let Ok(n) = v.parse() {
                    read_count = n;
                }
            if let Some(v) = f.strip_prefix("mixed_secs=")
                && let Ok(n) = v.parse() {
                    mixed_secs = n;
                }
            if let Some(v) = f.strip_prefix("payload_bytes=")
                && let Ok(n) = v.parse() {
                    payload_bytes = n;
                }
        }
        if writers == 0 {
            writers = 1;
        }

        let baseline = fetch_db_stats(node).await.context("baseline stats")?;
        print_and_add_check(
            &mut result,
            Check {
                name: "Baseline stats fetched".to_string(),
                passed: true,
                detail: Some(format!(
                    "synchronous={}, cache={}KiB, mmap={}B, journal={}, db={}B",
                    baseline.synchronous,
                    baseline.cache_bytes / 1024,
                    baseline.mmap_size,
                    baseline.journal_mode,
                    baseline.db_bytes,
                )),
            },
        );

        // -------- Phase A: write burst (N concurrent writers totaling write_count files) --------
        // Sequential await defeats HopNet's consensus batching — every upload pays a full
        // HotStuff round-trip alone. Concurrent writers fill the batcher, exposing real
        // ingest throughput.
        let bench_dir = format!("/db-bench-{}", timestamp_ms());
        let phase_a_start = Instant::now();
        let per_writer = write_count.div_ceil(writers);
        let a_succeeded_atomic = Arc::new(AtomicUsize::new(0));
        let mut a_handles = Vec::new();
        for w in 0..writers {
            let n = node.clone();
            let dir = bench_dir.clone();
            let counter = a_succeeded_atomic.clone();
            let payload_size = payload_bytes;
            a_handles.push(tokio::spawn(async move {
                for i in 0..per_writer {
                    let fname = format!("w-{}-{:05}.bin", w, i);
                    let payload = make_payload(payload_size);
                    if upload_file(&n, &dir, &fname, payload).await.is_ok() {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }
        for h in a_handles {
            let _ = h.await;
        }
        let a_succeeded = a_succeeded_atomic.load(Ordering::Relaxed) as u64;
        let a_total_attempted = (writers * per_writer) as u64;
        let after_a = fetch_db_stats(node).await.context("post-A stats")?;
        let summary_a = diff_phase(
            "write-burst",
            phase_a_start.elapsed(),
            a_total_attempted,
            a_succeeded,
            &baseline,
            &after_a,
        );
        print_phase(&mut result, &summary_a);

        // -------- Phase B: read burst (list root repeatedly) --------
        let phase_b_start = Instant::now();
        let mut b_succeeded = 0u64;
        for _ in 0..read_count {
            if list_files(node, &bench_dir).await.is_ok() {
                b_succeeded += 1;
            }
        }
        let after_b = fetch_db_stats(node).await.context("post-B stats")?;
        let summary_b = diff_phase(
            "read-burst",
            phase_b_start.elapsed(),
            read_count as u64,
            b_succeeded,
            &after_a,
            &after_b,
        );
        print_phase(&mut result, &summary_b);

        // -------- Phase C: sustained mixed (concurrent writers + readers for mixed_secs) --------
        let phase_c_start = Instant::now();
        let deadline = phase_c_start + Duration::from_secs(mixed_secs);
        let writes_done = Arc::new(AtomicUsize::new(0));
        let reads_done = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for w in 0..writers {
            let n = node.clone();
            let dir = bench_dir.clone();
            let counter = writes_done.clone();
            let payload_size = payload_bytes;
            handles.push(tokio::spawn(async move {
                let mut i = 0u64;
                while Instant::now() < deadline {
                    let fname = format!("m-{}-{:08}.bin", w, i);
                    let payload = make_payload(payload_size);
                    if upload_file(&n, &dir, &fname, payload).await.is_ok() {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                    i += 1;
                }
            }));
        }
        for _ in 0..readers {
            let n = node.clone();
            let dir = bench_dir.clone();
            let counter = reads_done.clone();
            handles.push(tokio::spawn(async move {
                while Instant::now() < deadline {
                    if list_files(&n, &dir).await.is_ok() {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }
        for h in handles {
            let _ = h.await;
        }
        let after_c = fetch_db_stats(node).await.context("post-C stats")?;
        let total_ops_c =
            (writes_done.load(Ordering::Relaxed) + reads_done.load(Ordering::Relaxed)) as u64;
        let summary_c = diff_phase(
            "mixed-sustained",
            phase_c_start.elapsed(),
            total_ops_c,
            total_ops_c,
            &after_b,
            &after_c,
        );
        print_phase(&mut result, &summary_c);

        // Cumulative latency note (histogram is process-lifetime, not per-phase)
        print_and_add_check(
            &mut result,
            Check {
                name: "Commit latency (cumulative across run)".to_string(),
                passed: true,
                detail: Some(format!(
                    "p50={}us p90={}us p99={}us p999={}us max={}us count={}",
                    after_c.commit_latency_us.p50_us,
                    after_c.commit_latency_us.p90_us,
                    after_c.commit_latency_us.p99_us,
                    after_c.commit_latency_us.p999_us,
                    after_c.commit_latency_us.max_us,
                    after_c.commit_latency_us.count,
                )),
            },
        );

        // Write JSON report for matrix-runner consumption
        let report = BenchReport {
            timestamp_ms: timestamp_ms(),
            pragmas: PragmaSnapshot {
                journal_mode: baseline.journal_mode.clone(),
                synchronous: baseline.synchronous.clone(),
                cache_bytes: baseline.cache_bytes,
                mmap_size: baseline.mmap_size,
                temp_store: baseline.temp_store.clone(),
                page_size: baseline.page_size,
            },
            baseline,
            phases: vec![summary_a, summary_b, summary_c],
            final_stats: after_c,
        };

        let report_path = format!("/tmp/db-pragma-bench-{}.json", report.timestamp_ms);
        match serde_json::to_string_pretty(&report).map(|s| std::fs::write(&report_path, s)) {
            Ok(Ok(())) => print_and_add_check(
                &mut result,
                Check {
                    name: "Wrote JSON report".to_string(),
                    passed: true,
                    detail: Some(report_path),
                },
            ),
            Ok(Err(e)) => print_and_add_check(
                &mut result,
                Check {
                    name: "Failed to write JSON report".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                },
            ),
            Err(e) => print_and_add_check(
                &mut result,
                Check {
                    name: "Failed to serialize JSON report".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                },
            ),
        }

        result.duration = start.elapsed();
        Ok(result)
    }
}
