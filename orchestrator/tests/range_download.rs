use anyhow::Result;
use std::time::{Duration, Instant};

use crate::NodeInfo;
use crate::tests::files::{download_file, download_file_headers, download_file_range, upload_file};
use crate::tests::{Check, TestResult, TestScenario, print_and_add_check};
use crate::tests::{get_max_view, wait_for_minimum_view};

pub struct RangeDownload;

impl TestScenario for RangeDownload {
    fn name(&self) -> &'static str {
        "range-download"
    }

    fn description(&self) -> &'static str {
        "Verify HTTP range request support: Accept-Ranges, Content-Length, Content-Type detection, partial content (206), and range-not-satisfiable (416)"
    }

    async fn run(&self, _mesh_id: u32, nodes: &[NodeInfo], flags: &[String]) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();

        println!("\nRunning range-download checks:");

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis();

        // --- Upload test files ---

        // 1KB PNG-like file (starts with PNG magic bytes for MIME detection)
        let png_filename = format!("range-test-{}.png", timestamp);
        let mut png_contents = vec![0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A]; // PNG header
        png_contents.extend(vec![0xAB; 1016]); // Pad to 1024 bytes
        let png_size = png_contents.len();

        // Plain text file with known content
        let txt_filename = format!("range-test-{}.txt", timestamp);
        let txt_contents: Vec<u8> = (0..1024u32).map(|i| (i % 256) as u8).collect();
        let txt_size = txt_contents.len();

        let current_max_view = get_max_view(nodes).await?;

        // Upload both files
        upload_file(&nodes[0], "/", &png_filename, png_contents.clone()).await?;
        upload_file(&nodes[0], "/", &txt_filename, txt_contents.clone()).await?;

        print_and_add_check(
            &mut result,
            Check {
                name: "Upload test files".to_string(),
                passed: true,
                detail: Some(format!(
                    "{} ({}B), {} ({}B)",
                    png_filename, png_size, txt_filename, txt_size
                )),
            },
        );

        // Wait for consensus
        let target_view = current_max_view + 2;
        let reached = wait_for_minimum_view(nodes, target_view, Duration::from_secs(30)).await?;
        if !reached {
            print_and_add_check(
                &mut result,
                Check {
                    name: "Consensus sync".to_string(),
                    passed: false,
                    detail: Some("Timeout waiting for consensus".to_string()),
                },
            );
            result.duration = start.elapsed();
            return Ok(result);
        }

        // --- Check 1: Accept-Ranges header ---
        let (full_png, png_headers) =
            download_file_headers(&nodes[0], &format!("/{}", png_filename)).await?;

        let has_accept_ranges = png_headers.accept_ranges.as_deref() == Some("bytes");
        print_and_add_check(
            &mut result,
            Check {
                name: "Accept-Ranges: bytes header present".to_string(),
                passed: has_accept_ranges,
                detail: Some(format!("got: {:?}", png_headers.accept_ranges)),
            },
        );

        // --- Check 2: Content-Length matches uploaded size ---
        let content_length_correct = png_headers.content_length == Some(png_size as u64);
        print_and_add_check(
            &mut result,
            Check {
                name: "Content-Length matches file size".to_string(),
                passed: content_length_correct,
                detail: Some(format!(
                    "expected: {}, got: {:?}",
                    png_size, png_headers.content_length
                )),
            },
        );

        // --- Check 3: Content-Type detection ---
        let png_type_ok = png_headers.content_type.as_deref() == Some("image/png");
        print_and_add_check(
            &mut result,
            Check {
                name: "Content-Type: image/png for .png file".to_string(),
                passed: png_type_ok,
                detail: Some(format!("got: {:?}", png_headers.content_type)),
            },
        );

        let (_, txt_headers) =
            download_file_headers(&nodes[0], &format!("/{}", txt_filename)).await?;
        // text/plain may include charset
        let txt_type_ok = txt_headers
            .content_type
            .as_ref()
            .map(|t| t.starts_with("text/plain"))
            .unwrap_or(false);
        print_and_add_check(
            &mut result,
            Check {
                name: "Content-Type: text/plain for .txt file".to_string(),
                passed: txt_type_ok,
                detail: Some(format!("got: {:?}", txt_headers.content_type)),
            },
        );

        // --- Check 4: Basic range request (first 100 bytes) ---
        let (range_bytes, range_headers) =
            download_file_range(&nodes[0], &format!("/{}", txt_filename), 0, Some(99)).await?;

        let status_206 = range_headers.status == 206;
        print_and_add_check(
            &mut result,
            Check {
                name: "Range bytes=0-99 returns 206".to_string(),
                passed: status_206,
                detail: Some(format!("status: {}", range_headers.status)),
            },
        );

        let range_content_correct = range_bytes == &txt_contents[0..100];
        print_and_add_check(
            &mut result,
            Check {
                name: "Range bytes=0-99 content matches".to_string(),
                passed: range_content_correct,
                detail: Some(format!("got {} bytes, expected 100", range_bytes.len())),
            },
        );

        let expected_content_range = format!("bytes 0-99/{}", txt_size);
        let content_range_ok =
            range_headers.content_range.as_deref() == Some(&expected_content_range);
        print_and_add_check(
            &mut result,
            Check {
                name: "Content-Range header correct".to_string(),
                passed: content_range_ok,
                detail: Some(format!(
                    "expected: {}, got: {:?}",
                    expected_content_range, range_headers.content_range
                )),
            },
        );

        let range_cl_ok = range_headers.content_length == Some(100);
        print_and_add_check(
            &mut result,
            Check {
                name: "Partial Content-Length is 100".to_string(),
                passed: range_cl_ok,
                detail: Some(format!("got: {:?}", range_headers.content_length)),
            },
        );

        // --- Check 5: Range at end of file (bytes=900-) ---
        let tail_start = (txt_size - 124) as u64;
        let (tail_bytes, tail_headers) =
            download_file_range(&nodes[0], &format!("/{}", txt_filename), tail_start, None).await?;

        let tail_status_ok = tail_headers.status == 206;
        print_and_add_check(
            &mut result,
            Check {
                name: format!("Range bytes={}- returns 206", tail_start).to_string(),
                passed: tail_status_ok,
                detail: Some(format!("status: {}", tail_headers.status)),
            },
        );

        let expected_tail = &txt_contents[tail_start as usize..];
        let tail_content_ok = tail_bytes == expected_tail;
        print_and_add_check(
            &mut result,
            Check {
                name: "Tail range content matches".to_string(),
                passed: tail_content_ok,
                detail: Some(format!(
                    "got {} bytes, expected {}",
                    tail_bytes.len(),
                    expected_tail.len()
                )),
            },
        );

        // --- Check 6: Range beyond file size returns 416 ---
        let huge_start = txt_size as u64 + 1000;
        let (_, bad_headers) =
            download_file_range(&nodes[0], &format!("/{}", txt_filename), huge_start, None).await?;

        let status_416 = bad_headers.status == 416;
        print_and_add_check(
            &mut result,
            Check {
                name: "Range beyond file size returns 416".to_string(),
                passed: status_416,
                detail: Some(format!("status: {}", bad_headers.status)),
            },
        );

        // --- Check 7 (optional): Multi-chunk range spanning boundary ---
        if flags.contains(&"multi-chunk".to_string()) {
            println!("\n  Running multi-chunk range test (large file)...");

            let big_filename = format!("range-big-{}.bin", timestamp);
            // 41MB file = 2 chunks (40MB + 1MB)
            let big_size: usize = 41 * 1024 * 1024;
            let big_contents: Vec<u8> = (0..big_size).map(|i| (i % 251) as u8).collect();

            let big_view = get_max_view(nodes).await?;
            upload_file(&nodes[0], "/", &big_filename, big_contents.clone()).await?;

            // Wait longer for large file consensus + distribution
            let big_target = big_view + 2;
            let big_reached =
                wait_for_minimum_view(nodes, big_target, Duration::from_secs(120)).await?;

            if big_reached {
                // Request range spanning chunk boundary: last 2MB of chunk 0 + first 1MB of chunk 1
                let span_start = (39 * 1024 * 1024) as u64; // 39MB into file
                let span_end = (41 * 1024 * 1024 - 1) as u64; // end of file

                let (span_bytes, span_headers) = download_file_range(
                    &nodes[0],
                    &format!("/{}", big_filename),
                    span_start,
                    Some(span_end),
                )
                .await?;

                let span_ok = span_headers.status == 206
                    && span_bytes == &big_contents[span_start as usize..=span_end as usize];
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Multi-chunk range spanning boundary".to_string(),
                        passed: span_ok,
                        detail: Some(format!(
                            "status: {}, bytes: {} (expected {})",
                            span_headers.status,
                            span_bytes.len(),
                            span_end - span_start + 1
                        )),
                    },
                );
            } else {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Multi-chunk range test".to_string(),
                        passed: false,
                        detail: Some("Timeout waiting for large file consensus".to_string()),
                    },
                );
            }
        }

        result.duration = start.elapsed();
        Ok(result)
    }
}
