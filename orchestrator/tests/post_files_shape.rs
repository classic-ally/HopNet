use anyhow::Result;
use std::time::{Duration, Instant};

use crate::tests::files::{list_files_from_all_nodes, upload_files_multi, verify_listings_identical};
use crate::tests::{get_max_view, print_and_add_check, wait_for_minimum_view, Check, TestResult, TestScenario};
use crate::NodeInfo;

/// Baseline test (Phase 3.2): proves a single multipart request carrying N files
/// lands in exactly one consensus view advance with all N files visible on every
/// node. Tripwire for any future refactor that splits N files into per-file
/// transactions.
pub struct PostFilesConsensusShape;

impl TestScenario for PostFilesConsensusShape {
    fn name(&self) -> &'static str {
        "post-files-consensus-shape"
    }

    fn description(&self) -> &'static str {
        "Upload N files in one POST /files request; assert exactly one view advance and all N files appear in listings on every node"
    }

    async fn run(&self, _mesh_id: u32, nodes: &[NodeInfo], _flags: &[String]) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();
        const FILE_COUNT: usize = 5;

        println!("\nRunning checks:");

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let test_path = "/";
        let mut files = Vec::with_capacity(FILE_COUNT);
        let mut filenames = Vec::with_capacity(FILE_COUNT);
        for i in 0..FILE_COUNT {
            let filename = format!("shape-{}-{}.txt", timestamp, i);
            let contents = format!("file {} body for {}", i, timestamp).into_bytes();
            filenames.push(filename.clone());
            files.push((filename, contents));
        }

        let view_before = match get_max_view(nodes).await {
            Ok(v) => v,
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Failed to get initial view".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        };
        print_and_add_check(&mut result, Check {
            name: format!("Initial view: {}", view_before),
            passed: true,
            detail: None,
        });

        match upload_files_multi(&nodes[0], test_path, files).await {
            Ok(_) => print_and_add_check(&mut result, Check {
                name: format!("Upload {} files in single request to node 0", FILE_COUNT),
                passed: true,
                detail: None,
            }),
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Multi-file upload failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        let target_view = view_before + 1;
        let timeout = Duration::from_secs(30);
        match wait_for_minimum_view(nodes, target_view, timeout).await {
            Ok(true) => print_and_add_check(&mut result, Check {
                name: format!("All nodes reached view {}", target_view),
                passed: true,
                detail: None,
            }),
            Ok(false) => {
                print_and_add_check(&mut result, Check {
                    name: "Consensus propagation timeout".to_string(),
                    passed: false,
                    detail: Some(format!("did not reach view {} within {}s", target_view, timeout.as_secs())),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Consensus check failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Settle period before reading view_after — distribution attestation may
        // land in the same view as insert_files, but follow-up updates (e.g.
        // placement_height) can advance further. Wait briefly so view_after
        // captures the final position post-batch.
        tokio::time::sleep(Duration::from_secs(2)).await;

        let view_after = match get_max_view(nodes).await {
            Ok(v) => v,
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Failed to get final view".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        };
        let views_consumed = view_after - view_before;
        let shape_ok = views_consumed <= 2; // insert_files + optional placement update
        print_and_add_check(&mut result, Check {
            name: format!("{} files consumed {} views (≤2 expected)", FILE_COUNT, views_consumed),
            passed: shape_ok,
            detail: Some(format!("view {} → view {}", view_before, view_after)),
        });

        let listings = match list_files_from_all_nodes(nodes, test_path).await {
            Ok(l) => l,
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "List files failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        match verify_listings_identical(&listings) {
            Ok(_) => print_and_add_check(&mut result, Check {
                name: "All nodes return identical listings".to_string(),
                passed: true,
                detail: None,
            }),
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Listing mismatch across nodes".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        if let Some(files_array) = listings[0].as_array() {
            let mut missing = Vec::new();
            for filename in &filenames {
                let full_path = format!("/{}", filename);
                if !files_array.iter().any(|f| f["path"].as_str() == Some(&full_path)) {
                    missing.push(filename.clone());
                }
            }
            let all_present = missing.is_empty();
            print_and_add_check(&mut result, Check {
                name: format!("All {} files appear in listing", FILE_COUNT),
                passed: all_present,
                detail: if all_present {
                    None
                } else {
                    Some(format!("missing: {:?}", missing))
                },
            });
        } else {
            print_and_add_check(&mut result, Check {
                name: "Invalid listing format".to_string(),
                passed: false,
                detail: Some("listing is not a JSON array".to_string()),
            });
        }

        result.duration = start.elapsed();
        result.details = format!(
            "Uploaded {} files in 1 request, {} views consumed, listings consistent across {} nodes",
            FILE_COUNT,
            views_consumed,
            nodes.len()
        );

        Ok(result)
    }
}
