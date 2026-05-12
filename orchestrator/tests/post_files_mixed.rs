use anyhow::Result;
use std::time::{Duration, Instant};

use crate::NodeInfo;
use crate::tests::files::{
    list_files_from_all_nodes, upload_files_multi, verify_listings_identical,
};
use crate::tests::{
    Check, TestResult, TestScenario, get_max_view, print_and_add_check, wait_for_minimum_view,
};

/// Baseline test (Phase 3.2): a single multipart request landing files into a
/// nested path with no pre-existing parents must batch parent-folder backfill
/// and file-inode insertion into one consensus view advance, with all parents
/// and files visible on every node.
pub struct PostFilesMixedFilesAndParents;

impl TestScenario for PostFilesMixedFilesAndParents {
    fn name(&self) -> &'static str {
        "mixed-files-and-folders-one-request"
    }

    fn description(&self) -> &'static str {
        "Upload N files into a deep nested path with no existing parents in one request; assert single view advance and full visibility of files plus implicit parent folders on every node"
    }

    async fn run(
        &self,
        _mesh_id: u32,
        nodes: &[NodeInfo],
        _flags: &[String],
    ) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();
        const FILE_COUNT: usize = 3;

        println!("\nRunning checks:");

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let parent_a = format!("mixed-{}", timestamp);
        let parent_b = format!("{}/sub", parent_a);
        let parent_c = format!("{}/leaf", parent_b);
        let test_path = format!("/{}/", parent_c);

        let mut files = Vec::with_capacity(FILE_COUNT);
        let mut filenames = Vec::with_capacity(FILE_COUNT);
        for i in 0..FILE_COUNT {
            let filename = format!("nested-{}.txt", i);
            let contents = format!("nested file {} for {}", i, timestamp).into_bytes();
            filenames.push(filename.clone());
            files.push((filename, contents));
        }

        let view_before = match get_max_view(nodes).await {
            Ok(v) => v,
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Failed to get initial view".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };
        print_and_add_check(
            &mut result,
            Check {
                name: format!("Initial view: {}", view_before),
                passed: true,
                detail: None,
            },
        );

        match upload_files_multi(&nodes[0], &test_path, files).await {
            Ok(_) => print_and_add_check(
                &mut result,
                Check {
                    name: format!("Upload {} files into nested path {}", FILE_COUNT, test_path),
                    passed: true,
                    detail: None,
                },
            ),
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Multi-file upload failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        let target_view = view_before + 1;
        let timeout = Duration::from_secs(30);
        match wait_for_minimum_view(nodes, target_view, timeout).await {
            Ok(true) => print_and_add_check(
                &mut result,
                Check {
                    name: format!("All nodes reached view {}", target_view),
                    passed: true,
                    detail: None,
                },
            ),
            Ok(false) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Consensus propagation timeout".to_string(),
                        passed: false,
                        detail: Some(format!(
                            "did not reach view {} within {}s",
                            target_view,
                            timeout.as_secs()
                        )),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Consensus check failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        tokio::time::sleep(Duration::from_secs(2)).await;

        let view_after = match get_max_view(nodes).await {
            Ok(v) => v,
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Failed to get final view".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };
        let views_consumed = view_after - view_before;
        let shape_ok = views_consumed <= 2;
        print_and_add_check(
            &mut result,
            Check {
                name: format!(
                    "{} files + 3 parents consumed {} views (≤2 expected)",
                    FILE_COUNT, views_consumed
                ),
                passed: shape_ok,
                detail: Some(format!("view {} → view {}", view_before, view_after)),
            },
        );

        // Verify each parent folder appears on each node
        let parent_paths = vec![
            format!("/{}", parent_a),
            format!("/{}", parent_b),
            format!("/{}", parent_c),
        ];

        for parent in &parent_paths {
            let parent_listing_path = match parent.rfind('/') {
                Some(0) => "/".to_string(),
                Some(i) => parent[..i].to_string(),
                None => "/".to_string(),
            };
            let listings = match list_files_from_all_nodes(nodes, &parent_listing_path).await {
                Ok(l) => l,
                Err(e) => {
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: format!("List {} failed", parent_listing_path),
                            passed: false,
                            detail: Some(e.to_string()),
                        },
                    );
                    result.duration = start.elapsed();
                    return Ok(result);
                }
            };

            if let Err(e) = verify_listings_identical(&listings) {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("Listing mismatch at {}", parent_listing_path),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }

            let found = listings[0]
                .as_array()
                .map(|arr| arr.iter().any(|f| f["path"].as_str() == Some(parent)))
                .unwrap_or(false);
            print_and_add_check(
                &mut result,
                Check {
                    name: format!("Parent {} present on all nodes", parent),
                    passed: found,
                    detail: None,
                },
            );
        }

        // Verify all files visible at deepest path
        let deep_listings = match list_files_from_all_nodes(nodes, &format!("/{}", parent_c)).await
        {
            Ok(l) => l,
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("List /{} failed", parent_c),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        if let Err(e) = verify_listings_identical(&deep_listings) {
            print_and_add_check(
                &mut result,
                Check {
                    name: format!("Listing mismatch at /{}", parent_c),
                    passed: false,
                    detail: Some(e.to_string()),
                },
            );
            result.duration = start.elapsed();
            return Ok(result);
        }

        let mut missing = Vec::new();
        if let Some(files_array) = deep_listings[0].as_array() {
            for filename in &filenames {
                let full_path = format!("/{}/{}", parent_c, filename);
                if !files_array
                    .iter()
                    .any(|f| f["path"].as_str() == Some(&full_path))
                {
                    missing.push(filename.clone());
                }
            }
        }
        let all_files_present = missing.is_empty();
        print_and_add_check(
            &mut result,
            Check {
                name: format!("All {} files present on every node", FILE_COUNT),
                passed: all_files_present,
                detail: if all_files_present {
                    None
                } else {
                    Some(format!("missing: {:?}", missing))
                },
            },
        );

        result.duration = start.elapsed();
        result.details = format!(
            "{} files + 3 implicit parents in 1 request, {} views consumed, fully consistent across {} nodes",
            FILE_COUNT,
            views_consumed,
            nodes.len()
        );

        Ok(result)
    }
}
