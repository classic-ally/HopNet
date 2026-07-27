use anyhow::Result;
use std::time::{Duration, Instant};

use crate::NodeInfo;
use crate::tests::files::{
    download_file_from_all_nodes_with_timeout, get_fragment_distribution, upload_file,
};
use crate::tests::{Check, TestResult, TestScenario, print_and_add_check};

/// Upload a file large enough that distribution does not complete before we can
/// observe placement_height IS NULL, then verify all nodes converge on the same
/// placement height. A file spanning multiple RS chunks (>= 80 MB) ensures the
/// distribution pipeline takes observable time even on fast container networking,
/// making the "unplaced by age" chart diagnostic meaningful.
pub struct UploadAndConfirmPlacement;

impl TestScenario for UploadAndConfirmPlacement {
    fn name(&self) -> &'static str {
        "upload-and-confirm-placement"
    }

    fn description(&self) -> &'static str {
        "Upload a large multi-chunk file and confirm placement_height transitions from NULL to a converged value across all nodes"
    }

    async fn run(
        &self,
        _mesh_id: u32,
        nodes: &[NodeInfo],
        _flags: &[String],
    ) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();

        let filename = "placement_test.dat";
        let path = "/";
        let full_path = "/placement_test.dat";

        // ~80 MB: spans 3 RS chunks (40 MB each), producing ~90 fragments.
        // Large enough that distribution across 4 workers + podman bridge
        // takes several seconds, so the NULL window is observable.
        let content_size = 80 * 1024 * 1024;
        let contents: Vec<u8> = (0..content_size).map(|i| (i % 251) as u8).collect();
        let content_hash = blake3::hash(&contents);

        // 1. Upload the file to node 0
        print_and_add_check(&mut result, Check {
            name: "Upload 80 MB file to node 0".to_string(),
            passed: true,
            detail: None,
        });

        upload_file(&nodes[0], path, filename, contents.clone()).await?;

        // 2. Poll until the blob appears on node 0 (consensus committed it),
        //    then immediately check placement_height before distribution can
        //    finish. The first successful response IS the NULL check.
        let appear_timeout = Duration::from_secs(30);
        let appear_start = Instant::now();
        let early = loop {
            if appear_start.elapsed() > appear_timeout {
                anyhow::bail!("Timeout waiting for blob to appear after upload");
            }
            match get_fragment_distribution(&nodes[0], full_path).await {
                Ok(dist) => break dist,
                Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
            }
        };

        let still_null = early.placement_height.is_none();
        print_and_add_check(&mut result, Check {
            name: "Placement height is NULL after commit (distribution in progress)".to_string(),
            passed: still_null,
            detail: if still_null {
                Some(format!(
                    "NULL for {:.1}s before distribution completed",
                    appear_start.elapsed().as_secs_f64()
                ))
            } else {
                Some(format!(
                    "Already placed at height {} — try larger file or measure distribution speed",
                    early.placement_height.unwrap()
                ))
            },
        });

        // 3. Wait for node 0 to finish distribution (placement_height set).
        //    Poll in tight loop so we can record latency.
        let dist_start = Instant::now();
        let ph0 = loop {
            if dist_start.elapsed() > Duration::from_secs(120) {
                anyhow::bail!("Timeout waiting for fragment distribution");
            }
            match get_fragment_distribution(&nodes[0], full_path).await {
                Ok(dist) if dist.placement_height.is_some() => break dist.placement_height.unwrap(),
                _ => tokio::time::sleep(Duration::from_millis(200)).await,
            }
        };
        let dist_secs = dist_start.elapsed().as_secs_f64();

        print_and_add_check(&mut result, Check {
            name: format!(
                "Node 0 placed at height {} (distribution took {:.1}s)",
                ph0, dist_secs
            ),
            passed: true,
            detail: None,
        });

        // 4. Poll remaining nodes until they converge on the same placement
        //    height. Since the placement commit is consensus-driven, every
        //    node that has reached that decided height will agree.
        let converge_timeout = Duration::from_secs(30);
        let converge_start = Instant::now();
        let mut all_converged = true;
        let mut heights = vec![ph0];

        for node in &nodes[1..] {
            loop {
                if converge_start.elapsed() > converge_timeout {
                    all_converged = false;
                    break;
                }
                match get_fragment_distribution(node, full_path).await {
                    Ok(dist) => {
                        if let Some(ph) = dist.placement_height {
                            heights.push(ph);
                            break;
                        }
                    }
                    Err(_) => {}
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }

        print_and_add_check(&mut result, Check {
            name: "All nodes converge on a non-NULL placement height".to_string(),
            passed: all_converged,
            detail: if all_converged {
                None
            } else {
                Some("One or more nodes did not see a placement height within 30s".to_string())
            },
        });

        // 5. Verify all nodes agree on the same placement height
        let first = heights[0];
        let all_agree = heights.iter().all(|ph| *ph == first);
        print_and_add_check(&mut result, Check {
            name: format!("All nodes agree on placement height {}", first),
            passed: all_agree,
            detail: if all_agree {
                None
            } else {
                Some(format!("Heights: {:?}", heights))
            },
        });

        if !all_converged || !all_agree {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // 6. Verify the file is retrievable and matches the uploaded content
        let dl = download_file_from_all_nodes_with_timeout(
            nodes,
            full_path,
            Duration::from_secs(60),
        )
        .await;
        let retrievable = dl.is_ok();
        let dl_err = dl.as_ref().err().map(|e| format!("{e:?}"));
        print_and_add_check(&mut result, Check {
            name: "File is retrievable from all nodes after placement".to_string(),
            passed: retrievable,
            detail: dl_err,
        });

        if retrievable {
            let data = dl.unwrap();
            let all_match = data.iter().all(|d| blake3::hash(d) == content_hash);
            print_and_add_check(&mut result, Check {
                name: "Retrieved content matches uploaded content (BLAKE3 hash)".to_string(),
                passed: all_match,
                detail: if all_match {
                    None
                } else {
                    Some("Hash mismatch across nodes".to_string())
                },
            });
        }

        result.duration = start.elapsed();
        Ok(result)
    }
}
