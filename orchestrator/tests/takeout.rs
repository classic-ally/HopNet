use anyhow::Result;
use hopnet_common::{TakeoutRecord, TakeoutStatus};
use reqwest::Client;
use std::collections::HashMap;
use std::io::Read;
use std::time::{Duration, Instant};

use crate::NodeInfo;
use crate::tests::files::upload_file;
use crate::tests::{Check, TestResult, TestScenario, print_and_add_check};

// ============================================================================
// Takeout HTTP Helpers
// ============================================================================

/// POST /takeout/initiate — start a new takeout for the authenticated user.
pub async fn initiate_takeout(node: &NodeInfo) -> Result<()> {
    let client = Client::new();
    let url = format!(
        "http://{}:{}/api/takeout/initiate",
        node.ip_address, node.port
    );

    let response = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "No body".to_string());
        anyhow::bail!("initiate_takeout failed with status {}: {}", status, body);
    }

    Ok(())
}

/// GET /takeout — list all takeouts for the authenticated user.
pub async fn list_takeouts(node: &NodeInfo) -> Result<Vec<TakeoutRecord>> {
    let client = Client::new();
    let url = format!("http://{}:{}/api/takeout", node.ip_address, node.port);

    let response = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "No body".to_string());
        anyhow::bail!("list_takeouts failed with status {}: {}", status, body);
    }

    Ok(response.json().await?)
}

/// Poll /takeout until a takeout reaches Ready status or the timeout elapses.
/// Returns the takeout ID.
pub async fn wait_for_takeout_ready(node: &NodeInfo, timeout: Duration) -> Result<String> {
    let deadline = Instant::now() + timeout;

    loop {
        let takeouts = list_takeouts(node).await?;

        if let Some(ready) = takeouts.iter().find(|t| t.status == TakeoutStatus::Ready) {
            return Ok(ready.id.clone());
        }

        if Instant::now() >= deadline {
            let statuses: Vec<_> = takeouts.iter().map(|t| format!("{:?}", t.status)).collect();
            anyhow::bail!(
                "Timed out waiting for takeout Ready after {:?}. Last observed statuses: {:?}",
                timeout,
                statuses
            );
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// GET /takeout/{id}/download — stream the compressed archive bytes.
pub async fn download_takeout_archive(node: &NodeInfo, takeout_id: &str) -> Result<Vec<u8>> {
    let client = Client::builder()
        .timeout(Duration::from_secs(120))
        .build()?;
    let url = format!(
        "http://{}:{}/api/takeout/{}/download",
        node.ip_address, node.port, takeout_id
    );

    let response = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "No body".to_string());
        anyhow::bail!(
            "download_takeout_archive failed with status {}: {}",
            status,
            body
        );
    }

    Ok(response.bytes().await?.to_vec())
}

/// Result of decompressing and reading a takeout archive.
/// Captures entry order so callers can verify manifest-first placement,
/// and parses the manifest into the production `TakeoutManifest` type so
/// any field-shape drift between writer and reader is caught at compile time.
pub struct ExtractedArchive {
    /// Tar entry paths in the order they appear in the archive.
    /// Includes directory entries; manifest is the first item.
    pub entry_order: Vec<String>,
    /// Manifest deserialized into the same type the takeout writer produces.
    pub manifest: hopnet_takeout::manifest::TakeoutManifest,
    /// Map of file-entry archive_path -> file bytes. Directory and manifest
    /// entries are omitted from this map.
    pub files: HashMap<String, Vec<u8>>,
}

/// Decompress a tar.gz archive, parse the manifest into the production type,
/// and capture entry order. Errors if the first entry is not `manifest.json`
/// or the manifest does not deserialize.
pub fn extract_tar_gz(archive_bytes: &[u8]) -> Result<ExtractedArchive> {
    let gz = flate2::read::GzDecoder::new(archive_bytes);
    let mut archive = tar::Archive::new(gz);

    let mut entry_order = Vec::new();
    let mut files: HashMap<String, Vec<u8>> = HashMap::new();
    let mut manifest_raw: Option<Vec<u8>> = None;

    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.to_string_lossy().to_string();
        entry_order.push(path.clone());

        let entry_type = entry.header().entry_type();
        if entry_type.is_file() {
            let mut buf = Vec::new();
            entry.read_to_end(&mut buf)?;
            if path == hopnet_takeout::manifest::MANIFEST_FILENAME {
                if manifest_raw.is_some() {
                    anyhow::bail!("Archive contains more than one manifest.json entry");
                }
                manifest_raw = Some(buf);
            } else {
                files.insert(path, buf);
            }
        }
        // Directory entries are tracked in entry_order but contribute no bytes.
    }

    let first = entry_order
        .first()
        .ok_or_else(|| anyhow::anyhow!("Archive is empty"))?;
    if first != hopnet_takeout::manifest::MANIFEST_FILENAME {
        anyhow::bail!(
            "Expected first archive entry to be manifest.json, got {}",
            first
        );
    }

    let manifest_raw =
        manifest_raw.ok_or_else(|| anyhow::anyhow!("Archive missing manifest.json"))?;
    let manifest: hopnet_takeout::manifest::TakeoutManifest = serde_json::from_slice(&manifest_raw)
        .map_err(|e| anyhow::anyhow!("Failed to parse manifest.json: {}", e))?;

    Ok(ExtractedArchive {
        entry_order,
        manifest,
        files,
    })
}

// ============================================================================
// Test Scenarios
// ============================================================================

/// Happy-path takeout: upload files, initiate takeout, wait for completion,
/// download the archive, and verify each uploaded file's bytes round-trip
/// through reconstruction + integrity check + archive assembly unchanged.
pub struct TakeoutHappyPath;

impl TestScenario for TakeoutHappyPath {
    fn name(&self) -> &'static str {
        "takeout-happy-path"
    }

    fn description(&self) -> &'static str {
        "Upload files, initiate takeout, wait for Ready, download archive, verify content byte-matches the originals"
    }

    async fn run(
        &self,
        _mesh_id: u32,
        nodes: &[NodeInfo],
        _flags: &[String],
    ) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();

        println!("\nRunning checks:");

        let node = &nodes[0];
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();

        // Step 1: Upload 30 files. Bumped from 3 to exercise the streaming
        // materialization pipeline beyond a single batch worth of work, and
        // to verify the reserved-conn pattern survives 30+ status updates.
        const FILE_COUNT: usize = 30;
        let files: Vec<(String, Vec<u8>)> = (0..FILE_COUNT)
            .map(|i| {
                let filename = format!("takeout-{:03}-{}.bin", i, timestamp);
                // Vary sizes: some tiny, some ~1KB, some ~4KB. Deterministic per index.
                let size = match i % 3 {
                    0 => 32usize,
                    1 => 1024,
                    _ => 4096,
                };
                let byte = (i % 251) as u8;
                let contents = std::iter::repeat_n(byte, size).collect::<Vec<u8>>();
                (filename, contents)
            })
            .collect();

        for (filename, contents) in &files {
            match upload_file(node, "/", filename, contents.clone()).await {
                Ok(_) => print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("Upload {}", filename),
                        passed: true,
                        detail: None,
                    },
                ),
                Err(e) => {
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: format!("Upload {} failed", filename),
                            passed: false,
                            detail: Some(e.to_string()),
                        },
                    );
                    result.duration = start.elapsed();
                    return Ok(result);
                }
            }
        }

        // Step 2: Initiate takeout
        match initiate_takeout(node).await {
            Ok(_) => print_and_add_check(
                &mut result,
                Check {
                    name: "Initiate takeout".to_string(),
                    passed: true,
                    detail: None,
                },
            ),
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Initiate takeout failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 3: Wait for takeout to reach Ready status
        let takeout_id = match wait_for_takeout_ready(node, Duration::from_secs(120)).await {
            Ok(id) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Takeout reached Ready status".to_string(),
                        passed: true,
                        detail: Some(id.clone()),
                    },
                );
                id
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Wait for Ready failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Step 4: Download archive
        let archive_bytes = match download_takeout_archive(node, &takeout_id).await {
            Ok(bytes) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Download archive".to_string(),
                        passed: true,
                        detail: Some(format!("{} bytes", bytes.len())),
                    },
                );
                bytes
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Download archive failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Step 5: Decompress, validate manifest-first ordering, parse manifest into typed struct
        let archive = match extract_tar_gz(&archive_bytes) {
            Ok(a) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Decompress archive + manifest first entry".to_string(),
                        passed: true,
                        detail: Some(format!(
                            "{} entries total, {} content files, manifest version {}",
                            a.entry_order.len(),
                            a.files.len(),
                            a.manifest.version
                        )),
                    },
                );
                a
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Decompress archive / manifest validation failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Step 6: Manifest schema sanity. Pin to spec version 2 with a literal so a
        // future MANIFEST_VERSION bump must consciously update this assertion (otherwise
        // checking against the constant is circular — both writer and reader share it).
        // v2: totals + entries live in the per-projection "drive" section.
        let expected_total_bytes: u64 = files.iter().map(|(_, c)| c.len() as u64).sum();
        let expected_files_count = files.len() as u64;

        print_and_add_check(
            &mut result,
            Check {
                name: "Manifest version == 2 (spec v2)".to_string(),
                passed: archive.manifest.version == 2,
                detail: Some(format!("got version {}", archive.manifest.version)),
            },
        );

        print_and_add_check(
            &mut result,
            Check {
                name: "Manifest takeout_id matches downloaded id".to_string(),
                passed: archive.manifest.takeout_id.to_string() == takeout_id,
                detail: Some(format!(
                    "manifest={}, downloaded={}",
                    archive.manifest.takeout_id, takeout_id
                )),
            },
        );

        let drive = match archive.manifest.projections.get("drive") {
            Some(section) => section,
            None => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Manifest contains a \"drive\" projection section".to_string(),
                        passed: false,
                        detail: Some(format!(
                            "sections: {:?}",
                            archive.manifest.projections.keys().collect::<Vec<_>>()
                        )),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };
        print_and_add_check(
            &mut result,
            Check {
                name: "Manifest contains a \"drive\" projection section".to_string(),
                passed: true,
                detail: None,
            },
        );

        let file_entries: Vec<_> = drive
            .entries
            .iter()
            .filter(|e| e.kind == hopnet_takeout::manifest::EntryKind::File)
            .collect();
        print_and_add_check(
            &mut result,
            Check {
                name: format!("Drive section total_files == {}", expected_files_count),
                passed: drive.total_files == expected_files_count
                    && file_entries.len() as u64 == expected_files_count,
                detail: Some(format!(
                    "total_files={}, file entries={}",
                    drive.total_files,
                    file_entries.len()
                )),
            },
        );

        let folder_entries = drive
            .entries
            .iter()
            .filter(|e| e.kind == hopnet_takeout::manifest::EntryKind::Folder)
            .count();
        print_and_add_check(
            &mut result,
            Check {
                name: "Drive section total_folders == 0".to_string(),
                passed: drive.total_folders == 0 && folder_entries == 0,
                detail: Some(format!(
                    "total_folders={}, folder entries={}",
                    drive.total_folders, folder_entries
                )),
            },
        );

        print_and_add_check(
            &mut result,
            Check {
                name: format!("Drive section total_bytes == {}", expected_total_bytes),
                passed: drive.total_bytes == expected_total_bytes,
                detail: Some(format!("total_bytes={}", drive.total_bytes)),
            },
        );

        // Step 7: Per-file verification — manifest entry shape, archive layout, content + hash match
        for (filename, expected_contents) in &files {
            // Find the manifest entry by logical path within the drive section.
            let manifest_entry = drive.entries.iter().find(|f| f.logical_path == *filename);

            let manifest_entry = match manifest_entry {
                Some(e) => e,
                None => {
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: format!("Manifest contains entry for {}", filename),
                            passed: false,
                            detail: Some(format!(
                                "manifest paths: {:?}",
                                drive
                                    .entries
                                    .iter()
                                    .map(|f| &f.logical_path)
                                    .collect::<Vec<_>>()
                            )),
                        },
                    );
                    continue;
                }
            };

            // Manifest size matches uploaded size
            print_and_add_check(
                &mut result,
                Check {
                    name: format!("Manifest size matches for {}", filename),
                    passed: manifest_entry.size as usize == expected_contents.len(),
                    detail: Some(format!(
                        "manifest={}, uploaded={}",
                        manifest_entry.size,
                        expected_contents.len()
                    )),
                },
            );

            // content_hash must equal blake3(plaintext || blob_id_bytes) — formula
            // unchanged from v1. Reconstructing it here catches any drift in the
            // takeout salting logic. Files must always carry blob_id + content_hash.
            let hash_matches = match (&manifest_entry.blob_id, &manifest_entry.content_hash) {
                (Some(blob_id), Some(content_hash)) => {
                    let mut hasher = blake3::Hasher::new();
                    hasher.update(expected_contents);
                    hasher.update(blob_id.as_bytes());
                    hasher.finalize().as_bytes() == content_hash.as_bytes()
                }
                _ => false,
            };
            print_and_add_check(
                &mut result,
                Check {
                    name: format!(
                        "Manifest content_hash matches reconstructed for {}",
                        filename
                    ),
                    passed: hash_matches,
                    detail: Some(format!(
                        "entry blob_id={:?} content_hash={:?}",
                        manifest_entry.blob_id, manifest_entry.content_hash
                    )),
                },
            );

            // Archive layout: file lives under `drive/<path>` (its projection
            // prefix), NOT at the root.
            let prefixed_path = format!("drive/{}", filename);
            let archived_bytes = match archive.files.get(&prefixed_path) {
                Some(b) => b,
                None => {
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: format!("Archive entry under drive/ prefix for {}", filename),
                            passed: false,
                            detail: Some(format!(
                                "expected at {}, archive paths: {:?}",
                                prefixed_path,
                                archive.files.keys().collect::<Vec<_>>()
                            )),
                        },
                    );
                    continue;
                }
            };

            // Byte-match between uploaded and archived content
            print_and_add_check(
                &mut result,
                Check {
                    name: format!("Byte-match {}", filename),
                    passed: archived_bytes == expected_contents,
                    detail: Some(format!(
                        "uploaded {} bytes, archived {} bytes",
                        expected_contents.len(),
                        archived_bytes.len()
                    )),
                },
            );
        }

        result.duration = start.elapsed();
        Ok(result)
    }
}
