use anyhow::Result;
use hopnet_common::{TakeoutRecord, TakeoutStatus};
use reqwest::Client;
use std::collections::HashMap;
use std::io::Read;
use std::time::{Duration, Instant};

use crate::tests::{Check, TestResult, TestScenario, print_and_add_check};
use crate::tests::files::upload_file;
use crate::NodeInfo;

// ============================================================================
// Takeout HTTP Helpers
// ============================================================================

/// POST /takeout/initiate — start a new takeout for the authenticated user.
pub async fn initiate_takeout(node: &NodeInfo) -> Result<()> {
    let client = Client::new();
    let url = format!("http://{}:{}/takeout/initiate", node.ip_address, node.port);

    let response = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_else(|_| "No body".to_string());
        anyhow::bail!("initiate_takeout failed with status {}: {}", status, body);
    }

    Ok(())
}

/// GET /takeout — list all takeouts for the authenticated user.
pub async fn list_takeouts(node: &NodeInfo) -> Result<Vec<TakeoutRecord>> {
    let client = Client::new();
    let url = format!("http://{}:{}/takeout", node.ip_address, node.port);

    let response = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_else(|_| "No body".to_string());
        anyhow::bail!("list_takeouts failed with status {}: {}", status, body);
    }

    Ok(response.json().await?)
}

/// Poll /takeout until a takeout reaches Ready status or the timeout elapses.
/// Returns the takeout ID.
pub async fn wait_for_takeout_ready(
    node: &NodeInfo,
    timeout: Duration,
) -> Result<String> {
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
                timeout, statuses
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
        "http://{}:{}/takeout/{}/download",
        node.ip_address, node.port, takeout_id
    );

    let response = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_else(|_| "No body".to_string());
        anyhow::bail!("download_takeout_archive failed with status {}: {}", status, body);
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
    pub manifest: hopnet::takeout::manifest::TakeoutManifest,
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
            if path == hopnet::takeout::manifest::MANIFEST_FILENAME {
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

    let first = entry_order.first().ok_or_else(|| anyhow::anyhow!("Archive is empty"))?;
    if first != hopnet::takeout::manifest::MANIFEST_FILENAME {
        anyhow::bail!("Expected first archive entry to be manifest.json, got {}", first);
    }

    let manifest_raw = manifest_raw.ok_or_else(|| anyhow::anyhow!("Archive missing manifest.json"))?;
    let manifest: hopnet::takeout::manifest::TakeoutManifest = serde_json::from_slice(&manifest_raw)
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

    async fn run(&self, _mesh_id: u32, nodes: &[NodeInfo], _flags: &[String]) -> Result<TestResult> {
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
                let contents = std::iter::repeat(byte).take(size).collect::<Vec<u8>>();
                (filename, contents)
            })
            .collect();

        for (filename, contents) in &files {
            match upload_file(node, "/", filename, contents.clone()).await {
                Ok(_) => print_and_add_check(&mut result, Check {
                    name: format!("Upload {}", filename),
                    passed: true,
                    detail: None,
                }),
                Err(e) => {
                    print_and_add_check(&mut result, Check {
                        name: format!("Upload {} failed", filename),
                        passed: false,
                        detail: Some(e.to_string()),
                    });
                    result.duration = start.elapsed();
                    return Ok(result);
                }
            }
        }

        // Step 2: Initiate takeout
        match initiate_takeout(node).await {
            Ok(_) => print_and_add_check(&mut result, Check {
                name: "Initiate takeout".to_string(),
                passed: true,
                detail: None,
            }),
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Initiate takeout failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 3: Wait for takeout to reach Ready status
        let takeout_id = match wait_for_takeout_ready(node, Duration::from_secs(120)).await {
            Ok(id) => {
                print_and_add_check(&mut result, Check {
                    name: "Takeout reached Ready status".to_string(),
                    passed: true,
                    detail: Some(id.clone()),
                });
                id
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Wait for Ready failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Step 4: Download archive
        let archive_bytes = match download_takeout_archive(node, &takeout_id).await {
            Ok(bytes) => {
                print_and_add_check(&mut result, Check {
                    name: "Download archive".to_string(),
                    passed: true,
                    detail: Some(format!("{} bytes", bytes.len())),
                });
                bytes
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Download archive failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Step 5: Decompress, validate manifest-first ordering, parse manifest into typed struct
        let archive = match extract_tar_gz(&archive_bytes) {
            Ok(a) => {
                print_and_add_check(&mut result, Check {
                    name: "Decompress archive + manifest first entry".to_string(),
                    passed: true,
                    detail: Some(format!(
                        "{} entries total, {} content files, manifest version {}",
                        a.entry_order.len(), a.files.len(), a.manifest.version
                    )),
                });
                a
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Decompress archive / manifest validation failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Step 6: Manifest schema sanity. Pin to spec version 1 with a literal so a
        // future MANIFEST_VERSION bump must consciously update this assertion (otherwise
        // checking against the constant is circular — both writer and reader share it).
        let expected_total_bytes: u64 = files.iter().map(|(_, c)| c.len() as u64).sum();
        let expected_files_count = files.len() as u64;

        print_and_add_check(&mut result, Check {
            name: "Manifest version == 1 (spec v1)".to_string(),
            passed: archive.manifest.version == 1,
            detail: Some(format!("got version {}", archive.manifest.version)),
        });

        print_and_add_check(&mut result, Check {
            name: "Manifest takeout_id matches downloaded id".to_string(),
            passed: archive.manifest.takeout_id.to_string() == takeout_id,
            detail: Some(format!(
                "manifest={}, downloaded={}",
                archive.manifest.takeout_id, takeout_id
            )),
        });

        print_and_add_check(&mut result, Check {
            name: format!("Manifest total_files == {}", expected_files_count),
            passed: archive.manifest.total_files == expected_files_count
                && archive.manifest.files.len() as u64 == expected_files_count,
            detail: Some(format!(
                "total_files={}, files.len()={}",
                archive.manifest.total_files, archive.manifest.files.len()
            )),
        });

        print_and_add_check(&mut result, Check {
            name: "Manifest total_folders == 0".to_string(),
            passed: archive.manifest.total_folders == 0
                && archive.manifest.folders.is_empty(),
            detail: Some(format!(
                "total_folders={}, folders.len()={}",
                archive.manifest.total_folders, archive.manifest.folders.len()
            )),
        });

        print_and_add_check(&mut result, Check {
            name: format!("Manifest total_bytes == {}", expected_total_bytes),
            passed: archive.manifest.total_bytes == expected_total_bytes,
            detail: Some(format!("total_bytes={}", archive.manifest.total_bytes)),
        });

        // Step 7: Per-file verification — manifest entry shape, archive layout, content + hash match
        for (filename, expected_contents) in &files {
            // Find the manifest entry by user-facing path (no `files/` prefix in manifest).
            let manifest_entry = archive
                .manifest
                .files
                .iter()
                .find(|f| f.path == *filename);

            let manifest_entry = match manifest_entry {
                Some(e) => e,
                None => {
                    print_and_add_check(&mut result, Check {
                        name: format!("Manifest contains entry for {}", filename),
                        passed: false,
                        detail: Some(format!(
                            "manifest paths: {:?}",
                            archive.manifest.files.iter().map(|f| &f.path).collect::<Vec<_>>()
                        )),
                    });
                    continue;
                }
            };

            // Manifest size matches uploaded size
            print_and_add_check(&mut result, Check {
                name: format!("Manifest size matches for {}", filename),
                passed: manifest_entry.size as usize == expected_contents.len(),
                detail: Some(format!(
                    "manifest={}, uploaded={}",
                    manifest_entry.size, expected_contents.len()
                )),
            });

            // file_hash must equal blake3(plaintext || source_data_block_id_bytes).
            // Reconstructing the formula here catches any drift in the takeout salting logic.
            let mut hasher = blake3::Hasher::new();
            hasher.update(expected_contents);
            hasher.update(manifest_entry.source_data_block_id.as_bytes());
            let computed = hasher.finalize();
            let expected_hash_bytes = manifest_entry.file_hash.as_bytes();
            print_and_add_check(&mut result, Check {
                name: format!("Manifest file_hash matches reconstructed for {}", filename),
                passed: computed.as_bytes() == expected_hash_bytes,
                detail: Some(format!(
                    "expected hash from manifest: {}",
                    manifest_entry.file_hash
                )),
            });

            // Archive layout: file lives under `files/<path>`, NOT at the root.
            let prefixed_path = format!(
                "{}{}",
                hopnet::takeout::manifest::ARCHIVE_FILES_PREFIX,
                filename
            );
            let archived_bytes = match archive.files.get(&prefixed_path) {
                Some(b) => b,
                None => {
                    print_and_add_check(&mut result, Check {
                        name: format!("Archive entry under files/ prefix for {}", filename),
                        passed: false,
                        detail: Some(format!(
                            "expected at {}, archive paths: {:?}",
                            prefixed_path,
                            archive.files.keys().collect::<Vec<_>>()
                        )),
                    });
                    continue;
                }
            };

            // Byte-match between uploaded and archived content
            print_and_add_check(&mut result, Check {
                name: format!("Byte-match {}", filename),
                passed: archived_bytes == expected_contents,
                detail: Some(format!(
                    "uploaded {} bytes, archived {} bytes",
                    expected_contents.len(), archived_bytes.len()
                )),
            });
        }

        result.duration = start.elapsed();
        Ok(result)
    }
}
