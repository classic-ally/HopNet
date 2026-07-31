use anyhow::Result;
use chrono::Utc;
use flate2::{Compression, write::GzEncoder};
use hopnet::db::CustomUUID;
use hopnet::types::Blake3Hash;
use hopnet_takeout::manifest::{
    EntryKind, MANIFEST_FILENAME, ManifestEntry, ProjectionSection, TakeoutManifest,
};
use std::collections::BTreeMap;
use hopnet_common::{
    ImportPathCounts, ImportPathRow, ImportPathStatus, ImportRecord, ImportStatus, InodeType,
};
use reqwest::{Client, StatusCode};
use std::io::Write;
use std::time::{Duration, Instant};
use tar::{Builder, Header};

use crate::NodeInfo;
use crate::tests::{Check, TestResult, TestScenario, print_and_add_check};

// ============================================================================
// Helpers — shared by all import scenarios. The archive *bytes* are the only
// swappable input; replacing `build_minimal_import_archive` with a future
// `archive_from_real_takeout(node)` lets the same scenarios round-trip in
// Phase 5 without restructuring.
// ============================================================================

/// Construct a baseline-valid v2 `TakeoutManifest` for upload tests (no
/// sections). Scenarios mutate fields (e.g. version, section totals) to
/// exercise rejection paths.
pub fn default_test_manifest() -> TakeoutManifest {
    TakeoutManifest {
        version: 2,
        takeout_id: CustomUUID::new(None),
        created_at: Utc::now(),
        source_username: "import-upload-test".to_string(),
        projections: BTreeMap::new(),
    }
}

/// Build a tar.gz archive with `manifest` written as the first entry under
/// `manifest.json`, then each `(archive_path, bytes)` written under its
/// path. Mirrors the writer side of `src/takeout/archive.rs::create_archive`
/// at minimal scope for tests.
pub fn build_minimal_import_archive(
    manifest: &TakeoutManifest,
    file_entries: Vec<(String, Vec<u8>)>,
) -> Result<Vec<u8>> {
    let mut buf: Vec<u8> = Vec::new();
    {
        let gz = GzEncoder::new(&mut buf, Compression::default());
        let mut tar = Builder::new(gz);

        let manifest_bytes = manifest.to_archive_bytes()?;
        let mut header = Header::new_gnu();
        header.set_path(MANIFEST_FILENAME)?;
        header.set_size(manifest_bytes.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        tar.append(&header, manifest_bytes.as_slice())?;

        for (path, bytes) in file_entries {
            let mut h = Header::new_gnu();
            h.set_path(&path)?;
            h.set_size(bytes.len() as u64);
            h.set_mode(0o644);
            h.set_cksum();
            tar.append(&h, bytes.as_slice())?;
        }

        let gz = tar.into_inner()?;
        gz.finish()?.flush()?;
    }
    Ok(buf)
}

/// Build a tar.gz archive whose first entry is NOT `manifest.json`. Used to
/// exercise the missing-manifest rejection path.
pub fn build_archive_with_wrong_first_entry() -> Result<Vec<u8>> {
    let mut buf: Vec<u8> = Vec::new();
    {
        let gz = GzEncoder::new(&mut buf, Compression::default());
        let mut tar = Builder::new(gz);
        let body = b"surprise non-manifest entry";
        let mut h = Header::new_gnu();
        h.set_path("drive/foo.txt")?;
        h.set_size(body.len() as u64);
        h.set_mode(0o644);
        h.set_cksum();
        tar.append(&h, &body[..])?;
        let gz = tar.into_inner()?;
        gz.finish()?.flush()?;
    }
    Ok(buf)
}

/// POST `/takeout/import` with a multipart `archive` field carrying the given
/// bytes. Caller inspects the returned `Response` for status + body.
pub async fn upload_import_archive(node: &NodeInfo, bytes: Vec<u8>) -> Result<reqwest::Response> {
    let client = Client::new();
    let url = format!("http://{}:{}/api/takeout/import", node.ip_address, node.port);
    let part = reqwest::multipart::Part::bytes(bytes).file_name("archive.tar.gz");
    let form = reqwest::multipart::Form::new().part("archive", part);
    let resp = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .multipart(form)
        .send()
        .await?;
    Ok(resp)
}

/// Convenience wrapper used by scenarios that only care about the status code.
async fn initiate_import_status(node: &NodeInfo, bytes: Vec<u8>) -> Result<StatusCode> {
    Ok(upload_import_archive(node, bytes).await?.status())
}

/// GET /takeout/import — fetch the user's current import (singleton).
async fn get_current_import(node: &NodeInfo) -> Result<Option<ImportRecord>> {
    let client = Client::new();
    let url = format!("http://{}:{}/api/takeout/import", node.ip_address, node.port);
    let resp = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .send()
        .await?;
    if !resp.status().is_success() {
        anyhow::bail!("get_current_import {} returned {}", url, resp.status());
    }
    Ok(resp.json().await?)
}

/// Poll all nodes for a non-None current import. Returns the first one observed.
/// Bounds wait by the supplied timeout.
async fn wait_for_import_visible(nodes: &[NodeInfo], timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let mut all_have = true;
        for node in nodes {
            match get_current_import(node).await? {
                Some(_) => {}
                None => {
                    all_have = false;
                    break;
                }
            }
        }
        if all_have {
            return Ok(());
        }
        if Instant::now() >= deadline {
            anyhow::bail!("Not all nodes saw the import within {:?}", timeout);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Poll `node` until its `imports.status` field equals `target`. Used by
/// extraction-side scenarios to confirm the bg task flipped to `Importing`
/// before observing per-path state.
async fn wait_for_import_status(
    node: &NodeInfo,
    target: ImportStatus,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(record) = get_current_import(node).await?
            && record.status == target
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "Import did not reach status {:?} on node {}:{} within {:?}",
                target,
                node.ip_address,
                node.port,
                timeout
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// GET /takeout/import/paths — owner-node-local per-import path table dump.
/// 3.7 supersedes with an aggregate-status route reachable from any node.
pub async fn get_import_paths(node: &NodeInfo) -> Result<Vec<ImportPathRow>> {
    let client = Client::new();
    let url = format!(
        "http://{}:{}/api/takeout/import/paths",
        node.ip_address, node.port
    );
    let resp = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .send()
        .await?;
    if !resp.status().is_success() {
        anyhow::bail!("get_import_paths {} returned {}", url, resp.status());
    }
    Ok(resp.json().await?)
}

/// Wait until `node` reports at least `count` rows in its per-import path
/// table. The table is seeded transactionally early in extraction, so this
/// also serves as "extraction has begun" signal.
async fn wait_for_import_paths_count(
    node: &NodeInfo,
    count: usize,
    timeout: Duration,
) -> Result<Vec<ImportPathRow>> {
    let deadline = Instant::now() + timeout;
    loop {
        let rows = get_import_paths(node).await.unwrap_or_default();
        if rows.len() >= count {
            return Ok(rows);
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "Path table on node {}:{} only has {} rows after {:?} (expected ≥ {})",
                node.ip_address,
                node.port,
                rows.len(),
                timeout,
                count
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Compute `blake3(plaintext ∥ blob_id.as_bytes())` matching the
/// takeout-side formula in `hopnet_takeout::export`. Tests use
/// this both to fabricate manifests with correct hashes (happy path) and to
/// fabricate manifests whose hashes intentionally diverge from supplied bytes
/// (mismatch path).
pub fn compute_archive_file_hash(bytes: &[u8], source_data_block_id: &CustomUUID) -> Blake3Hash {
    let mut hasher = blake3::Hasher::new();
    hasher.update(bytes);
    hasher.update(source_data_block_id.as_bytes());
    Blake3Hash::new(hasher.finalize())
}

/// Assemble one v2 projection section from folder paths + (file entry,
/// bytes) pairs. Totals are computed from the inputs.
pub fn build_projection_section(
    folders: &[String],
    files: &[(ManifestEntry, Vec<u8>)],
) -> ProjectionSection {
    let mut entries: Vec<ManifestEntry> = folders
        .iter()
        .map(|path| ManifestEntry {
            logical_path: path.clone(),
            kind: EntryKind::Folder,
            size: 0,
            blob_id: None,
            content_hash: None,
            metadata: serde_json::json!({}),
        })
        .collect();
    entries.extend(files.iter().map(|(f, _)| f.clone()));
    ProjectionSection {
        total_files: files.len() as u64,
        total_folders: folders.len() as u64,
        total_bytes: files.iter().map(|(f, _)| f.size).sum(),
        entries,
    }
}

/// Build a fully-formed v2 import archive: the manifest declares a "drive"
/// section with the supplied folders + files (each file with size + blob_id
/// + correctly computed `content_hash`); tar payload writes file bytes under
/// the canonical `drive/` projection prefix.
pub fn build_import_archive_with_files(
    folders: Vec<String>,
    files: Vec<(ManifestEntry, Vec<u8>)>,
) -> Result<Vec<u8>> {
    let mut projections = BTreeMap::new();
    projections.insert(
        "drive".to_string(),
        build_projection_section(&folders, &files),
    );

    let manifest = TakeoutManifest {
        version: 2,
        takeout_id: CustomUUID::new(None),
        created_at: Utc::now(),
        source_username: "import-extraction-test".to_string(),
        projections,
    };

    let payload: Vec<(String, Vec<u8>)> = files
        .into_iter()
        .map(|(f, bytes)| (format!("drive/{}", f.logical_path), bytes))
        .collect();
    build_minimal_import_archive(&manifest, payload)
}

// ============================================================================
// Scenario
// ============================================================================

/// Verifies the consensus + route plumbing for `create_import`:
///   1. POST creates a Pending row visible on all nodes
///   2. GET returns a consistent record across nodes
///   3. Re-POST on the originating node is rejected (route-level gate)
///   4. POST on a different node for the same user is rejected (consensus-level gate
///      is the ultimate enforcement, but the route-level gate also fires after
///      consensus has propagated the first import)
pub struct ImportCreateActiveConflict;

impl TestScenario for ImportCreateActiveConflict {
    fn name(&self) -> &'static str {
        "import-create-active-conflict"
    }

    fn description(&self) -> &'static str {
        "POST /takeout/import creates a pending import, all nodes see the same row via consensus, and concurrent POSTs (same node or different node) are rejected with 429."
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

        // Step 1: Initial POST on node[0] with a valid manifest-only archive
        let archive = match build_minimal_import_archive(&default_test_manifest(), vec![]) {
            Ok(b) => b,
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "build_minimal_import_archive".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };
        let status = match initiate_import_status(&nodes[0], archive).await {
            Ok(s) => s,
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "POST /takeout/import on node[0]".to_string(),
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
                name: "POST /takeout/import on node[0] returns 201".to_string(),
                passed: status == StatusCode::CREATED,
                detail: Some(format!("got {}", status)),
            },
        );

        // Step 2: Wait for consensus propagation; verify all nodes see the import
        if let Err(e) = wait_for_import_visible(nodes, Duration::from_secs(15)).await {
            print_and_add_check(
                &mut result,
                Check {
                    name: "Import visible on all nodes via GET /takeout/import".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                },
            );
            result.duration = start.elapsed();
            return Ok(result);
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "Import visible on all nodes via GET /takeout/import".to_string(),
                passed: true,
                detail: None,
            },
        );

        // Step 3: Cross-node consistency — same record everywhere
        let mut records: Vec<ImportRecord> = Vec::new();
        for (i, node) in nodes.iter().enumerate() {
            match get_current_import(node).await {
                Ok(Some(r)) => records.push(r),
                Ok(None) => {
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: format!("Node {} returned None for current import", i),
                            passed: false,
                            detail: None,
                        },
                    );
                    result.duration = start.elapsed();
                    return Ok(result);
                }
                Err(e) => {
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: format!("GET /takeout/import on node {} failed", i),
                            passed: false,
                            detail: Some(e.to_string()),
                        },
                    );
                    result.duration = start.elapsed();
                    return Ok(result);
                }
            }
        }
        let first_id = records[0].id.to_string();
        let all_match_id = records.iter().all(|r| r.id.to_string() == first_id);
        // Status is Pending immediately after `create_import` commits, then
        // flips to Importing once the bg extraction task (Phase 3.4) submits
        // its `update_import_status` consensus txn. Either is a valid "active
        // import" observation for this conflict-gate test.
        let all_active = records
            .iter()
            .all(|r| r.status == ImportStatus::Pending || r.status == ImportStatus::Importing);
        print_and_add_check(
            &mut result,
            Check {
                name: "All nodes report the same import id".to_string(),
                passed: all_match_id,
                detail: Some(format!(
                    "ids: {:?}",
                    records.iter().map(|r| r.id.to_string()).collect::<Vec<_>>()
                )),
            },
        );
        print_and_add_check(
            &mut result,
            Check {
                name: "All nodes report active status (Pending or Importing)".to_string(),
                passed: all_active,
                detail: Some(format!(
                    "statuses: {:?}",
                    records.iter().map(|r| &r.status).collect::<Vec<_>>()
                )),
            },
        );

        // Step 4: Re-POST on node[0] should be rejected (eligibility fires before
        // body is read; empty bytes is fine)
        match initiate_import_status(&nodes[0], vec![]).await {
            Ok(StatusCode::TOO_MANY_REQUESTS) => print_and_add_check(
                &mut result,
                Check {
                    name: "Re-POST on node[0] returns 429".to_string(),
                    passed: true,
                    detail: None,
                },
            ),
            Ok(other) => print_and_add_check(
                &mut result,
                Check {
                    name: "Re-POST on node[0] returns 429".to_string(),
                    passed: false,
                    detail: Some(format!("got {}", other)),
                },
            ),
            Err(e) => print_and_add_check(
                &mut result,
                Check {
                    name: "Re-POST on node[0]".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                },
            ),
        }

        // Step 5: POST on node[1] (same user, different node) should also be rejected
        if nodes.len() >= 2 {
            match initiate_import_status(&nodes[1], vec![]).await {
                Ok(StatusCode::TOO_MANY_REQUESTS) => print_and_add_check(
                    &mut result,
                    Check {
                        name: "POST on node[1] returns 429 (cross-node gate)".to_string(),
                        passed: true,
                        detail: None,
                    },
                ),
                Ok(other) => print_and_add_check(
                    &mut result,
                    Check {
                        name: "POST on node[1] returns 429 (cross-node gate)".to_string(),
                        passed: false,
                        detail: Some(format!("got {}", other)),
                    },
                ),
                Err(e) => print_and_add_check(
                    &mut result,
                    Check {
                        name: "POST on node[1]".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                ),
            }
        }

        result.duration = start.elapsed();
        Ok(result)
    }
}

// ============================================================================
// Phase 3.3 upload-side scenarios
// ============================================================================

async fn assert_no_import_record(nodes: &[NodeInfo], result: &mut TestResult) {
    let mut any_present = false;
    for (i, node) in nodes.iter().enumerate() {
        match get_current_import(node).await {
            Ok(None) => {}
            Ok(Some(r)) => {
                any_present = true;
                print_and_add_check(
                    result,
                    Check {
                        name: format!("Node {} unexpectedly has import record", i),
                        passed: false,
                        detail: Some(r.id.to_string()),
                    },
                );
            }
            Err(e) => {
                print_and_add_check(
                    result,
                    Check {
                        name: format!("GET /takeout/import on node {} failed", i),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
            }
        }
    }
    if !any_present {
        print_and_add_check(
            result,
            Check {
                name: "No import row exists on any node".to_string(),
                passed: true,
                detail: None,
            },
        );
    }
}

/// Happy path: a valid manifest-only archive is accepted, returns 201, and
/// produces a Pending import row visible on every node.
pub struct ImportUploadHappyPath;

impl TestScenario for ImportUploadHappyPath {
    fn name(&self) -> &'static str {
        "import-upload-happy-path"
    }
    fn description(&self) -> &'static str {
        "POST /takeout/import with a valid manifest-only tar.gz returns 201 and produces a Pending import row visible on every node"
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

        let archive = build_minimal_import_archive(&default_test_manifest(), vec![])?;
        let resp = upload_import_archive(&nodes[0], archive).await?;
        let status = resp.status();
        print_and_add_check(
            &mut result,
            Check {
                name: "Upload returns 201 Created".to_string(),
                passed: status == StatusCode::CREATED,
                detail: Some(format!("got {}", status)),
            },
        );

        if status == StatusCode::CREATED {
            if let Err(e) = wait_for_import_visible(nodes, Duration::from_secs(30)).await {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Import visible on all nodes".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
            } else {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Import visible on all nodes".to_string(),
                        passed: true,
                        detail: None,
                    },
                );
            }
        }

        result.duration = start.elapsed();
        Ok(result)
    }
}

/// Reject archives whose manifest carries a future schema version. Server
/// returns 400 and never submits `create_import` (no row appears).
pub struct ImportUploadVersionRejected;

impl TestScenario for ImportUploadVersionRejected {
    fn name(&self) -> &'static str {
        "import-upload-version-rejected"
    }
    fn description(&self) -> &'static str {
        "POST /takeout/import with a manifest version other than 2 (here: a future v3) returns 400 and produces no import row"
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

        let mut manifest = default_test_manifest();
        manifest.version = 3;
        let archive = build_minimal_import_archive(&manifest, vec![])?;
        let resp = upload_import_archive(&nodes[0], archive).await?;
        let status = resp.status();
        print_and_add_check(
            &mut result,
            Check {
                name: "Upload returns 400 Bad Request".to_string(),
                passed: status == StatusCode::BAD_REQUEST,
                detail: Some(format!("got {}", status)),
            },
        );

        // Give consensus a moment in case anything was erroneously submitted.
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_no_import_record(nodes, &mut result).await;

        result.duration = start.elapsed();
        Ok(result)
    }
}

/// Reject archives whose first tar entry is not `manifest.json`. Returns 400.
pub struct ImportUploadMissingManifest;

impl TestScenario for ImportUploadMissingManifest {
    fn name(&self) -> &'static str {
        "import-upload-missing-manifest"
    }
    fn description(&self) -> &'static str {
        "POST /takeout/import with a tar.gz whose first entry is not manifest.json returns 400 and produces no import row"
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

        let archive = build_archive_with_wrong_first_entry()?;
        let resp = upload_import_archive(&nodes[0], archive).await?;
        let status = resp.status();
        print_and_add_check(
            &mut result,
            Check {
                name: "Upload returns 400 Bad Request".to_string(),
                passed: status == StatusCode::BAD_REQUEST,
                detail: Some(format!("got {}", status)),
            },
        );

        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_no_import_record(nodes, &mut result).await;

        result.duration = start.elapsed();
        Ok(result)
    }
}

/// Reject archives whose declared `total_bytes` exceeds available capacity
/// (× RS expansion + safety margin). Returns 507.
pub struct ImportUploadQuotaExceeded;

impl TestScenario for ImportUploadQuotaExceeded {
    fn name(&self) -> &'static str {
        "import-upload-quota-exceeded"
    }
    fn description(&self) -> &'static str {
        "POST /takeout/import with a manifest claiming total_bytes far above network capacity returns 507 and produces no import row"
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

        let mut manifest = default_test_manifest();
        // 1 PB plaintext × 3 (RS) = 3 PB required; vastly exceeds any realistic
        // mesh capacity in test infrastructure. v2: declared per section.
        manifest.projections.insert(
            "drive".to_string(),
            ProjectionSection {
                total_files: 0,
                total_folders: 0,
                total_bytes: 1_000_000_000_000_000,
                entries: vec![],
            },
        );
        let archive = build_minimal_import_archive(&manifest, vec![])?;
        let resp = upload_import_archive(&nodes[0], archive).await?;
        let status = resp.status();
        print_and_add_check(
            &mut result,
            Check {
                name: "Upload returns 507 Insufficient Storage".to_string(),
                passed: status == StatusCode::INSUFFICIENT_STORAGE,
                detail: Some(format!("got {}", status)),
            },
        );

        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_no_import_record(nodes, &mut result).await;

        result.duration = start.elapsed();
        Ok(result)
    }
}

// ============================================================================
// Phase 3.4 extraction-side scenarios
// ============================================================================

/// Build a 5-entry payload (2 folders, 3 files, all hashes correct) for the
/// extraction scenarios. Returns `(folders, file_pairs)` so the caller can
/// pass through unchanged or mutate one entry's bytes for the mismatch test.
fn extraction_payload() -> (Vec<String>, Vec<(ManifestEntry, Vec<u8>)>) {
    let folders = vec!["alpha".to_string(), "alpha/beta".to_string()];
    let files: Vec<(ManifestEntry, Vec<u8>)> = vec![
        ("alpha/one.txt", b"first file content".to_vec()),
        ("alpha/two.txt", b"second file content".to_vec()),
        ("alpha/beta/three.txt", b"third file deeper".to_vec()),
    ]
    .into_iter()
    .map(|(p, bytes)| (make_file_entry(p, bytes.clone()), bytes))
    .collect();
    (folders, files)
}

/// Build one v2 file entry with a fresh blob id and a correctly computed
/// `content_hash` for `bytes`.
fn make_file_entry(path: &str, bytes: Vec<u8>) -> ManifestEntry {
    let id = CustomUUID::new(None);
    let hash = compute_archive_file_hash(&bytes, &id);
    ManifestEntry {
        logical_path: path.to_string(),
        kind: EntryKind::File,
        size: bytes.len() as u64,
        blob_id: Some(id),
        content_hash: Some(hash),
        metadata: serde_json::json!({}),
    }
}

/// Happy path: archive with 2 folders + 3 files (all hashes correct). After
/// upload, the bg extraction task flips the consensus row to `Importing`,
/// seeds the per-import path table with 5 Pending rows, hashes each file,
/// and leaves them Pending (3.5 picks up creation). No row is marked failed.
pub struct ImportExtractionHappyPath;

impl TestScenario for ImportExtractionHappyPath {
    fn name(&self) -> &'static str {
        "import-extraction-happy-path"
    }
    fn description(&self) -> &'static str {
        "Multi-entry archive with correct hashes: extraction seeds 5 Pending path rows on owner, status flips to Importing, no rows marked failed"
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

        let (folders, files) = extraction_payload();
        let archive = build_import_archive_with_files(folders, files)?;

        let resp = upload_import_archive(&nodes[0], archive).await?;
        let status = resp.status();
        print_and_add_check(
            &mut result,
            Check {
                name: "Upload returns 201 Created".to_string(),
                passed: status == StatusCode::CREATED,
                detail: Some(format!("got {}", status)),
            },
        );
        if status != StatusCode::CREATED {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // Wait for status flip to Importing on the owner node.
        match wait_for_import_status(&nodes[0], ImportStatus::Importing, Duration::from_secs(30))
            .await
        {
            Ok(_) => print_and_add_check(
                &mut result,
                Check {
                    name: "Status flips to Importing on owner".to_string(),
                    passed: true,
                    detail: None,
                },
            ),
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Status flips to Importing on owner".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Owner node should expose 5 path rows after seeding.
        let rows = match wait_for_import_paths_count(&nodes[0], 5, Duration::from_secs(20)).await {
            Ok(r) => r,
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Owner reports 5 import-path rows".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        let folder_count = rows
            .iter()
            .filter(|r| r.path_type == InodeType::Folder)
            .count();
        let file_count = rows
            .iter()
            .filter(|r| r.path_type == InodeType::File)
            .count();
        // The creation walk starts as soon as extraction seeds rows, and
        // consensus is now fast enough that it can finish entries before this
        // poll observes them — Imported is a legal state here; Failed is not.
        let all_seeded = rows
            .iter()
            .all(|r| {
                r.status == ImportPathStatus::Pending || r.status == ImportPathStatus::Imported
            });
        let any_failed = rows.iter().any(|r| r.error_code.is_some());

        print_and_add_check(
            &mut result,
            Check {
                name: "2 folder rows + 3 file rows present".to_string(),
                passed: folder_count == 2 && file_count == 3,
                detail: Some(format!("folders={} files={}", folder_count, file_count)),
            },
        );
        print_and_add_check(
            &mut result,
            Check {
                name: "All 5 rows seeded (Pending, or Imported if creation raced ahead)"
                    .to_string(),
                passed: all_seeded,
                detail: Some(format!(
                    "statuses: {:?}",
                    rows.iter().map(|r| &r.status).collect::<Vec<_>>()
                )),
            },
        );
        print_and_add_check(
            &mut result,
            Check {
                name: "No row carries an error_code".to_string(),
                passed: !any_failed,
                detail: if any_failed {
                    Some(format!(
                        "error_codes: {:?}",
                        rows.iter()
                            .filter_map(|r| r.error_code.clone())
                            .collect::<Vec<_>>()
                    ))
                } else {
                    None
                },
            },
        );

        // Non-owner nodes should reject the per-path debug route with 404.
        if nodes.len() >= 2 {
            let client = Client::new();
            let url = format!(
                "http://{}:{}/api/takeout/import/paths",
                nodes[1].ip_address, nodes[1].port
            );
            let resp = client
                .get(&url)
                .header("Authorization", format!("Bearer {}", nodes[1].jwt_token))
                .send()
                .await?;
            print_and_add_check(
                &mut result,
                Check {
                    name: "Non-owner /paths returns 404".to_string(),
                    passed: resp.status() == StatusCode::NOT_FOUND,
                    detail: Some(format!("got {}", resp.status())),
                },
            );
        }

        result.duration = start.elapsed();
        Ok(result)
    }
}

/// Mismatch path: archive's tar payload differs from manifest's declared
/// `file_hash` for one file. Extraction marks that row failed with
/// `error_code = "hash_mismatch"`; other file + folder rows remain Pending.
/// Status remains `Importing` (no creation walk yet — that lands in 3.5).
pub struct ImportExtractionHashMismatch;

impl TestScenario for ImportExtractionHashMismatch {
    fn name(&self) -> &'static str {
        "import-extraction-hash-mismatch"
    }
    fn description(&self) -> &'static str {
        "Archive whose tar bytes for one file diverge from the manifest's file_hash: extraction marks that row failed (hash_mismatch); peer rows stay Pending"
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

        // Build the payload, then corrupt the *bytes* of one file so the bytes-vs-manifest hash
        // diverges. The manifest hash is left correct for the original bytes.
        let (folders, mut files) = extraction_payload();
        let target_path = files[1].0.logical_path.clone();
        files[1].1 = b"CORRUPTED CONTENT".to_vec();
        files[1].0.size = files[1].1.len() as u64;
        let archive = build_import_archive_with_files(folders, files)?;

        let resp = upload_import_archive(&nodes[0], archive).await?;
        let status = resp.status();
        print_and_add_check(
            &mut result,
            Check {
                name: "Upload returns 201 Created".to_string(),
                passed: status == StatusCode::CREATED,
                detail: Some(format!("got {}", status)),
            },
        );
        if status != StatusCode::CREATED {
            result.duration = start.elapsed();
            return Ok(result);
        }

        match wait_for_import_status(&nodes[0], ImportStatus::Importing, Duration::from_secs(30))
            .await
        {
            Ok(_) => print_and_add_check(
                &mut result,
                Check {
                    name: "Status flips to Importing on owner".to_string(),
                    passed: true,
                    detail: None,
                },
            ),
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Status flips to Importing on owner".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Wait for the corrupted row to land as failed. Peer rows seeded as
        // Pending in manifest seeding; poll until target flips to Failed.
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut last_rows: Vec<ImportPathRow>;
        loop {
            last_rows = get_import_paths(&nodes[0]).await.unwrap_or_default();
            let target = last_rows.iter().find(|r| r.path == target_path);
            if let Some(row) = target
                && row.status == ImportPathStatus::Failed
            {
                break;
            }
            if Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        let target_row = last_rows.iter().find(|r| r.path == target_path);
        let target_failed_with_mismatch = matches!(
            target_row,
            Some(r) if r.status == ImportPathStatus::Failed
                && r.error_code.as_deref() == Some("hash_mismatch")
        );
        print_and_add_check(
            &mut result,
            Check {
                name: "Corrupted file row marked Failed with hash_mismatch".to_string(),
                passed: target_failed_with_mismatch,
                detail: Some(format!(
                    "row: {:?}",
                    target_row.map(|r| (&r.status, &r.error_code))
                )),
            },
        );

        let other_files_still_pending = last_rows
            .iter()
            .filter(|r| r.path_type == InodeType::File && r.path != target_path)
            .all(|r| r.status == ImportPathStatus::Pending);
        print_and_add_check(
            &mut result,
            Check {
                name: "Other file rows remain Pending".to_string(),
                passed: other_files_still_pending,
                detail: None,
            },
        );

        let folders_still_pending = last_rows
            .iter()
            .filter(|r| r.path_type == InodeType::Folder)
            .all(|r| r.status == ImportPathStatus::Pending);
        print_and_add_check(
            &mut result,
            Check {
                name: "Folder rows remain Pending".to_string(),
                passed: folders_still_pending,
                detail: None,
            },
        );

        // 3.5: extraction chains into creation phase. Status reaches Completed
        // even when one file failed extraction; per-file failures live in
        // import_paths. (`Failed` terminal is reserved for catastrophic infra
        // errors per spec.) Either Importing (race) or Completed is acceptable.
        let record = get_current_import(&nodes[0]).await?;
        let acceptable_terminal = matches!(
            record,
            Some(ref r)
                if r.status == ImportStatus::Importing || r.status == ImportStatus::Completed
        );
        print_and_add_check(
            &mut result,
            Check {
                name: "Import status is Importing or Completed".to_string(),
                passed: acceptable_terminal,
                detail: Some(format!("got {:?}", record.map(|r| r.status))),
            },
        );

        result.duration = start.elapsed();
        Ok(result)
    }
}

// ============================================================================
// Phase 3.5 creation-side scenarios
// ============================================================================

/// Every file in `expected` queryable on every node, byte-exact.
async fn assert_files_visible_on_all_nodes(
    nodes: &[NodeInfo],
    expected: &[(String, Vec<u8>)],
) -> Result<()> {
    for (path, want) in expected {
        let bodies = crate::tests::files::download_file_from_all_nodes(nodes, path).await?;
        for (i, got) in bodies.iter().enumerate() {
            if got != want {
                anyhow::bail!(
                    "Node {} returned {} bytes for {}, expected {} bytes",
                    i,
                    got.len(),
                    path,
                    want.len()
                );
            }
        }
    }
    Ok(())
}

/// Happy path: 2 folders + 3 files, all hashes correct. End-to-end import
/// runs through extraction + creation, lands at status Completed, every
/// import_paths row is Imported, all files queryable on all 3 nodes.
pub struct ImportCreationHappyPath;

impl TestScenario for ImportCreationHappyPath {
    fn name(&self) -> &'static str {
        "import-creation-happy-path"
    }
    fn description(&self) -> &'static str {
        "Multi-entry archive with correct hashes: extraction → creation walk → status Completed; all import_paths rows Imported; all files queryable on every node"
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

        let (folders, files) = extraction_payload();
        let expected_files: Vec<(String, Vec<u8>)> = files
            .iter()
            .map(|(f, bytes)| (f.logical_path.clone(), bytes.clone()))
            .collect();
        let archive = build_import_archive_with_files(folders, files)?;

        let resp = upload_import_archive(&nodes[0], archive).await?;
        let status = resp.status();
        print_and_add_check(
            &mut result,
            Check {
                name: "Upload returns 201 Created".to_string(),
                passed: status == StatusCode::CREATED,
                detail: Some(format!("got {}", status)),
            },
        );
        if status != StatusCode::CREATED {
            result.duration = start.elapsed();
            return Ok(result);
        }

        match wait_for_import_status(&nodes[0], ImportStatus::Completed, Duration::from_secs(60))
            .await
        {
            Ok(_) => print_and_add_check(
                &mut result,
                Check {
                    name: "Status reaches Completed on owner".to_string(),
                    passed: true,
                    detail: None,
                },
            ),
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Status reaches Completed on owner".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // All path rows Imported.
        let rows = get_import_paths(&nodes[0]).await.unwrap_or_default();
        let imported_count = rows
            .iter()
            .filter(|r| r.status == ImportPathStatus::Imported)
            .count();
        let any_failed = rows.iter().any(|r| r.status == ImportPathStatus::Failed);
        print_and_add_check(
            &mut result,
            Check {
                name: "All 5 path rows Imported".to_string(),
                passed: imported_count == 5 && !any_failed,
                detail: Some(format!(
                    "imported={} total={} statuses={:?}",
                    imported_count,
                    rows.len(),
                    rows.iter()
                        .map(|r| (&r.path, &r.status))
                        .collect::<Vec<_>>()
                )),
            },
        );

        // Cross-node consensus: every node should report Completed.
        let mut all_completed = true;
        for (i, node) in nodes.iter().enumerate() {
            match get_current_import(node).await? {
                Some(r) if r.status == ImportStatus::Completed => {}
                other => {
                    all_completed = false;
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: format!("Node {} reports Completed", i),
                            passed: false,
                            detail: Some(format!("got {:?}", other.map(|r| r.status))),
                        },
                    );
                }
            }
        }
        if all_completed {
            print_and_add_check(
                &mut result,
                Check {
                    name: "All nodes report Completed".to_string(),
                    passed: true,
                    detail: None,
                },
            );
        }

        // Every file queryable + byte-exact on every node.
        match assert_files_visible_on_all_nodes(nodes, &expected_files).await {
            Ok(_) => print_and_add_check(
                &mut result,
                Check {
                    name: "Every file queryable + byte-exact on all nodes".to_string(),
                    passed: true,
                    detail: None,
                },
            ),
            Err(e) => print_and_add_check(
                &mut result,
                Check {
                    name: "Every file queryable + byte-exact on all nodes".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                },
            ),
        }

        result.duration = start.elapsed();
        Ok(result)
    }
}

/// Mixed-failure: one file's tar bytes diverge from manifest hash so
/// extraction marks it Failed; the creation walk then imports the rest.
/// Terminal status is still Completed (per-file failures don't escalate).
/// The 2 surviving files should be queryable; the corrupted file is not.
pub struct ImportCreationMixedFailure;

impl TestScenario for ImportCreationMixedFailure {
    fn name(&self) -> &'static str {
        "import-creation-mixed-failure"
    }
    fn description(&self) -> &'static str {
        "One file extraction fails on hash; creation walk imports the rest; status Completed; survivors queryable, failure isolated"
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

        let (folders, mut files) = extraction_payload();
        let target_path = files[1].0.logical_path.clone();
        files[1].1 = b"CORRUPTED CONTENT".to_vec();
        files[1].0.size = files[1].1.len() as u64;
        let surviving: Vec<(String, Vec<u8>)> = files
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != 1)
            .map(|(_, (f, bytes))| (f.logical_path.clone(), bytes.clone()))
            .collect();
        let archive = build_import_archive_with_files(folders, files)?;

        let resp = upload_import_archive(&nodes[0], archive).await?;
        let status = resp.status();
        print_and_add_check(
            &mut result,
            Check {
                name: "Upload returns 201 Created".to_string(),
                passed: status == StatusCode::CREATED,
                detail: Some(format!("got {}", status)),
            },
        );
        if status != StatusCode::CREATED {
            result.duration = start.elapsed();
            return Ok(result);
        }

        match wait_for_import_status(&nodes[0], ImportStatus::Completed, Duration::from_secs(60))
            .await
        {
            Ok(_) => print_and_add_check(
                &mut result,
                Check {
                    name: "Status reaches Completed".to_string(),
                    passed: true,
                    detail: None,
                },
            ),
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Status reaches Completed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        let rows = get_import_paths(&nodes[0]).await.unwrap_or_default();
        let target = rows.iter().find(|r| r.path == target_path);
        let target_failed = matches!(
            target,
            Some(r) if r.status == ImportPathStatus::Failed
                && r.error_code.as_deref() == Some("hash_mismatch")
        );
        print_and_add_check(
            &mut result,
            Check {
                name: "Corrupted row remains Failed/hash_mismatch".to_string(),
                passed: target_failed,
                detail: Some(format!(
                    "row: {:?}",
                    target.map(|r| (&r.status, &r.error_code))
                )),
            },
        );

        let other_imported = rows
            .iter()
            .filter(|r| r.path != target_path)
            .all(|r| r.status == ImportPathStatus::Imported);
        print_and_add_check(
            &mut result,
            Check {
                name: "All other rows (folders + survivors) Imported".to_string(),
                passed: other_imported,
                detail: Some(format!(
                    "non-target statuses: {:?}",
                    rows.iter()
                        .filter(|r| r.path != target_path)
                        .map(|r| (&r.path, &r.status))
                        .collect::<Vec<_>>()
                )),
            },
        );

        // Surviving files queryable on every node.
        match assert_files_visible_on_all_nodes(nodes, &surviving).await {
            Ok(_) => print_and_add_check(
                &mut result,
                Check {
                    name: "Surviving files queryable on all nodes".to_string(),
                    passed: true,
                    detail: None,
                },
            ),
            Err(e) => print_and_add_check(
                &mut result,
                Check {
                    name: "Surviving files queryable on all nodes".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                },
            ),
        }

        // Corrupted file should not be queryable. download_file returns Err
        // when the path doesn't resolve.
        let corrupted_visible = crate::tests::files::download_file(&nodes[0], &target_path)
            .await
            .is_ok();
        print_and_add_check(
            &mut result,
            Check {
                name: "Corrupted file is not queryable".to_string(),
                passed: !corrupted_visible,
                detail: Some(format!("download_file returned ok={}", corrupted_visible)),
            },
        );

        result.duration = start.elapsed();
        Ok(result)
    }
}

// ============================================================================
// Phase 3.6 + 3.7 — write gate, status counts, owner-restart resume
// ============================================================================

/// GET /takeout/import/status — owner-node-local aggregate counts.
async fn get_import_status_counts(node: &NodeInfo) -> Result<ImportPathCounts> {
    let client = Client::new();
    let url = format!(
        "http://{}:{}/api/takeout/import/status",
        node.ip_address, node.port
    );
    let resp = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .send()
        .await?;
    if !resp.status().is_success() {
        anyhow::bail!(
            "get_import_status_counts {} returned {}",
            url,
            resp.status()
        );
    }
    Ok(resp.json().await?)
}

/// POST /files returning the raw status code (existing `upload_file` bails on
/// non-success). Used by the write-gate scenario to assert 409.
async fn upload_file_status(
    node: &NodeInfo,
    path: &str,
    filename: &str,
    contents: Vec<u8>,
) -> Result<StatusCode> {
    let client = Client::new();
    let url = format!("http://{}:{}/api/files", node.ip_address, node.port);
    let len = contents.len();
    let part = reqwest::multipart::Part::bytes(contents).file_name(filename.to_string());
    let form = reqwest::multipart::Form::new()
        .text("path", path.to_string())
        .part(format!("file_{}", len), part);
    let resp = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .multipart(form)
        .send()
        .await?;
    Ok(resp.status())
}

/// Verifies the per-user import write gate: while a user has an active
/// import (status Pending or Importing), POST /files returns 409. After
/// Completed, writes succeed again.
pub struct ImportWriteGate;

impl TestScenario for ImportWriteGate {
    fn name(&self) -> &'static str {
        "import-write-gate"
    }
    fn description(&self) -> &'static str {
        "POST /files returns 409 mid-import for the same user; succeeds after import completes"
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

        let (folders, files) = extraction_payload();
        let archive = build_import_archive_with_files(folders, files)?;

        let resp = upload_import_archive(&nodes[0], archive).await?;
        let status = resp.status();
        print_and_add_check(
            &mut result,
            Check {
                name: "Upload returns 201 Created".to_string(),
                passed: status == StatusCode::CREATED,
                detail: Some(format!("got {}", status)),
            },
        );
        if status != StatusCode::CREATED {
            result.duration = start.elapsed();
            return Ok(result);
        }

        if let Err(e) = wait_for_import_visible(nodes, Duration::from_secs(15)).await {
            print_and_add_check(
                &mut result,
                Check {
                    name: "Import visible on all nodes".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                },
            );
            result.duration = start.elapsed();
            return Ok(result);
        }

        match upload_file_status(&nodes[1], "/gated", "during.txt", b"x".to_vec()).await {
            Ok(StatusCode::CONFLICT) => print_and_add_check(
                &mut result,
                Check {
                    name: "POST /files on node[1] mid-import returns 409".to_string(),
                    passed: true,
                    detail: None,
                },
            ),
            Ok(other) => print_and_add_check(
                &mut result,
                Check {
                    name: "POST /files on node[1] mid-import returns 409".to_string(),
                    passed: false,
                    detail: Some(format!("got {}", other)),
                },
            ),
            Err(e) => print_and_add_check(
                &mut result,
                Check {
                    name: "POST /files on node[1] during import".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                },
            ),
        }

        if let Err(e) =
            wait_for_import_status(&nodes[0], ImportStatus::Completed, Duration::from_secs(60))
                .await
        {
            print_and_add_check(
                &mut result,
                Check {
                    name: "Status reaches Completed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                },
            );
            result.duration = start.elapsed();
            return Ok(result);
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "Status reaches Completed".to_string(),
                passed: true,
                detail: None,
            },
        );

        // Wait for terminal status to propagate to node[1].
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if let Some(r) = get_current_import(&nodes[1]).await?
                && r.status == ImportStatus::Completed
            {
                break;
            }
            if Instant::now() >= deadline {
                anyhow::bail!("Completed status didn't reach node[1] within 15s");
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        match upload_file_status(&nodes[1], "/gated", "after.txt", b"y".to_vec()).await {
            Ok(s) if s.is_success() => print_and_add_check(
                &mut result,
                Check {
                    name: "POST /files on node[1] after Completed returns 2xx".to_string(),
                    passed: true,
                    detail: Some(format!("got {}", s)),
                },
            ),
            Ok(other) => print_and_add_check(
                &mut result,
                Check {
                    name: "POST /files on node[1] after Completed returns 2xx".to_string(),
                    passed: false,
                    detail: Some(format!("got {}", other)),
                },
            ),
            Err(e) => print_and_add_check(
                &mut result,
                Check {
                    name: "POST /files on node[1] after Completed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                },
            ),
        }

        result.duration = start.elapsed();
        Ok(result)
    }
}

/// Verifies `GET /takeout/import/status` returns aggregate counts after a
/// mixed-failure import. Owner returns counts; non-owner returns 404.
pub struct ImportStatusCounts;

impl TestScenario for ImportStatusCounts {
    fn name(&self) -> &'static str {
        "import-status-counts"
    }
    fn description(&self) -> &'static str {
        "GET /takeout/import/status returns aggregate counts on owner; 404 on non-owner"
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

        let (folders, mut files) = extraction_payload();
        files[1].1 = b"CORRUPTED CONTENT".to_vec();
        files[1].0.size = files[1].1.len() as u64;
        let archive = build_import_archive_with_files(folders, files)?;

        let resp = upload_import_archive(&nodes[0], archive).await?;
        if resp.status() != StatusCode::CREATED {
            print_and_add_check(
                &mut result,
                Check {
                    name: "Upload returns 201 Created".to_string(),
                    passed: false,
                    detail: Some(format!("got {}", resp.status())),
                },
            );
            result.duration = start.elapsed();
            return Ok(result);
        }

        if let Err(e) =
            wait_for_import_status(&nodes[0], ImportStatus::Completed, Duration::from_secs(60))
                .await
        {
            print_and_add_check(
                &mut result,
                Check {
                    name: "Status reaches Completed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                },
            );
            result.duration = start.elapsed();
            return Ok(result);
        }

        let counts = match get_import_status_counts(&nodes[0]).await {
            Ok(c) => c,
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "GET /takeout/import/status on owner".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // 5 total: 2 folders + 3 files. 1 file fails hash, 2 succeed,
        // 2 folders succeed → imported=4, failed=1.
        let matches_expected = counts.total == 5
            && counts.pending == 0
            && counts.imported == 4
            && counts.skipped == 0
            && counts.failed == 1;
        print_and_add_check(
            &mut result,
            Check {
                name: "Owner reports total=5 imported=4 failed=1".to_string(),
                passed: matches_expected,
                detail: Some(format!("got {:?}", counts)),
            },
        );

        if nodes.len() >= 2 {
            let client = Client::new();
            let url = format!(
                "http://{}:{}/api/takeout/import/status",
                nodes[1].ip_address, nodes[1].port
            );
            let resp = client
                .get(&url)
                .header("Authorization", format!("Bearer {}", nodes[1].jwt_token))
                .send()
                .await?;
            print_and_add_check(
                &mut result,
                Check {
                    name: "Non-owner /status returns 404".to_string(),
                    passed: resp.status() == StatusCode::NOT_FOUND,
                    detail: Some(format!("got {}", resp.status())),
                },
            );
        }

        result.duration = start.elapsed();
        Ok(result)
    }
}

/// Verifies owner-restart resume: stop the owner mid-import, restart it, and
/// re-login via stored passphrase. The login hook drains the resume registry
/// and the creation walk completes.
pub struct ImportResumeAfterRestart;

impl TestScenario for ImportResumeAfterRestart {
    fn name(&self) -> &'static str {
        "import-resume-after-restart"
    }
    fn description(&self) -> &'static str {
        "Stop owner mid-import, restart, re-login: import completes via the resume hook"
    }
    async fn run(&self, mesh_id: u32, nodes: &[NodeInfo], _flags: &[String]) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();
        println!("\nRunning checks:");

        // Hold the takeout-side `before_import_creation_walk` barrier on the
        // owner BEFORE upload. The bg task will flip status to Importing,
        // seed the path table, then block at the barrier — giving us a
        // deterministic mid-import state to stop on.
        let client = Client::new();
        let hold_url = format!(
            "http://{}:{}/api/test/barriers/takeout/before_import_creation_walk/hold",
            nodes[0].ip_address, nodes[0].port
        );
        let hold_resp = client
            .post(&hold_url)
            .header("Authorization", format!("Bearer {}", nodes[0].jwt_token))
            .send()
            .await?;
        if !hold_resp.status().is_success() {
            print_and_add_check(
                &mut result,
                Check {
                    name: "Hold barrier on owner pre-upload".to_string(),
                    passed: false,
                    detail: Some(format!("hold returned {}", hold_resp.status())),
                },
            );
            result.duration = start.elapsed();
            return Ok(result);
        }

        let (folders, files) = extraction_payload();
        let expected_files: Vec<(String, Vec<u8>)> = files
            .iter()
            .map(|(f, bytes)| (f.logical_path.clone(), bytes.clone()))
            .collect();
        let archive = build_import_archive_with_files(folders, files)?;

        let resp = upload_import_archive(&nodes[0], archive).await?;
        if resp.status() != StatusCode::CREATED {
            print_and_add_check(
                &mut result,
                Check {
                    name: "Upload returns 201 Created".to_string(),
                    passed: false,
                    detail: Some(format!("got {}", resp.status())),
                },
            );
            result.duration = start.elapsed();
            return Ok(result);
        }

        // Wait for status Importing — extraction has flipped status; bg task
        // is now blocked at the barrier, so creation hasn't started.
        if let Err(e) =
            wait_for_import_status(&nodes[0], ImportStatus::Importing, Duration::from_secs(30))
                .await
        {
            print_and_add_check(
                &mut result,
                Check {
                    name: "Status reaches Importing pre-stop".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                },
            );
            result.duration = start.elapsed();
            return Ok(result);
        }

        // Confirm path table is seeded (extraction completed) but no rows
        // are Imported (creation hasn't started thanks to the barrier).
        let counts_pre_stop = get_import_status_counts(&nodes[0])
            .await
            .unwrap_or_default();
        print_and_add_check(
            &mut result,
            Check {
                name: "Path table seeded with all rows Pending pre-stop".to_string(),
                passed: counts_pre_stop.total == 5
                    && counts_pre_stop.pending == 5
                    && counts_pre_stop.imported == 0,
                detail: Some(format!("counts: {:?}", counts_pre_stop)),
            },
        );

        let docker = bollard::Docker::connect_with_local_defaults()?;
        if let Err(e) =
            crate::tests::persistence::stop_node(&docker, mesh_id, nodes[0].node_id).await
        {
            print_and_add_check(
                &mut result,
                Check {
                    name: "Stop owner container".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                },
            );
            result.duration = start.elapsed();
            return Ok(result);
        }
        if let Err(e) =
            crate::tests::persistence::start_node(&docker, mesh_id, nodes[0].node_id).await
        {
            print_and_add_check(
                &mut result,
                Check {
                    name: "Restart owner container".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                },
            );
            result.duration = start.elapsed();
            return Ok(result);
        }
        match crate::tests::persistence::wait_for_node_ready(&nodes[0], Duration::from_secs(30))
            .await
        {
            Ok(true) => print_and_add_check(
                &mut result,
                Check {
                    name: "Owner responsive after restart".to_string(),
                    passed: true,
                    detail: None,
                },
            ),
            _ => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Owner responsive after restart".to_string(),
                        passed: false,
                        detail: None,
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Query consensus state from node[1] — it didn't restart, JWT still
        // valid. Replicated `imports` row reflects whatever the cluster sees;
        // since node[0] is back up but no resume hook has fired, status must
        // still be Importing.
        let pre_login = get_current_import(&nodes[1]).await.ok().flatten();
        let stayed_importing =
            matches!(pre_login, Some(ref r) if r.status == ImportStatus::Importing);
        print_and_add_check(
            &mut result,
            Check {
                name: "Status stays Importing pre-login (queried from peer)".to_string(),
                passed: stayed_importing,
                detail: Some(format!("got {:?}", pre_login.map(|r| r.status))),
            },
        );

        let fresh_jwt = match crate::get_jwt_token(
            &docker,
            mesh_id,
            nodes[0].node_id,
            crate::sys::detect_runtime(&docker).await?,
        )
        .await
        {
            Ok(t) => t,
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Re-login after restart".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };
        let mut nodes_vec = nodes.to_vec();
        nodes_vec[0].jwt_token = fresh_jwt;
        let nodes = &nodes_vec;

        // Generous timeout: post-restart consensus needs warmup before
        // ballots succeed (similar pattern to `restart-persistence`).
        match wait_for_import_status(&nodes[0], ImportStatus::Completed, Duration::from_secs(180))
            .await
        {
            Ok(_) => print_and_add_check(
                &mut result,
                Check {
                    name: "Status reaches Completed via resume".to_string(),
                    passed: true,
                    detail: None,
                },
            ),
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Status reaches Completed via resume".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        let counts = get_import_status_counts(&nodes[0])
            .await
            .unwrap_or_default();
        print_and_add_check(
            &mut result,
            Check {
                name: "All 5 path rows Imported (no leftover Pending)".to_string(),
                passed: counts.imported == 5 && counts.pending == 0 && counts.failed == 0,
                detail: Some(format!("counts: {:?}", counts)),
            },
        );

        let mut visible_ok = true;
        for (path, want) in &expected_files {
            let bodies = match crate::tests::files::download_file_from_all_nodes(nodes, path).await
            {
                Ok(b) => b,
                Err(e) => {
                    visible_ok = false;
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: format!("Download {} after resume", path),
                            passed: false,
                            detail: Some(e.to_string()),
                        },
                    );
                    break;
                }
            };
            for (i, got) in bodies.iter().enumerate() {
                if got != want {
                    visible_ok = false;
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: format!("Node {} byte-exact match for {}", i, path),
                            passed: false,
                            detail: Some(format!(
                                "got {} bytes, expected {}",
                                got.len(),
                                want.len()
                            )),
                        },
                    );
                }
            }
        }
        if visible_ok {
            print_and_add_check(
                &mut result,
                Check {
                    name: "Every file byte-exact on every node post-resume".to_string(),
                    passed: true,
                    detail: None,
                },
            );
        }

        result.duration = start.elapsed();
        Ok(result)
    }
}

// ============================================================================
// RFC-015 D5b — skip-unknown-sections contract
// ============================================================================

/// Archive whose manifest carries a "photos" section (no translator
/// registered on the mesh) alongside a normal drive section. The drive
/// section imports fully; every photos row is marked Skipped with
/// `error_code = "no_translator"`; the import still reaches Completed —
/// unknown sections are reported, never failed.
pub struct ImportUnknownProjectionSkipped;

impl TestScenario for ImportUnknownProjectionSkipped {
    fn name(&self) -> &'static str {
        "import-unknown-projection-skipped"
    }
    fn description(&self) -> &'static str {
        "Manifest with a drive section + an unknown photos section: drive imports, photos rows Skipped (no_translator), import Completed"
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

        // Drive section: the standard 2-folder + 3-file payload.
        let (folders, files) = extraction_payload();
        let expected_files: Vec<(String, Vec<u8>)> = files
            .iter()
            .map(|(f, bytes)| (f.logical_path.clone(), bytes.clone()))
            .collect();

        // Photos section: one file with a correct hash — extraction verifies
        // it fine; only the creation walk lacks a translator.
        let photo_path = "album/pic.jpg".to_string();
        let photo_bytes = b"not actually a jpeg".to_vec();
        let photo_entry = make_file_entry(&photo_path, photo_bytes.clone());

        let mut projections = BTreeMap::new();
        projections.insert(
            "drive".to_string(),
            build_projection_section(&folders, &files),
        );
        projections.insert(
            "photos".to_string(),
            build_projection_section(&[], &[(photo_entry.clone(), photo_bytes.clone())]),
        );
        let manifest = TakeoutManifest {
            version: 2,
            takeout_id: CustomUUID::new(None),
            created_at: Utc::now(),
            source_username: "import-unknown-projection-test".to_string(),
            projections,
        };
        let mut payload: Vec<(String, Vec<u8>)> = files
            .iter()
            .map(|(f, bytes)| (format!("drive/{}", f.logical_path), bytes.clone()))
            .collect();
        payload.push((format!("photos/{}", photo_path), photo_bytes));
        let archive = build_minimal_import_archive(&manifest, payload)?;

        let resp = upload_import_archive(&nodes[0], archive).await?;
        let status = resp.status();
        print_and_add_check(
            &mut result,
            Check {
                name: "Upload returns 201 Created".to_string(),
                passed: status == StatusCode::CREATED,
                detail: Some(format!("got {}", status)),
            },
        );
        if status != StatusCode::CREATED {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // The unknown section must not block completion.
        match wait_for_import_status(&nodes[0], ImportStatus::Completed, Duration::from_secs(60))
            .await
        {
            Ok(_) => print_and_add_check(
                &mut result,
                Check {
                    name: "Status reaches Completed despite unknown section".to_string(),
                    passed: true,
                    detail: None,
                },
            ),
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Status reaches Completed despite unknown section".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Photos row Skipped with the structured no_translator code.
        let rows = get_import_paths(&nodes[0]).await.unwrap_or_default();
        let photo_row = rows.iter().find(|r| r.path == photo_path);
        let photo_skipped = matches!(
            photo_row,
            Some(r) if r.status == ImportPathStatus::Skipped
                && r.error_code.as_deref() == Some("no_translator")
        );
        print_and_add_check(
            &mut result,
            Check {
                name: "Photos row marked Skipped with no_translator".to_string(),
                passed: photo_skipped,
                detail: Some(format!(
                    "row: {:?}",
                    photo_row.map(|r| (&r.status, &r.error_code))
                )),
            },
        );

        // Every drive row Imported.
        let drive_imported = rows
            .iter()
            .filter(|r| r.path != photo_path)
            .all(|r| r.status == ImportPathStatus::Imported);
        print_and_add_check(
            &mut result,
            Check {
                name: "All drive rows (2 folders + 3 files) Imported".to_string(),
                passed: drive_imported && rows.len() == 6,
                detail: Some(format!(
                    "statuses: {:?}",
                    rows.iter()
                        .map(|r| (&r.path, &r.status))
                        .collect::<Vec<_>>()
                )),
            },
        );

        // Aggregate counts report the skip (never a failure).
        let counts = get_import_status_counts(&nodes[0])
            .await
            .unwrap_or_default();
        print_and_add_check(
            &mut result,
            Check {
                name: "Counts: total=6 imported=5 skipped=1 failed=0".to_string(),
                passed: counts.total == 6
                    && counts.imported == 5
                    && counts.skipped == 1
                    && counts.failed == 0,
                detail: Some(format!("got {:?}", counts)),
            },
        );

        // Drive files queryable + byte-exact on every node.
        match assert_files_visible_on_all_nodes(nodes, &expected_files).await {
            Ok(_) => print_and_add_check(
                &mut result,
                Check {
                    name: "Drive files queryable + byte-exact on all nodes".to_string(),
                    passed: true,
                    detail: None,
                },
            ),
            Err(e) => print_and_add_check(
                &mut result,
                Check {
                    name: "Drive files queryable + byte-exact on all nodes".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                },
            ),
        }

        result.duration = start.elapsed();
        Ok(result)
    }
}
