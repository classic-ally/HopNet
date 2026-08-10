use anyhow::Result;
use reqwest::{Client, multipart};
use serde::Deserialize;
use std::time::{Duration, Instant};

use crate::NodeInfo;
use crate::tests::{Check, TestResult, TestScenario, print_and_add_check};

/// Test that DocumentProvider write APIs work and replicate across all nodes
pub struct DocumentProviderWriteConsistency;

// Local test types - use String for IDs since CustomUUID serializes to string in JSON
#[derive(Debug, Deserialize)]
struct RegisterDeviceResponse {
    device_id: String,
    api_key: String,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct DocumentProviderItem {
    id: String,
    name: String,
    mime_type: String,
    size: i64,
    last_modified: i64,
    parent_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EnumerateResponse {
    items: Vec<DocumentProviderItem>,
}

/// Default timeout for polling effect propagation across nodes
const PROPAGATION_TIMEOUT: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_secs(2);

impl TestScenario for DocumentProviderWriteConsistency {
    fn name(&self) -> &'static str {
        "documentprovider-write-consistency"
    }

    fn description(&self) -> &'static str {
        "Test DocumentProvider upload, rename, move, and delete operations replicate across all nodes"
    }

    async fn run(
        &self,
        _mesh_id: u32,
        nodes: &[NodeInfo],
        _flags: &[String],
    ) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();
        let client = crate::insecure_client();

        println!("\nRunning DocumentProvider write API checks:");

        // Generate unique names
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let device_name = format!("docprovider-test-{}", timestamp);
        let test_filename = format!("dp-test-{}.txt", timestamp);
        let test_content = format!("DocumentProvider test content {}", timestamp);
        let folder_name = format!("dp-folder-{}", timestamp);

        // Step 1: Register device on node 0
        let api_key = match register_device(&client, &nodes[0], &device_name).await {
            Ok(resp) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("Register device '{}'", device_name),
                        passed: true,
                        detail: Some(format!("device_id: {}", resp.device_id)),
                    },
                );
                resp.api_key
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Device registration failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Wait for device registration to propagate — poll until enumerate works on all nodes
        if !poll_until_enumerate_works(&client, nodes, &api_key, PROPAGATION_TIMEOUT).await {
            print_and_add_check(
                &mut result,
                Check {
                    name: "Device registration propagation timeout".to_string(),
                    passed: false,
                    detail: Some(format!(
                        "Not all nodes accepted device token within {}s",
                        PROPAGATION_TIMEOUT.as_secs()
                    )),
                },
            );
            result.duration = start.elapsed();
            return Ok(result);
        }
        print_and_add_check(
            &mut result,
            Check {
                name: format!("Device token propagated to all {} nodes", nodes.len()),
                passed: true,
                detail: None,
            },
        );

        // Step 2: Upload file via DocumentProvider API on node 0
        match upload_file(
            &client,
            &nodes[0],
            &api_key,
            "root",
            &test_filename,
            test_content.as_bytes(),
        )
        .await
        {
            Ok(_) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("Upload '{}' via DocumentProvider on node 0", test_filename),
                        passed: true,
                        detail: Some(format!("{} bytes", test_content.len())),
                    },
                );
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Upload failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 3: Poll until file appears on ALL nodes
        let file_id = match poll_for_file_on_all_nodes(
            &client,
            nodes,
            &api_key,
            None,
            &test_filename,
            PROPAGATION_TIMEOUT,
        )
        .await
        {
            Some(id) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!(
                            "File '{}' found on all {} nodes with same ID",
                            test_filename,
                            nodes.len()
                        ),
                        passed: true,
                        detail: Some(format!("id: {}", id)),
                    },
                );
                id
            }
            None => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("File '{}' not found on all nodes", test_filename),
                        passed: false,
                        detail: Some(format!(
                            "Did not appear within {}s",
                            PROPAGATION_TIMEOUT.as_secs()
                        )),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Step 3b: Ranged downloads from node 1 (cross-node reconstruction).
        // Should: serve the full body as 200 with Accept-Ranges: bytes so
        // SAF clients discover range support.
        // Should: serve bytes=8-15 as 206 with Content-Range and exactly the
        // sliced bytes; open-ended bytes=8- runs to EOF.
        // Should not: satisfy a range starting at EOF — 416 with
        // Content-Range: bytes */{len}.
        {
            let content = test_content.as_bytes();
            let len = content.len();
            let ranged_ok = check_ranged_downloads(&client, &nodes[1], &api_key, &file_id, content)
                .await;
            match ranged_ok {
                Ok(()) => print_and_add_check(
                    &mut result,
                    Check {
                        name: "Ranged downloads (200/206/open-ended/416) from node 1".to_string(),
                        passed: true,
                        detail: Some(format!("{} bytes total", len)),
                    },
                ),
                Err(e) => {
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: "Ranged downloads failed".to_string(),
                            passed: false,
                            detail: Some(e.to_string()),
                        },
                    );
                    result.duration = start.elapsed();
                    return Ok(result);
                }
            }
        }

        // Step 4: Rename file via PATCH on node 1
        let renamed_filename = format!("renamed-{}.txt", timestamp);
        let rename_node = if nodes.len() > 1 { 1 } else { 0 };

        match rename_item(
            &client,
            &nodes[rename_node],
            &api_key,
            &file_id,
            &renamed_filename,
        )
        .await
        {
            Ok(_) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!(
                            "Rename file to '{}' on node {}",
                            renamed_filename, rename_node
                        ),
                        passed: true,
                        detail: None,
                    },
                );
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Rename failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 5: Poll until renamed file appears on ALL nodes
        match poll_for_file_on_all_nodes(
            &client,
            nodes,
            &api_key,
            None,
            &renamed_filename,
            PROPAGATION_TIMEOUT,
        )
        .await
        {
            Some(_) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!(
                            "Renamed file '{}' found on all {} nodes",
                            renamed_filename,
                            nodes.len()
                        ),
                        passed: true,
                        detail: None,
                    },
                );
            }
            None => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("Renamed file '{}' not found on all nodes", renamed_filename),
                        passed: false,
                        detail: Some(format!(
                            "Did not appear within {}s",
                            PROPAGATION_TIMEOUT.as_secs()
                        )),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 6: Create folder for move test
        match create_folder(&client, &nodes[0], &api_key, "root", &folder_name).await {
            Ok(_) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("Create folder '{}'", folder_name),
                        passed: true,
                        detail: None,
                    },
                );
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Folder creation failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Poll until folder appears on node 0 (where we'll query for its ID)
        let folder_id = match poll_for_file_on_all_nodes(
            &client,
            &nodes[0..1],
            &api_key,
            None,
            &folder_name,
            PROPAGATION_TIMEOUT,
        )
        .await
        {
            Some(id) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("Folder '{}' created", folder_name),
                        passed: true,
                        detail: Some(format!("id: {}", id)),
                    },
                );
                id
            }
            None => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Folder not found after creation".to_string(),
                        passed: false,
                        detail: None,
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Step 7: Move file into folder via PATCH on node 2
        let move_node = if nodes.len() > 2 { 2 } else { 0 };

        match move_item(&client, &nodes[move_node], &api_key, &file_id, &folder_id).await {
            Ok(_) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("Move file into '{}' on node {}", folder_name, move_node),
                        passed: true,
                        detail: None,
                    },
                );
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Move failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 8: Poll until file appears in folder on ALL nodes
        match poll_for_file_on_all_nodes(
            &client,
            nodes,
            &api_key,
            Some(&folder_id),
            &renamed_filename,
            PROPAGATION_TIMEOUT,
        )
        .await
        {
            Some(_) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("File found in folder on all {} nodes", nodes.len()),
                        passed: true,
                        detail: None,
                    },
                );
            }
            None => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "File not found in folder on all nodes".to_string(),
                        passed: false,
                        detail: Some(format!(
                            "Did not appear within {}s",
                            PROPAGATION_TIMEOUT.as_secs()
                        )),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 9: Delete file via DELETE on node 0
        match delete_item(&client, &nodes[0], &api_key, &file_id).await {
            Ok(_) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Delete file on node 0".to_string(),
                        passed: true,
                        detail: None,
                    },
                );
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Delete failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 10: Poll until file is gone from ALL nodes
        if poll_until_file_deleted(
            &client,
            nodes,
            &api_key,
            Some(&folder_id),
            &renamed_filename,
            PROPAGATION_TIMEOUT,
        )
        .await
        {
            print_and_add_check(
                &mut result,
                Check {
                    name: format!("File deleted from all {} nodes", nodes.len()),
                    passed: true,
                    detail: None,
                },
            );
        } else {
            print_and_add_check(
                &mut result,
                Check {
                    name: "File still exists on some nodes after deletion".to_string(),
                    passed: false,
                    detail: Some(format!(
                        "Did not disappear within {}s",
                        PROPAGATION_TIMEOUT.as_secs()
                    )),
                },
            );
            result.duration = start.elapsed();
            return Ok(result);
        }

        result.duration = start.elapsed();
        result.details = format!(
            "DocumentProvider write APIs tested: upload, rename, move, delete across {} nodes",
            nodes.len()
        );

        Ok(result)
    }
}

// ============================================================================
// Polling Helpers
// ============================================================================

/// Poll until enumerate works (device token accepted) on all nodes
async fn poll_until_enumerate_works(
    client: &Client,
    nodes: &[NodeInfo],
    api_key: &str,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let mut all_ok = true;
        for node in nodes {
            match enumerate(client, node, api_key, None).await {
                Ok(_) => {}
                _ => {
                    all_ok = false;
                    break;
                }
            }
        }
        if all_ok {
            return true;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
    false
}

/// Poll until a file with the given name appears on all nodes, returning its ID
async fn poll_for_file_on_all_nodes(
    client: &Client,
    nodes: &[NodeInfo],
    api_key: &str,
    parent_id: Option<&str>,
    filename: &str,
    timeout: Duration,
) -> Option<String> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let mut ids: Vec<String> = Vec::new();
        let mut all_found = true;
        for node in nodes {
            match get_item_id(client, node, api_key, parent_id, filename).await {
                Ok(Some(id)) => ids.push(id),
                _ => {
                    all_found = false;
                    break;
                }
            }
        }
        if all_found && !ids.is_empty() {
            let first = &ids[0];
            if ids.iter().all(|id| id == first) {
                return Some(first.clone());
            }
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
    None
}

/// Poll until a file is deleted from all nodes
async fn poll_until_file_deleted(
    client: &Client,
    nodes: &[NodeInfo],
    api_key: &str,
    parent_id: Option<&str>,
    filename: &str,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let mut all_gone = true;
        for node in nodes {
            match get_item_id(client, node, api_key, parent_id, filename).await {
                Ok(None) => {}
                _ => {
                    all_gone = false;
                    break;
                }
            }
        }
        if all_gone {
            return true;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
    false
}

// ============================================================================
// API Helpers
// ============================================================================

/// Register a device on a node
async fn register_device(
    client: &Client,
    node: &NodeInfo,
    device_name: &str,
) -> Result<RegisterDeviceResponse> {
    let url = format!(
        "https://{}:{}/api/devices/register",
        node.ip_address, node.port
    );

    let response = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .header("Content-Type", "application/json")
        .json(&serde_json::json!({ "device_name": device_name }))
        .timeout(Duration::from_secs(10))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("Registration failed with status {}: {}", status, body);
    }

    Ok(response.json().await?)
}

/// Convert a parent ID to the format expected by the upload endpoint
fn format_parent_item_identifier(parent_id: &str) -> String {
    if parent_id == "root" {
        "NSFileProviderRootContainerItemIdentifier".to_string()
    } else {
        format!("item:{}", parent_id)
    }
}

/// Upload a file via DocumentProvider API
async fn upload_file(
    client: &Client,
    node: &NodeInfo,
    api_key: &str,
    parent_id: &str,
    filename: &str,
    content: &[u8],
) -> Result<()> {
    let url = format!(
        "https://{}:{}/api/integrations/documentprovider/upload",
        node.ip_address, node.port
    );

    let file_part = multipart::Part::bytes(content.to_vec())
        .file_name(filename.to_string())
        .mime_str("application/octet-stream")?;

    let form = multipart::Form::new()
        .text(
            "parent_item_identifier",
            format_parent_item_identifier(parent_id),
        )
        .part(format!("file_{}", content.len()), file_part);

    let response = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", api_key))
        .multipart(form)
        .timeout(Duration::from_secs(30))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("Upload failed with status {}: {}", status, body);
    }

    Ok(())
}

/// Create a folder via DocumentProvider API
async fn create_folder(
    client: &Client,
    node: &NodeInfo,
    api_key: &str,
    parent_id: &str,
    folder_name: &str,
) -> Result<()> {
    let url = format!(
        "https://{}:{}/api/integrations/documentprovider/upload",
        node.ip_address, node.port
    );

    let form = multipart::Form::new()
        .text(
            "parent_item_identifier",
            format_parent_item_identifier(parent_id),
        )
        .text("folder_name", folder_name.to_string());

    let response = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", api_key))
        .multipart(form)
        .timeout(Duration::from_secs(30))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("Folder creation failed with status {}: {}", status, body);
    }

    Ok(())
}

/// Enumerate items via DocumentProvider API
async fn enumerate(
    client: &Client,
    node: &NodeInfo,
    api_key: &str,
    parent_id: Option<&str>,
) -> Result<Vec<DocumentProviderItem>> {
    let mut url = format!(
        "https://{}:{}/api/integrations/documentprovider/enumerate",
        node.ip_address, node.port
    );

    if let Some(pid) = parent_id {
        url = format!("{}?parent_id={}", url, pid);
    }

    let response = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", api_key))
        .timeout(Duration::from_secs(10))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("Enumerate failed with status {}: {}", status, body);
    }

    let resp: EnumerateResponse = response.json().await?;
    Ok(resp.items)
}

/// Get item ID by name
async fn get_item_id(
    client: &Client,
    node: &NodeInfo,
    api_key: &str,
    parent_id: Option<&str>,
    name: &str,
) -> Result<Option<String>> {
    let items = enumerate(client, node, api_key, parent_id).await?;
    Ok(items
        .into_iter()
        .find(|item| item.name == name)
        .map(|item| item.id))
}

/// Rename an item via PATCH
async fn rename_item(
    client: &Client,
    node: &NodeInfo,
    api_key: &str,
    item_id: &str,
    new_name: &str,
) -> Result<()> {
    let url = format!(
        "https://{}:{}/api/integrations/documentprovider/item",
        node.ip_address, node.port
    );

    let response = client
        .patch(&url)
        .header("Authorization", format!("Bearer {}", api_key))
        .header("Content-Type", "application/json")
        .json(&serde_json::json!({
            "id": item_id,
            "name": new_name
        }))
        .timeout(Duration::from_secs(10))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("Rename failed with status {}: {}", status, body);
    }

    Ok(())
}

/// Move an item via PATCH
async fn move_item(
    client: &Client,
    node: &NodeInfo,
    api_key: &str,
    item_id: &str,
    new_parent_id: &str,
) -> Result<()> {
    let url = format!(
        "https://{}:{}/api/integrations/documentprovider/item",
        node.ip_address, node.port
    );

    let response = client
        .patch(&url)
        .header("Authorization", format!("Bearer {}", api_key))
        .header("Content-Type", "application/json")
        .json(&serde_json::json!({
            "id": item_id,
            "parentId": new_parent_id
        }))
        .timeout(Duration::from_secs(10))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("Move failed with status {}: {}", status, body);
    }

    Ok(())
}

/// Download an item with an optional Range header; returns
/// (status, content_range, accept_ranges, body).
async fn download_range(
    client: &Client,
    node: &NodeInfo,
    api_key: &str,
    item_id: &str,
    range: Option<&str>,
) -> Result<(reqwest::StatusCode, Option<String>, Option<String>, Vec<u8>)> {
    let url = format!(
        "https://{}:{}/api/integrations/documentprovider/download?id={}",
        node.ip_address, node.port, item_id
    );

    let mut request = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", api_key))
        .timeout(Duration::from_secs(30));
    if let Some(range) = range {
        request = request.header("Range", range);
    }
    let response = request.send().await?;

    let status = response.status();
    let header_str = |name: &str| {
        response
            .headers()
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    };
    let content_range = header_str("content-range");
    let accept_ranges = header_str("accept-ranges");
    let body = response.bytes().await?.to_vec();
    Ok((status, content_range, accept_ranges, body))
}

/// Assert the download endpoint's range contract against known content.
async fn check_ranged_downloads(
    client: &Client,
    node: &NodeInfo,
    api_key: &str,
    item_id: &str,
    content: &[u8],
) -> Result<()> {
    let len = content.len();

    // Full download: 200, advertises range support, exact bytes.
    let (status, _, accept_ranges, body) =
        download_range(client, node, api_key, item_id, None).await?;
    anyhow::ensure!(status == reqwest::StatusCode::OK, "full download: {status}");
    anyhow::ensure!(
        accept_ranges.as_deref() == Some("bytes"),
        "full download missing Accept-Ranges: bytes (got {accept_ranges:?})"
    );
    anyhow::ensure!(body == content, "full download bytes mismatch");

    // Bounded range: 206 with the exact slice and Content-Range.
    let (status, content_range, _, body) =
        download_range(client, node, api_key, item_id, Some("bytes=8-15")).await?;
    anyhow::ensure!(
        status == reqwest::StatusCode::PARTIAL_CONTENT,
        "bounded range: {status}"
    );
    anyhow::ensure!(
        content_range.as_deref() == Some(&format!("bytes 8-15/{len}")),
        "bounded range Content-Range mismatch (got {content_range:?})"
    );
    anyhow::ensure!(body == content[8..16], "bounded range bytes mismatch");

    // Open-ended range: 206 to EOF.
    let (status, content_range, _, body) =
        download_range(client, node, api_key, item_id, Some("bytes=8-")).await?;
    anyhow::ensure!(
        status == reqwest::StatusCode::PARTIAL_CONTENT,
        "open-ended range: {status}"
    );
    anyhow::ensure!(
        content_range.as_deref() == Some(&format!("bytes 8-{}/{len}", len - 1)),
        "open-ended Content-Range mismatch (got {content_range:?})"
    );
    anyhow::ensure!(body == content[8..], "open-ended range bytes mismatch");

    // Range starting at EOF: 416 with the total size, empty body.
    let (status, content_range, _, body) =
        download_range(client, node, api_key, item_id, Some(&format!("bytes={len}-"))).await?;
    anyhow::ensure!(
        status == reqwest::StatusCode::RANGE_NOT_SATISFIABLE,
        "EOF range: {status}"
    );
    anyhow::ensure!(
        content_range.as_deref() == Some(&format!("bytes */{len}")),
        "416 Content-Range mismatch (got {content_range:?})"
    );
    anyhow::ensure!(body.is_empty(), "416 body must be empty");

    Ok(())
}

/// Delete an item via DELETE
async fn delete_item(client: &Client, node: &NodeInfo, api_key: &str, item_id: &str) -> Result<()> {
    let url = format!(
        "https://{}:{}/api/integrations/documentprovider/item?id={}",
        node.ip_address, node.port, item_id
    );

    let response = client
        .delete(&url)
        .header("Authorization", format!("Bearer {}", api_key))
        .timeout(Duration::from_secs(10))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("Delete failed with status {}: {}", status, body);
    }

    Ok(())
}
