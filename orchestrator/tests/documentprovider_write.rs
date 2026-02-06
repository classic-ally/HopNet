use anyhow::Result;
use reqwest::{Client, multipart};
use serde::Deserialize;
use std::time::{Duration, Instant};

use crate::tests::{Check, TestResult, TestScenario, print_and_add_check};
use crate::tests::{get_max_view, wait_for_minimum_view};
use crate::NodeInfo;

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

impl TestScenario for DocumentProviderWriteConsistency {
    fn name(&self) -> &'static str {
        "documentprovider-write-consistency"
    }

    fn description(&self) -> &'static str {
        "Test DocumentProvider upload, rename, move, and delete operations replicate across all nodes"
    }

    async fn run(&self, _mesh_id: u32, nodes: &[NodeInfo], _flags: &[String]) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();
        let client = Client::new();

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

        // Step 1: Get initial consensus view
        let mut current_view = match get_max_view(nodes).await {
            Ok(view) => {
                print_and_add_check(&mut result, Check {
                    name: format!("Initial consensus view: {}", view),
                    passed: true,
                    detail: None,
                });
                view
            }
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

        // Step 2: Register device on node 0
        let api_key = match register_device(&client, &nodes[0], &device_name).await {
            Ok(resp) => {
                print_and_add_check(&mut result, Check {
                    name: format!("Register device '{}'", device_name),
                    passed: true,
                    detail: Some(format!("device_id: {}", resp.device_id)),
                });
                resp.api_key
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Device registration failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Wait for device registration to propagate
        // Get actual view after operation - don't assume +1 increment
        current_view = get_max_view(nodes).await.unwrap_or(current_view + 1);
        if !wait_for_consensus(&mut result, nodes, current_view, "device registration").await {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // Step 3: Upload file via DocumentProvider API on node 0
        match upload_file(&client, &nodes[0], &api_key, "root", &test_filename, test_content.as_bytes()).await {
            Ok(_) => {
                print_and_add_check(&mut result, Check {
                    name: format!("Upload '{}' via DocumentProvider on node 0", test_filename),
                    passed: true,
                    detail: Some(format!("{} bytes", test_content.len())),
                });
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Upload failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Wait for upload to propagate
        current_view = get_max_view(nodes).await.unwrap_or(current_view + 1);
        if !wait_for_consensus(&mut result, nodes, current_view, "file upload").await {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // Step 4: Verify file appears on ALL nodes via enumerate
        let file_id = match verify_file_on_all_nodes(&client, nodes, &api_key, None, &test_filename, &mut result).await {
            Some(id) => id,
            None => {
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Step 5: Rename file via PATCH on node 1
        let renamed_filename = format!("renamed-{}.txt", timestamp);
        let rename_node = if nodes.len() > 1 { 1 } else { 0 };

        match rename_item(&client, &nodes[rename_node], &api_key, &file_id, &renamed_filename).await {
            Ok(_) => {
                print_and_add_check(&mut result, Check {
                    name: format!("Rename file to '{}' on node {}", renamed_filename, rename_node),
                    passed: true,
                    detail: None,
                });
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Rename failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Wait for rename to propagate
        current_view = get_max_view(nodes).await.unwrap_or(current_view + 1);
        if !wait_for_consensus(&mut result, nodes, current_view, "rename").await {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // Step 6: Verify rename on ALL nodes
        if verify_file_on_all_nodes(&client, nodes, &api_key, None, &renamed_filename, &mut result).await.is_none() {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // Step 7: Create folder for move test
        match create_folder(&client, &nodes[0], &api_key, "root", &folder_name).await {
            Ok(_) => {
                print_and_add_check(&mut result, Check {
                    name: format!("Create folder '{}'", folder_name),
                    passed: true,
                    detail: None,
                });
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Folder creation failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Wait for folder creation
        current_view = get_max_view(nodes).await.unwrap_or(current_view + 1);
        if !wait_for_consensus(&mut result, nodes, current_view, "folder creation").await {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // Get folder ID
        let folder_id = match get_item_id(&client, &nodes[0], &api_key, None, &folder_name).await {
            Ok(Some(id)) => id,
            Ok(None) => {
                print_and_add_check(&mut result, Check {
                    name: "Folder not found after creation".to_string(),
                    passed: false,
                    detail: None,
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Failed to get folder ID".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Step 8: Move file into folder via PATCH on node 2
        let move_node = if nodes.len() > 2 { 2 } else { 0 };

        match move_item(&client, &nodes[move_node], &api_key, &file_id, &folder_id).await {
            Ok(_) => {
                print_and_add_check(&mut result, Check {
                    name: format!("Move file into '{}' on node {}", folder_name, move_node),
                    passed: true,
                    detail: None,
                });
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Move failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Wait for move to propagate
        current_view = get_max_view(nodes).await.unwrap_or(current_view + 1);
        if !wait_for_consensus(&mut result, nodes, current_view, "move").await {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // Step 9: Verify file is now in folder on ALL nodes
        if verify_file_on_all_nodes(&client, nodes, &api_key, Some(&folder_id), &renamed_filename, &mut result).await.is_none() {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // Step 10: Delete file via DELETE on node 0
        match delete_item(&client, &nodes[0], &api_key, &file_id).await {
            Ok(_) => {
                print_and_add_check(&mut result, Check {
                    name: "Delete file on node 0".to_string(),
                    passed: true,
                    detail: None,
                });
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Delete failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Wait for deletion to propagate
        current_view = get_max_view(nodes).await.unwrap_or(current_view + 1);
        if !wait_for_consensus(&mut result, nodes, current_view, "deletion").await {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // Step 11: Verify file is gone from ALL nodes
        match verify_file_deleted_on_all_nodes(&client, nodes, &api_key, Some(&folder_id), &renamed_filename).await {
            Ok(true) => {
                print_and_add_check(&mut result, Check {
                    name: format!("File deleted from all {} nodes", nodes.len()),
                    passed: true,
                    detail: None,
                });
            }
            Ok(false) => {
                print_and_add_check(&mut result, Check {
                    name: "File still exists on some nodes after deletion".to_string(),
                    passed: false,
                    detail: None,
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
            Err(e) => {
                print_and_add_check(&mut result, Check {
                    name: "Deletion verification failed".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                result.duration = start.elapsed();
                return Ok(result);
            }
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
// Helper Functions
// ============================================================================

/// Wait for consensus and add check to result
async fn wait_for_consensus(result: &mut TestResult, nodes: &[NodeInfo], target_view: u64, operation: &str) -> bool {
    let timeout = Duration::from_secs(30);
    match wait_for_minimum_view(nodes, target_view, timeout).await {
        Ok(true) => {
            print_and_add_check(result, Check {
                name: format!("Consensus propagated after {} (view {})", operation, target_view),
                passed: true,
                detail: None,
            });
            true
        }
        Ok(false) => {
            print_and_add_check(result, Check {
                name: format!("Consensus timeout after {}", operation),
                passed: false,
                detail: Some(format!("Did not reach view {} within {}s", target_view, timeout.as_secs())),
            });
            false
        }
        Err(e) => {
            print_and_add_check(result, Check {
                name: format!("Consensus check failed after {}", operation),
                passed: false,
                detail: Some(e.to_string()),
            });
            false
        }
    }
}

/// Register a device on a node
async fn register_device(client: &Client, node: &NodeInfo, device_name: &str) -> Result<RegisterDeviceResponse> {
    let url = format!("http://{}:{}/devices/register", node.ip_address, node.port);

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
/// "root" -> NSFileProviderRootContainerItemIdentifier
/// UUID -> item:{uuid}
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
    let url = format!("http://{}:{}/integrations/documentprovider/upload", node.ip_address, node.port);

    let file_part = multipart::Part::bytes(content.to_vec())
        .file_name(filename.to_string())
        .mime_str("application/octet-stream")?;

    let form = multipart::Form::new()
        .text("parent_item_identifier", format_parent_item_identifier(parent_id))
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

/// Create a folder via DocumentProvider API (uses same upload endpoint with no file)
async fn create_folder(
    client: &Client,
    node: &NodeInfo,
    api_key: &str,
    parent_id: &str,
    folder_name: &str,
) -> Result<()> {
    let url = format!("http://{}:{}/integrations/documentprovider/upload", node.ip_address, node.port);

    let form = multipart::Form::new()
        .text("parent_item_identifier", format_parent_item_identifier(parent_id))
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
    let mut url = format!("http://{}:{}/integrations/documentprovider/enumerate", node.ip_address, node.port);

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
    tracing::debug!(
        "Looking for '{}' in {} items on node {}: {:?}",
        name, items.len(), node.port,
        items.iter().map(|i| &i.name).collect::<Vec<_>>()
    );
    Ok(items.into_iter().find(|item| item.name == name).map(|item| item.id))
}

/// Rename an item via PATCH
async fn rename_item(
    client: &Client,
    node: &NodeInfo,
    api_key: &str,
    item_id: &str,
    new_name: &str,
) -> Result<()> {
    let url = format!("http://{}:{}/integrations/documentprovider/item", node.ip_address, node.port);

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
    let url = format!("http://{}:{}/integrations/documentprovider/item", node.ip_address, node.port);

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

/// Delete an item via DELETE
async fn delete_item(
    client: &Client,
    node: &NodeInfo,
    api_key: &str,
    item_id: &str,
) -> Result<()> {
    let url = format!(
        "http://{}:{}/integrations/documentprovider/item?id={}",
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

/// Verify a file exists on all nodes and return its ID
async fn verify_file_on_all_nodes(
    client: &Client,
    nodes: &[NodeInfo],
    api_key: &str,
    parent_id: Option<&str>,
    filename: &str,
    result: &mut TestResult,
) -> Option<String> {
    let mut file_ids: Vec<String> = Vec::new();
    let mut all_found = true;

    for (i, node) in nodes.iter().enumerate() {
        match get_item_id(client, node, api_key, parent_id, filename).await {
            Ok(Some(id)) => {
                file_ids.push(id);
            }
            Ok(None) => {
                print_and_add_check(result, Check {
                    name: format!("File '{}' not found on node {}", filename, i),
                    passed: false,
                    detail: None,
                });
                all_found = false;
            }
            Err(e) => {
                print_and_add_check(result, Check {
                    name: format!("Enumerate failed on node {}", i),
                    passed: false,
                    detail: Some(e.to_string()),
                });
                all_found = false;
            }
        }
    }

    if !all_found {
        return None;
    }

    // Verify all nodes have the same ID (deterministic)
    let first_id = &file_ids[0];
    let all_same = file_ids.iter().all(|id| id == first_id);

    if all_same {
        print_and_add_check(result, Check {
            name: format!("File '{}' found on all {} nodes with same ID", filename, nodes.len()),
            passed: true,
            detail: Some(format!("id: {}", first_id)),
        });
        Some(first_id.clone())
    } else {
        print_and_add_check(result, Check {
            name: "File IDs differ across nodes".to_string(),
            passed: false,
            detail: Some(format!("IDs: {:?}", file_ids)),
        });
        None
    }
}

/// Verify a file is deleted from all nodes
async fn verify_file_deleted_on_all_nodes(
    client: &Client,
    nodes: &[NodeInfo],
    api_key: &str,
    parent_id: Option<&str>,
    filename: &str,
) -> Result<bool> {
    for node in nodes {
        match get_item_id(client, node, api_key, parent_id, filename).await? {
            Some(_) => return Ok(false), // File still exists
            None => continue,
        }
    }
    Ok(true) // File not found on any node
}
