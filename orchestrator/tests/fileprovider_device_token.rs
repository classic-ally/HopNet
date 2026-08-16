use anyhow::Result;
use reqwest::{Client, multipart};
use serde::Deserialize;
use std::time::{Duration, Instant};

use crate::NodeInfo;
use crate::tests::{Check, TestResult, TestScenario, print_and_add_check};

// ============================================================================
// Response Types (local to test file)
// ============================================================================

#[derive(Debug, Deserialize)]
struct RegisterDeviceResponse {
    device_id: String,
    api_key: String,
}

#[derive(Debug, Deserialize, Clone)]
struct FileProviderItem {
    identifier: String,
    filename: String,
    parent_item_identifier: String,
    item_type: String, // "File" or "Folder"
    file_size: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FPEnumerateResponse {
    items: Vec<FileProviderItem>,
    next_page: Option<String>,
    current_consensus_height: u64,
}

#[derive(Debug, Deserialize)]
struct FPChangesResponse {
    items: Vec<FileProviderItem>,
    deleted_identifiers: Vec<String>,
    current_consensus_height: u64,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct DocumentProviderItem {
    id: String,
    name: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DPEnumerateResponse {
    items: Vec<DocumentProviderItem>,
}

const PROPAGATION_TIMEOUT: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_secs(2);

// ============================================================================
// Test 1: Device Token Session Bootstrap
// ============================================================================

pub struct DeviceTokenSessionBootstrap;

impl TestScenario for DeviceTokenSessionBootstrap {
    fn name(&self) -> &'static str {
        "device-token-session-bootstrap"
    }

    fn description(&self) -> &'static str {
        "Verify device token can establish sessions and perform file operations on nodes where the user never logged in"
    }

    async fn run(
        &self,
        _mesh_id: u32,
        nodes: &[NodeInfo],
        _flags: &[String],
    ) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();
        let client = super::device_client();

        println!("\nRunning device token session bootstrap checks:");

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let device_name = format!("bootstrap-test-{}", timestamp);
        let test_filename = format!("bootstrap-{}.txt", timestamp);
        let test_content = format!("Device token session bootstrap test content {}", timestamp);

        // Step 1: Register device on node 0 (JWT-authenticated)
        let register_response = match register_device(&client, &nodes[0], &device_name).await {
            Ok(resp) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("Register device '{}' on node 0", device_name),
                        passed: true,
                        detail: Some(format!("device_id: {}", resp.device_id)),
                    },
                );
                resp
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

        // Step 2: Poll until device token is accepted on all nodes (via FP enumerate)
        if !poll_until_fp_enumerate_works(
            &client,
            nodes,
            &register_response.api_key,
            PROPAGATION_TIMEOUT,
        )
        .await
        {
            print_and_add_check(
                &mut result,
                Check {
                    name: "Device token propagation timeout".to_string(),
                    passed: false,
                    detail: Some(format!(
                        "Not all nodes accepted token within {}s",
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

        // Step 3: Upload file on node 2 (user never logged in here) via DocumentProvider
        let upload_node = if nodes.len() > 2 { 2 } else { 0 };
        match dp_upload_file(
            &client,
            &nodes[upload_node],
            &register_response.api_key,
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
                        name: format!(
                            "Upload '{}' on node {} via DocumentProvider",
                            test_filename, upload_node
                        ),
                        passed: true,
                        detail: Some(format!("{} bytes", test_content.len())),
                    },
                );
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "File upload failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 4: Poll until file appears on all nodes (via DP enumerate)
        let file_id = match poll_for_dp_item_on_all_nodes(
            &client,
            nodes,
            &register_response.api_key,
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
                            "File '{}' replicated to all {} nodes",
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
                        name: "File replication timeout".to_string(),
                        passed: false,
                        detail: Some(format!(
                            "File did not appear on all nodes within {}s",
                            PROPAGATION_TIMEOUT.as_secs()
                        )),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Step 5: Download file on node 1 (different node) via DocumentProvider
        let download_node = if nodes.len() > 1 { 1 } else { 0 };
        match dp_download_file(
            &client,
            &nodes[download_node],
            &register_response.api_key,
            &file_id,
        )
        .await
        {
            Ok(downloaded) => {
                let content_matches = downloaded == test_content.as_bytes();
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!(
                            "Download file from node {} and verify content",
                            download_node
                        ),
                        passed: content_matches,
                        detail: if content_matches {
                            Some(format!("{} bytes, content matches", downloaded.len()))
                        } else {
                            Some(format!(
                                "Content mismatch: expected {} bytes, got {}",
                                test_content.len(),
                                downloaded.len()
                            ))
                        },
                    },
                );
                if !content_matches {
                    result.duration = start.elapsed();
                    return Ok(result);
                }
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "File download failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 6: Revoke device token
        match revoke_device(&client, &nodes[0], &register_response.device_id).await {
            Ok(_) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Revoke device token on node 0".to_string(),
                        passed: true,
                        detail: Some(format!("device_id: {}", register_response.device_id)),
                    },
                );
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Device revocation failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 7: Poll until all nodes reject the token (401)
        if poll_until_token_rejected(
            &client,
            nodes,
            &register_response.api_key,
            PROPAGATION_TIMEOUT,
        )
        .await
        {
            print_and_add_check(
                &mut result,
                Check {
                    name: format!("Revoked token rejected by all {} nodes", nodes.len()),
                    passed: true,
                    detail: None,
                },
            );
        } else {
            print_and_add_check(
                &mut result,
                Check {
                    name: "Token revocation propagation timeout".to_string(),
                    passed: false,
                    detail: Some(format!(
                        "Not all nodes rejected token within {}s",
                        PROPAGATION_TIMEOUT.as_secs()
                    )),
                },
            );
        }

        result.duration = start.elapsed();
        result.details = format!(
            "Device token session bootstrap verified: registered, uploaded on node {}, downloaded on node {}, content matched, revoked",
            upload_node, download_node,
        );
        Ok(result)
    }
}

// ============================================================================
// Test 2: FileProvider Device Token Auth
// ============================================================================

pub struct FileProviderDeviceTokenAuth;

impl TestScenario for FileProviderDeviceTokenAuth {
    fn name(&self) -> &'static str {
        "fileprovider-device-token-auth"
    }

    fn description(&self) -> &'static str {
        "Verify FileProvider-specific endpoints work with device token authentication"
    }

    async fn run(
        &self,
        _mesh_id: u32,
        nodes: &[NodeInfo],
        _flags: &[String],
    ) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();
        let client = super::device_client();

        println!("\nRunning FileProvider device token auth checks:");

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let device_name = format!("fp-auth-test-{}", timestamp);
        let folder_name = format!("fp-test-folder-{}", timestamp);
        let test_filename = format!("fp-test-{}.txt", timestamp);
        let test_content = format!("FileProvider device token auth test content {}", timestamp);

        // Step 1: Register device and wait for propagation
        let register_response = match register_device(&client, &nodes[0], &device_name).await {
            Ok(resp) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("Register device '{}'", device_name),
                        passed: true,
                        detail: Some(format!("device_id: {}", resp.device_id)),
                    },
                );
                resp
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

        if !poll_until_fp_enumerate_works(
            &client,
            nodes,
            &register_response.api_key,
            PROPAGATION_TIMEOUT,
        )
        .await
        {
            print_and_add_check(
                &mut result,
                Check {
                    name: "Device token propagation timeout".to_string(),
                    passed: false,
                    detail: Some(format!(
                        "Not all nodes accepted token within {}s",
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

        // Step 2: Enumerate root — assert 200 (may or may not be empty depending on prior tests)
        match fp_enumerate(&client, &nodes[0], &register_response.api_key, None).await {
            Ok(resp) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Enumerate root via FileProvider".to_string(),
                        passed: true,
                        detail: Some(format!("{} items", resp.items.len())),
                    },
                );
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Root enumeration failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 3: Create folder via FileProvider API
        match fp_create_folder(
            &client,
            &nodes[0],
            &register_response.api_key,
            "NSFileProviderRootContainerItemIdentifier",
            &folder_name,
        )
        .await
        {
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

        // Step 4: Poll until folder appears on all nodes
        let folder_id = match poll_for_fp_item_on_all_nodes(
            &client,
            nodes,
            &register_response.api_key,
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
                        name: format!(
                            "Folder '{}' found on all {} nodes",
                            folder_name,
                            nodes.len()
                        ),
                        passed: true,
                        detail: Some(format!("identifier: {}", id)),
                    },
                );
                id
            }
            None => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Folder propagation timeout".to_string(),
                        passed: false,
                        detail: Some(format!(
                            "Folder did not appear on all nodes within {}s",
                            PROPAGATION_TIMEOUT.as_secs()
                        )),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Step 5: Create file with content in the folder
        match fp_create_file(
            &client,
            &nodes[0],
            &register_response.api_key,
            &folder_id,
            &test_filename,
            test_content.as_bytes(),
        )
        .await
        {
            Ok(_) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("Create file '{}' in folder", test_filename),
                        passed: true,
                        detail: Some(format!("{} bytes", test_content.len())),
                    },
                );
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "File creation failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 6: Poll until file appears on all nodes
        let file_identifier = match poll_for_fp_item_on_all_nodes(
            &client,
            nodes,
            &register_response.api_key,
            Some(&folder_id),
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
                            "File '{}' found on all {} nodes",
                            test_filename,
                            nodes.len()
                        ),
                        passed: true,
                        detail: Some(format!("identifier: {}", id)),
                    },
                );
                id
            }
            None => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "File propagation timeout".to_string(),
                        passed: false,
                        detail: Some(format!(
                            "File did not appear on all nodes within {}s",
                            PROPAGATION_TIMEOUT.as_secs()
                        )),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // Step 7: Download on a different node and verify content
        let download_node = if nodes.len() > 1 { 1 } else { 0 };
        match fp_download(
            &client,
            &nodes[download_node],
            &register_response.api_key,
            &file_identifier,
        )
        .await
        {
            Ok(downloaded) => {
                let content_matches = downloaded == test_content.as_bytes();
                print_and_add_check(
                    &mut result,
                    Check {
                        name: format!("Download file from node {} via FileProvider", download_node),
                        passed: content_matches,
                        detail: if content_matches {
                            Some(format!("{} bytes, content matches", downloaded.len()))
                        } else {
                            Some(format!(
                                "Content mismatch: expected {} bytes, got {}",
                                test_content.len(),
                                downloaded.len()
                            ))
                        },
                    },
                );
                if !content_matches {
                    result.duration = start.elapsed();
                    return Ok(result);
                }
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "File download failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        // Step 8: Check changes endpoint
        match fp_changes(&client, &nodes[0], &register_response.api_key, 0).await {
            Ok(resp) => {
                let has_folder = resp.items.iter().any(|i| i.filename == folder_name);
                let has_file = resp.items.iter().any(|i| i.filename == test_filename);
                let changes_ok = has_folder && has_file;
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Changes endpoint returns created items".to_string(),
                        passed: changes_ok,
                        detail: Some(format!(
                            "{} items returned, folder={}, file={}",
                            resp.items.len(),
                            has_folder,
                            has_file
                        )),
                    },
                );
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Changes endpoint failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
            }
        }

        // Step 9: Revoke device token and verify 401 on all nodes
        match revoke_device(&client, &nodes[0], &register_response.device_id).await {
            Ok(_) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Revoke device token".to_string(),
                        passed: true,
                        detail: None,
                    },
                );
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Device revocation failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        }

        if poll_until_token_rejected(
            &client,
            nodes,
            &register_response.api_key,
            PROPAGATION_TIMEOUT,
        )
        .await
        {
            print_and_add_check(
                &mut result,
                Check {
                    name: format!(
                        "All {} nodes reject revoked token on FP endpoints",
                        nodes.len()
                    ),
                    passed: true,
                    detail: None,
                },
            );
        } else {
            print_and_add_check(
                &mut result,
                Check {
                    name: "Token revocation propagation timeout".to_string(),
                    passed: false,
                    detail: Some(format!(
                        "Not all nodes rejected token within {}s",
                        PROPAGATION_TIMEOUT.as_secs()
                    )),
                },
            );
        }

        result.duration = start.elapsed();
        result.details = format!(
            "FileProvider device token auth verified: enumerate, create folder, create file, download, changes, revocation across {} nodes",
            nodes.len(),
        );
        Ok(result)
    }
}

// ============================================================================
// Device Registration Helpers
// ============================================================================

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

async fn revoke_device(client: &Client, node: &NodeInfo, device_id: &str) -> Result<()> {
    let url = format!(
        "https://{}:{}/api/devices/{}",
        node.ip_address, node.port, device_id
    );

    let response = client
        .delete(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .timeout(Duration::from_secs(10))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("Revocation failed with status {}: {}", status, body);
    }

    Ok(())
}

// ============================================================================
// FileProvider API Helpers
// ============================================================================

async fn fp_enumerate(
    client: &Client,
    node: &NodeInfo,
    api_key: &str,
    parent_item_identifier: Option<&str>,
) -> Result<FPEnumerateResponse> {
    let mut url = format!(
        "https://{}:{}/api/integrations/fileprovider/enumerate",
        node.ip_address, node.port
    );
    if let Some(pid) = parent_item_identifier {
        url = format!("{}?parent_item_identifier={}", url, pid);
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
        anyhow::bail!("FP enumerate failed with status {}: {}", status, body);
    }

    Ok(response.json().await?)
}

async fn fp_create_folder(
    client: &Client,
    node: &NodeInfo,
    api_key: &str,
    parent_id: &str,
    folder_name: &str,
) -> Result<()> {
    let url = format!(
        "https://{}:{}/api/integrations/fileprovider/create",
        node.ip_address, node.port
    );

    let form = multipart::Form::new()
        .text("parent_item_identifier", parent_id.to_string())
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
        anyhow::bail!("FP create folder failed with status {}: {}", status, body);
    }

    Ok(())
}

async fn fp_create_file(
    client: &Client,
    node: &NodeInfo,
    api_key: &str,
    parent_id: &str,
    filename: &str,
    content: &[u8],
) -> Result<()> {
    let url = format!(
        "https://{}:{}/api/integrations/fileprovider/create",
        node.ip_address, node.port
    );

    let file_part = multipart::Part::bytes(content.to_vec())
        .file_name(filename.to_string())
        .mime_str("application/octet-stream")?;

    let form = multipart::Form::new()
        .text("parent_item_identifier", parent_id.to_string())
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
        anyhow::bail!("FP create file failed with status {}: {}", status, body);
    }

    Ok(())
}

async fn fp_download(
    client: &Client,
    node: &NodeInfo,
    api_key: &str,
    identifier: &str,
) -> Result<Vec<u8>> {
    let url = format!(
        "https://{}:{}/api/integrations/fileprovider/download?identifier={}",
        node.ip_address, node.port, identifier
    );

    let response = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", api_key))
        .timeout(Duration::from_secs(30))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("FP download failed with status {}: {}", status, body);
    }

    Ok(response.bytes().await?.to_vec())
}

async fn fp_changes(
    client: &Client,
    node: &NodeInfo,
    api_key: &str,
    since_height: u64,
) -> Result<FPChangesResponse> {
    let url = format!(
        "https://{}:{}/api/integrations/fileprovider/changes?since_height={}",
        node.ip_address, node.port, since_height
    );

    let response = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", api_key))
        .timeout(Duration::from_secs(10))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("FP changes failed with status {}: {}", status, body);
    }

    Ok(response.json().await?)
}

// ============================================================================
// DocumentProvider API Helpers
// ============================================================================

fn format_parent_item_identifier(parent_id: &str) -> String {
    if parent_id == "root" {
        "NSFileProviderRootContainerItemIdentifier".to_string()
    } else {
        format!("item:{}", parent_id)
    }
}

async fn dp_upload_file(
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
        anyhow::bail!("DP upload failed with status {}: {}", status, body);
    }

    Ok(())
}

async fn dp_download_file(
    client: &Client,
    node: &NodeInfo,
    api_key: &str,
    file_id: &str,
) -> Result<Vec<u8>> {
    let url = format!(
        "https://{}:{}/api/integrations/documentprovider/download?id={}",
        node.ip_address, node.port, file_id
    );

    let response = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", api_key))
        .timeout(Duration::from_secs(30))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("DP download failed with status {}: {}", status, body);
    }

    Ok(response.bytes().await?.to_vec())
}

async fn dp_enumerate(
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
        anyhow::bail!("DP enumerate failed with status {}: {}", status, body);
    }

    let resp: DPEnumerateResponse = response.json().await?;
    Ok(resp.items)
}

// ============================================================================
// Polling Helpers
// ============================================================================

async fn poll_until_fp_enumerate_works(
    client: &Client,
    nodes: &[NodeInfo],
    api_key: &str,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let mut all_ok = true;
        for node in nodes {
            match fp_enumerate(client, node, api_key, None).await {
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

async fn poll_for_fp_item_on_all_nodes(
    client: &Client,
    nodes: &[NodeInfo],
    api_key: &str,
    parent_item_identifier: Option<&str>,
    filename: &str,
    timeout: Duration,
) -> Option<String> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let mut ids: Vec<String> = Vec::new();
        let mut all_found = true;
        for node in nodes {
            match fp_enumerate(client, node, api_key, parent_item_identifier).await {
                Ok(resp) => {
                    if let Some(item) = resp.items.iter().find(|i| i.filename == filename) {
                        ids.push(item.identifier.clone());
                    } else {
                        all_found = false;
                        break;
                    }
                }
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

async fn poll_for_dp_item_on_all_nodes(
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
            match dp_enumerate(client, node, api_key, parent_id).await {
                Ok(items) => {
                    if let Some(item) = items.iter().find(|i| i.name == filename) {
                        ids.push(item.id.clone());
                    } else {
                        all_found = false;
                        break;
                    }
                }
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

async fn poll_until_token_rejected(
    client: &Client,
    nodes: &[NodeInfo],
    api_key: &str,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let mut all_rejected = true;
        for node in nodes {
            let url = format!(
                "https://{}:{}/api/integrations/fileprovider/enumerate",
                node.ip_address, node.port
            );
            match client
                .get(&url)
                .header("Authorization", format!("Bearer {}", api_key))
                .timeout(Duration::from_secs(10))
                .send()
                .await
            {
                Ok(resp) if resp.status().as_u16() == 401 || resp.status().as_u16() == 403 => {}
                _ => {
                    all_rejected = false;
                    break;
                }
            }
        }
        if all_rejected {
            return true;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
    false
}
