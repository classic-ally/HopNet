//! mount-cross-node-consistency (RFC-018): run the hopnet-mount FUSE
//! daemon INSIDE node 0's container and prove kernel IO converges across
//! the mesh — write through the mount, read via the other nodes' APIs;
//! write via a remote node's API, read through the mount (poke path);
//! delete remotely, watch it vanish from the mount. The containers get
//! /dev/fuse + CAP_SYS_ADMIN from the orchestrator for exactly this test.

use anyhow::{Context, Result};
use bollard::Docker;
use bollard::exec::{CreateExecOptions, StartExecResults};
use reqwest::Client;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tokio_stream::StreamExt;

use crate::NodeInfo;
use crate::tests::files::upload_file;
use crate::tests::{Check, TestResult, TestScenario, print_and_add_check};

use crate::naming::container_name;

pub struct MountCrossNodeConsistency;

const MOUNTPOINT: &str = "/hopdrive";
const MESH_WRITE: &str = "mesh-write.bin";
const API_WRITE: &str = "api-write.bin";

fn deterministic_bytes(len: usize, salt: u8) -> Vec<u8> {
    (0..len)
        .map(|i| ((i as u64 + salt as u64) % 251) as u8)
        .collect()
}

/// Run a command in the container via busybox sh, returning (exit_code, output).
async fn exec_capture(docker: &Docker, container: &str, script: &str) -> Result<(i64, String)> {
    let exec = docker
        .create_exec(
            container,
            CreateExecOptions {
                attach_stdout: Some(true),
                attach_stderr: Some(true),
                cmd: Some(vec![
                    "/bin/sh".to_string(),
                    "-c".to_string(),
                    script.to_string(),
                ]),
                ..Default::default()
            },
        )
        .await?;
    let mut collected = String::new();
    if let StartExecResults::Attached { mut output, .. } = docker.start_exec(&exec.id, None).await?
    {
        while let Some(chunk) = output.next().await {
            collected.push_str(&chunk?.to_string());
        }
    }
    let inspect = docker.inspect_exec(&exec.id).await?;
    Ok((inspect.exit_code.unwrap_or(-1), collected))
}

/// Launch the mount daemon (detached from the test's control flow; its
/// stderr is drained into `log` — the only diagnostics channel we have).
async fn launch_daemon(
    docker: &Docker,
    container: &str,
    api_key: &str,
    log: Arc<Mutex<String>>,
) -> Result<()> {
    let exec = docker
        .create_exec(
            container,
            CreateExecOptions {
                attach_stdout: Some(true),
                attach_stderr: Some(true),
                env: Some(vec![
                    format!("HOPNET_MOUNT_TOKEN={}", api_key),
                    "RUST_LOG=info".to_string(),
                    "HOME=/root".to_string(),
                    "PATH=/bin".to_string(),
                ]),
                cmd: Some(vec![
                    "/bin/hopnet-mount".to_string(),
                    "mount".to_string(),
                    MOUNTPOINT.to_string(),
                    "--url".to_string(),
                    "http://127.0.0.1:34630".to_string(),
                    "--cache-dir".to_string(),
                    "/tmp/hopnet-mount/cache".to_string(),
                    "--staging-dir".to_string(),
                    "/tmp/hopnet-mount/staging".to_string(),
                ]),
                ..Default::default()
            },
        )
        .await?;
    if let StartExecResults::Attached { mut output, .. } = docker.start_exec(&exec.id, None).await?
    {
        tokio::spawn(async move {
            while let Some(Ok(chunk)) = output.next().await {
                if let Ok(mut buf) = log.lock() {
                    buf.push_str(&chunk.to_string());
                }
            }
        });
    }
    Ok(())
}

/// Write `contents` at `dir/name` inside the container via Docker's tar
/// upload API — through the FUSE mount this is a real kernel write path
/// (create/write/release), no shell involved.
async fn upload_via_tar(
    docker: &Docker,
    container: &str,
    dir: &str,
    name: &str,
    contents: &[u8],
) -> Result<()> {
    let mut tar_builder = tar::Builder::new(Vec::new());
    let mut header = tar::Header::new_gnu();
    header.set_size(contents.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    tar_builder.append_data(&mut header, name, contents)?;
    let tar_bytes = tar_builder.into_inner()?;
    docker
        .upload_to_container(
            container,
            Some(
                bollard::query_parameters::UploadToContainerOptionsBuilder::new()
                    .path(dir)
                    .build(),
            ),
            bollard::body_full(bytes::Bytes::from(tar_bytes)),
        )
        .await
        .context("tar upload into container")?;
    Ok(())
}

/// Read one file back out of the container via the tar download API.
async fn download_via_tar(docker: &Docker, container: &str, path: &str) -> Result<Vec<u8>> {
    let stream = docker.download_from_container(
        container,
        Some(
            bollard::query_parameters::DownloadFromContainerOptionsBuilder::new()
                .path(path)
                .build(),
        ),
    );
    let mut tar_bytes = Vec::new();
    tokio::pin!(stream);
    while let Some(chunk) = stream.next().await {
        tar_bytes.extend_from_slice(&chunk?);
    }
    let mut archive = tar::Archive::new(&tar_bytes[..]);
    for entry in archive.entries()? {
        let mut entry = entry?;
        if entry.header().entry_type().is_file() {
            let mut content = Vec::new();
            std::io::Read::read_to_end(&mut entry, &mut content)?;
            return Ok(content);
        }
    }
    anyhow::bail!("no file entry in downloaded tar for {}", path)
}

/// Poll a node's mount API for a root-level file by name; returns the wire
/// item JSON once it resolves. `create` is a strict namespace op, so items
/// appear mesh-wide BEFORE their content upload lands — callers that need
/// bytes must keep polling until size/blob converge (see wait_for_content).
async fn wait_for_item(
    client: &Client,
    node: &NodeInfo,
    api_key: &str,
    name: &str,
    timeout: Duration,
) -> Result<serde_json::Value> {
    let url = format!(
        "https://{}:{}/api/integrations/mount/lookup",
        node.ip_address, node.port
    );
    let deadline = Instant::now() + timeout;
    loop {
        let resp = client
            .get(&url)
            .bearer_auth(api_key)
            .query(&[("name", name)])
            .send()
            .await;
        if let Ok(resp) = resp
            && resp.status().is_success()
        {
            return Ok(resp.json().await?);
        }
        if Instant::now() > deadline {
            anyhow::bail!("{} did not appear on node {} in time", name, node.node_id);
        }
        sleep(Duration::from_millis(500)).await;
    }
}

/// Poll until the named file's CONTENT is downloadable from `node` and
/// hash-matches — covers the async write-back window between the strict
/// namespace create and the content upload deciding.
async fn wait_for_content(
    client: &Client,
    node: &NodeInfo,
    api_key: &str,
    name: &str,
    expected: &[u8],
    timeout: Duration,
) -> Result<()> {
    let expected_hash = blake3::hash(expected);
    let deadline = Instant::now() + timeout;
    let mut last = String::from("item never resolved");
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            anyhow::bail!("{} on node {}: {}", name, node.node_id, last);
        }
        if let Ok(item) = wait_for_item(client, node, api_key, name, remaining).await {
            if item["size"].as_u64() == Some(expected.len() as u64) {
                match download_via_api(client, node, api_key, &item).await {
                    Ok(bytes) if blake3::hash(&bytes) == expected_hash => return Ok(()),
                    Ok(bytes) => last = format!("hash mismatch at {} bytes", bytes.len()),
                    Err(e) => last = e.to_string(),
                }
            } else {
                last = format!(
                    "size {:?}, content upload not decided yet",
                    item["size"].as_u64()
                );
            }
        }
        sleep(Duration::from_millis(500)).await;
    }
}

/// Download a file's full content via a node's mount API (blob-addressed,
/// same route the daemon uses).
async fn download_via_api(
    client: &Client,
    node: &NodeInfo,
    api_key: &str,
    item: &serde_json::Value,
) -> Result<Vec<u8>> {
    let size = item["size"].as_u64().context("item has no size")?;
    if size == 0 {
        return Ok(Vec::new());
    }
    let blob_id = item["blob_id"].as_str().context("item has no blob_id")?;
    let url = format!(
        "https://{}:{}/api/integrations/mount/download",
        node.ip_address, node.port
    );
    let resp = client
        .get(&url)
        .bearer_auth(api_key)
        .query(&[("blob_id", blob_id)])
        .header(reqwest::header::RANGE, format!("bytes=0-{}", size - 1))
        .send()
        .await?;
    anyhow::ensure!(
        resp.status().is_success(),
        "download from node {} returned {}",
        node.node_id,
        resp.status()
    );
    Ok(resp.bytes().await?.to_vec())
}

impl TestScenario for MountCrossNodeConsistency {
    fn name(&self) -> &'static str {
        "mount-cross-node-consistency"
    }

    fn description(&self) -> &'static str {
        "Mount the drive via FUSE inside node 0's container; kernel writes appear on all nodes, remote API writes and deletes appear through the mount"
    }

    async fn run(&self, mesh_id: u32, nodes: &[NodeInfo], _flags: &[String]) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();
        let client = crate::insecure_client();
        let docker = Docker::connect_with_local_defaults()?;
        let node0 = container_name(mesh_id, 0);
        anyhow::ensure!(nodes.len() >= 3, "needs a 3-node mesh");

        println!("\nRunning mount cross-node consistency checks:");

        // 1. Register a Mount device through the real registration route.
        let register_url = format!(
            "https://{}:{}/api/devices/register",
            nodes[0].ip_address, nodes[0].port
        );
        let api_key = match async {
            let resp = client
                .post(&register_url)
                .bearer_auth(&nodes[0].jwt_token)
                .json(&serde_json::json!({ "device_name": "orchestrator-mount" }))
                .send()
                .await?;
            anyhow::ensure!(
                resp.status().is_success(),
                "register returned {}",
                resp.status()
            );
            let body: serde_json::Value = resp.json().await?;
            body["api_key"]
                .as_str()
                .map(str::to_string)
                .context("no api_key in register response")
        }
        .await
        {
            Ok(key) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Register Mount device on node 0".to_string(),
                        passed: true,
                        detail: None,
                    },
                );
                key
            }
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "Register Mount device on node 0".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
                result.duration = start.elapsed();
                return Ok(result);
            }
        };

        // 2. Launch the daemon inside node 0's container.
        let daemon_log = Arc::new(Mutex::new(String::new()));
        if let Err(e) = launch_daemon(&docker, &node0, &api_key, daemon_log.clone()).await {
            print_and_add_check(
                &mut result,
                Check {
                    name: "Launch hopnet-mount in node 0's container".to_string(),
                    passed: false,
                    detail: Some(e.to_string()),
                },
            );
            result.duration = start.elapsed();
            return Ok(result);
        }

        // 3. Wait for the FUSE mount to appear in the container's namespace.
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut mounted = false;
        while Instant::now() < deadline {
            if let Ok((0, _)) = exec_capture(
                &docker,
                &node0,
                &format!("grep -q ' {} fuse' /proc/mounts", MOUNTPOINT),
            )
            .await
            {
                mounted = true;
                break;
            }
            sleep(Duration::from_millis(500)).await;
        }
        print_and_add_check(
            &mut result,
            Check {
                name: format!("FUSE mount live at {}", MOUNTPOINT),
                passed: mounted,
                detail: (!mounted).then(|| {
                    let log = daemon_log.lock().map(|l| l.clone()).unwrap_or_default();
                    format!(
                        "daemon output: {}",
                        log.chars().take(800).collect::<String>()
                    )
                }),
            },
        );
        if !mounted {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // 4. Kernel write through the mount (tar extraction = create/write/
        //    release through FUSE; strict upload lands it in consensus).
        let mesh_bytes = deterministic_bytes(1 << 20, 7);
        let write_ok = upload_via_tar(&docker, &node0, MOUNTPOINT, MESH_WRITE, &mesh_bytes)
            .await
            .map_err(|e| e.to_string());
        print_and_add_check(
            &mut result,
            Check {
                name: format!("Kernel write {} through the mount", MESH_WRITE),
                passed: write_ok.is_ok(),
                detail: write_ok.err(),
            },
        );
        if !result.passed {
            result.duration = start.elapsed();
            return Ok(result);
        }

        // 5. The kernel-written file is byte-identical via nodes 1 and 2's
        //    APIs (device tokens replicate; content needs remote fragments;
        //    the write-back upload is async behind the strict create).
        for node in &nodes[1..3] {
            let check = wait_for_content(
                &client,
                node,
                &api_key,
                MESH_WRITE,
                &mesh_bytes,
                Duration::from_secs(60),
            )
            .await;
            print_and_add_check(
                &mut result,
                Check {
                    name: format!(
                        "{} byte-identical via node {}'s API",
                        MESH_WRITE, node.node_id
                    ),
                    passed: check.is_ok(),
                    detail: check.err().map(|e| e.to_string()),
                },
            );
        }

        // 6. Remote API write on node 1 becomes readable THROUGH the mount
        //    on node 0 (SSE poke -> invalidation -> daemon read).
        let api_bytes = deterministic_bytes(512 * 1024, 31);
        let seeded = upload_file(&nodes[1], "/", API_WRITE, api_bytes.clone())
            .await
            .map_err(|e| e.to_string());
        if let Err(e) = &seeded {
            print_and_add_check(
                &mut result,
                Check {
                    name: format!("Seed {} via node 1's API", API_WRITE),
                    passed: false,
                    detail: Some(e.clone()),
                },
            );
        } else {
            let expected = blake3::hash(&api_bytes);
            let deadline = Instant::now() + Duration::from_secs(60);
            let mut read_back = false;
            let mut last_err = String::new();
            while Instant::now() < deadline {
                match download_via_tar(&docker, &node0, &format!("{}/{}", MOUNTPOINT, API_WRITE))
                    .await
                {
                    Ok(bytes) if blake3::hash(&bytes) == expected => {
                        read_back = true;
                        break;
                    }
                    Ok(bytes) => last_err = format!("stale content ({} bytes)", bytes.len()),
                    Err(e) => last_err = e.to_string(),
                }
                sleep(Duration::from_millis(500)).await;
            }
            print_and_add_check(
                &mut result,
                Check {
                    name: format!(
                        "{} readable through the mount after remote write",
                        API_WRITE
                    ),
                    passed: read_back,
                    detail: (!read_back).then_some(last_err),
                },
            );

            // 7. Remote delete via node 2's API vanishes from the mount.
            let deleted = async {
                let item = wait_for_item(
                    &client,
                    &nodes[2],
                    &api_key,
                    API_WRITE,
                    Duration::from_secs(30),
                )
                .await?;
                let id = item["id"].as_str().context("item has no id")?;
                let url = format!(
                    "https://{}:{}/api/integrations/mount/delete",
                    nodes[2].ip_address, nodes[2].port
                );
                let resp = client
                    .delete(&url)
                    .bearer_auth(&api_key)
                    .json(&serde_json::json!({ "id": id, "recursive": false }))
                    .send()
                    .await?;
                anyhow::ensure!(
                    resp.status().is_success(),
                    "delete returned {}",
                    resp.status()
                );
                Ok::<_, anyhow::Error>(())
            }
            .await;
            let mut vanished = false;
            if deleted.is_ok() {
                let deadline = Instant::now() + Duration::from_secs(60);
                while Instant::now() < deadline {
                    if let Ok((code, _)) = exec_capture(
                        &docker,
                        &node0,
                        &format!("test -e {}/{}", MOUNTPOINT, API_WRITE),
                    )
                    .await
                        && code != 0
                    {
                        vanished = true;
                        break;
                    }
                    sleep(Duration::from_millis(500)).await;
                }
            }
            print_and_add_check(
                &mut result,
                Check {
                    name: format!("{} gone from the mount after remote delete", API_WRITE),
                    passed: deleted.is_ok() && vanished,
                    detail: deleted.err().map(|e| e.to_string()),
                },
            );
        }

        // 8. Soft passthrough probe: report whether CAP_SYS_ADMIN armed the
        //    S9 path in-container. Informational — never fails the test.
        let log = daemon_log.lock().map(|l| l.clone()).unwrap_or_default();
        let passthrough_lines: Vec<&str> = log
            .lines()
            .filter(|l| l.to_lowercase().contains("passthrough"))
            .collect();
        println!(
            "  ℹ️  passthrough probe: {}",
            if passthrough_lines.is_empty() {
                "no passthrough lines in daemon output".to_string()
            } else {
                passthrough_lines.join(" | ")
            }
        );

        result.duration = start.elapsed();
        Ok(result)
    }
}
