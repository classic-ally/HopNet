//! Ingress → mesh SHARED-library publish: the cycle that closes the iCloud
//! cutover gate. Two users on different nodes; alice's fabricated daemon
//! publishes an SPL-bound library into the mesh shared library, bob reads
//! it byte-exactly, and bob's OWN daemon then resolves the same iCloud
//! identities to adoptions instead of re-uploads — the library-scoped
//! fingerprint dedup this cycle exists for. Per-scope responsibility
//! parking is asserted e2e by seeding personal photos with no personal
//! claim in the same pass.

use anyhow::{Context, Result};
use reqwest::Client;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::NodeInfo;
use crate::tests::multi_user::create_user;
use crate::tests::photos_ingress_publish::{fetch_resource_with_retry, wait_for_ids};
use crate::tests::photos_shared_library::{
    create_library, get_user_id, login_with_retry, post_member_action, record,
    wait_for_library_entry,
};
use crate::tests::{
    Check, TestResult, TestScenario, get_max_view, print_and_add_check, wait_for_minimum_view,
};
use hopnet::dev_seed;

const SHARED_COUNT: u32 = 4;
const PERSONAL_COUNT: u32 = 2;
/// Personal seeds start here so shared (0..) and personal identities never
/// collide.
const PERSONAL_START: u32 = 100;

pub struct PhotosIngressShared;

fn base_url(node: &NodeInfo) -> String {
    format!("http://{}:{}", node.ip_address, node.port)
}

/// Run the e2e driver, tolerating scripted non-zero exits; returns
/// (exit code, stdout JSON). Exit 3 = SOME scope parked on responsibility
/// (healthy scopes still drained — the pass is scope-partitioned).
fn run_driver_status(data_dir: &Path, args: &[String]) -> Result<(i32, serde_json::Value)> {
    let repo_root = env!("CARGO_MANIFEST_DIR");
    let output = std::process::Command::new("cargo")
        .current_dir(repo_root)
        .args([
            "run",
            "--quiet",
            "--manifest-path",
            "crates/ingress-publisher/Cargo.toml",
            "--features",
            "e2e-bin",
            "--bin",
            "ingress-publish-e2e",
            "--",
            "--data-dir",
        ])
        .arg(data_dir)
        .args(args)
        .output()
        .context("spawn ingress-publish-e2e")?;
    let code = output.status.code().unwrap_or(-1);
    let report = serde_json::from_slice(&output.stdout).with_context(|| {
        format!(
            "driver output json (exit {code})\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        )
    })?;
    Ok((code, report))
}

async fn driver(data_dir: PathBuf, args: Vec<String>) -> Result<(i32, serde_json::Value)> {
    tokio::task::spawn_blocking(move || run_driver_status(&data_dir, &args))
        .await
        .context("driver task")?
}

/// (photo_id, cloud_id, [(resource type, blake3)]) rows of a driver report.
type ReportPhoto = (String, String, Vec<(String, String)>);

fn report_photos(report: &serde_json::Value) -> Result<Vec<ReportPhoto>> {
    report["photos"]
        .as_array()
        .context("photos array")?
        .iter()
        .map(|photo| {
            let id = photo["photo_id"].as_str().context("photo_id")?.to_string();
            let cloud = photo["cloud_id"].as_str().unwrap_or_default().to_string();
            let resources = photo["resources"]
                .as_array()
                .context("resources")?
                .iter()
                .map(|r| {
                    Ok((
                        r["type"].as_str().context("type")?.to_string(),
                        r["blake3"].as_str().context("blake3")?.to_string(),
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok((id, cloud, resources))
        })
        .collect()
}

/// Register a device token under `node`'s bearer; returns (device_id, api_key).
async fn register_device(client: &Client, node: &NodeInfo, name: &str) -> Result<(String, String)> {
    let response = client
        .post(format!("{}/api/devices/register", base_url(node)))
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .json(&serde_json::json!({ "device_name": name }))
        .send()
        .await?;
    anyhow::ensure!(
        response.status().is_success(),
        "register: {}",
        response.status()
    );
    let body: serde_json::Value = response.json().await?;
    let api_key = body["api_key"]
        .as_str()
        .context("api_key missing")?
        .to_string();
    let device_id = api_key
        .split('.')
        .next()
        .context("token shape")?
        .to_string();
    Ok((device_id, api_key))
}

/// POST /api/photos/ingress/claim with an optional shared-library scope.
async fn claim_scope(
    client: &Client,
    node: &NodeInfo,
    device_id: &str,
    library_id: Option<&str>,
) -> Result<reqwest::StatusCode> {
    let mut body = serde_json::json!({ "device_id": device_id });
    if let Some(lib) = library_id {
        body["library_id"] = serde_json::json!(lib);
    }
    let response = client
        .post(format!("{}/api/photos/ingress/claim", base_url(node)))
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .json(&body)
        .send()
        .await?;
    Ok(response.status())
}

impl TestScenario for PhotosIngressShared {
    fn name(&self) -> &'static str {
        "photos-ingress-shared"
    }

    fn description(&self) -> &'static str {
        "Ingress daemon publishes an SPL-bound library into a mesh shared library: cross-member visibility, byte parity, library-scoped fingerprint dedup (adoption), per-scope parking, eviction"
    }

    async fn run(
        &self,
        _mesh_id: u32,
        nodes: &[NodeInfo],
        _flags: &[String],
    ) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();
        let client = Client::new();

        println!("\nRunning shared-ingress publish checks:");

        if nodes.len() < 2 {
            print_and_add_check(
                &mut result,
                Check {
                    name: "Insufficient nodes".to_string(),
                    passed: false,
                    detail: Some(format!("Need >= 2 nodes, got {}", nodes.len())),
                },
            );
            result.duration = start.elapsed();
            return Ok(result);
        }

        macro_rules! step {
            ($name:expr, $outcome:expr) => {
                step!($name, $outcome, |_| None)
            };
            ($name:expr, $outcome:expr, $detail:expr) => {
                match record(&mut result, $name, $outcome, $detail) {
                    Some(value) => value,
                    None => {
                        result.duration = start.elapsed();
                        return Ok(result);
                    }
                }
            };
        }

        let node_a = &nodes[0];
        let node_b = &nodes[1 % nodes.len()];

        // ── Step 1: two users, mesh library, membership ─────────────────

        let base_view = step!(
            "Get initial consensus view",
            get_max_view(nodes).await,
            |view| Some(format!("view {view}"))
        );
        let alice_pass = step!("Create user 'alice'", create_user(&nodes[0], "alice").await);
        step!(
            "User 'alice' creation consensus",
            async {
                match wait_for_minimum_view(nodes, base_view + 1, Duration::from_secs(30)).await {
                    Ok(true) => Ok(()),
                    Ok(false) => anyhow::bail!("timeout"),
                    Err(e) => Err(e),
                }
            }
            .await
        );
        let bob_pass = step!("Create user 'bob'", create_user(&nodes[0], "bob").await);

        let alice = step!(
            format!("Alice logs in on node {}", node_a.node_id),
            login_with_retry(node_a, "alice", &alice_pass).await
        );
        let bob = step!(
            format!("Bob logs in on node {}", node_b.node_id),
            login_with_retry(node_b, "bob", &bob_pass).await
        );
        let bob_id = step!("Resolve bob's user id", get_user_id(&client, &bob).await);

        let library_id = step!(
            "Alice creates the mesh shared library",
            create_library(&client, &alice, "SPL Cutover").await,
            |id| Some(id.clone())
        );

        step!(
            "Alice invites bob",
            post_member_action(&client, &alice, &library_id, "invite", Some(bob_id)).await
        );
        step!(
            "Bob accepts",
            async {
                wait_for_library_entry(&client, &bob, &library_id, Duration::from_secs(30)).await?;
                post_member_action(&client, &bob, &library_id, "accept", None).await
            }
            .await
        );
        // The claim handler needs APPLIED membership on the claiming node —
        // poll until bob's entry reads "member".
        step!(
            "Bob's membership applies",
            async {
                let deadline = Instant::now() + Duration::from_secs(30);
                loop {
                    let entry =
                        wait_for_library_entry(&client, &bob, &library_id, Duration::from_secs(10))
                            .await?;
                    if entry["status"].as_str() == Some("member") {
                        return Ok(());
                    }
                    anyhow::ensure!(Instant::now() < deadline, "still '{}'", entry["status"]);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
            .await
        );

        // ── Step 2: alice's daemon — device, scoped claim, seed ─────────

        let (device_a, token_a) = step!(
            "Register alice's ingress device",
            register_device(&client, &alice, "Alice Ingress E2E").await
        );
        step!(
            "Alice claims the shared scope",
            async {
                let status = claim_scope(&client, &alice, &device_a, Some(&library_id)).await?;
                anyhow::ensure!(status.is_success(), "claim: {status}");
                Ok(())
            }
            .await
        );

        let dir_a = tempfile::tempdir().context("temp dir a")?;
        // Personal photos are seeded FIRST so the shared seed's report
        // (which lists the whole store) can be split by id set.
        let personal_report = step!(
            format!("Seed {PERSONAL_COUNT} personal photos (no personal claim)"),
            driver(
                dir_a.path().to_path_buf(),
                vec![
                    "seed".into(),
                    "--count".into(),
                    PERSONAL_COUNT.to_string(),
                    "--start".into(),
                    PERSONAL_START.to_string(),
                ],
            )
            .await
        );
        let personal_ids: Vec<String> = report_photos(&personal_report.1)?
            .into_iter()
            .map(|(id, _, _)| id)
            .collect();
        let shared_seed = step!(
            format!("Seed {SHARED_COUNT} shared photos (mesh-bound)"),
            driver(
                dir_a.path().to_path_buf(),
                vec![
                    "seed".into(),
                    "--count".into(),
                    SHARED_COUNT.to_string(),
                    "--mesh-library-id".into(),
                    library_id.clone(),
                ],
            )
            .await
        );
        let shared_photos: Vec<ReportPhoto> = report_photos(&shared_seed.1)?
            .into_iter()
            .filter(|(id, _, _)| !personal_ids.contains(id))
            .collect();
        anyhow::ensure!(
            shared_photos.len() == SHARED_COUNT as usize,
            "shared seed split"
        );

        // ── Step 3: publish — shared drains, personal parks, in ONE pass ─

        let publish_args = |token: &str| {
            vec![
                "publish".into(),
                "--node-url".into(),
                base_url(&nodes[0]),
                "--device-token".into(),
                token.to_string(),
            ]
        };
        step!(
            "Publish: shared scope drains while personal scope parks",
            async {
                let (code, report) =
                    driver(dir_a.path().to_path_buf(), publish_args(&token_a)).await?;
                anyhow::ensure!(
                    report["published"].as_u64() == Some(SHARED_COUNT as u64)
                        && report["parked_responsibility"].as_bool() == Some(true)
                        && code == 3,
                    "exit {code}: {report}"
                );
                anyhow::ensure!(
                    report["evicted_blobs"].as_u64().unwrap_or(0) > 0,
                    "published shared blobs must spool-evict: {report}"
                );
                Ok(())
            }
            .await,
            |_| Some(format!(
                "published={SHARED_COUNT}, personal parked, blobs evicted"
            ))
        );

        // ── Step 4: bob sees the photos, byte-exact, on HIS node ────────

        let shared_ids: Vec<String> = shared_photos.iter().map(|(id, _, _)| id.clone()).collect();
        step!(
            format!("Bob's gallery holds the {SHARED_COUNT} shared photos"),
            async {
                dev_seed::enable_sidecar(&client, &base_url(node_b), &bob.jwt_token).await?;
                let total =
                    wait_for_ids(&client, &bob, &shared_ids, Duration::from_secs(90)).await?;
                for personal in &personal_ids {
                    anyhow::ensure!(
                        !shared_ids.contains(personal),
                        "personal photo leaked into the shared set"
                    );
                }
                Ok(total)
            }
            .await,
            |total| Some(format!("{total} gallery rows"))
        );

        step!(
            "Bob reads original bytes byte-exactly",
            async {
                let mut verified = 0usize;
                for (photo_id, _, resources) in &shared_photos {
                    for (kind, expected_hash) in resources {
                        let bytes = fetch_resource_with_retry(
                            &client,
                            &bob,
                            photo_id,
                            kind,
                            Duration::from_secs(30),
                        )
                        .await?;
                        let hash = blake3::hash(&bytes).to_hex().to_string();
                        anyhow::ensure!(&hash == expected_hash, "{photo_id}/{kind}: hash mismatch");
                        verified += 1;
                    }
                }
                Ok(verified)
            }
            .await,
            |n| Some(format!("{n} resources byte-verified"))
        );

        // ── Step 5: idempotency across the reset probe ──────────────────

        step!(
            "Reset + republish resolves shared as already_published",
            async {
                driver(dir_a.path().to_path_buf(), vec!["reset-published".into()]).await?;
                let (code, report) =
                    driver(dir_a.path().to_path_buf(), publish_args(&token_a)).await?;
                anyhow::ensure!(
                    report["already_published"].as_u64() == Some(SHARED_COUNT as u64)
                        && report["published"].as_u64() == Some(0)
                        && code == 3,
                    "confirm-first not honored (exit {code}): {report}"
                );
                Ok(())
            }
            .await
        );

        // ── Step 6: bob's OWN daemon adopts instead of re-uploading ─────
        // Same iCloud identities (same --start indices), different member:
        // the library-scoped fingerprint key makes bob's resolve match
        // alice's committed photos — the cross-member dedup property.

        let (device_b, token_b) = step!(
            "Register bob's ingress device",
            register_device(&client, &bob, "Bob Ingress E2E").await
        );
        step!(
            "Bob claims the shared scope for his own device",
            async {
                let status = claim_scope(&client, &bob, &device_b, Some(&library_id)).await?;
                anyhow::ensure!(status.is_success(), "claim: {status}");
                Ok(())
            }
            .await
        );

        let dir_b = tempfile::tempdir().context("temp dir b")?;
        step!(
            "Seed bob's daemon with the same iCloud identities",
            driver(
                dir_b.path().to_path_buf(),
                vec![
                    "seed".into(),
                    "--count".into(),
                    SHARED_COUNT.to_string(),
                    "--mesh-library-id".into(),
                    library_id.clone(),
                ],
            )
            .await
        );
        step!(
            "Bob's publish adopts all photos (cross-member dedup)",
            async {
                let (code, report) =
                    driver(dir_b.path().to_path_buf(), publish_args(&token_b)).await?;
                anyhow::ensure!(
                    code == 0
                        && report["adopted"].as_u64() == Some(SHARED_COUNT as u64)
                        && report["published"].as_u64() == Some(0),
                    "exit {code}: {report}"
                );
                anyhow::ensure!(
                    report["evicted_blobs"].as_u64().unwrap_or(0) > 0,
                    "adopted photos must free bob's spool: {report}"
                );
                Ok(())
            }
            .await,
            |_| Some(format!(
                "adopted={SHARED_COUNT}, published=0, spool evicted"
            ))
        );

        // ── Step 7: non-member scoped claims are refused ────────────────

        step!(
            "Non-member scoped claim is refused (403)",
            async {
                let (device_n, _) =
                    register_device(&client, &nodes[0], "Outsider Ingress E2E").await?;
                let status = claim_scope(&client, &nodes[0], &device_n, Some(&library_id)).await?;
                anyhow::ensure!(
                    status == reqwest::StatusCode::FORBIDDEN,
                    "expected 403, got {status}"
                );
                Ok(())
            }
            .await
        );

        result.duration = start.elapsed();
        Ok(result)
    }
}
