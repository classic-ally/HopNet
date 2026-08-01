use anyhow::{Context, Result};
use reqwest::Client;
use std::collections::HashSet;
use std::str::FromStr;
use std::time::{Duration, Instant};

use crate::NodeInfo;
use crate::tests::multi_user::{create_user, fetch_state_snapshots, login_user, node_with_token};
use crate::tests::{
    Check, TestResult, TestScenario, get_max_view, print_and_add_check, wait_for_minimum_view,
};
use hopnet::dev_seed;
use hopnet_common::CustomUUID;
use hopnet_photos::envelopes::{PhotoDeleteEntry, PhotoDeletePayload};

const SEED: u64 = 77;
const MONTHS: u32 = 3;

/// Shared-library membership lifecycle end to end: alice creates a library
/// and ingests into it, bob's invite pre-stages access rows WITHOUT granting
/// visibility (consent boundary), accept backfills his sidecar, tombstoned
/// photos are never granted to a later invitee (carol), members write with
/// equal standing, and a kick revokes access and purges the ex-member's
/// sidecar. Reads run against different nodes per user — users are
/// mesh-wide, any node serves.
pub struct PhotosSharedLibrary;

fn base_url(node: &NodeInfo) -> String {
    format!("http://{}:{}", node.ip_address, node.port)
}

fn bearer(node: &NodeInfo) -> String {
    format!("Bearer {}", node.jwt_token)
}

// ============================================================================
// HTTP helpers
// ============================================================================

/// Login with retry: the Argon2id unwrap takes seconds per attempt and the
/// user row must have applied on this node first.
async fn login_with_retry(node: &NodeInfo, username: &str, passphrase: &str) -> Result<NodeInfo> {
    let deadline = Instant::now() + Duration::from_secs(45);
    loop {
        match login_user(node, username, passphrase).await {
            Ok(token) => return Ok(node_with_token(node, &token)),
            Err(e) if Instant::now() > deadline => return Err(e),
            Err(_) => tokio::time::sleep(Duration::from_secs(2)).await,
        }
    }
}

/// GET /users/me — resolve the caller's numeric user_id (invite/kick target).
async fn get_user_id(client: &Client, node: &NodeInfo) -> Result<i32> {
    let response = client
        .get(format!("{}/api/users/me", base_url(node)))
        .header("Authorization", bearer(node))
        .send()
        .await?;
    anyhow::ensure!(
        response.status().is_success(),
        "GET /users/me: {}",
        response.status()
    );
    let body: serde_json::Value = response.json().await?;
    Ok(body["user_id"].as_i64().context("user_id missing")? as i32)
}

/// POST /photos/libraries — create a shared library, returns library_id.
async fn create_library(client: &Client, node: &NodeInfo, name: &str) -> Result<String> {
    let response = client
        .post(format!("{}/api/photos/libraries", base_url(node)))
        .header("Authorization", bearer(node))
        .json(&serde_json::json!({ "name": name }))
        .send()
        .await?;
    let status = response.status();
    anyhow::ensure!(
        status == reqwest::StatusCode::CREATED,
        "create library: {} {}",
        status,
        response.text().await.unwrap_or_default()
    );
    let body: serde_json::Value = response.json().await?;
    body["library_id"]
        .as_str()
        .map(str::to_string)
        .context("library_id missing")
}

/// GET /photos/libraries — the caller's memberships and pending invites.
async fn list_libraries(client: &Client, node: &NodeInfo) -> Result<Vec<serde_json::Value>> {
    let response = client
        .get(format!("{}/api/photos/libraries", base_url(node)))
        .header("Authorization", bearer(node))
        .send()
        .await?;
    anyhow::ensure!(
        response.status().is_success(),
        "GET /photos/libraries: {}",
        response.status()
    );
    Ok(response.json().await?)
}

/// POST /photos/libraries/{id}/{action} with an optional {user_id} body.
/// Expects 200 (all lifecycle actions respond 200 on success).
async fn post_member_action(
    client: &Client,
    node: &NodeInfo,
    library_id: &str,
    action: &str,
    target: Option<i32>,
) -> Result<()> {
    let mut request = client
        .post(format!(
            "{}/api/photos/libraries/{}/{}",
            base_url(node),
            library_id,
            action
        ))
        .header("Authorization", bearer(node));
    if let Some(user_id) = target {
        request = request.json(&serde_json::json!({ "user_id": user_id }));
    }
    let response = request.send().await?;
    let status = response.status();
    anyhow::ensure!(
        status == reqwest::StatusCode::OK,
        "{action}: {} {}",
        status,
        response.text().await.unwrap_or_default()
    );
    Ok(())
}

/// Multipart ingest into a shared library — same wire shape as
/// `dev_seed::post_photo` plus the `library_id` query param.
async fn ingest_into_library(
    client: &Client,
    node: &NodeInfo,
    photo: &dev_seed::GeneratedPhoto,
    library_id: &str,
) -> Result<String> {
    let mut form =
        reqwest::multipart::Form::new().text("asset", serde_json::to_string(&photo.asset)?);
    for (kind, bytes) in &photo.resources {
        form = form.part(
            kind.as_str().to_string(),
            reqwest::multipart::Part::bytes(bytes.clone())
                .file_name(format!("{}.jpg", kind.as_str())),
        );
    }
    let response = client
        .post(format!("{}/api/photos", base_url(node)))
        .query(&[("library_id", library_id)])
        .header("Authorization", bearer(node))
        .multipart(form)
        .send()
        .await?;
    let status = response.status();
    anyhow::ensure!(
        status == reqwest::StatusCode::CREATED,
        "library ingest: {} {}",
        status,
        response.text().await.unwrap_or_default()
    );
    let body: serde_json::Value = response.json().await?;
    body["photo_id"]
        .as_str()
        .map(str::to_string)
        .context("ingest response missing photo_id")
}

/// Soft-delete one photo through the photos transaction route. No JSON wire
/// helper exists for delete — the route takes {tx_type, payload: bincode
/// bytes}, so encode the envelope here (same shape the GUI submits).
async fn soft_delete_photo(client: &Client, node: &NodeInfo, photo_id: &str) -> Result<()> {
    let payload = PhotoDeletePayload {
        entries: vec![PhotoDeleteEntry {
            photo_id: CustomUUID::from_str(photo_id).context("parse photo_id")?,
            // UUIDv7 — the handler derives deleted_at from this timestamp.
            operation_id: CustomUUID::new(None),
        }],
    };
    let encoded = bincode::serde::encode_to_vec(&payload, bincode::config::standard())?;
    let response = client
        .post(format!("{}/api/photos/transaction", base_url(node)))
        .header("Authorization", bearer(node))
        .json(&serde_json::json!({ "tx_type": "photo_delete", "payload": encoded }))
        .send()
        .await?;
    let status = response.status();
    anyhow::ensure!(
        status.is_success(),
        "photo_delete: {} {}",
        status,
        response.text().await.unwrap_or_default()
    );
    Ok(())
}

// ============================================================================
// Polling / assertion helpers
// ============================================================================

async fn gallery_photo_ids(client: &Client, node: &NodeInfo) -> Result<HashSet<String>> {
    let response = client
        .get(format!("{}/api/photos/gallery?limit=200", base_url(node)))
        .header("Authorization", bearer(node))
        .send()
        .await?;
    anyhow::ensure!(
        response.status().is_success(),
        "gallery: {}",
        response.status()
    );
    let rows: Vec<serde_json::Value> = response.json().await?;
    Ok(rows
        .iter()
        .filter_map(|row| row["photo_id"].as_str().map(str::to_string))
        .collect())
}

/// Poll the caller's gallery until every `required` id is present AND every
/// `forbidden` id is absent (sidecar backfill / purge both run on ticks).
async fn wait_for_gallery(
    client: &Client,
    node: &NodeInfo,
    required: &[&str],
    forbidden: &[&str],
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let outcome = gallery_photo_ids(client, node).await;
        if let Ok(ids) = &outcome {
            if required.iter().all(|id| ids.contains(*id))
                && forbidden.iter().all(|id| !ids.contains(*id))
            {
                return Ok(());
            }
        }
        if Instant::now() > deadline {
            match outcome {
                Ok(ids) => anyhow::bail!(
                    "gallery after timeout: {} rows, required {:?}, forbidden {:?}, got {:?}",
                    ids.len(),
                    required,
                    forbidden,
                    ids
                ),
                Err(e) => return Err(e),
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Single-shot absence assertion (no polling — the ids must not be there NOW).
async fn gallery_excludes(client: &Client, node: &NodeInfo, forbidden: &[&str]) -> Result<usize> {
    let ids = gallery_photo_ids(client, node).await?;
    let leaked: Vec<&&str> = forbidden.iter().filter(|id| ids.contains(**id)).collect();
    anyhow::ensure!(leaked.is_empty(), "gallery leaked {:?}", leaked);
    Ok(ids.len())
}

async fn fetch_resource(
    client: &Client,
    node: &NodeInfo,
    photo_id: &str,
    kind: &str,
) -> Result<(u16, Vec<u8>)> {
    let response = client
        .get(format!(
            "{}/api/photos/{}/resource/{}",
            base_url(node),
            photo_id,
            kind
        ))
        .header("Authorization", bearer(node))
        .send()
        .await?;
    let status = response.status().as_u16();
    let bytes = response.bytes().await.map(|b| b.to_vec()).unwrap_or_default();
    Ok((status, bytes))
}

/// Poll until the resource fetch returns `want` (revocation applies at
/// consensus decide; the reading node may lag the submitting node briefly).
async fn wait_for_resource_status(
    client: &Client,
    node: &NodeInfo,
    photo_id: &str,
    kind: &str,
    want: u16,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let (status, _) = fetch_resource(client, node, photo_id, kind).await?;
        if status == want {
            return Ok(());
        }
        if Instant::now() > deadline {
            anyhow::bail!("expected {want}, still {status} after timeout");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Fetch a resource until it returns 200 with the exact expected bytes;
/// non-200s retry (first fetches on a non-publishing node trigger remote
/// fragment discovery over iroh). A 200 with WRONG bytes fails immediately.
async fn fetch_matching_resource(
    client: &Client,
    node: &NodeInfo,
    photo_id: &str,
    kind: &str,
    expected: &[u8],
    timeout: Duration,
) -> Result<usize> {
    let deadline = Instant::now() + timeout;
    loop {
        let (status, bytes) = fetch_resource(client, node, photo_id, kind).await?;
        if status == 200 {
            anyhow::ensure!(
                bytes == expected,
                "{} bytes fetched != {} expected",
                bytes.len(),
                expected.len()
            );
            anyhow::ensure!(!bytes.is_empty(), "empty resource body");
            return Ok(bytes.len());
        }
        if Instant::now() > deadline {
            anyhow::bail!("resource {kind} of {photo_id}: still {status} after timeout");
        }
        tokio::time::sleep(Duration::from_millis(1500)).await;
    }
}

fn find_library<'a>(
    rows: &'a [serde_json::Value],
    library_id: &str,
) -> Option<&'a serde_json::Value> {
    rows.iter()
        .find(|row| row["library_id"].as_str() == Some(library_id))
}

/// Poll until the caller's library listing contains `library_id`.
async fn wait_for_library_entry(
    client: &Client,
    node: &NodeInfo,
    library_id: &str,
    timeout: Duration,
) -> Result<serde_json::Value> {
    let deadline = Instant::now() + timeout;
    loop {
        let rows = list_libraries(client, node).await?;
        if let Some(entry) = find_library(&rows, library_id) {
            return Ok(entry.clone());
        }
        if Instant::now() > deadline {
            anyhow::bail!("library not listed after timeout ({} rows)", rows.len());
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Poll until the caller's library listing no longer contains `library_id`.
async fn wait_for_library_absent(
    client: &Client,
    node: &NodeInfo,
    library_id: &str,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let rows = list_libraries(client, node).await?;
        if find_library(&rows, library_id).is_none() {
            return Ok(());
        }
        if Instant::now() > deadline {
            anyhow::bail!("library still listed after timeout");
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Invite entry contract: decrypted name, "invited" status, inviter id.
fn check_invite_entry(
    entry: &serde_json::Value,
    want_name: &str,
    want_inviter: i32,
) -> Result<String> {
    let name = entry["name"].as_str().unwrap_or("");
    let status = entry["status"].as_str().unwrap_or("");
    let invited_by = entry["invited_by"].as_i64();
    anyhow::ensure!(
        name == want_name && status == "invited" && invited_by == Some(want_inviter as i64),
        "name='{name}', status='{status}', invited_by={invited_by:?} (expected '{want_name}', 'invited', {want_inviter})"
    );
    Ok(format!(
        "name='{name}', status='{status}', invited_by={want_inviter}"
    ))
}

/// Record one named check; Some(value) on pass, None on fail.
fn record<T>(
    result: &mut TestResult,
    name: impl Into<String>,
    outcome: Result<T>,
    detail: impl FnOnce(&T) -> Option<String>,
) -> Option<T> {
    match outcome {
        Ok(value) => {
            let detail = detail(&value);
            print_and_add_check(
                result,
                Check {
                    name: name.into(),
                    passed: true,
                    detail,
                },
            );
            Some(value)
        }
        Err(e) => {
            print_and_add_check(
                result,
                Check {
                    name: name.into(),
                    passed: false,
                    detail: Some(e.to_string()),
                },
            );
            None
        }
    }
}

async fn wait_view(nodes: &[NodeInfo], target: u64) -> Result<()> {
    match wait_for_minimum_view(nodes, target, Duration::from_secs(30)).await {
        Ok(true) => Ok(()),
        Ok(false) => anyhow::bail!("timeout waiting for view >= {target}"),
        Err(e) => Err(e),
    }
}

// ============================================================================
// Test
// ============================================================================

impl TestScenario for PhotosSharedLibrary {
    fn name(&self) -> &'static str {
        "photos-shared-library"
    }

    fn description(&self) -> &'static str {
        "Shared-library lifecycle: create, invite/accept consent, pre-stage isolation, tombstone exclusion, equal-standing writes, kick revocation"
    }

    async fn run(&self, mesh_id: u32, nodes: &[NodeInfo], _flags: &[String]) -> Result<TestResult> {
        let start = Instant::now();
        let mut result = TestResult::new();
        let client = Client::new();

        println!("\nRunning shared-library lifecycle checks:");

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

        // Every fatal step goes through `record`; failure short-circuits.
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

        // Users read from DIFFERENT nodes throughout — cross-node consistency
        // rides along on every check.
        let node_a = &nodes[0];
        let node_b = &nodes[1 % nodes.len()];
        let node_c = &nodes[2 % nodes.len()];

        // ── Step 1: two users on the mesh ───────────────────────────────

        let base_view = step!(
            "Get initial consensus view",
            get_max_view(nodes).await,
            |view| Some(format!("view {view}"))
        );

        let alice_pass = step!("Create user 'alice'", create_user(&nodes[0], "alice").await);
        step!(
            "User 'alice' creation consensus",
            wait_view(nodes, base_view + 1).await
        );
        let bob_pass = step!("Create user 'bob'", create_user(&nodes[0], "bob").await);
        step!(
            "User 'bob' creation consensus",
            wait_view(nodes, base_view + 2).await
        );

        let alice = step!(
            format!("Alice logs in on node {}", node_a.node_id),
            login_with_retry(node_a, "alice", &alice_pass).await
        );
        let bob = step!(
            format!("Bob logs in on node {}", node_b.node_id),
            login_with_retry(node_b, "bob", &bob_pass).await
        );
        let alice_id = step!(
            "Resolve alice user_id",
            get_user_id(&client, &alice).await,
            |id| Some(format!("user_id {id}"))
        );
        let bob_id = step!(
            "Resolve bob user_id",
            get_user_id(&client, &bob).await,
            |id| Some(format!("user_id {id}"))
        );

        // ── Step 2: alice creates the library and ingests 2 photos ─────

        let library_id = step!(
            "Alice creates library 'Vacation'",
            create_library(&client, &alice, "Vacation").await,
            |id| Some(format!("library_id {id}"))
        );
        step!(
            "Alice enables sidecar",
            dev_seed::enable_sidecar(&client, &base_url(&alice), &alice.jwt_token).await
        );

        let photo1 = dev_seed::generate_photo(SEED, 0, MONTHS);
        let photo2 = dev_seed::generate_photo(SEED, 1, MONTHS);
        let photo1_id = step!(
            "Alice ingests photo 1 into library",
            ingest_into_library(&client, &alice, &photo1, &library_id).await,
            |id| Some(format!("photo_id {id}"))
        );
        let photo2_id = step!(
            "Alice ingests photo 2 into library",
            ingest_into_library(&client, &alice, &photo2, &library_id).await,
            |id| Some(format!("photo_id {id}"))
        );
        step!(
            "Alice gallery shows both library photos",
            wait_for_gallery(
                &client,
                &alice,
                &[&photo1_id, &photo2_id],
                &[],
                Duration::from_secs(45),
            )
            .await
        );

        // ── Step 3: invite bob; pre-staging must NOT grant visibility ──

        step!(
            "Alice invites bob",
            post_member_action(&client, &alice, &library_id, "invite", Some(bob_id)).await
        );
        let invite_entry = step!(
            "Bob's listing shows the invite",
            wait_for_library_entry(&client, &bob, &library_id, Duration::from_secs(15)).await
        );
        step!(
            "Invite carries decrypted name and inviter",
            check_invite_entry(&invite_entry, "Vacation", alice_id),
            |detail| Some(detail.clone())
        );
        step!(
            "Bob enables sidecar",
            dev_seed::enable_sidecar(&client, &base_url(&bob), &bob.jwt_token).await
        );

        // Let the invite-poked convergence worker pre-stage access rows —
        // the consent boundary must hold regardless of pre-staged wraps.
        tokio::time::sleep(Duration::from_secs(6)).await;
        step!(
            "Bob gallery hides library photos before accept",
            gallery_excludes(&client, &bob, &[&photo1_id, &photo2_id]).await,
            |count| Some(format!("{count} rows visible"))
        );
        step!(
            "Bob resource fetch is 403 before accept",
            async {
                let (status, _) = fetch_resource(&client, &bob, &photo1_id, "original").await?;
                anyhow::ensure!(status == 403, "expected 403, got {status}");
                Ok(())
            }
            .await
        );

        // ── Step 4: accept → sidecar backfill makes the library visible ─

        step!(
            "Bob accepts the invite",
            post_member_action(&client, &bob, &library_id, "accept", None).await
        );
        step!(
            "Bob gallery backfills both photos",
            wait_for_gallery(
                &client,
                &bob,
                &[&photo1_id, &photo2_id],
                &[],
                Duration::from_secs(60),
            )
            .await
        );
        step!(
            "Bob fetches byte-identical original of photo 1",
            fetch_matching_resource(
                &client,
                &bob,
                &photo1_id,
                "original",
                &photo1.resources[0].1,
                Duration::from_secs(30),
            )
            .await,
            |len| Some(format!("{len} bytes"))
        );

        // ── Step 5: tombstoned photos are never granted to new invitees ─

        step!(
            "Alice soft-deletes photo 1",
            soft_delete_photo(&client, &alice, &photo1_id).await
        );

        let pre_carol_view = step!(
            "Get consensus view before carol",
            get_max_view(nodes).await,
            |view| Some(format!("view {view}"))
        );
        let carol_pass = step!("Create user 'carol'", create_user(&nodes[0], "carol").await);
        step!(
            "User 'carol' creation consensus",
            wait_view(nodes, pre_carol_view + 1).await
        );
        let carol = step!(
            format!("Carol logs in on node {}", node_c.node_id),
            login_with_retry(node_c, "carol", &carol_pass).await
        );
        let carol_id = step!(
            "Resolve carol user_id",
            get_user_id(&client, &carol).await,
            |id| Some(format!("user_id {id}"))
        );
        step!(
            "Carol enables sidecar",
            dev_seed::enable_sidecar(&client, &base_url(&carol), &carol.jwt_token).await
        );
        step!(
            "Alice invites carol",
            post_member_action(&client, &alice, &library_id, "invite", Some(carol_id)).await
        );
        step!(
            "Carol's listing shows the invite",
            wait_for_library_entry(&client, &carol, &library_id, Duration::from_secs(15)).await,
            |_| None
        );
        step!(
            "Carol accepts the invite",
            post_member_action(&client, &carol, &library_id, "accept", None).await
        );
        step!(
            "Carol gallery shows only photo 2",
            wait_for_gallery(
                &client,
                &carol,
                &[&photo2_id],
                &[&photo1_id],
                Duration::from_secs(60),
            )
            .await
        );
        // Settle, then re-assert: the tombstoned photo must not trickle in
        // on a later backfill tick.
        tokio::time::sleep(Duration::from_secs(3)).await;
        step!(
            "Tombstoned photo 1 stays absent from carol's gallery",
            gallery_excludes(&client, &carol, &[&photo1_id]).await,
            |count| Some(format!("{count} rows visible"))
        );

        // ── Step 6: equal-standing member writes ────────────────────────

        let photo3 = dev_seed::generate_photo(SEED, 2, MONTHS);
        let photo3_id = step!(
            "Bob ingests photo 3 into library",
            ingest_into_library(&client, &bob, &photo3, &library_id).await,
            |id| Some(format!("photo_id {id}"))
        );
        step!(
            "Alice gallery shows bob's photo 3",
            wait_for_gallery(&client, &alice, &[&photo3_id], &[], Duration::from_secs(60)).await
        );

        // ── Step 7: kick revokes access and purges the sidecar ──────────

        step!(
            "Alice kicks bob",
            post_member_action(&client, &alice, &library_id, "leave", Some(bob_id)).await
        );
        step!(
            "Bob resource fetch is 403 after kick",
            wait_for_resource_status(
                &client,
                &bob,
                &photo2_id,
                "original",
                403,
                Duration::from_secs(10),
            )
            .await
        );
        step!(
            "Bob's listing drops the library",
            wait_for_library_absent(&client, &bob, &library_id, Duration::from_secs(10)).await
        );
        // Purge rides the sidecar's 30 s membership diff — allow three ticks.
        step!(
            "Bob's sidecar purges the library photos",
            wait_for_gallery(
                &client,
                &bob,
                &[],
                &[&photo1_id, &photo2_id, &photo3_id],
                Duration::from_secs(90),
            )
            .await
        );

        // ── Divergence ──────────────────────────────────────────────────

        match fetch_state_snapshots(nodes).await {
            Ok(snapshots) => match crate::divergence::build_divergence_report(mesh_id, snapshots) {
                Ok(report) => {
                    if report.is_full_consensus() {
                        print_and_add_check(
                            &mut result,
                            Check {
                                name: "Zero divergence".to_string(),
                                passed: true,
                                detail: Some(format!(
                                    "{} tables, views {}-{}",
                                    report.table_reports.len(),
                                    report.view_range.0,
                                    report.view_range.1,
                                )),
                            },
                        );
                    } else {
                        let divergent: Vec<_> = report
                            .divergent_tables()
                            .iter()
                            .map(|t| t.table_name.as_str())
                            .collect();
                        print_and_add_check(
                            &mut result,
                            Check {
                                name: "Divergence detected".to_string(),
                                passed: false,
                                detail: Some(format!("Divergent tables: {:?}", divergent)),
                            },
                        );
                    }
                }
                Err(e) => {
                    print_and_add_check(
                        &mut result,
                        Check {
                            name: "Divergence report failed".to_string(),
                            passed: false,
                            detail: Some(e.to_string()),
                        },
                    );
                }
            },
            Err(e) => {
                print_and_add_check(
                    &mut result,
                    Check {
                        name: "State snapshot fetch failed".to_string(),
                        passed: false,
                        detail: Some(e.to_string()),
                    },
                );
            }
        }

        result.duration = start.elapsed();
        result.details = format!(
            "Shared-library lifecycle across {} nodes: invite/accept consent boundary, tombstone exclusion, equal-standing writes, kick revocation",
            nodes.len()
        );
        Ok(result)
    }
}
