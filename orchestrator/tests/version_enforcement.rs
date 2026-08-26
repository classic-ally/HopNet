use anyhow::{Context, Result};
use bollard::Docker;
use std::time::{Duration, Instant};
use tokio_stream::StreamExt;

use crate::tests::files::upload_file;
use crate::tests::{Check, NodeInfo, TestResult, TestScenario, print_and_add_check};

/// The mixed-version mesh (RFC-025 §Validation, orchestrator gates): one
/// node claims a different CalVer over the version seam, so its locked
/// family diverges while its compat generation stays in-window. The gate
/// proves the whole diagnosability story end to end: locked scopes
/// refused (the defuser SCREAM in the logs), compat scopes answering
/// (pongs keep refreshing), the two evidence clocks splitting
/// (visible-not-live), and VersionSkew named on the status views — in
/// BOTH directions.
///
/// Runs on the fast probe policy (probe_base=2;grace=1): the healthy
/// side's evidence of the skewed seat arrives ONLY through the prober
/// (consensus gossip has no defuser hook by design — sync and txforward
/// carry them), and the default cadence would not pong the seat for most
/// of a minute. The same policy arms vote-out against the
/// contact-starving seat — and the scenario ASSERTS that instead of
/// racing it: a version-skewed validator is pong-visible the whole time
/// yet still loses its seat, the S4 liveness/visibility split as
/// end-to-end behaviour (compat chatter shields nothing). Restore then
/// rides the auto-readmission machinery home. The AUTO profile (majority
/// at v=3) keeps the mesh deciding throughout.
pub struct MixedVersionMesh;

/// The version the skewed node claims — any valid CalVer that differs
/// from the build's.
const SKEW_VERSION: &str = "2026.9.0";

async fn get_json(node: &NodeInfo, path: &str) -> Result<serde_json::Value> {
    let resp = crate::call_node_api(node, path, true).await?;
    anyhow::ensure!(resp.status().is_success(), "{path} {}", resp.status());
    Ok(resp.json().await?)
}

/// Full stdout+stderr of a node's container — the first log-assertion
/// helper (the defuser scream is a log contract, RFC-025 S4).
pub(crate) async fn container_logs(docker: &Docker, mesh_id: u32, node_id: u32) -> Result<String> {
    let id = super::regenesis::find_container_id(docker, mesh_id, node_id).await?;
    let opts = bollard::query_parameters::LogsOptionsBuilder::new()
        .stdout(true)
        .stderr(true)
        .tail("5000")
        .build();
    let mut stream = docker.logs(&id, Some(opts));
    let mut out = String::new();
    while let Some(chunk) = stream.next().await {
        if let Ok(chunk) = chunk {
            out.push_str(&String::from_utf8_lossy(&chunk.into_bytes()));
        }
    }
    Ok(out)
}

/// The evidence row for `node_id` as seen by `observer`, or Null.
async fn evidence_row(observer: &NodeInfo, node_id: i64) -> Result<serde_json::Value> {
    let doc = get_json(observer, "/api/consensus/evidence").await?;
    Ok(doc["nodes"]
        .as_array()
        .and_then(|rows| rows.iter().find(|r| r["node_id"] == node_id).cloned())
        .unwrap_or(serde_json::Value::Null))
}

/// The prober's classify_pong scream — the healthy side's receipt (its
/// consensus paths carry no defuser hook, so the prober is what names
/// the skew there).
const PROBE_SCREAM: &str = "version skew: same epoch, different build";
/// The defuser's scream — an opportunistic extra receipt on the skewed
/// side (a boot-time sync dial racing ahead of the first pong gets
/// locked-refused through the defuser-hooked sync driver).
const DEFUSE_SCREAM: &str = "version skew: locked dial refused at the transport";

impl TestScenario for MixedVersionMesh {
    fn name(&self) -> &'static str {
        "mixed-version-mesh"
    }

    fn description(&self) -> &'static str {
        "Version-skewed node: locked refused, compat answering, VersionSkew named both ways"
    }

    async fn run(&self, mesh_id: u32, nodes: &[NodeInfo], _flags: &[String]) -> Result<TestResult> {
        let mut result = TestResult::new();
        anyhow::ensure!(nodes.len() == 3, "mixed-version-mesh expects a 3-node mesh");
        let docker = crate::sys::connect()?;

        println!("\nRunning mixed-version-mesh checks:");

        // 1. Baseline: nobody skewed, and the build's own version on the
        // summary.
        let base = get_json(&nodes[0], "/api/consensus/evidence").await?;
        let base_clean = base["nodes"]
            .as_array()
            .map(|rows| {
                rows.iter()
                    .all(|r| r["pong"].is_null() || r["pong"]["skew"] == false)
            })
            .unwrap_or(false);
        print_and_add_check(
            &mut result,
            Check {
                name: "Baseline: no skew anywhere".to_string(),
                passed: base_clean && base["summary"]["local_version"] == env!("CARGO_PKG_VERSION"),
                detail: None,
            },
        );

        // 2. The seam: node 2 comes back claiming SKEW_VERSION. Same
        // image, same volume — only the claimed identity moves, so its
        // locked ALPN diverges while compat/1 stays in-window.
        super::regenesis::recreate_node_with_env(
            &docker,
            mesh_id,
            2,
            None,
            &[("HOPNET_UPGRADE_VERSION_OVERRIDE", SKEW_VERSION)],
        )
        .await
        .context("recreate node 2 with the version override")?;
        let node2 = super::regenesis::reauth_node(&docker, mesh_id, &nodes[2])
            .await
            .context("reauth node 2")?;

        // 3. The mesh still decides — an upload through node 0 completes
        // on the majority pair, and its consensus traffic toward node 2
        // is exactly the locked dial the defuser classifies.
        let upload_ok = upload_file(
            &nodes[0],
            "/",
            "skew-window-probe.txt",
            b"decided without the skewed seat".to_vec(),
        )
        .await
        .is_ok();
        print_and_add_check(
            &mut result,
            Check {
                name: "Mesh decides while the skewed node is locked out".to_string(),
                passed: upload_ok,
                detail: None,
            },
        );

        // 4. The observer's view of the skewed peer: skew named with the
        // claimed version, the window NOT moved by a version override,
        // not stranded — and the two clocks split: sightings fresh
        // (compat pongs), contact starving (locked refused). Sampled
        // twice so the pong is provably REFRESHING, not a relic.
        let mut named = false;
        let mut clocks_split = false;
        let mut window_pinned = false;
        let mut pong_refreshing = false;
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline {
            let row = evidence_row(&nodes[0], 2).await?;
            let pong = &row["pong"];
            if pong["skew"] == true {
                named = pong["version"] == SKEW_VERSION && pong["stranded"] == false;
                window_pinned = pong["floor"] == 0 && pong["head"] == 1;
                let seen = row["seen_age_ms"].as_u64().unwrap_or(u64::MAX);
                let contact = row["age_ms"].as_u64().unwrap_or(0);
                clocks_split = seen < 10_000 && contact > seen;
                let first_age = pong["age_ms"].as_u64().unwrap_or(u64::MAX);
                tokio::time::sleep(Duration::from_secs(4)).await;
                let again = evidence_row(&nodes[0], 2).await?;
                let second_age = again["pong"]["age_ms"].as_u64().unwrap_or(u64::MAX);
                pong_refreshing = first_age < 6_000 && second_age < 6_000;
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "Skew named: claimed version, window unmoved, not stranded".to_string(),
                passed: named && window_pinned,
                detail: None,
            },
        );
        print_and_add_check(
            &mut result,
            Check {
                name: "Visible-not-live: sightings fresh, contact starving, pong refreshing"
                    .to_string(),
                passed: clocks_split && pong_refreshing,
                detail: None,
            },
        );

        // 5. The banner on the healthy side (live state — read inside the
        // window; its log scream is asserted after the restore, logs
        // persist).
        let view = get_json(&nodes[0], "/api/views/network-resilience").await?;
        let banner = view["consensus"]["version_skew"]
            .as_array()
            .map(|rows| {
                rows.iter()
                    .any(|r| r["node_id"] == 2 && r["version"] == SKEW_VERSION)
            })
            .unwrap_or(false)
            && view["consensus"]["stranded_peers"] == serde_json::json!([]);
        print_and_add_check(
            &mut result,
            Check {
                name: "Healthy side: VersionSkew banner names the seat".to_string(),
                passed: banner,
                detail: None,
            },
        );

        // 6. BOTH directions: the skewed node names the mesh as skewed
        // from its side — its local_version is the claim, every peer's
        // pong reads skewed, its banner fills, and a scream lands in its
        // log. The guaranteed scream is its own PROBER's (VersionSkew
        // outranks KickSync in classify_pong, so the prober names the
        // skew instead of kicking a sync); the defuser's appears too
        // whenever a boot-time sync dial races ahead of the first pong —
        // either receipt proves the side named it. The tip-advancing
        // upload keeps the traffic honest; polled, the prober needs a
        // round or two.
        let _ = upload_file(
            &nodes[0],
            "/",
            "skew-kick-sync.txt",
            b"advance the tip past the skewed node".to_vec(),
        )
        .await;
        let mut skewed_side = false;
        let mut skewed_detail = String::new();
        let deadline = Instant::now() + Duration::from_secs(25);
        while Instant::now() < deadline {
            let ev2 = get_json(&node2, "/api/consensus/evidence").await?;
            let claimed = ev2["summary"]["local_version"] == SKEW_VERSION;
            let peers_skewed = ev2["nodes"]
                .as_array()
                .map(|rows| {
                    let judged: Vec<_> = rows
                        .iter()
                        .filter(|r| r["self"] == false && r["pong"].is_object())
                        .collect();
                    !judged.is_empty() && judged.iter().all(|r| r["pong"]["skew"] == true)
                })
                .unwrap_or(false);
            let view2 = get_json(&node2, "/api/views/network-resilience").await?;
            let banner2 = view2["consensus"]["version_skew"]
                .as_array()
                .map(|rows| !rows.is_empty())
                .unwrap_or(false);
            let log2 = container_logs(&docker, mesh_id, 2).await?;
            let scream2 = log2.contains(PROBE_SCREAM) || log2.contains(DEFUSE_SCREAM);
            skewed_detail = format!(
                "claimed={claimed} peers_skewed={peers_skewed} banner={banner2} scream={scream2}"
            );
            if claimed && peers_skewed && banner2 && scream2 {
                skewed_side = true;
                break;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "Skewed side: peers named, banner filled, its defuser screams".to_string(),
                passed: skewed_side,
                detail: Some(skewed_detail),
            },
        );

        // 7. The S4 invariant as behaviour: the seat is pong-visible the
        // whole time, yet contact starvation still costs it the seat —
        // compat chatter shields nothing from vote-out.
        let client = crate::insecure_client();
        let voted_out = super::graceful_leave::wait_validator_count(
            &client,
            &nodes[..2],
            2,
            Some(2),
            Duration::from_secs(90),
        )
        .await
        .unwrap_or(false);
        print_and_add_check(
            &mut result,
            Check {
                name: "Visible-but-skewed seat is voted out (chatter shields nothing)".to_string(),
                passed: voted_out,
                detail: None,
            },
        );

        // 8. Restore: the node returns on its real identity and the
        // auto-readmission machinery re-seats it; the skew clears
        // everywhere.
        super::regenesis::recreate_node_with_env(&docker, mesh_id, 2, None, &[])
            .await
            .context("restore node 2")?;
        let node2 = super::regenesis::reauth_node(&docker, mesh_id, &node2)
            .await
            .context("reauth restored node 2")?;

        // The healthy side's prober scream — asserted post-restore (logs
        // persist; nothing here is racing anymore).
        let healthy_scream = container_logs(&docker, mesh_id, 0)
            .await?
            .contains(PROBE_SCREAM)
            || container_logs(&docker, mesh_id, 1)
                .await?
                .contains(PROBE_SCREAM);
        print_and_add_check(
            &mut result,
            Check {
                name: "Healthy side: the prober's skew scream is in the logs".to_string(),
                passed: healthy_scream,
                detail: None,
            },
        );

        let all_three = vec![nodes[0].clone(), nodes[1].clone(), node2.clone()];
        let readmitted = super::graceful_leave::wait_validator_count(
            &client,
            &all_three,
            3,
            None,
            Duration::from_secs(150),
        )
        .await
        .unwrap_or(false);
        let mut cleared = false;
        let deadline = Instant::now() + Duration::from_secs(60);
        while Instant::now() < deadline {
            let row = evidence_row(&nodes[0], 2).await?;
            let view = get_json(&nodes[0], "/api/views/network-resilience").await?;
            if row["pong"]["skew"] == false
                && view["consensus"]["version_skew"] == serde_json::json!([])
            {
                cleared = true;
                break;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "Restore: auto-readmission re-seats it and the skew clears".to_string(),
                passed: readmitted && cleared,
                detail: Some(format!("readmitted={readmitted} skew_cleared={cleared}")),
            },
        );

        Ok(result)
    }
}

/// The retired-generation dialer (RFC-025 §Validation, orchestrator
/// gates): a generation below the served window is TLS-accepted solely to
/// receive the structured `COMPAT_RETIRED` close naming the floor.
///
/// Two constructions make the tier reachable at all:
/// - the compiled head is 1 (floor 0, empty retired set), so the TARGET
///   runs with `HOPNET_UPGRADE_COMPAT_HEAD_OVERRIDE=2` — window [1,2],
///   generation 0 (the legacy literal) retired;
/// - the accept hook names unknown dialers `REJECT_UNKNOWN_NODE` BEFORE
///   the tier check, so the target is a FRESH, code-adopted,
///   never-registered container: pre-`setup_complete` the directory
///   answers is_known unconditionally, exposing the tier to a raw
///   dialer. (Against a registered mesh, a stranger still gets code 1 —
///   `iroh-reject-unknown` pins that.)
///
/// The raw dialer runs on the HOST and reaches the target through the
/// mesh relay's host-mapped port (rootless podman has no host->container
/// route).
pub struct RetiredDialer;

impl TestScenario for RetiredDialer {
    fn name(&self) -> &'static str {
        "retired-dialer"
    }

    fn description(&self) -> &'static str {
        "A below-window dialer receives the structured CompatRetired close naming the floor"
    }

    async fn run(&self, mesh_id: u32, nodes: &[NodeInfo], _flags: &[String]) -> Result<TestResult> {
        use hopnet_comms::alpn;
        use hopnet_comms::iroh::{self, Endpoint};

        let mut result = TestResult::new();
        anyhow::ensure!(!nodes.is_empty(), "retired-dialer expects a live mesh");
        let docker = crate::sys::connect()?;
        let runtime = crate::sys::detect_runtime(&docker).await?;

        println!("\nRunning retired-dialer checks:");

        // 1. The target: a fresh container whose head is raised to 2.
        // Set-around-create only — a leaked override would poison every
        // later mesh in this process.
        let target_id: u32 = nodes.len() as u32;
        let container_name = crate::naming::container_name(mesh_id, target_id);
        let network_name = crate::naming::network_name(mesh_id);
        unsafe { std::env::set_var("HOPNET_UPGRADE_COMPAT_HEAD_OVERRIDE", "2") };
        let created = crate::create_hopnet_container(
            &docker,
            mesh_id,
            target_id,
            &container_name,
            &network_name,
            runtime,
        )
        .await;
        unsafe { std::env::remove_var("HOPNET_UPGRADE_COMPAT_HEAD_OVERRIDE") };
        created?;
        tokio::time::sleep(Duration::from_secs(3)).await;

        // 2. The real mesh code: the target adopts OUR magic and starts
        // serving its (raised) families; setup_complete stays false — the
        // code flips nothing, only JoinInfo install does.
        let mesh_code = crate::get_mesh_code(&docker, mesh_id, runtime)
            .await?
            .ok_or_else(|| anyhow::anyhow!("mesh has no join-code channel (pre-RFC-025 image?)"))?;
        crate::submit_join_code(&docker, mesh_id, target_id, runtime, &mesh_code).await?;
        let magic = alpn::parse_mesh_code(&mesh_code)
            .ok_or_else(|| anyhow::anyhow!("unparseable mesh code {mesh_code}"))?;

        // 3. The target's pubkey from its pre-setup surface.
        let addresses = crate::get_external_addresses(&docker, mesh_id, runtime).await?;
        let (host, port) = addresses
            .iter()
            .find(|(id, _, _)| *id == target_id)
            .map(|(_, h, p)| (h.clone(), *p))
            .ok_or_else(|| anyhow::anyhow!("target address not found"))?;
        let setup_url = format!("https://{host}:{port}/api/setup");
        let pubkey_hex = crate::insecure_client()
            .get(&setup_url)
            .timeout(Duration::from_secs(10))
            .send()
            .await?
            .text()
            .await?
            .trim()
            .trim_matches('"')
            .to_string();
        anyhow::ensure!(pubkey_hex.len() == 64, "bad pubkey {pubkey_hex:?}");
        let pubkey_bytes: [u8; 32] = hex::decode(&pubkey_hex)?
            .try_into()
            .map_err(|_| anyhow::anyhow!("pubkey length"))?;
        let target_key = iroh::PublicKey::from_bytes(&pubkey_bytes)?;

        // 4. The raw dialer, relay-routed from the host.
        let relay_url: iroh::RelayUrl = crate::relay_host_url(&docker, mesh_id, runtime)
            .await?
            .parse()?;
        let secret_bytes: [u8; 32] = rand::random();
        let dialer = Endpoint::builder(iroh::endpoint::presets::Minimal)
            .relay_mode(iroh::RelayMode::Custom(iroh::RelayMap::from(
                relay_url.clone(),
            )))
            .secret_key(iroh::SecretKey::from_bytes(&secret_bytes))
            .bind()
            .await?;
        let addr = iroh::EndpointAddr::new(target_key).with_relay_url(relay_url);

        // 5. The money check: a generation-0 dial (the legacy literal —
        // exactly what a pre-enforcement binary offers) is TLS-accepted
        // and application-closed with REJECT_COMPAT_RETIRED, the reason
        // bytes naming the floor (1) and the target's version.
        let own_code = hopnet_common::version::parse_code(env!("CARGO_PKG_VERSION"))
            .ok_or_else(|| anyhow::anyhow!("unparseable build version"))?;
        let (retired_close, detail) = match tokio::time::timeout(
            Duration::from_secs(20),
            dialer.connect(addr.clone(), alpn::LEGACY_ALPN),
        )
        .await
        {
            Ok(Ok(conn)) => {
                match tokio::time::timeout(Duration::from_secs(10), conn.closed()).await {
                    Ok(iroh::endpoint::ConnectionError::ApplicationClosed(close)) => {
                        let code_ok = close.error_code
                            == iroh::endpoint::VarInt::from(alpn::REJECT_COMPAT_RETIRED);
                        let reason = alpn::parse_retired_reason(&close.reason);
                        (
                            code_ok && reason == Some((1, own_code)),
                            format!("close code {:?}, reason {reason:?}", close.error_code),
                        )
                    }
                    Ok(other) => (false, format!("closed without app code: {other}")),
                    Err(_) => (false, "accepted and never closed".to_string()),
                }
            }
            Ok(Err(e)) => (false, format!("connect failed: {e}")),
            Err(_) => (false, "connect timed out (relay unreachable?)".to_string()),
        };
        print_and_add_check(
            &mut result,
            Check {
                name: "Retired dial: structured CompatRetired names the floor".to_string(),
                passed: retired_close,
                detail: Some(detail),
            },
        );

        // 6. Control: the locked family at the target's exact identity is
        // SERVED — TLS completes and no hook close arrives. Only the
        // retired tier rejects.
        let locked = alpn::locked_alpn(&magic, own_code);
        let (served, detail) = match tokio::time::timeout(
            Duration::from_secs(20),
            dialer.connect(addr, locked.as_slice()),
        )
        .await
        {
            Ok(Ok(conn)) => {
                match tokio::time::timeout(Duration::from_secs(3), conn.closed()).await {
                    Ok(reason) => (false, format!("hook-closed: {reason}")),
                    Err(_) => (true, "TLS complete, connection stays open".to_string()),
                }
            }
            Ok(Err(e)) => (false, format!("connect failed: {e}")),
            Err(_) => (false, "connect timed out".to_string()),
        };
        print_and_add_check(
            &mut result,
            Check {
                name: "Control: the locked family is served, not rejected".to_string(),
                passed: served,
                detail: Some(detail),
            },
        );

        // 6b. The seeding clamp's never-joined arm (RFC-025
        // agreed-version), against real on-disk state: the fresh target
        // has NO agreed-version marker (code adoption is not joining),
        // so seed-guard allows any candidate — install-without-joining
        // stays newest-wins. Contrast: the joined coordinator holds.
        let id = super::regenesis::find_container_id(&docker, mesh_id, target_id).await?;
        // PID 1 IS the hopnet binary (store-path entrypoint, no PATH).
        const GUARD: &str = "export HOME=/root; \
             test ! -e /root/.local/share/hopnet/agreed-version && \
             \"$(readlink /proc/1/exe)\" seed-guard --candidate 2099.1.1; echo rc=$?";
        let (rc_fresh, out_fresh) = super::regenesis::exec_sh(&docker, &id, GUARD).await?;
        let fresh_allows = rc_fresh == 0 && out_fresh.contains("rc=0");
        let joined_id = super::regenesis::find_container_id(&docker, mesh_id, 0).await?;
        let (_, out_joined) = super::regenesis::exec_sh(
            &docker,
            &joined_id,
            "export HOME=/root; \
             \"$(readlink /proc/1/exe)\" seed-guard --candidate 2099.1.1; echo rc=$?",
        )
        .await?;
        let joined_holds = out_joined.contains("rc=3");
        print_and_add_check(
            &mut result,
            Check {
                name: "Seed guard: never-joined allows, a joined node holds".to_string(),
                passed: fresh_allows && joined_holds,
                detail: Some(format!(
                    "fresh: {:?}, joined: {:?}",
                    out_fresh.trim(),
                    out_joined.trim()
                )),
            },
        );

        // 7. Cleanup: the unregistered target must not linger into the
        // runner's divergence check.
        let _ = docker
            .stop_container(&id, None::<bollard::query_parameters::StopContainerOptions>)
            .await;
        let _ = docker
            .remove_container(
                &id,
                Some(
                    bollard::query_parameters::RemoveContainerOptionsBuilder::new()
                        .force(true)
                        .build(),
                ),
            )
            .await;
        for _ in 0..40 {
            if docker
                .inspect_container(
                    &container_name,
                    None::<bollard::query_parameters::InspectContainerOptions>,
                )
                .await
                .is_err()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        let _ = docker
            .remove_volume(
                &crate::naming::volume_name(mesh_id, target_id),
                None::<bollard::query_parameters::RemoveVolumeOptions>,
            )
            .await;

        Ok(result)
    }
}
