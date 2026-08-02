use anyhow::Result;
use bollard::Docker;
use reqwest::Client;
use std::time::{Duration, Instant};

use crate::tests::files::upload_file;
use crate::tests::multi_user::fetch_state_snapshots;
use crate::tests::persistence::{start_node, wait_for_node_ready};
use crate::tests::{Check, NodeInfo, TestResult, TestScenario, get_max_view, print_and_add_check};

/// Regenesis full cycle, S6 scope (RFC-019): freeze → abort round-trip →
/// freeze again → drain → seal → every node EXITS with the restart code →
/// containers restarted against their surviving volumes → boot gates +
/// import + genesis at H → the mesh resumes in epoch 2, decides new
/// writes at H+1 onward, and closes the rollback window.
pub struct RegenesisRestart;

/// Upgrade-target boundary (RFC-019 S6 awaiting-upgrade): the mesh claims
/// a staged 2026.8.1 (test-gated override), seals for it, and — running
/// 2026.8.0 — every node PARKS alive instead of exiting. Nodes are then
/// recreated one by one with the running-version override (the "binary
/// swap"): the first reaches epoch 2 but decides nothing (no quorum of
/// the carried set yet — the liveness gate), the rest complete it and the
/// mesh progresses.
pub struct RegenesisAwaitingUpgrade;

async fn post_json(
    node: &NodeInfo,
    path: &str,
    body: Option<serde_json::Value>,
) -> Result<(u16, String)> {
    let client = Client::new();
    let url = format!("http://{}:{}{}", node.ip_address, node.port, path);
    let mut req = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", node.jwt_token))
        .timeout(Duration::from_secs(70));
    if let Some(body) = body {
        req = req.json(&body);
    }
    let resp = req.send().await?;
    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap_or_default();
    Ok((status, text))
}

async fn get_json(node: &NodeInfo, path: &str) -> Result<serde_json::Value> {
    let resp = crate::call_node_api(node, path, true).await?;
    anyhow::ensure!(resp.status().is_success(), "{path} {}", resp.status());
    Ok(resp.json().await?)
}

async fn regenesis_status(node: &NodeInfo) -> Result<serde_json::Value> {
    get_json(node, "/api/views/regenesis-status").await
}

async fn regenesis_phase(node: &NodeInfo) -> Result<String> {
    Ok(regenesis_status(node).await?["phase"]
        .as_str()
        .unwrap_or("?")
        .to_string())
}

async fn decided_height(node: &NodeInfo) -> Result<u64> {
    let v = get_json(node, "/api/consensus").await?;
    Ok(v["last_decided_height"].as_u64().unwrap_or(0))
}

/// List/inspect calls can transiently FAIL TO PARSE under podman: a
/// container mid-shutdown reports state "stopping", which bollard's
/// typed enums do not know. Retry through those windows.
async fn find_container_id(docker: &Docker, mesh_id: u32, node_id: u32) -> Result<String> {
    let mut last: Option<anyhow::Error> = None;
    for _ in 0..20 {
        match docker
            .list_containers(Some(
                bollard::query_parameters::ListContainersOptionsBuilder::new()
                    .all(true)
                    .build(),
            ))
            .await
        {
            Ok(containers) => {
                for container in containers {
                    if let Some(labels) = &container.labels
                        && labels.get("hopnet.mesh_id") == Some(&mesh_id.to_string())
                        && labels.get("hopnet.node_id") == Some(&node_id.to_string())
                        && let Some(id) = &container.id
                    {
                        return Ok(id.clone());
                    }
                }
                anyhow::bail!("container for mesh {mesh_id} node {node_id} not found");
            }
            Err(e) => last = Some(e.into()),
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(last.unwrap_or_else(|| anyhow::anyhow!("container lookup kept failing")))
}

/// Wait until the node's container has EXITED and return its code — the
/// restart request's observable half (RFC-019 S6: exit 75 asks the
/// service manager for a fresh boot; the orchestrator IS the service
/// manager here). None = still running at the deadline.
pub async fn wait_for_exit_code(
    docker: &Docker,
    mesh_id: u32,
    node_id: u32,
    timeout: Duration,
) -> Result<Option<i64>> {
    let id = find_container_id(docker, mesh_id, node_id).await?;
    let start = Instant::now();
    loop {
        // Inspect errors are tolerated inside the deadline: podman
        // reports a transient "stopping" state bollard cannot parse —
        // exactly while the process is exiting, i.e. exactly when this
        // waits.
        if let Ok(info) = docker
            .inspect_container(
                &id,
                None::<bollard::query_parameters::InspectContainerOptions>,
            )
            .await
            && let Some(state) = info.state
            && state.status == Some(bollard::models::ContainerStateStatusEnum::EXITED)
        {
            return Ok(Some(state.exit_code.unwrap_or(-1)));
        }
        if start.elapsed() > timeout {
            return Ok(None); // still running
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

fn is_running(state: &Option<bollard::models::ContainerState>) -> bool {
    state
        .as_ref()
        .map(|s| s.status == Some(bollard::models::ContainerStateStatusEnum::RUNNING))
        .unwrap_or(false)
}

/// Stop + REMOVE the node's container and recreate it (same name, same
/// labels, same surviving data volume) with `extra_env` added to the
/// environment — the per-node "binary swap" primitive. Env is injected
/// through the orchestrator's process environment around the sequential
/// create, exactly how mesh_creation_env seeds mesh-wide values.
pub async fn recreate_node_with_env(
    docker: &Docker,
    mesh_id: u32,
    node_id: u32,
    extra_env: &[(&str, &str)],
) -> Result<()> {
    let runtime = crate::sys::detect_runtime(docker).await?;
    let id = find_container_id(docker, mesh_id, node_id).await?;
    let _ = docker
        .stop_container(&id, None::<bollard::query_parameters::StopContainerOptions>)
        .await; // may already be exited
    let mut removed = Ok(());
    for _ in 0..10 {
        removed = docker
            .remove_container(
                &id,
                None::<bollard::query_parameters::RemoveContainerOptions>,
            )
            .await;
        if removed.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    removed?;

    for (k, v) in extra_env {
        unsafe { std::env::set_var(k, v) };
    }
    let container_name = format!("hopnet-orchestrator-{mesh_id}-{node_id}");
    let network_name = format!("hopnet-orchestrator-{mesh_id}-0");
    let created =
        crate::create_hopnet_container(docker, &container_name, &network_name, runtime).await;
    for (k, _) in extra_env {
        unsafe { std::env::remove_var(k) };
    }
    created?;
    Ok(())
}

/// Re-resolve one node's address (containers change IP/port on recreate)
/// and mint a fresh JWT (the signing key rolls every boot).
async fn reauth_node(docker: &Docker, mesh_id: u32, node: &NodeInfo) -> Result<NodeInfo> {
    let runtime = crate::sys::detect_runtime(docker).await?;
    let addrs = crate::get_external_addresses(docker, mesh_id, runtime).await?;
    let (_, ip, port) = addrs
        .into_iter()
        .find(|(id, _, _)| *id == node.node_id)
        .ok_or_else(|| anyhow::anyhow!("node {} address not found", node.node_id))?;
    let mut fresh = NodeInfo {
        node_id: node.node_id,
        ip_address: ip,
        port: port as u32,
        jwt_token: String::new(),
    };
    if !wait_for_node_ready(&fresh, Duration::from_secs(90)).await? {
        anyhow::bail!("node {} never became ready", node.node_id);
    }
    fresh.jwt_token = crate::get_jwt_token(docker, mesh_id, node.node_id, runtime).await?;
    Ok(fresh)
}

/// Boot attestation gate + a start decided at the given target. Returns
/// the RUNNING version string the mesh attested.
async fn attest_and_freeze(
    result: &mut TestResult,
    nodes: &[NodeInfo],
    target_override: Option<&str>,
) -> Result<Option<String>> {
    // Every seated validator must show a committed running version — the
    // start precondition's input (S3 attestations).
    let mut running = None;
    for _ in 0..40 {
        let v = get_json(&nodes[0], "/api/views/upgrade-readiness").await?;
        let all_attested = v["mesh"]
            .as_array()
            .map(|m| !m.is_empty() && m.iter().all(|n| n["running"].is_string()))
            .unwrap_or(false);
        let staged_ok = match target_override {
            // Upgrade target: every node must also have STAGED it.
            Some(t) => v["mesh"]
                .as_array()
                .map(|m| m.iter().all(|n| n["staged"].as_str() == Some(t)))
                .unwrap_or(false),
            None => true,
        };
        if all_attested && staged_ok {
            running = v["running"].as_str().map(str::to_string);
            break;
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
    print_and_add_check(
        result,
        Check {
            name: "Every validator attested its versions (start precondition input)".to_string(),
            passed: running.is_some(),
            detail: running.clone(),
        },
    );
    let Some(running) = running else {
        return Ok(None);
    };

    let target = target_override.unwrap_or(&running).to_string();
    let (status, body) = post_json(
        &nodes[0],
        "/api/consensus/regenesis/start",
        Some(serde_json::json!({ "target_version": target })),
    )
    .await?;
    print_and_add_check(
        result,
        Check {
            name: format!("regenesis_start decided (target {target})"),
            passed: status == 200,
            detail: Some(format!("{status}: {body}")),
        },
    );
    if status != 200 {
        return Ok(None);
    }
    Ok(Some(running))
}

/// Poll until every node reports the sealed phase; returns the terminal
/// height from the status view.
async fn wait_sealed_everywhere(nodes: &[NodeInfo]) -> Result<Option<u64>> {
    for _ in 0..120 {
        let mut all = true;
        for node in nodes {
            if regenesis_phase(node).await.unwrap_or_default() != "sealed" {
                all = false;
                break;
            }
        }
        if all {
            let v = regenesis_status(&nodes[0]).await?;
            return Ok(v["seal_height"].as_str().and_then(|s| s.parse().ok()));
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    Ok(None)
}

impl TestScenario for RegenesisRestart {
    fn name(&self) -> &'static str {
        "regenesis-restart"
    }

    fn description(&self) -> &'static str {
        "Freeze/abort, seal, exit 75, restart into epoch 2, decide new heights (RFC-019 S6)"
    }

    async fn run(
        &self,
        mesh_id: u32,
        nodes: &[NodeInfo],
        _flags: &[String],
    ) -> Result<TestResult> {
        let mut result = TestResult::new();
        anyhow::ensure!(nodes.len() == 3, "regenesis-restart expects a 3-node mesh");
        let docker = Docker::connect_with_local_defaults()?;

        println!("\nRunning regenesis-restart checks:");

        // 1-2. Baseline traffic, attestations, same-version freeze.
        upload_file(&nodes[0], "/", "pre-freeze.txt", b"before the boundary".to_vec()).await?;
        let Some(running) = attest_and_freeze(&mut result, nodes, None).await? else {
            return Ok(result);
        };

        let phase = regenesis_phase(&nodes[1]).await.unwrap_or_default();
        print_and_add_check(
            &mut result,
            Check {
                name: "Committed phase is moratorium mesh-wide".to_string(),
                passed: phase == "moratorium",
                detail: Some(phase),
            },
        );

        // 3. The freeze is real at the client layer: a write 503s.
        let refused = match upload_file(
            &nodes[1],
            "/",
            "during-freeze.txt",
            b"must be refused".to_vec(),
        )
        .await
        {
            Ok(()) => Some("accepted (BUG)".to_string()),
            Err(e) if e.to_string().contains("503") => None,
            Err(e) => Some(format!("wrong refusal: {e}")),
        };
        print_and_add_check(
            &mut result,
            Check {
                name: "Client write refused with 503 during the moratorium".to_string(),
                passed: refused.is_none(),
                detail: refused,
            },
        );

        // 4. Abort round-trip: the window reopens losslessly.
        let (status, _) = post_json(&nodes[0], "/api/consensus/regenesis/abort", None).await?;
        let phase = regenesis_phase(&nodes[2]).await.unwrap_or_default();
        let reopened = status == 200 && phase == "normal";
        let tip_before = get_max_view(nodes).await.unwrap_or(0);
        upload_file(&nodes[1], "/", "post-abort.txt", b"writes work again".to_vec()).await?;
        let mut advanced = false;
        for _ in 0..20 {
            if get_max_view(nodes).await.unwrap_or(0) > tip_before {
                advanced = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "Abort reopens admission; writes and heights resume".to_string(),
                passed: reopened && advanced,
                detail: Some(format!("phase {phase}, heights advanced: {advanced}")),
            },
        );

        // 5+6. Freeze again, all the way through: drain → autonomous
        //    commit → seal → restart derivation → EXIT 75. The exit code
        //    IS the observable seal (an HTTP poll would race the grace
        //    window — the derivation only runs at the tail of the seal
        //    work, so 75 implies sealed).
        let tip_at_freeze = decided_height(&nodes[0]).await.unwrap_or(0);
        let (status, body) = post_json(
            &nodes[0],
            "/api/consensus/regenesis/start",
            Some(serde_json::json!({ "target_version": running })),
        )
        .await?;
        anyhow::ensure!(status == 200, "second start failed: {status} {body}");
        let mut exit_codes = Vec::new();
        for node in nodes {
            exit_codes.push(
                wait_for_exit_code(&docker, mesh_id, node.node_id, Duration::from_secs(300))
                    .await?,
            );
        }
        let all_exited_75 = exit_codes.iter().all(|c| *c == Some(75));
        print_and_add_check(
            &mut result,
            Check {
                name: "Drained moratorium seals on its own; every node exits with the restart code (75)"
                    .to_string(),
                passed: all_exited_75,
                detail: Some(format!("exit codes: {exit_codes:?}")),
            },
        );
        if !all_exited_75 {
            return Ok(result);
        }

        // 7. The service manager's half: start the containers against
        //    their surviving volumes; the boot transition crosses the
        //    boundary on the way up. The terminal height H is what every
        //    node resumes at (nothing new decides until we write).
        let mut fresh_nodes = Vec::new();
        for node in nodes {
            start_node(&docker, mesh_id, node.node_id).await?;
            fresh_nodes.push(reauth_node(&docker, mesh_id, node).await?);
        }
        let mut resumed_heights = Vec::new();
        for node in &fresh_nodes {
            resumed_heights.push(decided_height(node).await.unwrap_or(0));
        }
        let seal_height = resumed_heights[0];
        anyhow::ensure!(
            resumed_heights.iter().all(|h| *h == seal_height) && seal_height > tip_at_freeze,
            "nodes resumed at inconsistent heights: {resumed_heights:?} (tip at freeze {tip_at_freeze})"
        );
        let mut statuses = Vec::new();
        for node in &fresh_nodes {
            let v = regenesis_status(node).await?;
            statuses.push((
                v["phase"].as_str().unwrap_or("?").to_string(),
                v["epoch"].as_str().unwrap_or("?").to_string(),
                v["rollback_retained"].as_bool().unwrap_or(false),
            ));
        }
        let crossed = statuses
            .iter()
            .all(|(phase, epoch, retained)| phase == "normal" && epoch == "2" && *retained);
        print_and_add_check(
            &mut result,
            Check {
                name: "Every node booted into epoch 2 (normal phase, rollback retained)"
                    .to_string(),
                passed: crossed,
                detail: Some(format!("{statuses:?}")),
            },
        );

        // 8. Progression: a new write decides past H (heights continuous),
        //    and the rollback window closes at that first decide.
        upload_file(
            &fresh_nodes[0],
            "/",
            "epoch-2.txt",
            b"decided in the new epoch".to_vec(),
        )
        .await?;
        let mut progressed = false;
        for _ in 0..60 {
            let mut all = true;
            for node in &fresh_nodes {
                if decided_height(node).await.unwrap_or(0) <= seal_height {
                    all = false;
                    break;
                }
            }
            if all {
                progressed = true;
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        let mut rollback_closed = false;
        for _ in 0..30 {
            let mut all = true;
            for node in &fresh_nodes {
                let v = regenesis_status(node).await?;
                if v["rollback_retained"].as_bool().unwrap_or(true) {
                    all = false;
                    break;
                }
            }
            if all {
                rollback_closed = true;
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "Epoch 2 decides new writes past H; rollback window closes".to_string(),
                passed: progressed && rollback_closed,
                detail: Some(format!(
                    "progressed: {progressed}, rollback closed: {rollback_closed}"
                )),
            },
        );

        // 9. Coherence in the NEW epoch: hash-identical replicas at one
        //    height above H — strictly stronger than S5's sealed cluster.
        let snapshots = fetch_state_snapshots(&fresh_nodes).await?;
        let coherent = snapshots.windows(2).all(|w| {
            w[0].1.consensus_height == w[1].1.consensus_height
                && w[0].1.manifest.top_hash == w[1].1.manifest.top_hash
        }) && snapshots
            .first()
            .map(|(_, s)| s.consensus_height > seal_height)
            .unwrap_or(false);
        print_and_add_check(
            &mut result,
            Check {
                name: "Replicas hash-identical in epoch 2, above the boundary".to_string(),
                passed: coherent,
                detail: Some(format!(
                    "heights: {:?}",
                    snapshots
                        .iter()
                        .map(|(id, s)| (*id, s.consensus_height))
                        .collect::<Vec<_>>()
                )),
            },
        );

        Ok(result)
    }
}

impl TestScenario for RegenesisAwaitingUpgrade {
    fn name(&self) -> &'static str {
        "regenesis-awaiting-upgrade"
    }

    fn description(&self) -> &'static str {
        "Upgrade-target seal parks nodes alive; per-node 'binary swap' completes epoch 2 (RFC-019 S6)"
    }

    async fn run(
        &self,
        mesh_id: u32,
        nodes: &[NodeInfo],
        _flags: &[String],
    ) -> Result<TestResult> {
        let mut result = TestResult::new();
        anyhow::ensure!(
            nodes.len() == 3,
            "regenesis-awaiting-upgrade expects a 3-node mesh"
        );
        let docker = Docker::connect_with_local_defaults()?;
        const TARGET: &str = "2026.8.1";

        println!("\nRunning regenesis-awaiting-upgrade checks:");

        // 1-2. Attestations INCLUDING the staged claim, then an
        //      upgrade-target freeze; the drained mesh seals on its own.
        upload_file(&nodes[0], "/", "pre-upgrade.txt", b"epoch 1 data".to_vec()).await?;
        if attest_and_freeze(&mut result, nodes, Some(TARGET))
            .await?
            .is_none()
        {
            return Ok(result);
        }
        let seal_height = wait_sealed_everywhere(nodes).await?.unwrap_or(0);
        print_and_add_check(
            &mut result,
            Check {
                name: "Upgrade-target moratorium seals on its own".to_string(),
                passed: seal_height > 0,
                detail: Some(format!("seal_height {seal_height}")),
            },
        );
        if seal_height == 0 {
            return Ok(result);
        }

        // 3. PARKED ALIVE: running != target, so nobody exits — the
        //    process keeps serving status (and refusing writes) while it
        //    waits for its operator.
        tokio::time::sleep(Duration::from_secs(8)).await;
        let mut parked = Vec::new();
        for node in nodes {
            let id = find_container_id(&docker, mesh_id, node.node_id).await?;
            let info = docker
                .inspect_container(
                    &id,
                    None::<bollard::query_parameters::InspectContainerOptions>,
                )
                .await?;
            let v = regenesis_status(node).await?;
            parked.push((
                is_running(&info.state),
                v["awaiting_upgrade"].as_bool().unwrap_or(false),
                v["target_version"].as_str().unwrap_or("?").to_string(),
            ));
        }
        let all_parked = parked
            .iter()
            .all(|(alive, awaiting, target)| *alive && *awaiting && target == TARGET);
        let write_refused = matches!(
            upload_file(&nodes[0], "/", "while-parked.txt", b"never".to_vec()).await,
            Err(e) if e.to_string().contains("503")
        );
        print_and_add_check(
            &mut result,
            Check {
                name: "Sealed-for-upgrade nodes park ALIVE (status up, writes 503)".to_string(),
                passed: all_parked && write_refused,
                detail: Some(format!("{parked:?}, write refused: {write_refused}")),
            },
        );

        // 4. The first "binary swap": node 0 recreated with the running-
        //    version override crosses into epoch 2 — and decides NOTHING
        //    (the liveness gate: no quorum of the carried set has booted).
        recreate_node_with_env(
            &docker,
            mesh_id,
            nodes[0].node_id,
            &[("HOPNET_UPGRADE_VERSION_OVERRIDE", TARGET)],
        )
        .await?;
        let node0 = reauth_node(&docker, mesh_id, &nodes[0]).await?;
        let v = regenesis_status(&node0).await?;
        let crossed = v["phase"].as_str() == Some("normal")
            && v["epoch"].as_str() == Some("2")
            && v["running_version"].as_str() == Some(TARGET);
        let h_before = decided_height(&node0).await.unwrap_or(0);
        tokio::time::sleep(Duration::from_secs(6)).await;
        let h_after = decided_height(&node0).await.unwrap_or(0);
        let alone_and_stalled = h_before == seal_height && h_after == seal_height;
        print_and_add_check(
            &mut result,
            Check {
                name: "First upgraded node reaches epoch 2 but decides nothing alone".to_string(),
                passed: crossed && alone_and_stalled,
                detail: Some(format!(
                    "status crossed: {crossed}, decided {h_before}->{h_after} (H={seal_height})"
                )),
            },
        );

        // 5. Swap the rest; the carried set reaches quorum and the mesh
        //    progresses past H.
        let mut fresh_nodes = vec![node0];
        for node in &nodes[1..] {
            recreate_node_with_env(
                &docker,
                mesh_id,
                node.node_id,
                &[("HOPNET_UPGRADE_VERSION_OVERRIDE", TARGET)],
            )
            .await?;
            fresh_nodes.push(reauth_node(&docker, mesh_id, node).await?);
        }
        upload_file(
            &fresh_nodes[1],
            "/",
            "epoch-2-upgrade.txt",
            b"decided by the upgraded mesh".to_vec(),
        )
        .await?;
        let mut progressed = false;
        for _ in 0..60 {
            let mut all = true;
            for node in &fresh_nodes {
                if decided_height(node).await.unwrap_or(0) <= seal_height {
                    all = false;
                    break;
                }
            }
            if all {
                progressed = true;
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        let epochs: Vec<String> = {
            let mut e = Vec::new();
            for node in &fresh_nodes {
                e.push(
                    regenesis_status(node).await?["epoch"]
                        .as_str()
                        .unwrap_or("?")
                        .to_string(),
                );
            }
            e
        };
        print_and_add_check(
            &mut result,
            Check {
                name: "Upgraded quorum completes epoch 2 and decides past H".to_string(),
                passed: progressed && epochs.iter().all(|e| e == "2"),
                detail: Some(format!("progressed: {progressed}, epochs: {epochs:?}")),
            },
        );

        Ok(result)
    }
}

/// Straggler rejoin (RFC-019 S7): one node is stopped BEFORE the freeze
/// and stays down through the whole boundary. When it comes back it is
/// alone on a sealed epoch with peers that refuse its history — it must
/// discover that, fetch and verify the new epoch from a peer, stage it,
/// exit 75 on its own, and come back as a full member of epoch 2.
pub struct StragglerRejoin;

/// Diverged-node rebuild (RFC-019 S7): a node whose local state cannot
/// produce the certified artifact fails its own import gate and parks.
/// It must then rebuild from peers rather than fail that same local gate
/// on every boot.
pub struct DivergedNodeRebuild;

/// Wait until the node reports the given epoch in its status view.
async fn wait_for_epoch(node: &NodeInfo, epoch: &str, timeout: Duration) -> Result<bool> {
    let start = Instant::now();
    loop {
        if let Ok(v) = regenesis_status(node).await
            && v["epoch"].as_str() == Some(epoch)
        {
            return Ok(true);
        }
        if start.elapsed() > timeout {
            return Ok(false);
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Drive the mesh across a same-version boundary and bring every listed
/// node back up in epoch 2. Returns the terminal height H and the
/// re-authenticated nodes.
async fn cross_boundary(
    result: &mut TestResult,
    docker: &Docker,
    mesh_id: u32,
    participants: &[NodeInfo],
) -> Result<Option<(u64, Vec<NodeInfo>)>> {
    let Some(_running) = attest_and_freeze(result, participants, None).await? else {
        return Ok(None);
    };
    let mut exit_codes = Vec::new();
    for node in participants {
        exit_codes
            .push(wait_for_exit_code(docker, mesh_id, node.node_id, Duration::from_secs(300)).await?);
    }
    let all_exited = exit_codes.iter().all(|c| *c == Some(75));
    print_and_add_check(
        result,
        Check {
            name: "Participating nodes seal and exit with the restart code".to_string(),
            passed: all_exited,
            detail: Some(format!("exit codes: {exit_codes:?}")),
        },
    );
    if !all_exited {
        return Ok(None);
    }

    let mut fresh = Vec::new();
    for node in participants {
        start_node(docker, mesh_id, node.node_id).await?;
        fresh.push(reauth_node(docker, mesh_id, node).await?);
    }
    let h = decided_height(&fresh[0]).await.unwrap_or(0);
    let mut epochs = Vec::new();
    for node in &fresh {
        epochs.push(
            regenesis_status(node).await?["epoch"]
                .as_str()
                .unwrap_or("?")
                .to_string(),
        );
    }
    print_and_add_check(
        result,
        Check {
            name: "The remaining mesh crossed into epoch 2".to_string(),
            passed: epochs.iter().all(|e| e == "2"),
            detail: Some(format!("epochs: {epochs:?}, H={h}")),
        },
    );
    Ok(Some((h, fresh)))
}

impl TestScenario for StragglerRejoin {
    fn name(&self) -> &'static str {
        "straggler-rejoin"
    }

    fn description(&self) -> &'static str {
        "A node offline through a regenesis discovers, fetches, and rejoins epoch 2 (RFC-019 S7)"
    }

    async fn run(
        &self,
        mesh_id: u32,
        nodes: &[NodeInfo],
        _flags: &[String],
    ) -> Result<TestResult> {
        let mut result = TestResult::new();
        anyhow::ensure!(nodes.len() == 3, "straggler-rejoin expects a 3-node mesh");
        let docker = Docker::connect_with_local_defaults()?;

        println!("\nRunning straggler-rejoin checks:");

        // Baseline traffic while everyone is present.
        upload_file(&nodes[0], "/", "pre-boundary.txt", b"before the boundary".to_vec()).await?;

        // Node 2 goes down BEFORE the freeze and misses everything. The
        // remaining two are a majority of three, so the mesh stays live.
        let straggler = nodes[2].clone();
        crate::tests::persistence::stop_node(&docker, mesh_id, straggler.node_id).await?;
        print_and_add_check(
            &mut result,
            Check {
                name: "Straggler stopped before the freeze".to_string(),
                passed: true,
                detail: Some(format!("node {}", straggler.node_id)),
            },
        );

        let participants = [nodes[0].clone(), nodes[1].clone()];
        let Some((h, fresh)) = cross_boundary(&mut result, &docker, mesh_id, &participants).await?
        else {
            return Ok(result);
        };

        // New work lands in epoch 2 while the straggler is still away.
        upload_file(&fresh[0], "/", "post-boundary.txt", b"after the boundary".to_vec()).await?;
        let mut progressed = false;
        for _ in 0..60 {
            if decided_height(&fresh[0]).await.unwrap_or(0) > h {
                progressed = true;
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "Epoch 2 decides new work while the straggler is away".to_string(),
                passed: progressed,
                detail: Some(format!("H was {h}")),
            },
        );

        // The straggler wakes on the sealed epoch. Nothing pushes the new
        // epoch to it: it must notice, fetch, verify, stage, and ask for
        // its own restart. The exit code is that request's observable
        // half — no HTTP poking required.
        start_node(&docker, mesh_id, straggler.node_id).await?;
        let code = wait_for_exit_code(
            &docker,
            mesh_id,
            straggler.node_id,
            Duration::from_secs(300),
        )
        .await?;
        print_and_add_check(
            &mut result,
            Check {
                name: "Straggler discovers the boundary and requests its own restart".to_string(),
                passed: code == Some(75),
                detail: Some(format!("exit code: {code:?}")),
            },
        );
        if code != Some(75) {
            return Ok(result);
        }

        // Its boot path rebuilds it from the staged, verified inputs.
        start_node(&docker, mesh_id, straggler.node_id).await?;
        let rejoined = reauth_node(&docker, mesh_id, &straggler).await?;
        let crossed = wait_for_epoch(&rejoined, "2", Duration::from_secs(120)).await?;
        let status = regenesis_status(&rejoined).await?;
        print_and_add_check(
            &mut result,
            Check {
                name: "Straggler boots into epoch 2 with no boundary error".to_string(),
                passed: crossed && status["boundary_error"].is_null(),
                detail: Some(format!(
                    "epoch {:?}, boundary_error {:?}",
                    status["epoch"], status["boundary_error"]
                )),
            },
        );

        // It converges with the mesh and serves the files it never saw
        // decided — the rebuild really did adopt the mesh's state.
        let all_nodes = [fresh[0].clone(), fresh[1].clone(), rejoined.clone()];
        let mut converged = false;
        for _ in 0..120 {
            let mut heights = Vec::new();
            for node in &all_nodes {
                heights.push(decided_height(node).await.unwrap_or(0));
            }
            if heights.iter().all(|x| *x == heights[0]) && heights[0] > h {
                converged = true;
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "Rejoined node converges with the mesh tip".to_string(),
                passed: converged,
                detail: None,
            },
        );

        let snapshots = fetch_state_snapshots(&all_nodes).await?;
        let coherent = snapshots.windows(2).all(|w| w[0] == w[1]);
        print_and_add_check(
            &mut result,
            Check {
                name: "Rejoined node holds state identical to the mesh".to_string(),
                passed: coherent,
                detail: None,
            },
        );

        Ok(result)
    }
}

impl TestScenario for DivergedNodeRebuild {
    fn name(&self) -> &'static str {
        "diverged-node-rebuild"
    }

    fn description(&self) -> &'static str {
        "A node that fails its own boundary gate rebuilds from peers instead of wedging (RFC-019 S7)"
    }

    async fn run(
        &self,
        mesh_id: u32,
        nodes: &[NodeInfo],
        _flags: &[String],
    ) -> Result<TestResult> {
        let mut result = TestResult::new();
        anyhow::ensure!(nodes.len() == 3, "diverged-node-rebuild expects a 3-node mesh");
        let docker = Docker::connect_with_local_defaults()?;

        println!("\nRunning diverged-node-rebuild checks:");

        upload_file(&nodes[0], "/", "pre-boundary.txt", b"before the boundary".to_vec()).await?;

        // Node 2 is taken down before the freeze, like the straggler —
        // but here it will also be unable to cross on its own, because
        // by the time it returns the mesh is a full epoch ahead and it
        // has no sealed state of its own to build from. Its ONLY route
        // back is the peer rebuild.
        let diverged = nodes[2].clone();
        crate::tests::persistence::stop_node(&docker, mesh_id, diverged.node_id).await?;

        let participants = [nodes[0].clone(), nodes[1].clone()];
        let Some((h, fresh)) = cross_boundary(&mut result, &docker, mesh_id, &participants).await?
        else {
            return Ok(result);
        };

        // Cross a SECOND boundary, so the returning node is two epochs
        // behind: it must walk the lineage chain hop by hop and import
        // only the latest snapshot.
        upload_file(&fresh[0], "/", "between-epochs.txt", b"epoch two work".to_vec()).await?;
        let Some((h2, fresh2)) = cross_boundary(&mut result, &docker, mesh_id, &fresh).await?
        else {
            return Ok(result);
        };
        let mut epochs = Vec::new();
        for node in &fresh2 {
            epochs.push(
                regenesis_status(node).await?["epoch"]
                    .as_str()
                    .unwrap_or("?")
                    .to_string(),
            );
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "The mesh is now two epochs ahead of the absent node".to_string(),
                passed: epochs.iter().all(|e| e == "3"),
                detail: Some(format!("epochs: {epochs:?}, H2={h2}, H1={h}")),
            },
        );

        // The absent node returns, two epochs behind.
        start_node(&docker, mesh_id, diverged.node_id).await?;
        let code = wait_for_exit_code(
            &docker,
            mesh_id,
            diverged.node_id,
            Duration::from_secs(300),
        )
        .await?;
        print_and_add_check(
            &mut result,
            Check {
                name: "Two-epochs-behind node stages a rebuild and requests a restart".to_string(),
                passed: code == Some(75),
                detail: Some(format!("exit code: {code:?}")),
            },
        );
        if code != Some(75) {
            return Ok(result);
        }

        start_node(&docker, mesh_id, diverged.node_id).await?;
        let rebuilt = reauth_node(&docker, mesh_id, &diverged).await?;
        let crossed = wait_for_epoch(&rebuilt, "3", Duration::from_secs(120)).await?;
        print_and_add_check(
            &mut result,
            Check {
                name: "It lands directly on the LATEST epoch, skipping the intermediate snapshot"
                    .to_string(),
                passed: crossed,
                detail: Some(format!(
                    "epoch {:?}",
                    regenesis_status(&rebuilt).await?["epoch"]
                )),
            },
        );

        let all_nodes = [fresh2[0].clone(), fresh2[1].clone(), rebuilt.clone()];
        let snapshots = fetch_state_snapshots(&all_nodes).await?;
        let coherent = snapshots.windows(2).all(|w| w[0] == w[1]);
        print_and_add_check(
            &mut result,
            Check {
                name: "Rebuilt node holds state identical to the mesh".to_string(),
                passed: coherent,
                detail: None,
            },
        );

        Ok(result)
    }
}

/// Rollback drill (RFC-019 S8): the escape hatch for a bad epoch. The
/// mesh seals for an upgrade target, one node crosses into epoch 2, and
/// then the whole mesh is told to ABANDON the boundary and come back on
/// its retained epoch-1 databases — after which it must actually RUN
/// again, not merely survive. Then the negative half: once the window
/// has closed, rollback is correctly refused.
pub struct RegenesisRollback;

async fn rollback(node: &NodeInfo) -> Result<(u16, String)> {
    post_json(node, "/api/consensus/regenesis/rollback", None).await
}

impl TestScenario for RegenesisRollback {
    fn name(&self) -> &'static str {
        "regenesis-rollback"
    }

    fn description(&self) -> &'static str {
        "Abandon a crossed boundary, restore the retained epoch, and run again (RFC-019 S8)"
    }

    async fn run(
        &self,
        mesh_id: u32,
        nodes: &[NodeInfo],
        _flags: &[String],
    ) -> Result<TestResult> {
        const TARGET: &str = "2026.8.1";
        let mut result = TestResult::new();
        anyhow::ensure!(nodes.len() == 3, "regenesis-rollback expects a 3-node mesh");
        let docker = Docker::connect_with_local_defaults()?;

        println!("\nRunning regenesis-rollback checks:");

        upload_file(&nodes[0], "/", "pre-rollback.txt", b"epoch one work".to_vec()).await?;
        let Some(_) = attest_and_freeze(&mut result, nodes, Some(TARGET)).await? else {
            return Ok(result);
        };
        let Some(seal_height) = wait_sealed_everywhere(nodes).await? else {
            print_and_add_check(
                &mut result,
                Check {
                    name: "Upgrade-target moratorium seals on its own".to_string(),
                    passed: false,
                    detail: Some("never sealed".to_string()),
                },
            );
            return Ok(result);
        };

        // Cross exactly ONE node. This is the drill's entry position and
        // it is stable: with no quorum of the carried set booted, nothing
        // decides, so the rollback window stays open indefinitely. Do NOT
        // try to hold it with a timer — on a real upgrade boundary a live
        // quorum closes the window in ~15s, because the version
        // attestation job decides H+1 as soon as it can.
        recreate_node_with_env(
            &docker,
            mesh_id,
            nodes[0].node_id,
            &[("HOPNET_UPGRADE_VERSION_OVERRIDE", TARGET)],
        )
        .await?;
        let node0 = reauth_node(&docker, mesh_id, &nodes[0]).await?;
        let v = regenesis_status(&node0).await?;
        let in_window = v["epoch"].as_str() == Some("2")
            && v["rollback_retained"].as_bool() == Some(true)
            && decided_height(&node0).await.unwrap_or(0) == seal_height;
        print_and_add_check(
            &mut result,
            Check {
                name: "One node crosses into epoch 2 with the rollback window open".to_string(),
                passed: in_window,
                detail: Some(format!(
                    "epoch {:?}, retained {:?}, H={seal_height}",
                    v["epoch"], v["rollback_retained"]
                )),
            },
        );
        if !in_window {
            return Ok(result);
        }

        // Abandon it. The node writes its own marker and restarts; the
        // boot path discards the epoch-2 database and restores the
        // retained one.
        let (status, body) = rollback(&node0).await?;
        anyhow::ensure!(status == 202, "rollback refused: {status} {body}");
        let code =
            wait_for_exit_code(&docker, mesh_id, node0.node_id, Duration::from_secs(180)).await?;
        print_and_add_check(
            &mut result,
            Check {
                name: "Rollback request restarts the crossed node".to_string(),
                passed: code == Some(75),
                detail: Some(format!("exit code: {code:?}")),
            },
        );
        if code != Some(75) {
            return Ok(result);
        }
        start_node(&docker, mesh_id, node0.node_id).await?;
        let node0 = reauth_node(&docker, mesh_id, &node0).await?;
        let v = regenesis_status(&node0).await?;
        let restored = v["epoch"].as_str() == Some("1")
            && v["phase"].as_str() == Some("normal")
            && v["rollback_retained"].as_bool() == Some(false)
            && v["boundary_error"].is_null();
        print_and_add_check(
            &mut result,
            Check {
                name: "It comes back on the retained epoch, with the seal cleared".to_string(),
                passed: restored,
                detail: Some(format!(
                    "epoch {:?}, phase {:?}, retained {:?}, boundary_error {:?}",
                    v["epoch"], v["phase"], v["rollback_retained"], v["boundary_error"]
                )),
            },
        );

        // The other two never crossed — they are parked ALIVE on their
        // epoch-1 databases, so the same request abandons the seal in
        // place. Recreate them without the version override: the whole
        // mesh is going back to the version it was running.
        let mut fresh = vec![node0];
        for node in &nodes[1..] {
            let (status, body) = rollback(node).await?;
            anyhow::ensure!(status == 202, "parked rollback refused: {status} {body}");
            let code =
                wait_for_exit_code(&docker, mesh_id, node.node_id, Duration::from_secs(180))
                    .await?;
            anyhow::ensure!(code == Some(75), "parked node did not restart: {code:?}");
            recreate_node_with_env(&docker, mesh_id, node.node_id, &[]).await?;
            fresh.push(reauth_node(&docker, mesh_id, node).await?);
        }
        let mut epochs = Vec::new();
        for node in &fresh {
            epochs.push(
                regenesis_status(node).await?["epoch"]
                    .as_str()
                    .unwrap_or("?")
                    .to_string(),
            );
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "The whole mesh is back on epoch 1".to_string(),
                passed: epochs.iter().all(|e| e == "1"),
                detail: Some(format!("epochs: {epochs:?}")),
            },
        );

        // THE check that matters: a rollback that leaves the mesh frozen
        // is not a rollback. The committed Sealed phase refused every
        // submission, so this only passes because the boot path cleared
        // it on all three.
        upload_file(&fresh[0], "/", "post-rollback.txt", b"the mesh lives".to_vec()).await?;
        let mut progressed = false;
        for _ in 0..90 {
            if decided_height(&fresh[0]).await.unwrap_or(0) > seal_height {
                progressed = true;
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "The rolled-back mesh accepts writes and decides again".to_string(),
                passed: progressed,
                detail: Some(format!("H was {seal_height}")),
            },
        );
        let snapshots = fetch_state_snapshots(&fresh).await?;
        print_and_add_check(
            &mut result,
            Check {
                name: "Rolled-back replicas agree".to_string(),
                passed: snapshots.windows(2).all(|w| w[0] == w[1]),
                detail: None,
            },
        );
        if !progressed {
            return Ok(result);
        }

        // Negative half: recovery from a rollback is another regenesis,
        // FORWARD. Take the mesh across a same-version boundary, let it
        // decide past H, and the window is gone for good.
        let Some(_) = attest_and_freeze(&mut result, &fresh, None).await? else {
            return Ok(result);
        };
        let mut exits = Vec::new();
        for node in &fresh {
            exits.push(
                wait_for_exit_code(&docker, mesh_id, node.node_id, Duration::from_secs(300))
                    .await?,
            );
        }
        anyhow::ensure!(
            exits.iter().all(|c| *c == Some(75)),
            "second boundary did not seal: {exits:?}"
        );
        let mut reborn = Vec::new();
        for node in &fresh {
            start_node(&docker, mesh_id, node.node_id).await?;
            reborn.push(reauth_node(&docker, mesh_id, node).await?);
        }
        upload_file(&reborn[0], "/", "epoch-two.txt", b"forward only".to_vec()).await?;
        let h2 = regenesis_status(&reborn[0]).await?["seal_height"]
            .as_str()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        let mut closed = false;
        for _ in 0..90 {
            let v = regenesis_status(&reborn[0]).await?;
            if v["rollback_retained"].as_bool() == Some(false)
                && decided_height(&reborn[0]).await.unwrap_or(0) > h2
            {
                closed = true;
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        let (status, _) = rollback(&reborn[0]).await?;
        print_and_add_check(
            &mut result,
            Check {
                name: "Once the new epoch decides, the window closes and rollback is refused"
                    .to_string(),
                passed: closed && status == 409,
                detail: Some(format!("window closed: {closed}, rollback status: {status}")),
            },
        );

        Ok(result)
    }
}
