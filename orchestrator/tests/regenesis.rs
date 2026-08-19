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

/// RFC-020 §Cutover rehearsal ("Rehearsed before real"): a disposable
/// mesh crosses the EXACT boundary the live mesh will — born on the last
/// released image (initialize-regime schema, pre-split host section),
/// sealed for THIS build, each node recreated onto THIS build's image
/// with no version override (the binary's real identity is the point).
/// Crossing evidence: epoch 2, the cutover version, every module stamped
/// at its chain head (adopt-at-baseline + fast-forward). A fresh node
/// then joins THROUGH the old-shape artifact, exercising the host@3
/// import mapping and the scratch-database splice (RFC-020 S5).
///
/// Before running, load both images into this checkout's namespace:
/// `scripts/build-release-image.sh v<CUTOVER_OLD_RELEASE>` for the old
/// one, and the usual `nix build .#packages.<system>.dockerImage &&
/// orchestrator load-image` for the current one.
pub struct RegenesisCutover;

/// The release the rehearsal crosses FROM — the newest tag whose image
/// predates the chain regime. The mesh-creation env derives the old
/// image ref (`hopnet:<hash>-<this>`) from it.
pub(crate) const CUTOVER_OLD_RELEASE: &str = "2026.8.5";

async fn post_json(
    node: &NodeInfo,
    path: &str,
    body: Option<serde_json::Value>,
) -> Result<(u16, String)> {
    let client = crate::insecure_client();
    let url = format!("https://{}:{}{}", node.ip_address, node.port, path);
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
///
/// The checkout label is part of the match, not decoration. `mesh_id` and
/// `node_id` are NOT unique across checkouts — resource NAMES are
/// per-checkout (`naming::prefix`) but these labels are bare, so another
/// worktree running its own mesh 0 carries `mesh_id=0, node_id=1` too.
/// Matching on the pair alone could return that container: `is_running`
/// then reads a healthy foreign node, and `wait_for_exit_code` waits out
/// its whole deadline on a container that will never exit — reporting a
/// node that restarted in seconds as "did not restart". Volumes were
/// already filtered this way in `main.rs`; the container lookups were not.
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
                        && labels
                            .get(crate::naming::CHECKOUT_LABEL)
                            .map(String::as_str)
                            == Some(crate::naming::checkout_hash())
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
    let mut last_err: Option<String> = None;
    loop {
        // Typed inspect first. Its errors are tolerated INSIDE the deadline
        // because podman reports a transient "stopping" state bollard's
        // enums do not know — exactly while the process is exiting, i.e.
        // exactly when this waits.
        match docker
            .inspect_container(
                &id,
                None::<bollard::query_parameters::InspectContainerOptions>,
            )
            .await
        {
            Ok(info) => {
                if let Some(state) = info.state
                    && state.status == Some(bollard::models::ContainerStateStatusEnum::EXITED)
                {
                    return Ok(Some(state.exit_code.unwrap_or(-1)));
                }
            }
            Err(e) => last_err = Some(e.to_string()),
        }

        // Fallback that does not go through bollard's typed models at all.
        // A whole `inspect_container` response fails to deserialize if ANY
        // field carries a value the stubs reject, so a container can sit in
        // `exited` for the entire window while the typed path keeps
        // erroring — which is how a node that restarted correctly within
        // seconds was reported as "did not restart" after 180s of silence.
        if let Some(code) = raw_exit_code(&id).await {
            return Ok(Some(code));
        }

        if start.elapsed() > timeout {
            // Never return a bare None again: if the typed path was
            // erroring, say so, or the next reader re-debugs this from
            // scratch.
            if let Some(e) = last_err {
                anyhow::bail!(
                    "mesh {mesh_id} node {node_id} never observed as exited within {timeout:?}; \
                     the last container inspect FAILED: {e}"
                );
            }
            return Ok(None); // genuinely still running
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Exit code straight from the container runtime's own formatter, bypassing
/// bollard's models. None when the container has not exited (or the runtime
/// CLI is unavailable, in which case the typed path above is authoritative).
async fn raw_exit_code(id: &str) -> Option<i64> {
    for runtime in ["podman", "docker"] {
        let out = tokio::process::Command::new(runtime)
            .args([
                "inspect",
                "--format",
                "{{.State.Status}} {{.State.ExitCode}}",
                id,
            ])
            .output()
            .await;
        let Ok(out) = out else { continue };
        if !out.status.success() {
            continue;
        }
        let text = String::from_utf8_lossy(&out.stdout);
        let mut parts = text.split_whitespace();
        if parts.next() == Some("exited")
            && let Some(code) = parts.next().and_then(|c| c.parse::<i64>().ok())
        {
            return Some(code);
        }
        return None; // reachable runtime, container simply not exited yet
    }
    None
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
///
/// `image` recreates the node onto a DIFFERENT image (the RFC-020
/// cutover's real binary swap). Explicit rather than through
/// `extra_env`: `HOPNET_ORCH_IMAGE` may already carry a mesh-wide
/// binding (a mesh born on an old release image), so the previous value
/// is restored — not removed — after the create.
pub async fn recreate_node_with_env(
    docker: &Docker,
    mesh_id: u32,
    node_id: u32,
    image: Option<&str>,
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
                Some(
                    bollard::query_parameters::RemoveContainerOptionsBuilder::new()
                        .force(true)
                        .build(),
                ),
            )
            .await;
        if removed.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    removed?;

    // Go through `naming` rather than formatting the prefix here: resource
    // names are per-checkout, so a literal `hopnet-orchestrator-` recreates
    // the node onto a network this checkout never created.
    let container_name = crate::naming::container_name(mesh_id, node_id);

    // Removal returning Ok is not the same as the NAME being free: podman
    // reports the delete complete while its storage layer still holds the
    // registration, and the create then fails with "container name ... is
    // already in use". Wait for the name to actually stop resolving.
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

    let prev_image = std::env::var("HOPNET_ORCH_IMAGE").ok();
    if let Some(image) = image {
        unsafe { std::env::set_var("HOPNET_ORCH_IMAGE", image) };
    }
    for (k, v) in extra_env {
        unsafe { std::env::set_var(k, v) };
    }
    let network_name = crate::naming::network_name(mesh_id);
    let created = crate::create_hopnet_container(
        docker,
        mesh_id,
        node_id,
        &container_name,
        &network_name,
        runtime,
    )
    .await;
    for (k, _) in extra_env {
        unsafe { std::env::remove_var(k) };
    }
    if image.is_some() {
        match &prev_image {
            Some(v) => unsafe { std::env::set_var("HOPNET_ORCH_IMAGE", v) },
            None => unsafe { std::env::remove_var("HOPNET_ORCH_IMAGE") },
        }
    }
    created?;
    Ok(())
}

/// Re-resolve one node's address (containers change IP/port on recreate)
/// and mint a fresh JWT (the signing key rolls every boot).
pub(crate) async fn reauth_node(
    docker: &Docker,
    mesh_id: u32,
    node: &NodeInfo,
) -> Result<NodeInfo> {
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

    async fn run(&self, mesh_id: u32, nodes: &[NodeInfo], _flags: &[String]) -> Result<TestResult> {
        let mut result = TestResult::new();
        anyhow::ensure!(nodes.len() == 3, "regenesis-restart expects a 3-node mesh");
        let docker = crate::sys::connect()?;

        println!("\nRunning regenesis-restart checks:");

        // 1-2. Baseline traffic, attestations, same-version freeze.
        upload_file(
            &nodes[0],
            "/",
            "pre-freeze.txt",
            b"before the boundary".to_vec(),
        )
        .await?;
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
        upload_file(
            &nodes[1],
            "/",
            "post-abort.txt",
            b"writes work again".to_vec(),
        )
        .await?;
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

    async fn run(&self, mesh_id: u32, nodes: &[NodeInfo], _flags: &[String]) -> Result<TestResult> {
        let mut result = TestResult::new();
        anyhow::ensure!(
            nodes.len() == 3,
            "regenesis-awaiting-upgrade expects a 3-node mesh"
        );
        let docker = crate::sys::connect()?;
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
            None,
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
                None,
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

/// The status view's `schema_ordinals` as a comparable map.
fn ordinal_map(status: &serde_json::Value) -> std::collections::BTreeMap<String, u64> {
    status["schema_ordinals"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|e| Some((e["module"].as_str()?.to_string(), e["ordinal"].as_u64()?)))
        .collect()
}

impl TestScenario for RegenesisCutover {
    fn name(&self) -> &'static str {
        "regenesis-cutover"
    }

    fn description(&self) -> &'static str {
        "Old-release mesh crosses into THIS build: adopt-at-baseline, fast-forward, pre-split artifact join (RFC-020 §Cutover)"
    }

    async fn run(&self, mesh_id: u32, nodes: &[NodeInfo], _flags: &[String]) -> Result<TestResult> {
        let mut result = TestResult::new();
        anyhow::ensure!(nodes.len() == 3, "regenesis-cutover expects a 3-node mesh");
        let docker = crate::sys::connect()?;
        // The cutover targets the binary under test: the version this
        // orchestrator was compiled with IS the cutover release.
        let target: &str = env!("CARGO_PKG_VERSION");
        let new_image = format!("hopnet:{}", crate::naming::checkout_hash());

        println!("\nRunning regenesis-cutover checks:");

        // 0. The mesh really was born on the old release — guards a
        //    misloaded image turning the rehearsal into a self-cross.
        let born_on = regenesis_status(&nodes[0]).await?["running_version"]
            .as_str()
            .unwrap_or("?")
            .to_string();
        print_and_add_check(
            &mut result,
            Check {
                name: format!("Mesh born on the old release image ({CUTOVER_OLD_RELEASE})"),
                passed: born_on == CUTOVER_OLD_RELEASE,
                detail: Some(born_on.clone()),
            },
        );
        if born_on != CUTOVER_OLD_RELEASE {
            return Ok(result);
        }

        // 1. Epoch-1 data written under the OLD schema regime — the bytes
        //    the crossing must carry — then the upgrade-target freeze.
        upload_file(
            &nodes[0],
            "/",
            "pre-cutover.txt",
            b"written under the initialize regime".to_vec(),
        )
        .await?;
        if attest_and_freeze(&mut result, nodes, Some(target))
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

        // 2. Parked alive awaiting the cutover binary (the full park
        //    semantics are the awaiting-upgrade scenario's job; asserted
        //    lightly here).
        tokio::time::sleep(Duration::from_secs(8)).await;
        let mut awaiting = Vec::new();
        for node in nodes {
            awaiting.push(
                regenesis_status(node).await?["awaiting_upgrade"]
                    .as_bool()
                    .unwrap_or(false),
            );
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "Sealed nodes park alive awaiting the cutover binary".to_string(),
                passed: awaiting.iter().all(|a| *a),
                detail: Some(format!("{awaiting:?}")),
            },
        );

        // 3. The real binary swap: each node recreated onto THIS build's
        //    image, no version override. Crossing evidence per node:
        //    phase normal, epoch 2, running the cutover release, every
        //    module stamped at its chain head — the visible outcome of
        //    adopt-at-baseline + fast-forward on a legacy database.
        let expected: std::collections::BTreeMap<String, u64> = hopnet::db::chains::chains()
            .iter()
            .map(|c| (c.module.to_string(), u64::from(c.head())))
            .collect();
        let mut fresh_nodes = Vec::new();
        let mut crossings = Vec::new();
        for node in nodes {
            recreate_node_with_env(&docker, mesh_id, node.node_id, Some(&new_image), &[]).await?;
            let fresh = reauth_node(&docker, mesh_id, node).await?;
            let v = regenesis_status(&fresh).await?;
            crossings.push((
                node.node_id,
                v["phase"].as_str() == Some("normal"),
                v["epoch"].as_str() == Some("2"),
                v["running_version"].as_str() == Some(target),
                ordinal_map(&v) == expected,
            ));
            fresh_nodes.push(fresh);
        }
        let all_crossed = crossings.iter().all(|(_, p, e, r, o)| *p && *e && *r && *o);
        print_and_add_check(
            &mut result,
            Check {
                name: "Every swapped node crosses: epoch 2, cutover version, chains at head"
                    .to_string(),
                passed: all_crossed,
                detail: Some(format!(
                    "(node, phase, epoch, version, ordinals): {crossings:?}; expected heads: {expected:?}"
                )),
            },
        );
        if !all_crossed {
            return Ok(result);
        }

        // 4. The crossed mesh DECIDES — post-boundary work commits on
        //    every node.
        upload_file(
            &fresh_nodes[1],
            "/",
            "post-cutover.txt",
            b"decided by the cutover release".to_vec(),
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
        print_and_add_check(
            &mut result,
            Check {
                name: "Cutover mesh decides past the boundary".to_string(),
                passed: progressed,
                detail: Some(format!("progressed: {progressed} (H={seal_height})")),
            },
        );

        // 5. A FRESH node joins the crossed mesh. The epoch-2 genesis
        //    artifact was serialized by the OLD binary — pre-split host@3
        //    sections — so this join exercises resolve_import_plan's
        //    one-time mapping and the scratch-database splice.
        //    HOPNET_ORCH_IMAGE still carries the mesh-wide OLD binding;
        //    move it to the new image — every node from here on runs the
        //    cutover release, which is also what a real operator's
        //    environment looks like after the crossing.
        unsafe { std::env::set_var("HOPNET_ORCH_IMAGE", &new_image) };
        let runtime = crate::sys::detect_runtime(&docker).await?;
        crate::add_nodes_to_mesh(&docker, mesh_id, 1, runtime).await?;
        let joined = reauth_node(
            &docker,
            mesh_id,
            &NodeInfo {
                node_id: nodes.len() as u32,
                ip_address: String::new(),
                port: 0,
                jwt_token: String::new(),
            },
        )
        .await?;
        let on_latest = wait_for_epoch(&joined, "2", Duration::from_secs(300)).await?;
        let joined_ordinals = ordinal_map(&regenesis_status(&joined).await?);
        print_and_add_check(
            &mut result,
            Check {
                name: "Fresh node joins THROUGH the pre-split artifact, stamped at head"
                    .to_string(),
                passed: on_latest && joined_ordinals == expected,
                detail: Some(format!(
                    "epoch 2: {on_latest}, ordinals: {joined_ordinals:?}"
                )),
            },
        );

        // 6. Lived-through ≡ fresh-joined: identical state everywhere.
        let mut all_nodes = fresh_nodes.clone();
        all_nodes.push(joined);
        let tip = decided_height(&fresh_nodes[0]).await.unwrap_or(seal_height);
        let (converged, heights) = wait_for_convergence(&all_nodes, tip, 120).await;
        let snapshots = fetch_state_snapshots(&all_nodes).await?;
        let (coherent, detail) = coherence(&snapshots);
        print_and_add_check(
            &mut result,
            Check {
                name: "Fresh-joined state is identical to lived-through state".to_string(),
                passed: converged && coherent,
                detail: Some(format!("heights: {heights:?}, {detail}")),
            },
        );

        Ok(result)
    }
}

/// RFC-021 nix activation: with the nix deployment contract declared and
/// a verified staged generation planted in each node's volume, the sealed
/// upgrade boundary does NOT park — every node flips its profile symlink
/// to the staged generation and exits 75. The "restart into the new
/// binary" half is simulated by recreating with the running-version
/// override (the REAL profile-exec restart is the NixOS module's job,
/// covered by the upgrade-vm-test flake check); the mesh then crosses and
/// decides past H. The awaiting-upgrade scenario is this one's negative
/// half: the same boundary with no provider contract parks.
pub struct RegenesisNixActivation;

/// Run a script in the container via busybox sh, returning
/// (exit_code, output) — mount.rs's exec_capture, shared shape.
async fn exec_sh(docker: &Docker, container: &str, script: &str) -> Result<(i64, String)> {
    use bollard::exec::{CreateExecOptions, StartExecResults};
    use tokio_stream::StreamExt;
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

impl TestScenario for RegenesisNixActivation {
    fn name(&self) -> &'static str {
        "regenesis-nix-activation"
    }

    fn description(&self) -> &'static str {
        "Upgrade boundary with a staged nix generation: flip + exit 75 instead of parking (RFC-021)"
    }

    async fn run(&self, mesh_id: u32, nodes: &[NodeInfo], _flags: &[String]) -> Result<TestResult> {
        let mut result = TestResult::new();
        anyhow::ensure!(
            nodes.len() == 3,
            "regenesis-nix-activation expects a 3-node mesh"
        );
        let docker = crate::sys::connect()?;
        const TARGET: &str = "2026.8.1";
        const DATA: &str = "/root/.local/share/hopnet";

        println!("\nRunning regenesis-nix-activation checks:");

        // 1. Plant a verified staged generation in every node's data
        //    volume: a fake store dir whose bin/hopnet answers --version
        //    with the target, the out-link, and the provenance record —
        //    the exact disk state stage() leaves behind. The volume
        //    survives the exit and the recreate.
        let plant = format!(
            "set -e\n\
             mkdir -p {DATA}/staged {DATA}/staged-store/bin\n\
             printf '#!/bin/sh\\necho {TARGET}\\n' > {DATA}/staged-store/bin/hopnet\n\
             chmod +x {DATA}/staged-store/bin/hopnet\n\
             ln -sfn {DATA}/staged-store {DATA}/staged/v{TARGET}\n\
             printf '{{\"version\":\"{TARGET}\",\"flake_ref\":\"git+https://test.invalid/HopNet.git?ref=v{TARGET}\",\"out_path\":\"{DATA}/staged-store\"}}' > {DATA}/staged/v{TARGET}.json\n"
        );
        let mut planted = true;
        for node in nodes {
            let id = find_container_id(&docker, mesh_id, node.node_id).await?;
            let (code, out) = exec_sh(&docker, &id, &plant).await?;
            if code != 0 {
                planted = false;
                println!("  planting failed on node {}: {out}", node.node_id);
            }
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "Staged generation planted in every node's volume".to_string(),
                passed: planted,
                detail: None,
            },
        );
        if !planted {
            return Ok(result);
        }

        // 2. Attestations + the upgrade-target start (the staged claim
        //    comes from the test-gated override, same as the awaiting
        //    scenario — attestation-from-report has its own unit tests).
        let tip_at_freeze = decided_height(&nodes[0]).await.unwrap_or(0);
        if attest_and_freeze(&mut result, nodes, Some(TARGET))
            .await?
            .is_none()
        {
            return Ok(result);
        }

        // 3. THE RFC-021 assertion: instead of parking awaiting-upgrade
        //    (the negative half, regenesis-awaiting-upgrade), every node
        //    activates its staged generation and exits with the restart
        //    code.
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
                name: "Sealed upgrade boundary activates instead of parking: every node exits 75"
                    .to_string(),
                passed: all_exited_75,
                detail: Some(format!("exit codes: {exit_codes:?}")),
            },
        );
        if !all_exited_75 {
            return Ok(result);
        }

        // 4. The supervisor restart into the new binary, simulated with
        //    the running-version override (containers exec the image's
        //    store binary, not the profile — the profile-exec restart is
        //    the module's job, proven in the VM test).
        let mut fresh_nodes = Vec::new();
        for node in nodes {
            recreate_node_with_env(
                &docker,
                mesh_id,
                node.node_id,
                None,
                &[("HOPNET_UPGRADE_VERSION_OVERRIDE", TARGET)],
            )
            .await?;
            fresh_nodes.push(reauth_node(&docker, mesh_id, node).await?);
        }

        // 5. The volume carries the evidence of the flip: the profile
        //    symlink points at the staged generation, and no
        //    awaiting-upgrade marker was ever written on the activation
        //    path.
        let mut flipped = true;
        let mut detail = String::new();
        for node in &fresh_nodes {
            let id = find_container_id(&docker, mesh_id, node.node_id).await?;
            let (_, out) = exec_sh(
                &docker,
                &id,
                &format!(
                    "readlink {DATA}/profile; test -e {DATA}/awaiting-upgrade && echo MARKER || echo no-marker"
                ),
            )
            .await?;
            let ok = out.contains("staged-store") && out.contains("no-marker");
            if !ok {
                flipped = false;
            }
            detail = out.split_whitespace().collect::<Vec<_>>().join(" ");
        }
        print_and_add_check(
            &mut result,
            Check {
                name: "Profile symlink flipped to the staged generation; no park marker"
                    .to_string(),
                passed: flipped,
                detail: Some(detail),
            },
        );

        // 6. The upgraded mesh crosses and decides past H.
        let mut epochs = Vec::new();
        for node in &fresh_nodes {
            let v = regenesis_status(node).await?;
            epochs.push(v["epoch"].as_str().unwrap_or("?").to_string());
        }
        upload_file(
            &fresh_nodes[0],
            "/",
            "post-upgrade.txt",
            b"decided by the upgraded epoch".to_vec(),
        )
        .await?;
        let (progressed, heights) = wait_for_convergence(&fresh_nodes, tip_at_freeze, 120).await;
        print_and_add_check(
            &mut result,
            Check {
                name: "Upgraded mesh completes epoch 2 and decides past H".to_string(),
                passed: progressed && epochs.iter().all(|e| e == "2"),
                detail: Some(format!("epochs: {epochs:?}, heights: {heights:?}")),
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

/// Wait until every node reports the same decided height, above
/// `floor`. A node that has just rebuilt from a snapshot starts at its
/// epoch's genesis height and still has to decided-value-sync the tail,
/// so comparing state before this settles compares a node mid-catch-up
/// against the tip and reports a divergence that is really a race.
async fn wait_for_convergence(nodes: &[NodeInfo], floor: u64, secs: u64) -> (bool, Vec<u64>) {
    let mut heights = Vec::new();
    for _ in 0..secs {
        heights.clear();
        for node in nodes {
            heights.push(decided_height(node).await.unwrap_or(0));
        }
        if heights.iter().all(|h| *h == heights[0]) && heights[0] > floor {
            return (true, heights);
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    (false, heights)
}

/// Do these replicas hold the same state? Compares the manifest top
/// hash at a common height — NOT the whole report, which carries the
/// node id and would never match. Returns a detail string naming the
/// heights and hashes, so a failure says what diverged instead of just
/// that something did.
fn coherence(snapshots: &[(u32, hopnet_common::NodeStateReport)]) -> (bool, String) {
    let agreed = snapshots.windows(2).all(|w| {
        w[0].1.consensus_height == w[1].1.consensus_height
            && w[0].1.manifest.top_hash == w[1].1.manifest.top_hash
    });
    let detail = snapshots
        .iter()
        .map(|(id, s)| {
            format!(
                "node {id} @{} {}",
                s.consensus_height,
                &s.manifest.top_hash.to_string()[..8.min(s.manifest.top_hash.to_string().len())]
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    (agreed, detail)
}

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
    expect_epoch: &str,
) -> Result<Option<(u64, Vec<NodeInfo>)>> {
    let Some(_running) = attest_and_freeze(result, participants, None).await? else {
        return Ok(None);
    };
    let mut exit_codes = Vec::new();
    for node in participants {
        exit_codes.push(
            wait_for_exit_code(docker, mesh_id, node.node_id, Duration::from_secs(300)).await?,
        );
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
            name: format!("The remaining mesh crossed into epoch {expect_epoch}"),
            passed: epochs.iter().all(|e| e == expect_epoch),
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

    async fn run(&self, mesh_id: u32, nodes: &[NodeInfo], _flags: &[String]) -> Result<TestResult> {
        let mut result = TestResult::new();
        anyhow::ensure!(nodes.len() == 3, "straggler-rejoin expects a 3-node mesh");
        let docker = crate::sys::connect()?;

        println!("\nRunning straggler-rejoin checks:");

        // Baseline traffic while everyone is present.
        upload_file(
            &nodes[0],
            "/",
            "pre-boundary.txt",
            b"before the boundary".to_vec(),
        )
        .await?;

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
        let Some((h, fresh)) =
            cross_boundary(&mut result, &docker, mesh_id, &participants, "2").await?
        else {
            return Ok(result);
        };

        // New work lands in epoch 2 while the straggler is still away.
        upload_file(
            &fresh[0],
            "/",
            "post-boundary.txt",
            b"after the boundary".to_vec(),
        )
        .await?;
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
        let (converged, heights) = wait_for_convergence(&all_nodes, h, 120).await;
        print_and_add_check(
            &mut result,
            Check {
                name: "Rejoined node converges with the mesh tip".to_string(),
                passed: converged,
                detail: Some(format!("heights: {heights:?}")),
            },
        );

        let snapshots = fetch_state_snapshots(&all_nodes).await?;
        let (coherent, detail) = coherence(&snapshots);
        print_and_add_check(
            &mut result,
            Check {
                name: "Rejoined node holds state identical to the mesh".to_string(),
                passed: coherent,
                detail: Some(detail),
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

    async fn run(&self, mesh_id: u32, nodes: &[NodeInfo], _flags: &[String]) -> Result<TestResult> {
        let mut result = TestResult::new();
        anyhow::ensure!(
            nodes.len() == 3,
            "diverged-node-rebuild expects a 3-node mesh"
        );
        let docker = crate::sys::connect()?;

        println!("\nRunning diverged-node-rebuild checks:");

        upload_file(
            &nodes[0],
            "/",
            "pre-boundary.txt",
            b"before the boundary".to_vec(),
        )
        .await?;

        // Node 2 is taken down before the freeze, like the straggler —
        // but here it will also be unable to cross on its own, because
        // by the time it returns the mesh is a full epoch ahead and it
        // has no sealed state of its own to build from. Its ONLY route
        // back is the peer rebuild.
        let diverged = nodes[2].clone();
        crate::tests::persistence::stop_node(&docker, mesh_id, diverged.node_id).await?;

        let participants = [nodes[0].clone(), nodes[1].clone()];
        let Some((h, fresh)) =
            cross_boundary(&mut result, &docker, mesh_id, &participants, "2").await?
        else {
            return Ok(result);
        };

        // Cross a SECOND boundary, so the returning node is two epochs
        // behind: it must walk the lineage chain hop by hop and import
        // only the latest snapshot.
        upload_file(
            &fresh[0],
            "/",
            "between-epochs.txt",
            b"epoch two work".to_vec(),
        )
        .await?;
        let Some((h2, fresh2)) = cross_boundary(&mut result, &docker, mesh_id, &fresh, "3").await?
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
        let code = wait_for_exit_code(&docker, mesh_id, diverged.node_id, Duration::from_secs(300))
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

        // It rebuilt AT the epoch-3 genesis height and still has to sync
        // the tail; comparing state before that settles compares a node
        // mid-catch-up against the tip.
        let all_nodes = [fresh2[0].clone(), fresh2[1].clone(), rebuilt.clone()];
        let (converged, heights) = wait_for_convergence(&all_nodes, h2, 120).await;
        print_and_add_check(
            &mut result,
            Check {
                name: "Rebuilt node converges with the mesh tip".to_string(),
                passed: converged,
                detail: Some(format!("heights: {heights:?}, H2={h2}")),
            },
        );

        let snapshots = fetch_state_snapshots(&all_nodes).await?;
        let (coherent, detail) = coherence(&snapshots);
        print_and_add_check(
            &mut result,
            Check {
                name: "Rebuilt node holds state identical to the mesh".to_string(),
                passed: coherent,
                detail: Some(detail),
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

    async fn run(&self, mesh_id: u32, nodes: &[NodeInfo], _flags: &[String]) -> Result<TestResult> {
        const TARGET: &str = "2026.8.1";
        let mut result = TestResult::new();
        anyhow::ensure!(nodes.len() == 3, "regenesis-rollback expects a 3-node mesh");
        let docker = crate::sys::connect()?;

        println!("\nRunning regenesis-rollback checks:");

        upload_file(
            &nodes[0],
            "/",
            "pre-rollback.txt",
            b"epoch one work".to_vec(),
        )
        .await?;
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
            None,
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
        // Recreate rather than start, so the override that carried this
        // node across is dropped: abandoning a bad upgrade means going
        // back to the old binary too, and it leaves the whole mesh on one
        // version — otherwise the next boundary would target a version
        // only this node runs, and the others would park instead of
        // sealing.
        recreate_node_with_env(&docker, mesh_id, node0.node_id, None, &[]).await?;
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
            let code = wait_for_exit_code(&docker, mesh_id, node.node_id, Duration::from_secs(180))
                .await?;
            anyhow::ensure!(code == Some(75), "parked node did not restart: {code:?}");
            recreate_node_with_env(&docker, mesh_id, node.node_id, None, &[]).await?;
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
        upload_file(
            &fresh[0],
            "/",
            "post-rollback.txt",
            b"the mesh lives".to_vec(),
        )
        .await?;
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
        // Wait for the write to reach EVERY replica before comparing state.
        // `progressed` above only needs the node it was submitted through to
        // advance, so comparing immediately caught a follower mid-apply and
        // reported a divergence that was really a lag (heights 9/9/8, and of
        // course differing hashes at differing heights).
        let (converged, heights) = wait_for_convergence(&fresh, seal_height, 120).await;
        anyhow::ensure!(
            converged,
            "rolled-back mesh never converged past H={seal_height}: {heights:?}"
        );
        let snapshots = fetch_state_snapshots(&fresh).await?;
        let (coherent, detail) = coherence(&snapshots);
        print_and_add_check(
            &mut result,
            Check {
                name: "Rolled-back replicas agree".to_string(),
                passed: coherent,
                detail: Some(detail),
            },
        );
        if !progressed {
            return Ok(result);
        }

        // Negative half: recovery from a rollback is another regenesis,
        // FORWARD. Take the mesh across a same-version boundary, let it
        // decide past H, and the window is gone for good.
        if attest_and_freeze(&mut result, &fresh, None)
            .await?
            .is_none()
        {
            return Ok(result);
        }
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

        // H, read from the epoch-2 genesis BEFORE any new work is submitted.
        // The new epoch's genesis sits AT the boundary height and is its
        // last decided block, so this is exactly H — and unlike
        // `seal_height` it is still present after the crossing.
        //
        // Two sources that look right and are not. `seal_height` from the
        // status view is JSON null here: the new epoch is deliberately born
        // with no `regenesis_state` row, so the original `unwrap_or(0)` made
        // the comparison below `> 0`, which the genesis satisfies on the
        // first iteration — the check rested entirely on its sibling
        // conjunct and proved nothing about deciding PAST the boundary. And
        // `attest_and_freeze` returns the attested running VERSION, not a
        // height. Polling for phase "sealed" does not work either: this is a
        // SAME-version boundary, so nodes seal and exit 75 immediately
        // rather than lingering in that phase to be observed.
        let h2 = decided_height(&reborn[0]).await?;
        anyhow::ensure!(h2 > 0, "epoch-2 genesis height must be real, got {h2}");

        upload_file(&reborn[0], "/", "epoch-two.txt", b"forward only".to_vec()).await?;
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
                detail: Some(format!(
                    "window closed: {closed}, rollback status: {status}"
                )),
            },
        );

        Ok(result)
    }
}
