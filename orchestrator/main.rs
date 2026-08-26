use anyhow::Result;
use bollard::Docker;
use bollard::models::{
    ContainerCreateBody, EndpointSettings, HostConfig, NetworkCreateResponse, NetworkingConfig,
};
use bollard::query_parameters::{
    CreateContainerOptionsBuilder, ListContainersOptionsBuilder, ListNetworksOptionsBuilder,
};
use clap::{Parser, Subcommand};
use serde_json::json;
use std::collections::HashMap;
use tokio_stream::StreamExt;

mod divergence;
mod naming;
mod sys;
mod tests;

/// Node information for API calls
#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub node_id: u32,
    pub ip_address: String,
    pub port: u32,
    pub jwt_token: String,
}

#[derive(Parser)]
#[command(name = "hopnet-orchestrator")]
#[command(about = "HopNet Docker Orchestrator for consensus testing")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Create a new mesh network
    Create {
        /// Number of nodes to create
        #[arg(short, long, default_value_t = 1)]
        nodes: u32,
        /// Skip cleanup on failure (leave containers running for log inspection)
        #[arg(long)]
        no_cleanup: bool,
    },
    /// Add nodes to an existing mesh
    Add {
        /// Mesh network ID to add nodes to
        #[arg(short, long)]
        mesh_id: u32,
        /// Number of nodes to add
        #[arg(short, long, default_value_t = 1)]
        nodes: u32,
    },
    /// List all running HopNet meshes
    List,
    /// Delete an entire mesh network and all its containers
    Delete {
        /// Mesh network ID to delete
        #[arg(short, long)]
        mesh_id: u32,
        /// Skip confirmation prompt
        #[arg(short = 'y', long)]
        yes: bool,
    },
    /// Clean up hanging networks for meshes with 0 nodes
    Cleanup {
        /// Skip confirmation prompt
        #[arg(short = 'y', long)]
        yes: bool,
    },
    /// Print mesh sign-in credentials and per-node URLs for browser access
    Creds {
        /// Mesh network ID
        #[arg(short, long)]
        mesh_id: u32,
    },
    /// Show detailed status of a mesh and its nodes
    Status {
        /// Mesh network ID to examine
        #[arg(short, long)]
        mesh_id: u32,
    },
    /// Show consensus history for a specific node
    History {
        /// Mesh network ID
        #[arg(short, long)]
        mesh_id: u32,
        /// Node ID within the mesh
        #[arg(short, long)]
        node: u32,
        /// Optional: Show detailed state for a specific view
        #[arg(short, long)]
        view: Option<i32>,
    },
    /// Check for state divergence across nodes in a mesh
    Divergence {
        /// Mesh network ID to examine
        #[arg(short, long)]
        mesh_id: u32,
    },
    /// Load the nix-built docker archive, retagged for this checkout.
    ///
    /// Rewrites the archive's embedded repo tag (hopnet:latest) to this
    /// checkout's hopnet:<hash> while streaming it into the container
    /// runtime — no shared intermediate tag exists at any point, so
    /// concurrent loads from other checkouts cannot collide.
    LoadImage {
        /// Path to the docker-archive (nix build output symlink)
        #[arg(long, default_value = "./result")]
        archive: std::path::PathBuf,
    },
    /// Print this checkout's resource prefix and image ref
    Prefix,
    /// Run integration test on a mesh.
    ///
    /// If `--mesh-id` is omitted, a fresh mesh is auto-created for this run,
    /// then deleted on test pass. On test failure the mesh is left up so the
    /// failing state can be inspected (use `delete --mesh-id <id>` to clean up).
    /// Use `--keep-on-pass` to retain an auto-created mesh even if the test passes.
    Test {
        /// Mesh network ID to test. If omitted, a fresh mesh is auto-created.
        #[arg(short, long)]
        mesh_id: Option<u32>,
        /// Test name to run
        #[arg(short, long)]
        test: Option<String>,
        /// List all available tests
        #[arg(short, long)]
        list: bool,
        /// Optional flags to modify test behavior (e.g., --flags wait-for-distribution)
        #[arg(long)]
        flags: Vec<String>,
        /// Number of nodes in an auto-created mesh (ignored if --mesh-id supplied).
        #[arg(long, default_value_t = 3)]
        auto_nodes: u32,
        /// Keep an auto-created mesh alive even on test pass (debugging aid).
        #[arg(long)]
        keep_on_pass: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Auto-detect and connect to container runtime (Docker or Podman)
    let socket_path = sys::detect_socket_path()?;
    let docker = Docker::connect_with_unix(
        socket_path.strip_prefix("unix://").unwrap_or(&socket_path),
        120,
        bollard::API_DEFAULT_VERSION,
    )?;

    // Detect runtime type for adaptive behavior
    let runtime = sys::detect_runtime(&docker).await?;

    match &cli.command {
        Some(Commands::Create { nodes, no_cleanup }) => {
            let mesh_id = get_next_mesh_id(&docker).await?;
            println!("Creating mesh {} with {} nodes", mesh_id, nodes);
            create_mesh(&docker, mesh_id, *nodes, *no_cleanup, runtime).await?;
        }
        Some(Commands::Add { mesh_id, nodes }) => {
            println!("Adding {} node(s) to mesh {}", nodes, mesh_id);
            add_nodes_to_mesh(&docker, *mesh_id, *nodes, runtime).await?;
        }
        Some(Commands::Delete { mesh_id, yes }) => {
            delete_mesh(&docker, *mesh_id, *yes).await?;
        }
        Some(Commands::Cleanup { yes }) => {
            cleanup_orphaned_networks(&docker, *yes).await?;
        }
        Some(Commands::LoadImage { archive }) => {
            load_image(&docker, archive).await?;
        }
        Some(Commands::Prefix) => {
            println!("checkout hash: {}", naming::checkout_hash());
            println!("resource prefix: {}", naming::prefix());
            println!("image ref: {}", naming::image_ref());
        }
        Some(Commands::Creds { mesh_id }) => {
            // Username is fixed by setup_node_0; the passphrase is persisted
            // in node 0's volume at mesh creation.
            let passphrase = load_mesh_passphrase(&docker, *mesh_id).await?;
            let addresses = get_external_addresses(&docker, *mesh_id, runtime).await?;
            println!("mesh {} credentials:", mesh_id);
            println!("  username:   allison");
            println!("  passphrase: {}", passphrase);
            println!("  nodes:");
            for (node_id, host, port) in addresses {
                println!("    node {}: https://{}:{}", node_id, host, port);
            }
            println!("open a node URL in a browser and sign in with the above.");
        }
        Some(Commands::Status { mesh_id }) => {
            show_mesh_status(&docker, *mesh_id, runtime).await?;
        }
        Some(Commands::History {
            mesh_id,
            node,
            view,
        }) => {
            show_node_history(&docker, *mesh_id, *node, *view, runtime).await?;
        }
        Some(Commands::Divergence { mesh_id }) => {
            divergence::check_divergence(&docker, *mesh_id, runtime).await?;
        }
        Some(Commands::Test {
            mesh_id,
            test,
            list,
            flags,
            auto_nodes,
            keep_on_pass,
        }) => {
            // List path / no-test path doesn't need a mesh; short-circuit.
            if *list || test.is_none() {
                tests::handle_test_command(
                    &docker,
                    mesh_id.unwrap_or(0),
                    test.as_deref(),
                    *list,
                    flags,
                    runtime,
                )
                .await?;
                return Ok(());
            }

            match mesh_id {
                Some(id) => {
                    // Existing behavior: use caller-provided mesh, no auto-cleanup.
                    tests::handle_test_command(
                        &docker,
                        *id,
                        test.as_deref(),
                        *list,
                        flags,
                        runtime,
                    )
                    .await?;
                }
                None => {
                    // Auto-managed mesh: create fresh, run test, divergence check, conditionally delete.
                    // Pass = test passed AND nodes are consistent. Either failure leaves the mesh up.
                    let auto_mesh_id = get_next_mesh_id(&docker).await?;
                    println!(
                        "Auto-creating mesh {} with {} nodes for test",
                        auto_mesh_id, auto_nodes
                    );
                    // Genesis-seeded config some tests require (lands in the
                    // forwarded HOPNET_GENESIS_* env at container creation).
                    if let Some(name) = test.as_deref() {
                        for (k, v) in tests::mesh_creation_env(name) {
                            println!("Seeding mesh-creation env {}={}", k, v);
                            // SAFETY: single-threaded CLI setup phase — no
                            // concurrent env readers yet.
                            unsafe { std::env::set_var(k, v) };
                        }
                    }
                    let node_count = test
                        .as_deref()
                        .and_then(tests::preferred_auto_nodes)
                        .unwrap_or(*auto_nodes);
                    create_mesh(&docker, auto_mesh_id, node_count, false, runtime).await?;

                    let test_result = tests::handle_test_command(
                        &docker,
                        auto_mesh_id,
                        test.as_deref(),
                        *list,
                        flags,
                        runtime,
                    )
                    .await;

                    if let Err(e) = &test_result {
                        eprintln!(
                            "\nTest failed; leaving mesh {} up for inspection",
                            auto_mesh_id
                        );
                        eprintln!("Inspect: orchestrator status --mesh-id {}", auto_mesh_id);
                        eprintln!(
                            "Inspect: orchestrator divergence --mesh-id {}",
                            auto_mesh_id
                        );
                        eprintln!(
                            "Clean up: orchestrator delete --mesh-id {} -y",
                            auto_mesh_id
                        );
                        return Err(anyhow::anyhow!("Test failed: {}", e));
                    }

                    // Test passed — verify no divergence before declaring success.
                    println!("\nTest passed; checking for state divergence across nodes");
                    let div_result =
                        divergence::check_divergence(&docker, auto_mesh_id, runtime).await;

                    match div_result {
                        Ok(()) => {
                            if *keep_on_pass {
                                println!(
                                    "\nNo divergence; --keep-on-pass set, leaving mesh {} up",
                                    auto_mesh_id
                                );
                            } else {
                                println!(
                                    "\nNo divergence; deleting auto-created mesh {}",
                                    auto_mesh_id
                                );
                                delete_mesh(&docker, auto_mesh_id, true).await?;
                            }
                        }
                        Err(e) => {
                            eprintln!(
                                "\nTest passed but divergence detected; leaving mesh {} up for inspection",
                                auto_mesh_id
                            );
                            eprintln!(
                                "Inspect: orchestrator divergence --mesh-id {}",
                                auto_mesh_id
                            );
                            eprintln!(
                                "Clean up: orchestrator delete --mesh-id {} -y",
                                auto_mesh_id
                            );
                            return Err(anyhow::anyhow!("Divergence detected after test: {}", e));
                        }
                    }
                }
            }
        }
        Some(Commands::List) | None => {
            list_meshes(&docker).await?;
        }
    }

    Ok(())
}

async fn get_next_mesh_id(docker: &Docker) -> Result<u32> {
    // Get all existing mesh IDs
    let networks = docker
        .list_networks(None::<bollard::query_parameters::ListNetworksOptions>)
        .await?;

    let mut mesh_ids: Vec<u32> = Vec::new();

    // Find this checkout's networks and extract mesh IDs
    for network in &networks {
        if let Some(ref name) = network.name
            && let Some(mesh_id) = naming::mesh_id_of(name)
        {
            mesh_ids.push(mesh_id);
        }
    }

    mesh_ids.sort();

    // Find the next available ID (starting from 0)
    let mut next_id = 0;
    for &id in &mesh_ids {
        if id == next_id {
            next_id += 1;
        } else {
            break;
        }
    }

    Ok(next_id)
}

async fn list_meshes(docker: &Docker) -> Result<()> {
    println!("HopNet Orchestrator - Listing Meshes");
    println!(
        "Namespace: {}*  (image {})",
        naming::prefix(),
        naming::image_ref()
    );

    // List this checkout's networks
    let networks = docker
        .list_networks(None::<bollard::query_parameters::ListNetworksOptions>)
        .await?;

    let mut meshes: HashMap<u32, Vec<String>> = HashMap::new();

    // Find this checkout's networks and extract mesh IDs
    for network in &networks {
        if let Some(ref name) = network.name
            && let Some(mesh_id) = naming::mesh_id_of(name)
        {
            meshes.entry(mesh_id).or_default();
        }
    }

    // List all containers to count nodes per mesh
    let options = ListContainersOptionsBuilder::default()
        .all(false) // Only running containers
        .build();

    let containers = docker.list_containers(Some(options)).await?;

    // Count containers per mesh
    for container in containers {
        if let Some(names) = &container.names {
            for name in names {
                if let Some(mesh_id) = naming::mesh_id_of(name) {
                    meshes.entry(mesh_id).or_default().push(name.clone());
                }
            }
        }
    }

    if meshes.is_empty() {
        println!("No HopNet meshes found.");
        println!(
            "\nTo create a mesh, use: cargo run --bin orchestrator create --mesh-id <ID> --nodes <COUNT>"
        );
    } else {
        println!("Active HopNet Meshes:");
        println!("{:<8} {:<12} Containers", "Mesh ID", "Nodes");
        println!("{}", "-".repeat(40));

        let mut mesh_ids: Vec<_> = meshes.keys().collect();
        mesh_ids.sort();

        for &mesh_id in mesh_ids {
            let containers = &meshes[&mesh_id];
            println!(
                "{:<8} {:<12} {}",
                mesh_id,
                containers.len(),
                containers
                    .iter()
                    .map(|s| &s[1..])
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
    }

    Ok(())
}

/// Clean up containers, volumes, and network for a mesh after a failure.
async fn cleanup_mesh_resources(
    docker: &Docker,
    mesh_id: u32,
    containers: Vec<(String, String, String)>,
    network_id: &str,
) {
    // The relay container isn't in the caller's list — remove it by name.
    {
        let relay = naming::relay_container_name(mesh_id);
        let _ = docker
            .stop_container(
                &relay,
                None::<bollard::query_parameters::StopContainerOptions>,
            )
            .await;
        let _ = docker
            .remove_container(
                &relay,
                None::<bollard::query_parameters::RemoveContainerOptions>,
            )
            .await;
    }

    // Stop and remove containers in parallel
    let mut tasks = Vec::new();
    for (name, container_id, _) in containers {
        let docker_clone = docker.clone();
        let task = tokio::spawn(async move {
            println!("  Stopping and removing container: {}", name);
            let _ = docker_clone
                .stop_container(
                    &container_id,
                    None::<bollard::query_parameters::StopContainerOptions>,
                )
                .await;
            let _ = docker_clone
                .remove_container(
                    &container_id,
                    None::<bollard::query_parameters::RemoveContainerOptions>,
                )
                .await;
        });
        tasks.push(task);
    }
    for task in tasks {
        let _ = task.await;
    }

    // Remove volumes
    if let Ok(volumes) = docker
        .list_volumes(None::<bollard::query_parameters::ListVolumesOptions>)
        .await
        && let Some(volume_list) = volumes.volumes
    {
        let mut tasks = Vec::new();
        for volume in volume_list {
            // Both labels must match: mesh id AND this checkout's hash —
            // another checkout's mesh 0 volumes carry a different hash (and
            // pre-namespace volumes carry none), so they are untouchable.
            let is_mesh_volume = volume
                .labels
                .get("hopnet.mesh_id")
                .map(|id| id == &mesh_id.to_string())
                .unwrap_or(false)
                && volume
                    .labels
                    .get(naming::CHECKOUT_LABEL)
                    .map(String::as_str)
                    == Some(naming::checkout_hash());
            if is_mesh_volume {
                let volume_name = volume.name.clone();
                let docker_clone = docker.clone();
                let task = tokio::spawn(async move {
                    println!("  Removing volume: {}", volume_name);
                    let _ = docker_clone
                        .remove_volume(
                            &volume_name,
                            None::<bollard::query_parameters::RemoveVolumeOptions>,
                        )
                        .await;
                });
                tasks.push(task);
            }
        }
        for task in tasks {
            let _ = task.await;
        }
    }

    // Remove network
    println!("  Removing network: {}", network_id);
    let _ = docker.remove_network(network_id).await;
}

/// Wait for mesh-initiated seating to converge (RFC-CONSENSUS-002 S5):
/// nodes never request seats, so a fresh mesh forms only as the validators'
/// seat-proposal scan runs. Poll node 0's validator count until it is stable
/// across two reads (the seating fixpoint reached) or the timeout elapses.
/// Robust to the profile's parity (odd majority meshes seat all N; even seat
/// N-1 with one pooled spare) without reimplementing the math here.
async fn wait_for_formation(
    docker: &Docker,
    mesh_id: u32,
    runtime: sys::ContainerRuntime,
    timeout: std::time::Duration,
) -> Result<u32> {
    let client = crate::insecure_client();
    let token = get_jwt_token(docker, mesh_id, 0, runtime).await?;
    let addrs = get_external_addresses(docker, mesh_id, runtime).await?;
    let (host, port) = addrs
        .iter()
        .find(|(id, _, _)| *id == 0)
        .map(|(_, h, p)| (h.clone(), *p))
        .ok_or_else(|| anyhow::anyhow!("node 0 address"))?;
    let url = format!("https://{}:{}/api/consensus/view", host, port);

    let start = std::time::Instant::now();
    let mut prev: Option<u32> = None;
    while start.elapsed() < timeout {
        // Query the pending height's validator set via the debug view.
        if let Ok(resp) = client
            .post(&url)
            .header("Authorization", format!("Bearer {}", token))
            // `i64::MAX`, not `u64::MAX`: heights map onto SQLite INTEGER
            // by lossless bit-cast, so anything above i64::MAX lands
            // NEGATIVE and the validator CTE's `effective_height <= ?`
            // matches ZERO rows — every height is >= 0. With u64::MAX this
            // probe never saw a validator, so it burned its full timeout on
            // every mesh creation and reported 0 seated.
            // `src/db/consensus.rs` pins i64::MAX as the safe ceiling.
            .json(&(i64::MAX as u64)) // any height >= tip resolves to the latest set
            .timeout(std::time::Duration::from_secs(5))
            .send()
            .await
            && let Ok(doc) = resp.json::<serde_json::Value>().await
        {
            let count = doc["validators_at_height"]
                .as_array()
                .map(|a| a.len() as u32)
                .unwrap_or(0);
            if count >= 2 && prev == Some(count) {
                println!("Mesh {} seated to {} validators", mesh_id, count);
                return Ok(count);
            }
            prev = Some(count);
        }
        tokio::time::sleep(std::time::Duration::from_secs(4)).await;
    }
    // Non-fatal: tests do their own baseline waits; report what we saw.
    Ok(prev.unwrap_or(0))
}

async fn create_mesh(
    docker: &Docker,
    mesh_id: u32,
    node_count: u32,
    no_cleanup: bool,
    runtime: sys::ContainerRuntime,
) -> Result<()> {
    println!("Creating mesh {} with {} nodes", mesh_id, node_count);

    // Create network for the mesh
    let network_name = naming::network_name(mesh_id);

    match create_hopnet_network(docker, &network_name).await {
        Ok(network_id) => {
            println!("Successfully created network: {}", network_id);

            let mut containers: Vec<(String, String, String)> = Vec::new(); // (container_name, container_id, ip_address)

            // The mesh's self-hosted iroh relay comes up FIRST — nodes bind
            // their endpoints against it at startup.
            if let Err(e) = create_relay_container(docker, mesh_id, &network_name).await {
                println!("Failed to start relay container: {}", e);
                if !no_cleanup {
                    cleanup_mesh_resources(docker, mesh_id, containers, &network_id).await;
                }
                return Err(anyhow::anyhow!("Mesh creation failed: relay container"));
            }

            // Create containers for each node
            for node_id in 0..node_count {
                let container_name = naming::container_name(mesh_id, node_id);
                println!("Creating HopNet container: {}", container_name);

                match create_hopnet_container(
                    docker,
                    mesh_id,
                    node_id,
                    &container_name,
                    &network_name,
                    runtime,
                )
                .await
                {
                    Ok((container_id, ip_address)) => {
                        println!(
                            "Successfully created container: {} with IP: {}",
                            container_id, ip_address
                        );
                        containers.push((container_name, container_id, ip_address));
                    }
                    Err(e) => {
                        println!("Failed to create container {}: {}", container_name, e);
                    }
                }
            }

            // Wait a moment for containers to be ready
            println!("Waiting for containers to be ready...");
            tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

            // Setup node 0 if it exists
            if let Some((container_name, _container_id, ip_address)) = containers.first() {
                // Host port is not derivable here — the bind-scan fallback
                // may have shifted it; the actual value lives in the
                // hopnet.host_port container label.
                println!("Setting up node 0 at IP: {}", ip_address);
                match setup_node_0(docker, mesh_id, container_name, runtime).await {
                    Ok(passphrase) => {
                        store_mesh_passphrase(docker, mesh_id, &passphrase).await?;
                    }
                    Err(e) => {
                        println!("Failed to setup node 0: {}", e);

                        if no_cleanup {
                            println!(
                                "Skipping cleanup (--no-cleanup flag set). Containers and network left running for inspection."
                            );
                            println!("Network: {}", network_name);
                            for (name, container_id, ip) in &containers {
                                println!("Container: {} (ID: {}, IP: {})", name, container_id, ip);
                            }
                        } else {
                            println!("Cleaning up mesh {} due to setup failure...", mesh_id);

                            cleanup_mesh_resources(docker, mesh_id, containers, &network_id).await;
                        }

                        return Err(anyhow::anyhow!(
                            "Mesh creation failed due to setup API failure"
                        ));
                    }
                }

                // Register additional nodes (1, 2, 3...) with node 0.
                // RFC-025 S5 ORDERING: the mesh code must be adopted on
                // each fresh container BEFORE POST /nodes — the
                // coordinator's ping probe is an iroh dial, and a fresh
                // endpoint is TLS-dead until adoption. The code exists
                // only after node 0's genesis, which is why it cannot
                // ride the container env (all containers are created
                // before setup_node_0 runs).
                if containers.len() > 1 {
                    let mesh_code = match get_mesh_code(docker, mesh_id, runtime).await {
                        Ok(Some(c)) => {
                            println!("Mesh code: {}", c);
                            Some(c)
                        }
                        // A mesh born on a pre-enforcement image (the
                        // RFC-020 cutover rehearsal): no join-code
                        // channel exists and none is needed — old fresh
                        // nodes bind legacy immediately.
                        Ok(None) => {
                            println!(
                                "Coordinator predates the join-code channel; registering directly"
                            );
                            None
                        }
                        Err(e) => {
                            println!("Failed to read the mesh code from node 0: {}", e);
                            if !no_cleanup {
                                cleanup_mesh_resources(docker, mesh_id, containers, &network_id)
                                    .await;
                            }
                            return Err(anyhow::anyhow!("Mesh creation failed: no mesh code"));
                        }
                    };
                    for (node_index, (container_name, _container_id, _node_ip)) in
                        containers.iter().enumerate().skip(1)
                    {
                        let node_id = node_index as u32; // node_id starts from 1 for additional nodes
                        println!(
                            "Registering node {} ({}) with node 0...",
                            node_id, container_name
                        );

                        if let Err(e) = async {
                            if let Some(code) = &mesh_code {
                                submit_join_code(docker, mesh_id, node_id, runtime, code).await?;
                            }
                            register_node_with_node_0(
                                docker,
                                mesh_id,
                                node_id,
                                container_name,
                                runtime,
                            )
                            .await
                        }
                        .await
                        {
                            println!("Failed to register node {}: {}", node_id, e);

                            if no_cleanup {
                                println!(
                                    "Skipping cleanup (--no-cleanup flag set). Containers and network left running for inspection."
                                );
                                println!("Network: {}", network_name);
                                for (name, container_id, ip) in &containers {
                                    println!(
                                        "Container: {} (ID: {}, IP: {})",
                                        name, container_id, ip
                                    );
                                }
                            } else {
                                println!(
                                    "Cleaning up mesh {} due to node registration failure...",
                                    mesh_id
                                );
                                cleanup_mesh_resources(docker, mesh_id, containers, &network_id)
                                    .await;
                            }

                            return Err(anyhow::anyhow!(
                                "Mesh creation failed due to node registration failure"
                            ));
                        }
                    }
                }
            }

            // Wait for mesh-initiated seating to converge before returning
            // (nodes no longer self-request; the mesh seats them).
            if node_count > 1 {
                let _ = wait_for_formation(
                    docker,
                    mesh_id,
                    runtime,
                    std::time::Duration::from_secs(120),
                )
                .await;
            }
        }
        Err(e) => {
            println!("Failed to create network: {}", e);
        }
    }

    Ok(())
}

pub(crate) async fn add_nodes_to_mesh(
    docker: &Docker,
    mesh_id: u32,
    node_count: u32,
    runtime: sys::ContainerRuntime,
) -> Result<()> {
    // Get existing containers to find the next node_id
    let existing_containers = get_mesh_containers(docker, mesh_id).await?;

    if existing_containers.is_empty() {
        return Err(anyhow::anyhow!(
            "Mesh {} does not exist. Create it first with 'create --nodes N'",
            mesh_id
        ));
    }

    // Find the highest existing node_id
    let mut max_node_id = 0u32;
    for container in &existing_containers {
        if let Some(names) = &container.names {
            for name in names {
                if let Some(node_id) = naming::node_id_of(name) {
                    max_node_id = max_node_id.max(node_id);
                }
            }
        }
    }

    let starting_node_id = max_node_id + 1;
    println!("Next available node_id: {}", starting_node_id);

    // Get the network name for this mesh
    let network_name = naming::network_name(mesh_id);

    // Verify network exists
    let networks = get_mesh_networks(docker, mesh_id).await?;
    if networks.is_empty() {
        return Err(anyhow::anyhow!("Network for mesh {} not found", mesh_id));
    }

    // Create new containers
    let mut new_containers: Vec<(String, String, String)> = Vec::new(); // (name, id, ip)

    for i in 0..node_count {
        let node_id = starting_node_id + i;
        let container_name = naming::container_name(mesh_id, node_id);
        println!("Creating container: {}", container_name);

        match create_hopnet_container(
            docker,
            mesh_id,
            node_id,
            &container_name,
            &network_name,
            runtime,
        )
        .await
        {
            Ok((container_id, ip_address)) => {
                println!(
                    "Successfully created container: {} with IP: {}",
                    container_id, ip_address
                );
                new_containers.push((container_name, container_id, ip_address));
            }
            Err(e) => {
                println!("Failed to create container {}: {}", container_name, e);
                return Err(e);
            }
        }
    }

    // Wait for containers to be ready
    println!("Waiting for containers to be ready...");
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    // The mesh code (RFC-025 S5): adopted on each fresh container BEFORE
    // registration — the coordinator's ping probe cannot complete TLS
    // against a code-less endpoint. Same HTTP seam as create_mesh (one
    // code path, deliberately not env).
    let mesh_code = get_mesh_code(docker, mesh_id, runtime).await?;
    match &mesh_code {
        Some(code) => println!("Mesh code: {}", code),
        None => println!("Coordinator predates the join-code channel; registering directly"),
    }

    // Register new nodes with node 0 (triggers catch-up based bootstrap)
    for (container_name, _container_id, _node_ip) in &new_containers {
        let node_id: u32 = naming::node_id_of(container_name)
            .expect("container_name was built by naming::container_name");

        println!(
            "Registering node {} ({}) with node 0...",
            node_id, container_name
        );

        if let Some(code) = &mesh_code {
            submit_join_code(docker, mesh_id, node_id, runtime, code).await?;
        }
        if let Err(e) =
            register_node_with_node_0(docker, mesh_id, node_id, container_name, runtime).await
        {
            println!("Failed to register node {}: {}", node_id, e);
            return Err(anyhow::anyhow!("Node registration failed: {}", e));
        }

        println!(
            "Successfully registered node {}. Bootstrap via catch-up initiated.",
            node_id
        );
    }

    println!(
        "Successfully added {} node(s) to mesh {}",
        node_count, mesh_id
    );
    Ok(())
}

async fn create_hopnet_network(docker: &Docker, network_name: &str) -> Result<String> {
    // Use the simple create_network method with just name
    let options = bollard::models::NetworkCreateRequest {
        name: network_name.to_string(),
        ..Default::default()
    };

    let response: NetworkCreateResponse = docker.create_network(options).await?;

    println!("Created network {} with ID: {}", network_name, response.id);

    Ok(response.id)
}

/// Load the nix-built gzipped docker-archive, rewriting its embedded
/// `RepoTags` (always `hopnet:latest` from the flake) to this checkout's
/// [`naming::image_ref`] on the fly. The rewritten tar streams straight
/// into the runtime's /images/load, so no shared intermediate tag ever
/// exists and concurrent loads from other checkouts cannot clobber each
/// other. Peak memory: the ~1 KB manifest plus the channel buffer.
async fn load_image(docker: &Docker, archive: &std::path::Path) -> Result<()> {
    let image_ref = naming::image_ref();
    println!("Loading {} as {}", archive.display(), image_ref);

    let (tx, rx) = tokio::sync::mpsc::channel::<bytes::Bytes>(16);

    let archive = archive.to_path_buf();
    let new_tag = image_ref.clone();
    let rewriter = tokio::task::spawn_blocking(move || -> Result<()> {
        struct ChanWriter(tokio::sync::mpsc::Sender<bytes::Bytes>);
        impl std::io::Write for ChanWriter {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0
                    .blocking_send(bytes::Bytes::copy_from_slice(buf))
                    .map_err(|_| std::io::Error::from(std::io::ErrorKind::BrokenPipe))?;
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let file = std::fs::File::open(&archive)?;
        let mut input = tar::Archive::new(flate2::read::GzDecoder::new(file));
        // Emit an UNCOMPRESSED tar — /images/load auto-detects the format,
        // and skipping re-gzip keeps the rewrite CPU-cheap.
        let mut out = tar::Builder::new(ChanWriter(tx));
        for entry in input.entries()? {
            let mut entry = entry?;
            let path = entry.path()?.into_owned();
            let mut header = entry.header().clone();
            if path == std::path::Path::new("manifest.json") {
                let mut buf = Vec::new();
                std::io::Read::read_to_end(&mut entry, &mut buf)?;
                let mut manifest: serde_json::Value = serde_json::from_slice(&buf)?;
                for item in manifest.as_array_mut().into_iter().flatten() {
                    item["RepoTags"] = json!([new_tag]);
                }
                let buf = serde_json::to_vec(&manifest)?;
                header.set_size(buf.len() as u64);
                header.set_cksum();
                out.append_data(&mut header, &path, buf.as_slice())?;
            } else {
                out.append_data(&mut header, &path, &mut entry)?;
            }
        }
        out.finish()?;
        Ok(())
    });

    let body = bollard::body_stream(tokio_stream::wrappers::ReceiverStream::new(rx));
    let options = bollard::query_parameters::ImportImageOptionsBuilder::default()
        .quiet(false)
        .build();
    let mut responses = docker.import_image(options, body, None);
    while let Some(msg) = responses.next().await {
        if let Some(s) = msg?.stream {
            print!("{}", s);
        }
    }
    rewriter.await??;

    println!("Image ready: {}  (prefix {})", image_ref, naming::prefix());
    Ok(())
}

/// Start the mesh's self-hosted iroh relay: the hopnet image with an
/// entrypoint override running `iroh-relay --dev` (plain HTTP on :3340).
/// Nodes get HOPNET_RELAY_URL pointing here — no n0 public relay or DNS
/// discovery dependency inside meshes (those services rate-limit under
/// repeated mesh churn and flake mesh creation).
async fn create_relay_container(
    docker: &Docker,
    mesh_id: u32,
    network_name: &str,
) -> Result<String> {
    let container_name = naming::relay_container_name(mesh_id);

    // The relay's :3340 is also host-mapped (like the nodes' HTTP port):
    // rootless podman blocks host->container-IP, and the retired-dialer
    // gate (RFC-025 S6) runs a raw iroh endpoint ON THE HOST that must
    // reach mesh members through this relay. Pseudo node id 499 keeps the
    // relay's port at the top of the mesh's 500-port range.
    let runtime = sys::detect_runtime(docker).await?;
    let needs_port_binding = runtime == sys::ContainerRuntime::Podman || cfg!(target_os = "macos");
    let (port_bindings, relay_host_port) = if needs_port_binding {
        let port = sys::find_available_port(mesh_id, 499).await?;
        let mut bindings = HashMap::new();
        bindings.insert(
            "3340/tcp".to_string(),
            Some(vec![bollard::models::PortBinding {
                host_ip: Some("0.0.0.0".to_string()),
                host_port: Some(port.to_string()),
            }]),
        );
        (Some(bindings), port)
    } else {
        (None, 3340)
    };

    let mut endpoints_config = HashMap::new();
    endpoints_config.insert(
        network_name.to_string(),
        EndpointSettings {
            ip_address: None,
            ..Default::default()
        },
    );

    let mut labels = HashMap::new();
    labels.insert("hopnet.mesh_id".to_string(), mesh_id.to_string());
    labels.insert("hopnet.role".to_string(), "relay".to_string());
    labels.insert("hopnet.host_port".to_string(), relay_host_port.to_string());
    labels.insert(
        naming::CHECKOUT_LABEL.to_string(),
        naming::checkout_hash().to_string(),
    );

    let config = ContainerCreateBody {
        image: Some(naming::image_ref()),
        entrypoint: Some(vec!["iroh-relay".to_string(), "--dev".to_string()]),
        labels: Some(labels),
        networking_config: Some(NetworkingConfig {
            endpoints_config: Some(endpoints_config),
        }),
        host_config: Some(HostConfig {
            port_bindings,
            ..Default::default()
        }),
        ..Default::default()
    };

    let options = CreateContainerOptionsBuilder::default()
        .name(&container_name)
        .build();
    let response = docker.create_container(Some(options), config).await?;
    docker
        .start_container(
            &response.id,
            None::<bollard::query_parameters::StartContainerOptions>,
        )
        .await?;

    println!(
        "Started iroh relay {} on network {} ({})",
        container_name,
        network_name,
        naming::relay_url(mesh_id)
    );
    Ok(response.id)
}

pub(crate) async fn create_hopnet_container(
    docker: &Docker,
    mesh_id: u32,
    node_id: u32,
    container_name: &str,
    network_name: &str,
    runtime: sys::ContainerRuntime,
) -> Result<(String, String)> {
    // Port bindings needed for Podman and Docker on macOS (can't access container IPs directly)
    let needs_port_binding = runtime == sys::ContainerRuntime::Podman || cfg!(target_os = "macos");
    let (port_bindings, host_port) = if needs_port_binding {
        // Find available port with collision detection
        let port = sys::find_available_port(mesh_id, node_id).await?;
        let mut bindings = HashMap::new();
        bindings.insert(
            "34632/tcp".to_string(),
            Some(vec![bollard::models::PortBinding {
                host_ip: Some("0.0.0.0".to_string()),
                host_port: Some(port.to_string()),
            }]),
        );
        (Some(bindings), port)
    } else {
        // Docker on Linux: no port mapping needed, we access container IPs directly
        (None, 34632)
    };

    // Network configuration - attach to our custom network
    let mut endpoints_config = HashMap::new();
    endpoints_config.insert(
        network_name.to_string(),
        EndpointSettings {
            ip_address: None, // Let Docker/Podman assign IP
            ..Default::default()
        },
    );

    // Container labels for tracking
    let mut labels = HashMap::new();
    labels.insert("hopnet.mesh_id".to_string(), mesh_id.to_string());
    labels.insert("hopnet.node_id".to_string(), node_id.to_string());
    labels.insert("hopnet.host_port".to_string(), host_port.to_string());
    labels.insert(
        naming::CHECKOUT_LABEL.to_string(),
        naming::checkout_hash().to_string(),
    );

    // Create a named volume for persistent storage (matches container naming pattern)
    let volume_name = naming::volume_name(mesh_id, node_id);

    // Create volume if it doesn't exist (idempotent operation)
    let mut vol_labels = HashMap::new();
    vol_labels.insert("hopnet.mesh_id".to_string(), mesh_id.to_string());
    vol_labels.insert("hopnet.node_id".to_string(), node_id.to_string());
    vol_labels.insert(
        naming::CHECKOUT_LABEL.to_string(),
        naming::checkout_hash().to_string(),
    );

    let volume_config = bollard::models::VolumeCreateOptions {
        name: Some(volume_name.clone()),
        driver: Some("local".to_string()),
        labels: Some(vol_labels),
        ..Default::default()
    };

    match docker.create_volume(volume_config).await {
        Ok(_) => {
            tracing::debug!("Created volume: {}", volume_name);
        }
        Err(e) => {
            // Volume might already exist, which is fine
            tracing::debug!("Volume creation result for {}: {:?}", volume_name, e);
        }
    }

    // Mount the volume to /root/.local/share/hopnet for database and fragment storage
    let binds = vec![format!("{}:/root/.local/share/hopnet", volume_name)];

    // Container configuration
    let config = ContainerCreateBody {
        image: Some(naming::image_ref()),
        labels: Some(labels),
        env: Some({
            let mut e = vec![
                "HOPNET_TEST_MODE=1".to_string(),
                // The image leaves HOME unset, so the node would derive its
                // data dir as /.local/share/hopnet — OUTSIDE the named
                // volume mounted at /root/.local/share/hopnet. Container
                // stop/start masked this (writable-layer persistence); a
                // remove+recreate (the S6 "binary swap") must find the data
                // in the volume.
                "HOME=/root".to_string(),
                // Self-hosted relay: no n0 public relay/discovery dependency.
                format!("HOPNET_RELAY_URL={}", naming::relay_url(mesh_id)),
                // Pin the loopback plaintext port: the endpoint-file seam is
                // disabled under HOPNET_TEST_MODE, so co-resident CLI
                // consumers (hopnet-mount in the mount scenario) need a
                // deterministic in-container port. The network surface stays
                // pinned-HTTPS on 34632.
                "HOPNET_HTTP_PORT=34630".to_string(),
            ];
            // Forward HOPNET_DB_* (pragma tuning), HOPNET_CONSENSUS_*/
            // HOPNET_QUORUM_* (timeouts, quorum profile),
            // HOPNET_GENESIS_* (mesh-creation inputs, e.g. the storage
            // policy seed), and HOPNET_UPGRADE_* (upgrade-provider
            // overrides) from the orchestrator process so tests can
            // configure meshes without rebuilding the image.
            for (k, v) in std::env::vars() {
                if k.starts_with("HOPNET_DB_")
                    || k.starts_with("HOPNET_CONSENSUS_")
                    || k.starts_with("HOPNET_QUORUM_")
                    || k.starts_with("HOPNET_GENESIS_")
                    || k.starts_with("HOPNET_UPGRADE_")
                    || k.starts_with("HOPNET_RESTART_")
                {
                    e.push(format!("{}={}", k, v));
                }
            }
            // Default consensus-policy seed (RFC-CONSENSUS-002 S5): mesh
            // FORMATION under mesh-initiated seating needs a small s_full
            // (the v=1 formation batch is exposed) and p_prove (stacked
            // batches need proven members). Only when a test hasn't set its
            // own HOPNET_GENESIS_CONSENSUS_POLICY. NOT probe_base/grace —
            // t_out stays >= 65s so stop/start tests keep kill windows.
            if std::env::var("HOPNET_GENESIS_CONSENSUS_POLICY").is_err() {
                e.push("HOPNET_GENESIS_CONSENSUS_POLICY=s_full=6;p_prove=6".to_string());
            }
            e
        }),
        networking_config: Some(NetworkingConfig {
            endpoints_config: Some(endpoints_config),
        }),
        host_config: Some(HostConfig {
            port_bindings,
            binds: Some(binds),
            // FUSE support for the mount-cross-node-consistency test: the
            // hopnet-mount daemon mounts /hopdrive inside the container.
            // SYS_ADMIN covers mount(2) as container root and the kernel's
            // backing-fd capability check, so passthrough (RFC-018 S9) can
            // arm in-container. Purely additive for every other test.
            devices: Some(vec![bollard::models::DeviceMapping {
                path_on_host: Some("/dev/fuse".to_string()),
                path_in_container: Some("/dev/fuse".to_string()),
                cgroup_permissions: Some("rwm".to_string()),
            }]),
            cap_add: Some(vec!["SYS_ADMIN".to_string()]),
            ..Default::default()
        }),
        ..Default::default()
    };

    // Create container
    let options = CreateContainerOptionsBuilder::default()
        .name(container_name)
        .build();

    let response = docker.create_container(Some(options), config).await?;

    let container_id = response.id;

    // Start the container
    docker
        .start_container(
            &container_id,
            None::<bollard::query_parameters::StartContainerOptions>,
        )
        .await?;

    // Get the container's IP address
    let container_info = docker
        .inspect_container(
            &container_id,
            None::<bollard::query_parameters::InspectContainerOptions>,
        )
        .await?;
    let ip_address = container_info
        .network_settings
        .and_then(|ns| ns.networks)
        .and_then(|networks| networks.get(network_name).cloned())
        .and_then(|endpoint| endpoint.ip_address)
        .unwrap_or_else(|| "unknown".to_string());

    println!(
        "Started container {} on network {} with IP {}",
        container_name, network_name, ip_address
    );

    Ok((container_id, ip_address))
}

async fn setup_node_0(
    docker: &Docker,
    mesh_id: u32,
    node_name: &str,
    runtime: sys::ContainerRuntime,
) -> Result<String> {
    let client = crate::insecure_client();

    // Get runtime-aware connection info for node 0
    let addresses = get_external_addresses(docker, mesh_id, runtime).await?;
    let (host, port) = addresses
        .iter()
        .find(|(id, _, _)| *id == 0)
        .map(|(_, h, p)| (h.clone(), *p))
        .ok_or_else(|| anyhow::anyhow!("Node 0 not found"))?;

    let url = format!("https://{}:{}/api/setup", host, port);

    let setup_data = json!({
        "username": "allison",
        "node_name": node_name,
    });

    println!("Calling setup API at: {}", url);
    println!("Setup data: {}", setup_data);

    let start_time = std::time::Instant::now();
    let timeout_duration = std::time::Duration::from_secs(30);
    let retry_interval = std::time::Duration::from_millis(500); // 500ms between retries

    loop {
        if start_time.elapsed() > timeout_duration {
            return Err(anyhow::anyhow!("Setup API call timed out after 30 seconds"));
        }

        println!(
            "Attempting setup API call... (elapsed: {:.1}s)",
            start_time.elapsed().as_secs_f32()
        );

        match client
            .post(&url)
            .json(&setup_data)
            .timeout(tokio::time::Duration::from_secs(15))
            .send()
            .await
        {
            Ok(response) => {
                let status = response.status();
                let response_text = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "No response body".to_string());

                if status == reqwest::StatusCode::CREATED {
                    println!("Node setup successful: {} Created", status);
                    println!("Response: {}", response_text);
                    // Parse passphrase from JSON response
                    let body: serde_json::Value = serde_json::from_str(&response_text)
                        .map_err(|e| anyhow::anyhow!("Failed to parse setup response: {}", e))?;
                    let passphrase = body
                        .get("passphrase")
                        .and_then(|p| p.as_str())
                        .ok_or_else(|| anyhow::anyhow!("No passphrase in setup response"))?
                        .to_string();
                    return Ok(passphrase);
                } else if status.is_server_error() || status.is_client_error() {
                    println!("Setup failed with status: {} - {}", status, response_text);
                    if start_time.elapsed() + retry_interval > timeout_duration {
                        return Err(anyhow::anyhow!(
                            "Setup API call failed with status: {}",
                            status
                        ));
                    }
                } else {
                    println!("Unexpected status: {} - {}", status, response_text);
                    if start_time.elapsed() + retry_interval > timeout_duration {
                        return Err(anyhow::anyhow!(
                            "Setup API call returned unexpected status: {}",
                            status
                        ));
                    }
                }
            }
            Err(e) => {
                println!("Setup API request failed: {} (retrying...)", e);
                if start_time.elapsed() + retry_interval > timeout_duration {
                    return Err(anyhow::anyhow!(
                        "Setup API call failed after retries: {}",
                        e
                    ));
                }
            }
        }

        // Wait before retrying
        tokio::time::sleep(retry_interval).await;
    }
}

pub async fn get_jwt_token(
    docker: &Docker,
    mesh_id: u32,
    node_id: u32,
    runtime: sys::ContainerRuntime,
) -> Result<String> {
    let client = crate::insecure_client();

    // Get runtime-aware connection info for this node
    let addresses = get_external_addresses(docker, mesh_id, runtime).await?;
    let (host, port) = addresses
        .iter()
        .find(|(id, _, _)| *id == node_id)
        .map(|(_, h, p)| (h.clone(), *p))
        .ok_or_else(|| anyhow::anyhow!("Node {} not found", node_id))?;

    let login_url = format!("https://{}:{}/api/login", host, port);

    let passphrase = load_mesh_passphrase(docker, mesh_id).await?;

    let login_data = json!({
        "username": "allison",
        "passphrase": passphrase
    });

    let start_time = std::time::Instant::now();
    // Login uses 1 GiB Argon2id key unwrap (3-5s per attempt)
    let timeout_duration = std::time::Duration::from_secs(30);
    let retry_interval = std::time::Duration::from_secs(2);

    loop {
        if start_time.elapsed() > timeout_duration {
            return Err(anyhow::anyhow!("Login request timed out after 30 seconds"));
        }

        if let Ok(response) = client
            .post(&login_url)
            .json(&login_data)
            .timeout(tokio::time::Duration::from_secs(15))
            .send()
            .await
        {
            let status = response.status();
            if status == reqwest::StatusCode::OK
                && let Ok(body) = response.json::<serde_json::Value>().await
                && let Some(token) = body.get("token").and_then(|t| t.as_str())
                && !token.is_empty()
            {
                return Ok(token.to_string());
            }
        }

        if start_time.elapsed() + retry_interval > timeout_duration {
            return Err(anyhow::anyhow!("Login failed after retries"));
        }

        tokio::time::sleep(retry_interval).await;
    }
}

/// Read the coordinator's mesh code (RFC-025 S5) from the JWT-protected
/// regenesis-status view. Exists only after node 0's genesis.
/// Read the coordinator's mesh code. `Ok(None)` means the status view has
/// NO `mesh_code` field at all — a pre-enforcement (pre-RFC-025) image,
/// which has no join-code channel to feed; the caller registers directly
/// (old fresh nodes bind legacy immediately, no TLS-dead window). A
/// present-but-null field is transient (pre-genesis) and stays an error.
pub(crate) async fn get_mesh_code(
    docker: &Docker,
    mesh_id: u32,
    runtime: sys::ContainerRuntime,
) -> Result<Option<String>> {
    let token = get_jwt_token(docker, mesh_id, 0, runtime).await?;
    let addresses = get_external_addresses(docker, mesh_id, runtime).await?;
    let (host, port) = addresses
        .iter()
        .find(|(id, _, _)| *id == 0)
        .map(|(_, h, p)| (h.clone(), *p))
        .ok_or_else(|| anyhow::anyhow!("Node 0 not found"))?;
    let url = format!("https://{}:{}/api/views/regenesis-status", host, port);
    let client = crate::insecure_client();
    let body: serde_json::Value = client
        .get(&url)
        .bearer_auth(&token)
        .timeout(tokio::time::Duration::from_secs(15))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    match body.get("mesh_code") {
        None => Ok(None),
        Some(v) => v
            .as_str()
            .map(|s| Some(s.to_string()))
            .ok_or_else(|| anyhow::anyhow!("coordinator has no mesh code yet (pre-genesis?)")),
    }
}

/// The relay's HOST-reachable URL. Podman (rootless: no host->container
/// IP route) and macOS use the host-mapped port; Docker on Linux reaches
/// the relay's network IP directly.
pub(crate) async fn relay_host_url(
    docker: &Docker,
    mesh_id: u32,
    runtime: sys::ContainerRuntime,
) -> Result<String> {
    let name = naming::relay_container_name(mesh_id);
    let info = docker
        .inspect_container(
            &name,
            None::<bollard::query_parameters::InspectContainerOptions>,
        )
        .await?;
    let use_host_port = runtime == sys::ContainerRuntime::Podman || cfg!(target_os = "macos");
    if use_host_port {
        let port = info
            .config
            .as_ref()
            .and_then(|c| c.labels.as_ref())
            .and_then(|l| l.get("hopnet.host_port"))
            .ok_or_else(|| anyhow::anyhow!("relay container has no hopnet.host_port label"))?;
        Ok(format!("http://localhost:{port}"))
    } else {
        let ip = info
            .network_settings
            .and_then(|ns| ns.networks)
            .and_then(|n| n.get(&naming::network_name(mesh_id)).cloned())
            .and_then(|e| e.ip_address)
            .ok_or_else(|| anyhow::anyhow!("relay container has no network IP"))?;
        Ok(format!("http://{ip}:3340"))
    }
}

/// Deliver the mesh code to a fresh container (RFC-025 S5): its endpoint
/// is TLS-dead until the code is adopted, so this MUST land before the
/// coordinator's POST /nodes ping probe can succeed. Open pre-setup
/// endpoint; 204 = adopted (idempotent re-POST is fine).
pub(crate) async fn submit_join_code(
    docker: &Docker,
    mesh_id: u32,
    node_id: u32,
    runtime: sys::ContainerRuntime,
    code: &str,
) -> Result<()> {
    let addresses = get_external_addresses(docker, mesh_id, runtime).await?;
    let (host, port) = addresses
        .iter()
        .find(|(id, _, _)| *id == node_id)
        .map(|(_, h, p)| (h.clone(), *p))
        .ok_or_else(|| anyhow::anyhow!("Node {} not found", node_id))?;
    let url = format!("https://{}:{}/api/setup/join-code", host, port);
    let client = crate::insecure_client();
    let start = std::time::Instant::now();
    loop {
        match client
            .post(&url)
            .json(&json!({ "code": code }))
            .timeout(tokio::time::Duration::from_secs(10))
            .send()
            .await
        {
            Ok(resp) if resp.status() == reqwest::StatusCode::NO_CONTENT => return Ok(()),
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                return Err(anyhow::anyhow!(
                    "join-code POST to node {node_id} refused: {status} {body}"
                ));
            }
            Err(e) if start.elapsed() < std::time::Duration::from_secs(15) => {
                tracing::debug!("join-code POST to node {node_id} not ready ({e}); retrying");
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
            Err(e) => return Err(anyhow::anyhow!("join-code POST to node {node_id}: {e}")),
        }
    }
}

async fn register_node_with_node_0(
    docker: &Docker,
    mesh_id: u32,
    node_id: u32,
    node_name: &str,
    runtime: sys::ContainerRuntime,
) -> Result<()> {
    let client = crate::insecure_client();

    // Get runtime-aware connection info for this node
    let addresses = get_external_addresses(docker, mesh_id, runtime).await?;
    let (node_host, node_port) = addresses
        .iter()
        .find(|(id, _, _)| *id == node_id)
        .map(|(_, h, p)| (h.clone(), *p))
        .ok_or_else(|| anyhow::anyhow!("Node {} not found", node_id))?;

    // Step 1: Get the public key from the node's /setup GET route
    let get_setup_url = format!("https://{}:{}/api/setup", node_host, node_port);
    println!("  Getting public key from: {}", get_setup_url);

    let start_time = std::time::Instant::now();
    let timeout_duration = std::time::Duration::from_secs(15);
    let retry_interval = std::time::Duration::from_millis(500);

    let pub_key = loop {
        if start_time.elapsed() > timeout_duration {
            return Err(anyhow::anyhow!(
                "Failed to retrieve public key from node {} after 15 seconds",
                node_id
            ));
        }

        match client
            .get(&get_setup_url)
            .timeout(tokio::time::Duration::from_secs(3))
            .send()
            .await
        {
            Ok(response) => {
                let status = response.status();
                if status == reqwest::StatusCode::NOT_FOUND {
                    // Parse the response to get the public key (it's a plain text string)
                    match response.text().await {
                        Ok(pubkey_raw) => {
                            // Remove quotes if present and trim whitespace
                            let pubkey = pubkey_raw.trim().trim_matches('"');
                            if !pubkey.is_empty() && pubkey.len() == 64 {
                                // ed25519 public keys are 64 hex chars
                                println!("  Retrieved public key: {}", pubkey);
                                break pubkey.to_string();
                            } else {
                                println!("  Invalid pubkey format '{}', retrying...", pubkey_raw);
                            }
                        }
                        Err(e) => {
                            println!("  Failed to read response text: {}, retrying...", e);
                        }
                    }
                } else {
                    println!("  Unexpected status {}, retrying...", status);
                }
            }
            Err(e) => {
                println!("  Failed to get setup info: {}, retrying...", e);
            }
        }

        if start_time.elapsed() + retry_interval > timeout_duration {
            return Err(anyhow::anyhow!(
                "Failed to retrieve public key from node {} after retries",
                node_id
            ));
        }

        tokio::time::sleep(retry_interval).await;
    };

    // Step 2: Get JWT token for authentication with node 0 (after node 0 setup is complete)
    println!("  Getting JWT token for node 0 authentication...");
    let jwt_token = get_jwt_token(docker, mesh_id, 0, runtime).await?; // node 0 has node_id = 0

    // Step 3: Register the node with node 0 via POST /nodes
    let (node_0_host, node_0_port) = addresses
        .iter()
        .find(|(id, _, _)| *id == 0)
        .map(|(_, h, p)| (h.clone(), *p))
        .ok_or_else(|| anyhow::anyhow!("Node 0 not found"))?;
    let register_url = format!("https://{}:{}/api/nodes", node_0_host, node_0_port);
    let node_data = json!({
        "name": node_name,
        "owner": 0,
        "pubkey": pub_key
    });

    println!("  Registering node with node 0 at: {}", register_url);
    println!("  Node data: {}", node_data);

    let start_time = std::time::Instant::now();
    let timeout_duration = std::time::Duration::from_secs(30);
    let retry_interval = std::time::Duration::from_secs(2);

    loop {
        if start_time.elapsed() > timeout_duration {
            return Err(anyhow::anyhow!(
                "Node registration timed out after 30 seconds"
            ));
        }

        match client
            .post(&register_url)
            .header("Authorization", format!("Bearer {}", jwt_token))
            .json(&node_data)
            .timeout(tokio::time::Duration::from_secs(10))
            .send()
            .await
        {
            Ok(response) => {
                let status = response.status();
                let response_text = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "No response body".to_string());

                if status == reqwest::StatusCode::CREATED {
                    println!("  Node registration successful: {} Created", status);
                    println!("  Response: {}", response_text);
                    return Ok(());
                } else if status == reqwest::StatusCode::GATEWAY_TIMEOUT {
                    // 504 means iroh ping timed out — node discovery may still be in progress
                    println!("  Registration returned 504 (iroh discovery pending, retrying...)");
                } else {
                    println!(
                        "  Registration failed with status: {} - {}",
                        status, response_text
                    );
                    return Err(anyhow::anyhow!(
                        "Node registration failed with status: {} - {}",
                        status,
                        response_text
                    ));
                }
            }
            Err(e) => {
                println!("  Registration request failed: {} (retrying...)", e);
                if start_time.elapsed() + retry_interval > timeout_duration {
                    return Err(anyhow::anyhow!(
                        "Node registration failed after retries: {}",
                        e
                    ));
                }
            }
        }

        tokio::time::sleep(retry_interval).await;
    }
}

async fn delete_mesh(docker: &Docker, mesh_id: u32, skip_confirmation: bool) -> Result<()> {
    println!("Deleting mesh {}", mesh_id);

    // Find all containers and networks for this mesh
    let containers = get_mesh_containers(docker, mesh_id).await?;
    let networks = get_mesh_networks(docker, mesh_id).await?;

    if containers.is_empty() && networks.is_empty() {
        println!("No mesh found with ID {}", mesh_id);
        return Ok(());
    }

    // Show what will be deleted
    println!("Found mesh {} with:", mesh_id);
    if !containers.is_empty() {
        println!("  Containers ({}):", containers.len());
        for container in &containers {
            let name = container
                .names
                .as_ref()
                .and_then(|names| names.first())
                .map(|name| &name[1..]) // Remove leading '/'
                .unwrap_or("unnamed");
            let status = container.status.as_deref().unwrap_or("unknown");
            println!("    - {} ({})", name, status);
        }
    }
    if !networks.is_empty() {
        println!("  Networks ({}):", networks.len());
        for network in &networks {
            let name = network.name.as_deref().unwrap_or("unnamed");
            println!("    - {}", name);
        }
    }

    // Confirmation prompt
    if !skip_confirmation {
        println!(
            "\nThis will permanently delete all containers and networks for mesh {}.",
            mesh_id
        );
        print!("Are you sure? (y/N): ");
        use std::io::{self, Write};
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let input = input.trim().to_lowercase();

        if input != "y" && input != "yes" {
            println!("Deletion cancelled.");
            return Ok(());
        }
    }

    // Delete containers first (in parallel)
    if !containers.is_empty() {
        println!("Stopping and removing containers...");

        let mut tasks = Vec::new();
        for container in containers {
            if let Some(id) = container.id {
                let name = container
                    .names
                    .as_ref()
                    .and_then(|names| names.first())
                    .map(|name| name[1..].to_string()) // Remove leading '/' and own the string
                    .unwrap_or_else(|| "unnamed".to_string());

                let docker_clone = docker.clone();
                let task = tokio::spawn(async move {
                    println!("  Stopping container: {}", name);
                    if let Err(e) = docker_clone
                        .stop_container(
                            &id,
                            None::<bollard::query_parameters::StopContainerOptions>,
                        )
                        .await
                    {
                        println!("    Warning: Failed to stop container {}: {}", name, e);
                    }

                    println!("  Removing container: {}", name);
                    if let Err(e) = docker_clone
                        .remove_container(
                            &id,
                            None::<bollard::query_parameters::RemoveContainerOptions>,
                        )
                        .await
                    {
                        println!("    Warning: Failed to remove container {}: {}", name, e);
                    }
                });
                tasks.push(task);
            }
        }

        // Wait for all container deletions to complete
        for task in tasks {
            let _ = task.await;
        }
    }

    // Delete volumes for this mesh (in parallel)
    println!("Removing volumes...");
    let volumes = docker
        .list_volumes(None::<bollard::query_parameters::ListVolumesOptions>)
        .await?;
    if let Some(volume_list) = volumes.volumes {
        // Mesh-id label alone is ambiguous across checkouts (every checkout
        // has a mesh 0) — the checkout label must match too.
        let mesh_volumes: Vec<_> = volume_list
            .into_iter()
            .filter(|v| {
                v.labels
                    .get("hopnet.mesh_id")
                    .map(|id| id == &mesh_id.to_string())
                    .unwrap_or(false)
                    && v.labels.get(naming::CHECKOUT_LABEL).map(String::as_str)
                        == Some(naming::checkout_hash())
            })
            .collect();

        if !mesh_volumes.is_empty() {
            let mut tasks = Vec::new();
            for volume in mesh_volumes {
                let volume_name = volume.name.clone();
                let docker_clone = docker.clone();
                let task = tokio::spawn(async move {
                    println!("  Removing volume: {}", volume_name);
                    if let Err(e) = docker_clone
                        .remove_volume(
                            &volume_name,
                            None::<bollard::query_parameters::RemoveVolumeOptions>,
                        )
                        .await
                    {
                        println!(
                            "    Warning: Failed to remove volume {}: {}",
                            volume_name, e
                        );
                    }
                });
                tasks.push(task);
            }

            // Wait for all volume deletions to complete
            for task in tasks {
                let _ = task.await;
            }
        }
    }

    // Delete networks (in parallel)
    if !networks.is_empty() {
        println!("Removing networks...");

        let mut tasks = Vec::new();
        for network in networks {
            if let Some(id) = network.id {
                let name = network
                    .name
                    .as_ref()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "unnamed".to_string());

                let docker_clone = docker.clone();
                let task = tokio::spawn(async move {
                    println!("  Removing network: {}", name);
                    if let Err(e) = docker_clone.remove_network(&id).await {
                        println!("    Warning: Failed to remove network {}: {}", name, e);
                    }
                });
                tasks.push(task);
            }
        }

        // Wait for all network deletions to complete
        for task in tasks {
            let _ = task.await;
        }
    }

    println!("Mesh {} deleted successfully.", mesh_id);
    Ok(())
}

async fn get_mesh_containers(
    docker: &Docker,
    mesh_id: u32,
) -> Result<Vec<bollard::models::ContainerSummary>> {
    let options = ListContainersOptionsBuilder::default()
        .all(true) // Include stopped containers
        .build();

    let containers = docker.list_containers(Some(options)).await?;

    let mesh_containers: Vec<_> = containers
        .into_iter()
        .filter(|container| {
            if let Some(names) = &container.names {
                names
                    .iter()
                    .any(|name| naming::mesh_id_of(name) == Some(mesh_id))
            } else {
                false
            }
        })
        .collect();

    Ok(mesh_containers)
}

async fn get_mesh_networks(docker: &Docker, mesh_id: u32) -> Result<Vec<bollard::models::Network>> {
    let networks = docker
        .list_networks(None::<bollard::query_parameters::ListNetworksOptions>)
        .await?;

    let mesh_networks: Vec<_> = networks
        .into_iter()
        .filter(|network| {
            network
                .name
                .as_deref()
                .and_then(naming::mesh_id_of)
                .is_some_and(|id| id == mesh_id)
        })
        .collect();

    Ok(mesh_networks)
}

async fn cleanup_orphaned_networks(docker: &Docker, skip_confirmation: bool) -> Result<()> {
    println!("Scanning for orphaned mesh networks...");

    // Get this checkout's networks
    let networks = docker
        .list_networks(None::<bollard::query_parameters::ListNetworksOptions>)
        .await?;
    let hopnet_networks: Vec<_> = networks
        .into_iter()
        .filter(|network| network.name.as_deref().is_some_and(naming::is_ours))
        .collect();

    if hopnet_networks.is_empty() {
        println!("No HopNet networks found.");
        return Ok(());
    }

    // Get this checkout's containers
    let options = ListContainersOptionsBuilder::default()
        .all(true) // Include stopped containers
        .build();
    let containers = docker.list_containers(Some(options)).await?;
    let hopnet_containers: Vec<_> = containers
        .into_iter()
        .filter(|container| {
            if let Some(names) = &container.names {
                names.iter().any(|name| naming::is_ours(name))
            } else {
                false
            }
        })
        .collect();

    // Build a map of mesh_id -> container count
    let mut mesh_container_counts: std::collections::HashMap<u32, usize> =
        std::collections::HashMap::new();
    for container in &hopnet_containers {
        if let Some(names) = &container.names {
            for name in names {
                if let Some(mesh_id) = naming::mesh_id_of(name) {
                    *mesh_container_counts.entry(mesh_id).or_insert(0) += 1;
                }
            }
        }
    }

    // Find orphaned networks (networks for meshes with 0 containers)
    let mut orphaned_networks = Vec::new();
    for network in &hopnet_networks {
        if let Some(ref name) = network.name
            && let Some(mesh_id) = naming::mesh_id_of(name)
        {
            let container_count = mesh_container_counts.get(&mesh_id).unwrap_or(&0);
            if *container_count == 0 {
                orphaned_networks.push((mesh_id, network));
            }
        }
    }

    if orphaned_networks.is_empty() {
        println!("No orphaned networks found.");
        return Ok(());
    }

    // Group orphaned networks by mesh_id
    let mut orphaned_by_mesh: std::collections::HashMap<u32, Vec<&bollard::models::Network>> =
        std::collections::HashMap::new();
    for (mesh_id, network) in orphaned_networks {
        orphaned_by_mesh.entry(mesh_id).or_default().push(network);
    }

    // Show what will be cleaned up
    println!(
        "Found orphaned networks for {} mesh(es):",
        orphaned_by_mesh.len()
    );
    let mut total_networks = 0;
    for (&mesh_id, networks) in &orphaned_by_mesh {
        println!("  Mesh {} ({} networks):", mesh_id, networks.len());
        for network in networks {
            let name = network.name.as_deref().unwrap_or("unnamed");
            println!("    - {}", name);
            total_networks += 1;
        }
    }

    // Confirmation prompt
    if !skip_confirmation {
        println!(
            "\nThis will permanently delete {} orphaned network(s).",
            total_networks
        );
        print!("Are you sure? (y/N): ");
        use std::io::{self, Write};
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let input = input.trim().to_lowercase();

        if input != "y" && input != "yes" {
            println!("Cleanup cancelled.");
            return Ok(());
        }
    }

    // Delete orphaned networks
    println!("Removing orphaned networks...");
    let mut deleted_count = 0;
    for networks in orphaned_by_mesh.values() {
        for network in networks {
            if let Some(ref id) = network.id {
                let name = network.name.as_deref().unwrap_or("unnamed");
                println!("  Removing network: {}", name);
                match docker.remove_network(id).await {
                    Ok(_) => {
                        deleted_count += 1;
                    }
                    Err(e) => {
                        println!("    Warning: Failed to remove network {}: {}", name, e);
                    }
                }
            }
        }
    }

    println!(
        "Cleanup completed. Removed {} orphaned network(s).",
        deleted_count
    );
    Ok(())
}

async fn show_mesh_status(
    docker: &Docker,
    mesh_id: u32,
    runtime: sys::ContainerRuntime,
) -> Result<()> {
    println!("Mesh {} Status", mesh_id);

    // Get containers for this mesh
    let containers = get_mesh_containers(docker, mesh_id).await?;

    if containers.is_empty() {
        println!("No containers found for mesh {}.", mesh_id);
        return Ok(());
    }

    println!("Found {} node(s):", containers.len());
    println!(
        "{:<8} {:<12} {:<8} {:<12} Phase",
        "Node ID", "Status", "Role", "View"
    );
    println!("{}", "-".repeat(50));

    // Sort containers by node ID
    let mut node_data: Vec<(u32, &bollard::models::ContainerSummary)> = Vec::new();
    for container in &containers {
        if let Some(names) = &container.names {
            for name in names {
                if let Some(node_id) = naming::node_id_of(name) {
                    node_data.push((node_id, container));
                    break;
                }
            }
        }
    }

    // Sort by node ID
    node_data.sort_by_key(|(node_id, _)| *node_id);

    // Query node statuses
    let mut node_statuses = Vec::new();
    for (node_id, _container) in node_data {
        let status = get_node_status(docker, mesh_id, node_id, runtime).await;
        node_statuses.push((node_id, status));
    }

    // Find the maximum view across all nodes to determine consensus health
    let max_view = node_statuses
        .iter()
        .filter_map(|(_, status)| status.as_ref().ok())
        .map(|status| status.view.parse::<u64>().unwrap_or(0))
        .max()
        .unwrap_or(0);

    // Display status table
    for (node_id, status) in node_statuses {
        match status {
            Ok(node_status) => {
                // Determine consensus status based on how far behind max view
                let node_view = node_status.view.parse::<u64>().unwrap_or(0);
                let consensus_status = if node_view == max_view {
                    "✅ UP"
                } else if max_view > 0 && node_view + 1 >= max_view {
                    "⚠️  SLOW"
                } else {
                    "❌ DESYNC"
                };

                println!(
                    "{:<8} {:<12} {:<8} {:<12} {}",
                    node_id,
                    consensus_status,
                    node_status.role,
                    node_status.view,
                    node_status.phase
                );
            }
            Err(e) => {
                println!(
                    "{:<8} {:<12} {:<8} {:<12} Error: {}",
                    node_id, "❌ DOWN", "-", "-", e
                );
            }
        }
    }

    Ok(())
}

async fn show_node_history(
    docker: &Docker,
    mesh_id: u32,
    node_id: u32,
    view: Option<i32>,
    runtime: sys::ContainerRuntime,
) -> Result<()> {
    // Get runtime-aware connection info and JWT token
    let addresses = get_external_addresses(docker, mesh_id, runtime).await?;
    let (host, port) = addresses
        .iter()
        .find(|(id, _, _)| *id == node_id)
        .map(|(_, h, p)| (h.clone(), *p))
        .ok_or_else(|| anyhow::anyhow!("Node {} not found", node_id))?;

    let jwt_token = get_jwt_token(docker, mesh_id, node_id, runtime).await?;
    let client = crate::insecure_client();

    // If specific view requested, show detailed view state
    if let Some(view_number) = view {
        println!(
            "Node {} View State for View {} (Mesh {})",
            node_id, view_number, mesh_id
        );
        println!("{}", "=".repeat(60));

        let url = format!("https://{}:{}/api/consensus/view", host, port);
        let response = client
            .post(&url)
            .header("Authorization", format!("Bearer {}", jwt_token))
            .json(&view_number)
            .send()
            .await?;

        if !response.status().is_success() {
            anyhow::bail!("Failed to fetch view state: HTTP {}", response.status());
        }

        let view_state: serde_json::Value = response.json().await?;

        // Extract and display view state
        println!(
            "Queried View:         {}",
            view_state["queried_view"].as_i64().unwrap_or(0)
        );
        println!(
            "Height at View:       {}",
            view_state["height_at_view"].as_i64().unwrap_or(0)
        );
        println!(
            "Active at Height:     {}",
            if view_state["is_active_at_height"].as_bool().unwrap_or(false) {
                "Yes"
            } else {
                "No"
            }
        );
        println!();

        // Display leader
        if let Some(leader) = view_state["leader_for_view"].as_object() {
            println!(
                "Leader:               Node {} ({})",
                leader["node_id"].as_i64().unwrap_or(0),
                leader["name"].as_str().unwrap_or("unknown")
            );
        } else {
            println!("Leader:               None");
        }
        println!();

        // Display validators
        if let Some(validators) = view_state["validators_at_height"].as_array() {
            println!("Validators ({}):", validators.len());
            for validator in validators {
                if let Some(obj) = validator.as_object() {
                    println!(
                        "  • Node {}: {} ({}:{})",
                        obj["node_id"].as_i64().unwrap_or(0),
                        obj["name"].as_str().unwrap_or("unknown"),
                        obj["ip_address"].as_str().unwrap_or("unknown"),
                        obj["port"].as_i64().unwrap_or(0)
                    );
                }
            }
        }
    } else {
        // Show full history table (existing behavior)
        println!("Node {} Consensus History (Mesh {})", node_id, mesh_id);

        let url = format!("https://{}:{}/api/consensus/history", host, port);
        let response = client
            .get(&url)
            .header("Authorization", format!("Bearer {}", jwt_token))
            .send()
            .await?;

        if !response.status().is_success() {
            anyhow::bail!("Failed to fetch history: HTTP {}", response.status());
        }

        let history: Vec<serde_json::Value> = response.json().await?;

        // Print table header
        println!(
            "{:<6} {:<7} {:<10} {:<7} {:<4} {:<10}",
            "View", "Height", "ProposeQC", "LockQC", "TC", "Block Hash"
        );
        println!("{}", "-".repeat(55));

        // Print each row
        for entry in history {
            let view = entry["view"].as_i64().unwrap_or(0);
            let height = entry["height"].as_i64().unwrap_or(0);
            let has_propose_qc = entry["has_propose_qc"].as_bool().unwrap_or(false);
            let has_lock_qc = entry["has_lock_qc"].as_bool().unwrap_or(false);
            let has_tc = entry["has_tc"].as_bool().unwrap_or(false);
            let block_hash = entry["block_hash"].as_str().unwrap_or("-");

            println!(
                "{:<6} {:<7} {:<10} {:<7} {:<4} {:<10}",
                view,
                height,
                if has_propose_qc { "✓" } else { "-" },
                if has_lock_qc { "✓" } else { "-" },
                if has_tc { "✓" } else { "-" },
                block_hash
            );
        }
    }

    Ok(())
}

#[derive(Debug)]
struct NodeStatus {
    role: String,
    view: String,
    phase: String,
}

async fn get_node_status(
    docker: &Docker,
    mesh_id: u32,
    node_id: u32,
    runtime: sys::ContainerRuntime,
) -> Result<NodeStatus> {
    let client = crate::insecure_client();

    // Get runtime-aware connection info
    let addresses = get_external_addresses(docker, mesh_id, runtime).await?;
    let (host, port) = addresses
        .iter()
        .find(|(id, _, _)| *id == node_id)
        .map(|(_, h, p)| (h.clone(), *p))
        .ok_or_else(|| anyhow::anyhow!("Node {} not found", node_id))?;

    // Get JWT token
    let jwt_token = match get_jwt_token(docker, mesh_id, node_id, runtime).await {
        Ok(token) => token,
        Err(_) => return Err(anyhow::anyhow!("Auth failed")),
    };

    // Query consensus state with JWT
    let consensus_url = format!("https://{}:{}/api/consensus", host, port);

    match client
        .get(&consensus_url)
        .header("Authorization", format!("Bearer {}", jwt_token))
        .timeout(tokio::time::Duration::from_secs(3))
        .send()
        .await
    {
        Ok(response) => {
            if response.status().is_success() {
                match response.json::<serde_json::Value>().await {
                    Ok(consensus_data) => {
                        // Extract leader info to determine role
                        let leader_id = consensus_data
                            .get("leader")
                            .and_then(|l| l.get("node_id"))
                            .and_then(|id| id.as_u64())
                            .unwrap_or(999) as u32;

                        let role = if leader_id == node_id {
                            "LEADER".to_string()
                        } else {
                            "FOLLOWER".to_string()
                        };

                        let view = consensus_data
                            .get("view")
                            .and_then(|v| v.as_u64())
                            .map(|v| v.to_string())
                            .unwrap_or_else(|| "?".to_string());

                        let phase = consensus_data
                            .get("phase")
                            .and_then(|p| p.as_str())
                            .unwrap_or("?")
                            .to_string();

                        Ok(NodeStatus { role, view, phase })
                    }
                    Err(_) => Err(anyhow::anyhow!("Invalid JSON")),
                }
            } else {
                Err(anyhow::anyhow!("HTTP {}", response.status()))
            }
        }
        Err(_) => Err(anyhow::anyhow!("Connection failed")),
    }
}

/// Internal metadata for a node (both internal and external addressing)
struct NodeMetadata {
    node_id: u32,
    container_ip: String,
    host_port: u16,
}

/// Extract complete node metadata from containers in a mesh
async fn get_node_metadata(docker: &Docker, mesh_id: u32) -> Result<Vec<NodeMetadata>> {
    let containers = get_mesh_containers(docker, mesh_id).await?;
    let mut metadata: Vec<NodeMetadata> = Vec::new();

    for container in &containers {
        if let Some(names) = &container.names {
            for name in names {
                if let Some(node_id) = naming::node_id_of(name) {
                    let container_id = container.id.as_ref().unwrap();
                    let container_info = docker
                        .inspect_container(
                            container_id,
                            None::<bollard::query_parameters::InspectContainerOptions>,
                        )
                        .await?;

                    // Extract container IP from networks
                    let networks = container_info
                        .network_settings
                        .and_then(|ns| ns.networks)
                        .unwrap_or_default();
                    let container_ip = networks
                        .values()
                        .find_map(|endpoint| endpoint.ip_address.as_ref())
                        .ok_or_else(|| {
                            anyhow::anyhow!("Container IP not found for node {}", node_id)
                        })?
                        .clone();

                    // Extract host port from labels
                    let labels = container_info
                        .config
                        .and_then(|c| c.labels)
                        .unwrap_or_default();
                    let host_port = labels
                        .get("hopnet.host_port")
                        .and_then(|p| p.parse::<u16>().ok())
                        .ok_or_else(|| {
                            anyhow::anyhow!("Host port label not found for node {}", node_id)
                        })?;

                    metadata.push(NodeMetadata {
                        node_id,
                        container_ip,
                        host_port,
                    });
                    break;
                }
            }
        }
    }

    metadata.sort_by_key(|m| m.node_id);
    Ok(metadata)
}

/// Get internal addresses for inter-container communication
/// Returns (node_id, ip_address, port) - always uses container IPs regardless of runtime
pub async fn get_internal_addresses(
    docker: &Docker,
    mesh_id: u32,
) -> Result<Vec<(u32, String, u16)>> {
    Ok(get_node_metadata(docker, mesh_id)
        .await?
        .into_iter()
        .map(|m| (m.node_id, m.container_ip, 34632u16))
        .collect())
}

/// Get external addresses for host-to-container communication
/// Returns (node_id, host, port) - adapts based on runtime
pub async fn get_external_addresses(
    docker: &Docker,
    mesh_id: u32,
    runtime: sys::ContainerRuntime,
) -> Result<Vec<(u32, String, u16)>> {
    // On macOS, always use localhost with host port (can't access container IPs directly)
    let use_host_port = runtime == sys::ContainerRuntime::Podman || cfg!(target_os = "macos");
    Ok(get_node_metadata(docker, mesh_id)
        .await?
        .into_iter()
        .map(|m| {
            if use_host_port {
                (m.node_id, "localhost".to_string(), m.host_port)
            } else {
                (m.node_id, m.container_ip, 34632)
            }
        })
        .collect())
}

const PASSPHRASE_PATH: &str = "/root/.local/share/hopnet/.passphrase";

/// Store a mesh passphrase inside node 0's container volume.
/// Uses Docker's tar upload API so the container image doesn't need a shell.
async fn store_mesh_passphrase(docker: &Docker, mesh_id: u32, passphrase: &str) -> Result<()> {
    let container_name = naming::container_name(mesh_id, 0);

    // Build an in-memory tar archive containing the passphrase file
    let mut tar_builder = tar::Builder::new(Vec::new());
    let passphrase_bytes = passphrase.as_bytes();
    let mut header = tar::Header::new_gnu();
    header.set_size(passphrase_bytes.len() as u64);
    header.set_mode(0o600);
    header.set_cksum();
    // The file path inside the tar is relative to the upload destination
    tar_builder.append_data(&mut header, ".passphrase", passphrase_bytes)?;
    let tar_bytes = tar_builder.into_inner()?;

    // Extract the parent directory from PASSPHRASE_PATH
    let parent_dir = std::path::Path::new(PASSPHRASE_PATH)
        .parent()
        .map(|p| p.to_str().unwrap_or("/"))
        .unwrap_or("/");

    docker
        .upload_to_container(
            &container_name,
            Some(
                bollard::query_parameters::UploadToContainerOptionsBuilder::new()
                    .path(parent_dir)
                    .build(),
            ),
            bollard::body_full(bytes::Bytes::from(tar_bytes)),
        )
        .await?;

    Ok(())
}

/// Load the mesh passphrase from node 0's container volume.
/// Uses Docker's tar download API so the container image doesn't need a shell.
async fn load_mesh_passphrase(docker: &Docker, mesh_id: u32) -> Result<String> {
    let container_name = naming::container_name(mesh_id, 0);

    let stream = docker.download_from_container(
        &container_name,
        Some(
            bollard::query_parameters::DownloadFromContainerOptionsBuilder::new()
                .path(PASSPHRASE_PATH)
                .build(),
        ),
    );

    // Collect the tar stream into bytes
    let mut tar_bytes = Vec::new();
    tokio::pin!(stream);
    while let Some(chunk) = stream.next().await {
        tar_bytes.extend_from_slice(&chunk?);
    }

    // Extract the passphrase from the tar archive
    let mut archive = tar::Archive::new(&tar_bytes[..]);
    for entry in archive.entries()? {
        let mut entry = entry?;
        let mut content = String::new();
        std::io::Read::read_to_string(&mut entry, &mut content)?;
        let passphrase = content.trim().to_string();
        if !passphrase.is_empty() {
            return Ok(passphrase);
        }
    }

    Err(anyhow::anyhow!(
        "No passphrase found in container for mesh {} (run setup first)",
        mesh_id
    ))
}

/// Centralised URL builder. Every API-path construction in tests must
/// flow through this function so a URL-scheme change (e.g. prefixing all
/// routes with /api) is a single-site edit.
pub fn node_url(node: &NodeInfo, path: &str) -> String {
    format!("https://{}:{}{}", node.ip_address, node.port, path)
}

/// Nodes serve pinned-HTTPS with per-node self-signed certs
/// (docs/specs/pinned-https.md); the orchestrator trusts the container
/// boundary rather than the cert, so every node-facing client skips
/// verification. The tls-pinned-https scenario exercises the real
/// SPKI-pinning path.
pub fn insecure_client_builder() -> reqwest::ClientBuilder {
    reqwest::Client::builder().danger_accept_invalid_certs(true)
}

pub fn insecure_client() -> reqwest::Client {
    insecure_client_builder()
        .build()
        .expect("reqwest client build")
}

/// Call a HopNet node API with authentication and optional retry
pub async fn call_node_api(
    node_info: &NodeInfo,
    path: &str,
    retry: bool,
) -> Result<reqwest::Response> {
    let url = node_url(node_info, path);
    let client = crate::insecure_client();

    let make_request = || async {
        client
            .get(&url)
            .header("Authorization", format!("Bearer {}", node_info.jwt_token))
            .send()
            .await
    };

    if retry {
        // Retry logic for idempotent operations
        let mut attempts = 0;
        let max_attempts = 3;

        loop {
            attempts += 1;
            match make_request().await {
                Ok(response) => return Ok(response),
                Err(e) if attempts < max_attempts => {
                    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
        }
    } else {
        // Single attempt for non-idempotent operations
        Ok(make_request().await?)
    }
}
