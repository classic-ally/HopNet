use anyhow::Result;
use bollard::Docker;
use bollard::query_parameters::{ListContainersOptionsBuilder, CreateContainerOptionsBuilder, ListNetworksOptionsBuilder};
use bollard::models::{ContainerCreateBody, HostConfig, NetworkingConfig, EndpointSettings, NetworkCreateResponse};
use clap::{Parser, Subcommand};
use serde_json::json;
use std::collections::HashMap;

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
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Connect to Docker daemon (or Podman via DOCKER_HOST)
    let docker = if let Ok(host) = std::env::var("DOCKER_HOST") {
        Docker::connect_with_unix(&host.strip_prefix("unix://").unwrap_or(&host), 120, bollard::API_DEFAULT_VERSION)?
    } else {
        Docker::connect_with_socket_defaults()?
    };
    
    match &cli.command {
        Some(Commands::Create { nodes, no_cleanup }) => {
            let mesh_id = get_next_mesh_id(&docker).await?;
            println!("Creating mesh {} with {} nodes", mesh_id, nodes);
            create_mesh(&docker, mesh_id, *nodes, *no_cleanup).await?;
        }
        Some(Commands::Add { mesh_id, nodes }) => {
            println!("Adding {} node(s) to mesh {}", nodes, mesh_id);
            add_nodes_to_mesh(&docker, *mesh_id, *nodes).await?;
        }
        Some(Commands::Delete { mesh_id, yes }) => {
            delete_mesh(&docker, *mesh_id, *yes).await?;
        }
        Some(Commands::Cleanup { yes }) => {
            cleanup_orphaned_networks(&docker, *yes).await?;
        }
        Some(Commands::Status { mesh_id }) => {
            show_mesh_status(&docker, *mesh_id).await?;
        }
        Some(Commands::History { mesh_id, node, view }) => {
            show_node_history(&docker, *mesh_id, *node, *view).await?;
        }
        Some(Commands::List) | None => {
            list_meshes(&docker).await?;
        }
    }
    
    Ok(())
}

async fn get_next_mesh_id(docker: &Docker) -> Result<u32> {
    // Get all existing mesh IDs
    let networks = docker.list_networks(None::<bollard::network::ListNetworksOptions<String>>).await?;
    
    let mut mesh_ids: Vec<u32> = Vec::new();
    
    // Find all hopnet-orchestrator networks and extract mesh IDs
    for network in &networks {
        if let Some(ref name) = network.name {
            if name.starts_with("hopnet-orchestrator-") {
                let parts: Vec<&str> = name.split('-').collect();
                if parts.len() >= 4 {
                    if let Ok(mesh_id) = parts[2].parse::<u32>() {
                        mesh_ids.push(mesh_id);
                    }
                }
            }
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
    
    // List all networks that match hopnet-orchestrator-* pattern
    let networks = docker.list_networks(None::<bollard::network::ListNetworksOptions<String>>).await?;
    
    let mut meshes: HashMap<u32, Vec<String>> = HashMap::new();
    
    // Find hopnet-orchestrator networks and extract mesh IDs
    for network in &networks {
        if let Some(ref name) = network.name {
            if name.starts_with("hopnet-orchestrator-") {
                // Parse mesh ID from network name: hopnet-orchestrator-{mesh_id}-{network_space}
                let parts: Vec<&str> = name.split('-').collect();
                if parts.len() >= 4 {
                    if let Ok(mesh_id) = parts[2].parse::<u32>() {
                        meshes.entry(mesh_id).or_insert_with(Vec::new);
                    }
                }
            }
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
                if name.starts_with("/hopnet-orchestrator-") {
                    // Parse mesh ID from container name: /hopnet-orchestrator-{mesh_id}-{node_id}
                    let clean_name = &name[1..]; // Remove leading '/'
                    let parts: Vec<&str> = clean_name.split('-').collect();
                    if parts.len() >= 4 {
                        if let Ok(mesh_id) = parts[2].parse::<u32>() {
                            meshes.entry(mesh_id).or_insert_with(Vec::new).push(name.clone());
                        }
                    }
                }
            }
        }
    }
    
    if meshes.is_empty() {
        println!("No HopNet meshes found.");
        println!("\nTo create a mesh, use: cargo run --bin orchestrator create --mesh-id <ID> --nodes <COUNT>");
    } else {
        println!("Active HopNet Meshes:");
        println!("{:<8} {:<12} {}", "Mesh ID", "Nodes", "Containers");
        println!("{}", "-".repeat(40));
        
        let mut mesh_ids: Vec<_> = meshes.keys().collect();
        mesh_ids.sort();
        
        for &mesh_id in mesh_ids {
            let containers = &meshes[&mesh_id];
            println!("{:<8} {:<12} {}", mesh_id, containers.len(), 
                containers.iter().map(|s| &s[1..]).collect::<Vec<_>>().join(", "));
        }
    }
    
    Ok(())
}

async fn create_mesh(docker: &Docker, mesh_id: u32, node_count: u32, no_cleanup: bool) -> Result<()> {
    println!("Creating mesh {} with {} nodes", mesh_id, node_count);
    
    // Create network for the mesh
    let network_name = format!("hopnet-orchestrator-{}-0", mesh_id);
    
    match create_hopnet_network(docker, &network_name).await {
        Ok(network_id) => {
            println!("Successfully created network: {}", network_id);
            
            let mut containers: Vec<(String, String, String)> = Vec::new(); // (container_name, container_id, ip_address)
            
            // Create containers for each node
            for node_id in 0..node_count {
                let container_name = format!("hopnet-orchestrator-{}-{}", mesh_id, node_id);
                println!("Creating HopNet container: {}", container_name);
                
                match create_hopnet_container(docker, &container_name, &network_name).await {
                    Ok((container_id, ip_address)) => {
                        println!("Successfully created container: {} with IP: {}", container_id, ip_address);
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
                println!("Setting up node 0 at IP: {} (host port: {})", ip_address, 40000 + (mesh_id * 500));
                if let Err(e) = setup_node_0(mesh_id, &container_name, &ip_address).await {
                    println!("Failed to setup node 0: {}", e);
                    
                    if no_cleanup {
                        println!("Skipping cleanup (--no-cleanup flag set). Containers and network left running for inspection.");
                        println!("Network: {}", network_name);
                        for (name, container_id, ip) in &containers {
                            println!("Container: {} (ID: {}, IP: {})", name, container_id, ip);
                        }
                    } else {
                        println!("Cleaning up mesh {} due to setup failure...", mesh_id);
                        
                        // Cleanup containers (in parallel)
                        let mut tasks = Vec::new();
                        for (name, container_id, _) in containers {
                            let docker_clone = docker.clone();
                            let task = tokio::spawn(async move {
                                println!("  Stopping and removing container: {}", name);
                                let _ = docker_clone.stop_container(&container_id, None::<bollard::container::StopContainerOptions>).await;
                                let _ = docker_clone.remove_container(&container_id, None::<bollard::container::RemoveContainerOptions>).await;
                            });
                            tasks.push(task);
                        }
                        // Wait for all cleanup tasks to complete
                        for task in tasks {
                            let _ = task.await;
                        }
                        
                        // Cleanup network
                        println!("  Removing network: {}", network_name);
                        let _ = docker.remove_network(&network_id).await;
                    }
                    
                    return Err(anyhow::anyhow!("Mesh creation failed due to setup API failure"));
                }
                
                // Register additional nodes (1, 2, 3...) with node 0
                if containers.len() > 1 {
                    let node_0_ip = ip_address;
                    for (node_index, (container_name, _container_id, node_ip)) in containers.iter().enumerate().skip(1) {
                        let node_id = node_index as u32; // node_id starts from 1 for additional nodes
                        println!("Registering node {} ({}) with node 0...", node_id, container_name);

                        if let Err(e) = register_node_with_node_0(mesh_id, node_0_ip, node_id, container_name, node_ip).await {
                            println!("Failed to register node {}: {}", node_id, e);
                            
                            if no_cleanup {
                                println!("Skipping cleanup (--no-cleanup flag set). Containers and network left running for inspection.");
                                println!("Network: {}", network_name);
                                for (name, container_id, ip) in &containers {
                                    println!("Container: {} (ID: {}, IP: {})", name, container_id, ip);
                                }
                            } else {
                                println!("Cleaning up mesh {} due to node registration failure...", mesh_id);
                                
                                // Cleanup containers (in parallel)
                                let mut tasks = Vec::new();
                                for (name, container_id, _) in containers {
                                    let docker_clone = docker.clone();
                                    let task = tokio::spawn(async move {
                                        println!("  Stopping and removing container: {}", name);
                                        let _ = docker_clone.stop_container(&container_id, None::<bollard::container::StopContainerOptions>).await;
                                        let _ = docker_clone.remove_container(&container_id, None::<bollard::container::RemoveContainerOptions>).await;
                                    });
                                    tasks.push(task);
                                }
                                // Wait for all cleanup tasks to complete
                                for task in tasks {
                                    let _ = task.await;
                                }
                                
                                // Cleanup network
                                println!("  Removing network: {}", network_name);
                                let _ = docker.remove_network(&network_id).await;
                            }
                            
                            return Err(anyhow::anyhow!("Mesh creation failed due to node registration failure"));
                        }
                    }
                }
            }
        }
        Err(e) => {
            println!("Failed to create network: {}", e);
        }
    }
    
    Ok(())
}

async fn add_nodes_to_mesh(docker: &Docker, mesh_id: u32, node_count: u32) -> Result<()> {
    // Get existing containers to find the next node_id
    let existing_containers = get_mesh_containers(docker, mesh_id).await?;

    if existing_containers.is_empty() {
        return Err(anyhow::anyhow!("Mesh {} does not exist. Create it first with 'create --nodes N'", mesh_id));
    }

    // Find the highest existing node_id
    let mut max_node_id = 0u32;
    for container in &existing_containers {
        if let Some(names) = &container.names {
            for name in names {
                if name.starts_with("/hopnet-orchestrator-") {
                    let clean_name = &name[1..];
                    let parts: Vec<&str> = clean_name.split('-').collect();
                    if parts.len() >= 4 {
                        if let Ok(node_id) = parts[3].parse::<u32>() {
                            max_node_id = max_node_id.max(node_id);
                        }
                    }
                }
            }
        }
    }

    let starting_node_id = max_node_id + 1;
    println!("Next available node_id: {}", starting_node_id);

    // Get the network name for this mesh
    let network_name = format!("hopnet-orchestrator-{}-0", mesh_id);

    // Verify network exists
    let networks = get_mesh_networks(docker, mesh_id).await?;
    if networks.is_empty() {
        return Err(anyhow::anyhow!("Network for mesh {} not found", mesh_id));
    }

    // Get node 0's IP address (needed for registration)
    let node_0_ip = {
        let node_0_container = existing_containers.iter()
            .find(|c| {
                c.names.as_ref().map_or(false, |names| {
                    names.iter().any(|n| n.ends_with("-0"))
                })
            })
            .ok_or_else(|| anyhow::anyhow!("Node 0 not found in mesh {}", mesh_id))?;

        let container_id = node_0_container.id.as_ref().unwrap();
        let container_info = docker.inspect_container(container_id, None::<bollard::container::InspectContainerOptions>).await?;

        container_info.network_settings
            .and_then(|ns| ns.networks)
            .and_then(|networks| networks.get(&network_name).cloned())
            .and_then(|endpoint| endpoint.ip_address)
            .ok_or_else(|| anyhow::anyhow!("Could not get node 0 IP address"))?
    };

    println!("Node 0 IP address: {}", node_0_ip);

    // Create new containers
    let mut new_containers: Vec<(String, String, String)> = Vec::new(); // (name, id, ip)

    for i in 0..node_count {
        let node_id = starting_node_id + i;
        let container_name = format!("hopnet-orchestrator-{}-{}", mesh_id, node_id);
        println!("Creating container: {}", container_name);

        match create_hopnet_container(docker, &container_name, &network_name).await {
            Ok((container_id, ip_address)) => {
                println!("Successfully created container: {} with IP: {}", container_id, ip_address);
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

    // Register new nodes with node 0 (triggers catch-up based bootstrap)
    for (container_name, _container_id, node_ip) in &new_containers {
        // Extract node_id from container name
        let parts: Vec<&str> = container_name.split('-').collect();
        let node_id: u32 = parts[3].parse().unwrap();

        println!("Registering node {} ({}) with node 0...", node_id, container_name);

        if let Err(e) = register_node_with_node_0(mesh_id, &node_0_ip, node_id, container_name, node_ip).await {
            println!("Failed to register node {}: {}", node_id, e);
            return Err(anyhow::anyhow!("Node registration failed: {}", e));
        }

        println!("Successfully registered node {}. Bootstrap via catch-up initiated.", node_id);
    }

    println!("Successfully added {} node(s) to mesh {}", node_count, mesh_id);
    Ok(())
}

async fn create_hopnet_network(docker: &Docker, network_name: &str) -> Result<String> {
    // Use the simple create_network method with just name
    let options = bollard::network::CreateNetworkOptions {
        name: network_name,
        ..Default::default()
    };
    
    let response: NetworkCreateResponse = docker.create_network(options).await?;
    
    println!("Created network {} with ID: {}", network_name, response.id);
    
    Ok(response.id)
}

async fn create_hopnet_container(docker: &Docker, container_name: &str, network_name: &str) -> Result<(String, String)> {
    // Extract mesh_id and node_id from container name for port mapping
    // container_name format: hopnet-orchestrator-{mesh_id}-{node_id}
    let parts: Vec<&str> = container_name.split('-').collect();
    let (mesh_id, node_id) = if parts.len() >= 4 {
        let mesh: u32 = parts[2].parse().unwrap_or(0);
        let node: u32 = parts[3].parse().unwrap_or(0);
        (mesh, node)
    } else {
        (0, 0)
    };

    // Map host port to container port 34633
    // Formula: 40000 + (mesh_id × 500) + node_id
    // Supports up to 500 nodes per mesh, 51 meshes (40000-65535)
    let host_port = 40000 + (mesh_id * 500) + node_id;
    let mut port_bindings = HashMap::new();
    port_bindings.insert(
        "34633/tcp".to_string(),
        Some(vec![bollard::models::PortBinding {
            host_ip: Some("0.0.0.0".to_string()),
            host_port: Some(host_port.to_string()),
        }])
    );

    // Network configuration - attach to our custom network
    let mut endpoints_config = HashMap::new();
    endpoints_config.insert(
        network_name.to_string(),
        EndpointSettings {
            ip_address: None, // Let Docker assign IP
            ..Default::default()
        }
    );

    // Container configuration
    let config = ContainerCreateBody {
        image: Some("hopnet:latest".to_string()),
        networking_config: Some(NetworkingConfig {
            endpoints_config: Some(endpoints_config),
        }),
        host_config: Some(HostConfig {
            port_bindings: Some(port_bindings),
            ..Default::default()
        }),
        ..Default::default()
    };
    
    // Create container
    let options = CreateContainerOptionsBuilder::default()
        .name(container_name)
        .build();
    
    let response = docker
        .create_container(Some(options), config)
        .await?;
    
    let container_id = response.id;
    
    // Start the container
    docker
        .start_container(&container_id, None::<bollard::container::StartContainerOptions<String>>)
        .await?;
    
    // Get the container's IP address
    let container_info = docker.inspect_container(&container_id, None::<bollard::container::InspectContainerOptions>).await?;
    let ip_address = container_info
        .network_settings
        .and_then(|ns| ns.networks)
        .and_then(|networks| networks.get(network_name).cloned())
        .and_then(|endpoint| endpoint.ip_address)
        .unwrap_or_else(|| "unknown".to_string());
    
    println!("Started container {} on network {} with IP {}", container_name, network_name, ip_address);
    
    Ok((container_id, ip_address))
}

async fn setup_node_0(mesh_id: u32, node_name: &str, ip_address: &str) -> Result<()> {
    let client = reqwest::Client::new();
    // Use localhost with mapped port (node 0 = 40000 + mesh_id * 500)
    let host_port = 40000 + (mesh_id * 500);
    let url = format!("http://localhost:{}/setup", host_port);

    let setup_data = json!({
        "username": "allison",
        "password": "testing",
        "node_name": node_name,
        "ip_address": ip_address,  // Container still uses its internal IP for node-to-node communication
        "port": 34633
    });
    
    println!("Calling setup API at: {}", url);
    println!("Setup data: {}", setup_data);
    
    let start_time = std::time::Instant::now();
    let timeout_duration = std::time::Duration::from_secs(15);
    let retry_interval = std::time::Duration::from_millis(500); // 500ms between retries
    
    loop {
        if start_time.elapsed() > timeout_duration {
            return Err(anyhow::anyhow!("Setup API call timed out after 15 seconds"));
        }
        
        println!("Attempting setup API call... (elapsed: {:.1}s)", start_time.elapsed().as_secs_f32());
        
        match client
            .post(&url)
            .json(&setup_data)
            .timeout(tokio::time::Duration::from_secs(3))
            .send()
            .await
        {
            Ok(response) => {
                let status = response.status();
                let response_text = response.text().await.unwrap_or_else(|_| "No response body".to_string());
                
                if status == reqwest::StatusCode::CREATED {
                    println!("Node setup successful: {} Created", status);
                    println!("Response: {}", response_text);
                    return Ok(());
                } else if status.is_server_error() || status.is_client_error() {
                    println!("Setup failed with status: {} - {}", status, response_text);
                    if start_time.elapsed() + retry_interval > timeout_duration {
                        return Err(anyhow::anyhow!("Setup API call failed with status: {}", status));
                    }
                } else {
                    println!("Unexpected status: {} - {}", status, response_text);
                    if start_time.elapsed() + retry_interval > timeout_duration {
                        return Err(anyhow::anyhow!("Setup API call returned unexpected status: {}", status));
                    }
                }
            }
            Err(e) => {
                println!("Setup API request failed: {} (retrying...)", e);
                if start_time.elapsed() + retry_interval > timeout_duration {
                    return Err(anyhow::anyhow!("Setup API call failed after retries: {}", e));
                }
            }
        }
        
        // Wait before retrying
        tokio::time::sleep(retry_interval).await;
    }
}

async fn get_jwt_token(mesh_id: u32, node_id: u32) -> Result<String> {
    let client = reqwest::Client::new();
    let host_port = 40000 + (mesh_id * 500) + node_id;
    let login_url = format!("http://localhost:{}/login", host_port);
    
    let login_data = json!({
        "username": "allison",
        "password": "testing"
    });
    
    let start_time = std::time::Instant::now();
    let timeout_duration = std::time::Duration::from_secs(10);
    let retry_interval = std::time::Duration::from_millis(500);
    
    loop {
        if start_time.elapsed() > timeout_duration {
            return Err(anyhow::anyhow!("Login request timed out after 10 seconds"));
        }
        
        match client
            .post(&login_url)
            .json(&login_data)
            .timeout(tokio::time::Duration::from_secs(3))
            .send()
            .await
        {
            Ok(response) => {
                let status = response.status();
                if status == reqwest::StatusCode::OK {
                    match response.text().await {
                        Ok(token_raw) => {
                            // Remove quotes if present and trim whitespace
                            let token = token_raw.trim().trim_matches('"');
                            if !token.is_empty() {
                                return Ok(token.to_string());
                            }
                        }
                        Err(_) => {}
                    }
                }
            }
            Err(_) => {}
        }
        
        if start_time.elapsed() + retry_interval > timeout_duration {
            return Err(anyhow::anyhow!("Login failed after retries"));
        }
        
        tokio::time::sleep(retry_interval).await;
    }
}

async fn register_node_with_node_0(mesh_id: u32, node_0_ip: &str, node_id: u32, node_name: &str, node_ip: &str) -> Result<()> {
    let client = reqwest::Client::new();

    // Step 1: Get the public key from the node's /setup GET route
    let node_host_port = 40000 + (mesh_id * 500) + node_id;
    let get_setup_url = format!("http://localhost:{}/setup", node_host_port);
    println!("  Getting public key from: {}", get_setup_url);
    
    let start_time = std::time::Instant::now();
    let timeout_duration = std::time::Duration::from_secs(15);
    let retry_interval = std::time::Duration::from_millis(500);
    
    let pub_key = loop {
        if start_time.elapsed() > timeout_duration {
            return Err(anyhow::anyhow!("Failed to retrieve public key from node {} after 15 seconds", node_ip));
        }
        
        match client.get(&get_setup_url).timeout(tokio::time::Duration::from_secs(3)).send().await {
            Ok(response) => {
                let status = response.status();
                if status == reqwest::StatusCode::NOT_FOUND {
                    // Parse the response to get the public key (it's a plain text string)
                    match response.text().await {
                        Ok(pubkey_raw) => {
                            // Remove quotes if present and trim whitespace
                            let pubkey = pubkey_raw.trim().trim_matches('"');
                            if !pubkey.is_empty() && pubkey.len() == 64 { // ed25519 public keys are 64 hex chars
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
            return Err(anyhow::anyhow!("Failed to retrieve public key from node {} after retries", node_ip));
        }
        
        tokio::time::sleep(retry_interval).await;
    };
    
    // Step 2: Get JWT token for authentication with node 0 (after node 0 setup is complete)
    println!("  Getting JWT token for node 0 authentication...");
    let jwt_token = get_jwt_token(mesh_id, 0).await?;  // node 0 has node_id = 0

    // Step 3: Register the node with node 0 via POST /nodes
    let node_0_host_port = 40000 + (mesh_id * 500);  // node 0 port
    let register_url = format!("http://localhost:{}/nodes", node_0_host_port);
    let node_data = json!({
        "node_id": node_id,
        "name": node_name,
        "ip_address": node_ip,
        "port": 34633,
        "owner": 0,
        "pubkey": pub_key
    });
    
    println!("  Registering node with node 0 at: {}", register_url);
    println!("  Node data: {}", node_data);
    
    let start_time = std::time::Instant::now();
    let timeout_duration = std::time::Duration::from_secs(15);
    let retry_interval = std::time::Duration::from_millis(500);
    
    loop {
        if start_time.elapsed() > timeout_duration {
            return Err(anyhow::anyhow!("Node registration timed out after 15 seconds"));
        }
        
        match client
            .post(&register_url)
            .header("Authorization", format!("Bearer {}", jwt_token))
            .json(&node_data)
            .timeout(tokio::time::Duration::from_secs(3))
            .send()
            .await
        {
            Ok(response) => {
                let status = response.status();
                let response_text = response.text().await.unwrap_or_else(|_| "No response body".to_string());

                if status == reqwest::StatusCode::CREATED {
                    println!("  Node registration successful: {} Created", status);
                    println!("  Response: {}", response_text);
                    return Ok(());
                } else {
                    // Don't retry on HTTP error responses - the request was received and processed
                    println!("  Registration failed with status: {} - {}", status, response_text);
                    return Err(anyhow::anyhow!("Node registration failed with status: {} - {}", status, response_text));
                }
            }
            Err(e) => {
                println!("  Registration request failed: {} (retrying...)", e);
                if start_time.elapsed() + retry_interval > timeout_duration {
                    return Err(anyhow::anyhow!("Node registration failed after retries: {}", e));
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
            let name = container.names.as_ref()
                .and_then(|names| names.first())
                .map(|name| &name[1..]) // Remove leading '/'
                .unwrap_or("unnamed");
            let status = container.status.as_ref().map(|s| s.as_str()).unwrap_or("unknown");
            println!("    - {} ({})", name, status);
        }
    }
    if !networks.is_empty() {
        println!("  Networks ({}):", networks.len());
        for network in &networks {
            let name = network.name.as_ref().map(|s| s.as_str()).unwrap_or("unnamed");
            println!("    - {}", name);
        }
    }
    
    // Confirmation prompt
    if !skip_confirmation {
        println!("\nThis will permanently delete all containers and networks for mesh {}.", mesh_id);
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
                let name = container.names.as_ref()
                    .and_then(|names| names.first())
                    .map(|name| name[1..].to_string()) // Remove leading '/' and own the string
                    .unwrap_or_else(|| "unnamed".to_string());
                
                let docker_clone = docker.clone();
                let task = tokio::spawn(async move {
                    println!("  Stopping container: {}", name);
                    if let Err(e) = docker_clone.stop_container(&id, None::<bollard::container::StopContainerOptions>).await {
                        println!("    Warning: Failed to stop container {}: {}", name, e);
                    }
                    
                    println!("  Removing container: {}", name);
                    if let Err(e) = docker_clone.remove_container(&id, None::<bollard::container::RemoveContainerOptions>).await {
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
    
    // Delete networks (in parallel)
    if !networks.is_empty() {
        println!("Removing networks...");
        
        let mut tasks = Vec::new();
        for network in networks {
            if let Some(id) = network.id {
                let name = network.name.as_ref()
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

async fn get_mesh_containers(docker: &Docker, mesh_id: u32) -> Result<Vec<bollard::models::ContainerSummary>> {
    let options = ListContainersOptionsBuilder::default()
        .all(true) // Include stopped containers
        .build();
    
    let containers = docker.list_containers(Some(options)).await?;
    
    let mesh_containers: Vec<_> = containers.into_iter()
        .filter(|container| {
            if let Some(names) = &container.names {
                names.iter().any(|name| {
                    if name.starts_with("/hopnet-orchestrator-") {
                        let clean_name = &name[1..];
                        let parts: Vec<&str> = clean_name.split('-').collect();
                        if parts.len() >= 4 {
                            if let Ok(id) = parts[2].parse::<u32>() {
                                return id == mesh_id;
                            }
                        }
                    }
                    false
                })
            } else {
                false
            }
        })
        .collect();
    
    Ok(mesh_containers)
}

async fn get_mesh_networks(docker: &Docker, mesh_id: u32) -> Result<Vec<bollard::models::Network>> {
    let networks = docker.list_networks(None::<bollard::network::ListNetworksOptions<String>>).await?;
    
    let mesh_networks: Vec<_> = networks.into_iter()
        .filter(|network| {
            if let Some(ref name) = network.name {
                if name.starts_with("hopnet-orchestrator-") {
                    let parts: Vec<&str> = name.split('-').collect();
                    if parts.len() >= 4 {
                        if let Ok(id) = parts[2].parse::<u32>() {
                            return id == mesh_id;
                        }
                    }
                }
            }
            false
        })
        .collect();
    
    Ok(mesh_networks)
}

async fn cleanup_orphaned_networks(docker: &Docker, skip_confirmation: bool) -> Result<()> {
    println!("Scanning for orphaned mesh networks...");
    
    // Get all hopnet-orchestrator networks
    let networks = docker.list_networks(None::<bollard::network::ListNetworksOptions<String>>).await?;
    let hopnet_networks: Vec<_> = networks.into_iter()
        .filter(|network| {
            if let Some(ref name) = network.name {
                name.starts_with("hopnet-orchestrator-")
            } else {
                false
            }
        })
        .collect();
    
    if hopnet_networks.is_empty() {
        println!("No HopNet networks found.");
        return Ok(());
    }
    
    // Get all hopnet-orchestrator containers
    let options = ListContainersOptionsBuilder::default()
        .all(true) // Include stopped containers
        .build();
    let containers = docker.list_containers(Some(options)).await?;
    let hopnet_containers: Vec<_> = containers.into_iter()
        .filter(|container| {
            if let Some(names) = &container.names {
                names.iter().any(|name| name.starts_with("/hopnet-orchestrator-"))
            } else {
                false
            }
        })
        .collect();
    
    // Build a map of mesh_id -> container count
    let mut mesh_container_counts: std::collections::HashMap<u32, usize> = std::collections::HashMap::new();
    for container in &hopnet_containers {
        if let Some(names) = &container.names {
            for name in names {
                if name.starts_with("/hopnet-orchestrator-") {
                    let clean_name = &name[1..]; // Remove leading '/'
                    let parts: Vec<&str> = clean_name.split('-').collect();
                    if parts.len() >= 4 {
                        if let Ok(mesh_id) = parts[2].parse::<u32>() {
                            *mesh_container_counts.entry(mesh_id).or_insert(0) += 1;
                        }
                    }
                }
            }
        }
    }
    
    // Find orphaned networks (networks for meshes with 0 containers)
    let mut orphaned_networks = Vec::new();
    for network in &hopnet_networks {
        if let Some(ref name) = network.name {
            let parts: Vec<&str> = name.split('-').collect();
            if parts.len() >= 4 {
                if let Ok(mesh_id) = parts[2].parse::<u32>() {
                    let container_count = mesh_container_counts.get(&mesh_id).unwrap_or(&0);
                    if *container_count == 0 {
                        orphaned_networks.push((mesh_id, network));
                    }
                }
            }
        }
    }
    
    if orphaned_networks.is_empty() {
        println!("No orphaned networks found.");
        return Ok(());
    }
    
    // Group orphaned networks by mesh_id
    let mut orphaned_by_mesh: std::collections::HashMap<u32, Vec<&bollard::models::Network>> = std::collections::HashMap::new();
    for (mesh_id, network) in orphaned_networks {
        orphaned_by_mesh.entry(mesh_id).or_insert_with(Vec::new).push(network);
    }
    
    // Show what will be cleaned up
    println!("Found orphaned networks for {} mesh(es):", orphaned_by_mesh.len());
    let mut total_networks = 0;
    for (&mesh_id, networks) in &orphaned_by_mesh {
        println!("  Mesh {} ({} networks):", mesh_id, networks.len());
        for network in networks {
            let name = network.name.as_ref().map(|s| s.as_str()).unwrap_or("unnamed");
            println!("    - {}", name);
            total_networks += 1;
        }
    }
    
    // Confirmation prompt
    if !skip_confirmation {
        println!("\nThis will permanently delete {} orphaned network(s).", total_networks);
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
                let name = network.name.as_ref().map(|s| s.as_str()).unwrap_or("unnamed");
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
    
    println!("Cleanup completed. Removed {} orphaned network(s).", deleted_count);
    Ok(())
}

async fn show_mesh_status(docker: &Docker, mesh_id: u32) -> Result<()> {
    println!("Mesh {} Status", mesh_id);
    
    // Get containers for this mesh
    let containers = get_mesh_containers(docker, mesh_id).await?;
    
    if containers.is_empty() {
        println!("No containers found for mesh {}.", mesh_id);
        return Ok(());
    }
    
    println!("Found {} node(s):", containers.len());
    println!("{:<8} {:<12} {:<8} {:<12} {}", "Node ID", "Status", "Role", "View", "Phase");
    println!("{}", "-".repeat(50));
    
    // Sort containers by node ID
    let mut node_data: Vec<(u32, &bollard::models::ContainerSummary)> = Vec::new();
    for container in &containers {
        if let Some(names) = &container.names {
            for name in names {
                if name.starts_with("/hopnet-orchestrator-") {
                    let clean_name = &name[1..]; // Remove leading '/'
                    let parts: Vec<&str> = clean_name.split('-').collect();
                    if parts.len() >= 4 {
                        if let Ok(node_id) = parts[3].parse::<u32>() {
                            node_data.push((node_id, container));
                            break;
                        }
                    }
                }
            }
        }
    }
    
    // Sort by node ID
    node_data.sort_by_key(|(node_id, _)| *node_id);
    
    // Get node IPs for API calls
    let mut node_statuses = Vec::new();
    for (node_id, container) in node_data {
        let container_id = container.id.as_ref().unwrap();
        
        // Get container IP
        let container_info = docker.inspect_container(container_id, None::<bollard::container::InspectContainerOptions>).await?;
        let networks = container_info.network_settings.and_then(|ns| ns.networks).unwrap_or_default();
        
        let ip_address = networks.values()
            .find_map(|endpoint| endpoint.ip_address.as_ref())
            .unwrap_or(&"unknown".to_string())
            .clone();
        
        // Query node status
        let status = get_node_status(mesh_id, node_id, &ip_address).await;
        node_statuses.push((node_id, status));
    }
    
    // Find the maximum view across all nodes to determine consensus health
    let max_view = node_statuses.iter()
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
                
                println!("{:<8} {:<12} {:<8} {:<12} {}", 
                    node_id, 
                    consensus_status, 
                    node_status.role, 
                    node_status.view, 
                    node_status.phase
                );
            }
            Err(e) => {
                println!("{:<8} {:<12} {:<8} {:<12} {}", 
                    node_id, 
                    "❌ DOWN", 
                    "-", 
                    "-", 
                    format!("Error: {}", e)
                );
            }
        }
    }
    
    Ok(())
}

async fn show_node_history(docker: &Docker, mesh_id: u32, node_id: u32, view: Option<i32>) -> Result<()> {
    // Get JWT token for authentication
    let jwt_token = get_jwt_token(mesh_id, node_id).await?;

    // Calculate host port
    let host_port = 40000 + (mesh_id * 500) + node_id;
    let client = reqwest::Client::new();

    // If specific view requested, show detailed view state
    if let Some(view_number) = view {
        println!("Node {} View State for View {} (Mesh {})", node_id, view_number, mesh_id);
        println!("{}", "=".repeat(60));

        let url = format!("http://localhost:{}/consensus/view", host_port);
        let response = client.post(&url)
            .header("Authorization", format!("Bearer {}", jwt_token))
            .json(&view_number)
            .send()
            .await?;

        if !response.status().is_success() {
            anyhow::bail!("Failed to fetch view state: HTTP {}", response.status());
        }

        let view_state: serde_json::Value = response.json().await?;

        // Extract and display view state
        println!("Queried View:         {}", view_state["queried_view"].as_i64().unwrap_or(0));
        println!("Height at View:       {}", view_state["height_at_view"].as_i64().unwrap_or(0));
        println!("Active at Height:     {}", if view_state["is_active_at_height"].as_bool().unwrap_or(false) { "Yes" } else { "No" });
        println!();

        // Display leader
        if let Some(leader) = view_state["leader_for_view"].as_object() {
            println!("Leader:               Node {} ({})",
                leader["node_id"].as_i64().unwrap_or(0),
                leader["name"].as_str().unwrap_or("unknown"));
        } else {
            println!("Leader:               None");
        }
        println!();

        // Display validators
        if let Some(validators) = view_state["validators_at_height"].as_array() {
            println!("Validators ({}):", validators.len());
            for validator in validators {
                if let Some(obj) = validator.as_object() {
                    println!("  • Node {}: {} ({}:{})",
                        obj["node_id"].as_i64().unwrap_or(0),
                        obj["name"].as_str().unwrap_or("unknown"),
                        obj["ip_address"].as_str().unwrap_or("unknown"),
                        obj["port"].as_i64().unwrap_or(0));
                }
            }
        }
    } else {
        // Show full history table (existing behavior)
        println!("Node {} Consensus History (Mesh {})", node_id, mesh_id);

        let url = format!("http://localhost:{}/consensus/history", host_port);
        let response = client.get(&url)
            .header("Authorization", format!("Bearer {}", jwt_token))
            .send()
            .await?;

        if !response.status().is_success() {
            anyhow::bail!("Failed to fetch history: HTTP {}", response.status());
        }

        let history: Vec<serde_json::Value> = response.json().await?;

        // Print table header
        println!("{:<6} {:<7} {:<10} {:<7} {:<4} {:<10}",
            "View", "Height", "ProposeQC", "LockQC", "TC", "Block Hash");
        println!("{}", "-".repeat(55));

        // Print each row
        for entry in history {
            let view = entry["view"].as_i64().unwrap_or(0);
            let height = entry["height"].as_i64().unwrap_or(0);
            let has_propose_qc = entry["has_propose_qc"].as_bool().unwrap_or(false);
            let has_lock_qc = entry["has_lock_qc"].as_bool().unwrap_or(false);
            let has_tc = entry["has_tc"].as_bool().unwrap_or(false);
            let block_hash = entry["block_hash"].as_str().unwrap_or("-");

            println!("{:<6} {:<7} {:<10} {:<7} {:<4} {:<10}",
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

async fn get_node_status(mesh_id: u32, node_id: u32, ip_address: &str) -> Result<NodeStatus> {
    let client = reqwest::Client::new();

    // First get JWT token
    let jwt_token = match get_jwt_token(mesh_id, node_id).await {
        Ok(token) => token,
        Err(_) => return Err(anyhow::anyhow!("Auth failed")),
    };

    // Query consensus state with JWT
    let host_port = 40000 + (mesh_id * 500) + node_id;
    let consensus_url = format!("http://localhost:{}/consensus", host_port);
    
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
                    Err(_) => Err(anyhow::anyhow!("Invalid JSON"))
                }
            } else {
                Err(anyhow::anyhow!("HTTP {}", response.status()))
            }
        }
        Err(_) => Err(anyhow::anyhow!("Connection failed"))
    }
}