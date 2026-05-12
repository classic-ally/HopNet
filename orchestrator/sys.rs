use anyhow::Result;
use bollard::Docker;

/// Container runtime type
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ContainerRuntime {
    Docker,
    Podman,
}

/// Auto-detect container runtime socket path
pub fn detect_socket_path() -> Result<String> {
    // Check if DOCKER_HOST is already set (respect user override)
    if let Ok(docker_host) = std::env::var("DOCKER_HOST") {
        println!("Using DOCKER_HOST from environment: {}", docker_host);
        return Ok(docker_host);
    }

    // Build candidate paths
    let mut candidates = Vec::new();

    // Rootless Podman socket (use XDG_RUNTIME_DIR standard)
    if let Ok(runtime_dir) = std::env::var("XDG_RUNTIME_DIR") {
        candidates.push(format!("unix://{}/podman/podman.sock", runtime_dir));
    }

    // Common socket paths
    candidates.push("unix:///var/run/docker.sock".to_string()); // Docker
    candidates.push("unix:///run/podman/podman.sock".to_string()); // Rootful Podman

    for socket_path in candidates {
        // Extract actual file path from unix:// URL
        let file_path = socket_path.strip_prefix("unix://").unwrap_or(&socket_path);
        if std::path::Path::new(file_path).exists() {
            // println!("Auto-detected container runtime socket: {}", socket_path);
            return Ok(socket_path);
        }
    }

    anyhow::bail!("No container runtime socket found. Please install Docker or Podman.")
}

/// Detect which container runtime we're connected to
pub async fn detect_runtime(docker: &Docker) -> Result<ContainerRuntime> {
    let version = docker.version().await?;

    // Check version info for "Podman" string
    let version_json = serde_json::to_string(&version)?;
    if version_json.contains("Podman") {
        // println!("Detected Podman runtime");
        Ok(ContainerRuntime::Podman)
    } else {
        // println!("Detected Docker runtime");
        Ok(ContainerRuntime::Docker)
    }
}

/// Check if a port is available for binding
async fn is_port_available(port: u32) -> bool {
    std::net::TcpListener::bind(format!("0.0.0.0:{}", port)).is_ok()
}

/// Find an available port for Podman port mapping (with collision detection)
pub async fn find_available_port(mesh_id: u32, node_id: u32) -> Result<u16> {
    let preferred = 40000 + (mesh_id * 500) + node_id;

    // Try preferred port first
    if is_port_available(preferred).await {
        return Ok(preferred as u16);
    }

    println!("  Port {} in use, scanning for alternative...", preferred);

    // Scan sequentially in mesh's range (up to 100 attempts)
    for offset in 1..100 {
        let candidate = preferred + offset;
        if candidate > 65535 {
            break; // Don't exceed valid port range
        }
        if is_port_available(candidate).await {
            println!("  Using port {}", candidate);
            return Ok(candidate as u16);
        }
    }

    anyhow::bail!(
        "No available ports found in range {}-{}",
        preferred,
        preferred + 100
    )
}
