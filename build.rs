use std::env;
use std::path::Path;
use std::process::Command;

fn main() {
    // Handle Tauri build if GUI feature is enabled
    #[cfg(feature = "gui")]
    tauri_build::build();

    // Skip frontend build if skip-frontend feature is enabled (for orchestrator-only builds)
    #[cfg(feature = "skip-frontend")]
    {
        println!("cargo:warning=skip-frontend feature enabled - skipping frontend build");
        return;
    }

    // Frontend build (always runs unless skip-frontend is set)
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let frontend_dir = Path::new(&manifest_dir).join("frontend");

    // Tell cargo to rerun if frontend files change
    println!("cargo:rerun-if-changed=frontend/src");
    println!("cargo:rerun-if-changed=frontend/package.json");
    println!("cargo:rerun-if-changed=frontend/package-lock.json");
    println!("cargo:rerun-if-changed=frontend/vite.config.ts");
    println!("cargo:rerun-if-changed=frontend/tsconfig.json");

    // Check if we're in release mode
    let profile = env::var("PROFILE").unwrap_or_default();
    let is_release = profile == "release";
    let dist_dir = frontend_dir.join("dist");

    // In debug mode, skip frontend build to speed up cargo check/build
    if !is_release {
        println!("cargo:warning=Debug build - skipping frontend build for speed");

        // Verify that dist exists (from a previous build)
        if !dist_dir.exists() {
            println!(
                "cargo:warning=No frontend dist found. Run 'cargo build --release' or 'cd frontend && pnpm build' to build frontend"
            );
            println!("cargo:warning=Continuing without frontend - app may not work correctly");
        } else {
            println!("cargo:warning=Using existing frontend dist from previous build");
        }

        return; // Skip frontend build in debug mode
    }

    // Release mode - always build frontend
    println!("cargo:warning=Release build - building frontend for production...");

    // Run pnpm install if node_modules doesn't exist
    if !frontend_dir.join("node_modules").exists() {
        println!("cargo:warning=Installing frontend dependencies...");
        match Command::new("pnpm")
            .args(["install"])
            .current_dir(&frontend_dir)
            .output()
        {
            Ok(output) => {
                if !output.status.success() {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    let stdout = String::from_utf8_lossy(&output.stdout);
                    panic!(
                        "pnpm install failed:\nSTDOUT: {}\nSTDERR: {}",
                        stdout, stderr
                    );
                }
                println!("cargo:warning=Frontend dependencies installed successfully");
            }
            Err(e) => {
                println!(
                    "cargo:warning=pnpm install failed: {} - skipping frontend build",
                    e
                );
                println!("cargo:warning=To build frontend, install pnpm or use pre-built dist");
            }
        }
    }

    // Run pnpm run build
    println!("cargo:warning=Building frontend with Vite...");
    match Command::new("pnpm")
        .args(["run", "build"])
        .current_dir(&frontend_dir)
        .output()
    {
        Ok(output) => {
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                let stdout = String::from_utf8_lossy(&output.stdout);
                panic!(
                    "pnpm run build failed:\nSTDOUT: {}\nSTDERR: {}",
                    stdout, stderr
                );
            }
        }
        Err(e) => {
            println!(
                "cargo:warning=pnpm run build failed: {} - skipping frontend build",
                e
            );
            println!("cargo:warning=To build frontend, install pnpm or use pre-built dist");
        }
    }

    // Verify that the dist directory was created
    if !dist_dir.exists() {
        panic!(
            "Frontend dist directory not found at: {} - either build frontend with pnpm or provide pre-built dist",
            dist_dir.display()
        );
    }

    println!("cargo:warning=Frontend build completed successfully");
}
