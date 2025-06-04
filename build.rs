use std::process::Command;
use std::env;
use std::path::Path;

fn main() {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let frontend_dir = Path::new(&manifest_dir).join("frontend");
    
    // Tell cargo to rerun if frontend files change
    println!("cargo:rerun-if-changed=frontend/src");
    println!("cargo:rerun-if-changed=frontend/package.json");
    println!("cargo:rerun-if-changed=frontend/package-lock.json");
    println!("cargo:rerun-if-changed=frontend/vite.config.ts");
    println!("cargo:rerun-if-changed=frontend/tsconfig.json");
    
    // Check if we're in a development environment
    let is_dev = env::var("PROFILE").unwrap_or_default() == "debug";
    
    if is_dev {
        println!("cargo:warning=Building frontend in development mode...");
    } else {
        println!("cargo:warning=Building frontend for production...");
    }
    
    // Run npm install if node_modules doesn't exist
    if !frontend_dir.join("node_modules").exists() {
        println!("cargo:warning=Installing frontend dependencies...");
        let output = Command::new("npm")
            .args(&["install"])
            .current_dir(&frontend_dir)
            .output()
            .expect("Failed to run npm install - make sure npm is installed and available in PATH");
            
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            panic!("npm install failed:\nSTDOUT: {}\nSTDERR: {}", stdout, stderr);
        }
        println!("cargo:warning=Frontend dependencies installed successfully");
    }
    
    // Run npm run build
    println!("cargo:warning=Building frontend with Vite...");
    let output = Command::new("npm")
        .args(&["run", "build"])
        .current_dir(&frontend_dir)
        .output()
        .expect("Failed to run npm run build - make sure npm is installed and available in PATH");
        
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        panic!("npm run build failed:\nSTDOUT: {}\nSTDERR: {}", stdout, stderr);
    }
    
    // Verify that the dist directory was created
    let dist_dir = frontend_dir.join("dist");
    if !dist_dir.exists() {
        panic!("Frontend build completed but dist directory was not created at: {}", dist_dir.display());
    }
    
    println!("cargo:warning=Frontend build completed successfully");
}