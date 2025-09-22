#![allow(unused_variables)]
#![allow(unused_mut)]

use std::process::Command;
use std::time::Duration;
use tokio::time::sleep;
use hopnet_common::fileprovider::TestResponse;
use hopnet_common::setup::InitialSetupPayload;

/// Integration test orchestrator for FileProvider testing
#[cfg(target_os = "macos")]
#[tokio::test]
async fn test_fileprovider_integration() -> Result<(), Box<dyn std::error::Error>> {
    // Setup cleanup handler to ensure processes are killed even on test failure/timeout
    let cleanup = |mut backend_process: Option<std::process::Child>| {
        if let Some(ref mut process) = backend_process {
            let _ = process.kill();
            println!("🧹 Backend process terminated");
        }
    };
    // 1. Verify no backend is running (TestSetup should fail)
    println!("🔍 Verifying no backend is running...");
    let output = run_swift_command("TestSetup", "health_check");
    if output.status.success() {
        return Err("TestSetup unexpectedly succeeded - there may be a backend already running. Stop any existing HopNet instances and try again.".into());
    }
    println!("✅ Confirmed no backend running (TestSetup failed as expected)");
    
    // 2. Start backend binary (already built by cargo test)
    println!("🚀 Starting HopNet backend in test mode...");
    let mut backend_process = Command::new("target/debug/hopnet")
        .spawn()
        .expect("Failed to start backend process");
    
    // Use closure to ensure cleanup even on early returns
    let result: Result<(), Box<dyn std::error::Error>> = async {
        // Give backend time to start
        sleep(Duration::from_secs(3)).await;
        
        // 4. Fetch test credentials from backend
        println!("🔑 Fetching test credentials...");
        let test_response = fetch_test_credentials().await?;
        println!("✅ Got credentials: API key = {}..., Backend URL = {}", 
                 &test_response.api_key[..8], test_response.backend_url);
        
        // 5. Setup test environment with credentials
        println!("🔧 Setting up test environment...");
        unsafe {
            std::env::set_var("HOPNET_TEST_API_KEY", &test_response.api_key);
            std::env::set_var("HOPNET_TEST_BACKEND_URL", &test_response.backend_url);
        }
        println!("📝 Set test environment variables");
        
        // 6. Verify backend health status is NotReady (no setup completed yet)
        println!("🔍 Verifying backend health status is NotReady...");
        let output = run_swift_command("TestSetup", "expect_not_ready");
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(format!("TestSetup expect_not_ready failed:\nStdout: {}\nStderr: {}", 
                              stdout, stderr).into());
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        println!("✅ Backend health status validated: {}", stdout.trim());
        
        // 7. Perform initial setup to get backend to Ready state
        println!("🔧 Performing initial setup...");
        setup_backend(&test_response.backend_url).await?;
        println!("✅ Backend setup completed");
        
        // 8. Verify backend health status is now Ready
        println!("🔍 Verifying backend health status is now Ready...");
        let output = run_swift_command("TestSetup", "expect_ready");
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(format!("TestSetup expect_ready failed after setup:\nStdout: {}\nStderr: {}", 
                              stdout, stderr).into());
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        println!("✅ Backend health status after setup: {}", stdout.trim());
        
        // 9. Verify root container is empty after setup
        println!("📂 Verifying root container is empty...");
        let output = run_swift_command("TestSetup", "verify_empty_root");
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(format!("TestSetup verify_empty_root failed:\nStdout: {}\nStderr: {}", 
                              stdout, stderr).into());
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        println!("✅ Root container verification: {}", stdout.trim());
        
        // 10. Test folder creation with comprehensive verification
        println!("📁 Testing folder creation...");
        let output = run_swift_command("TestCreation", "folder_creation");
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(format!("TestCreation folder_creation failed:\nStdout: {}\nStderr: {}", 
                              stdout, stderr).into());
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        println!("✅ Folder creation test: {}", stdout.trim());
        
        // 11. Test multiple folder name variations
        println!("📁 Testing multiple folder name variations...");
        let output = run_swift_command("TestCreation", "multiple_folder_names");
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(format!("TestCreation multiple_folder_names failed:\nStdout: {}\nStderr: {}", 
                              stdout, stderr).into());
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        println!("✅ Multiple folder names test: {}", stdout.trim());
        
        // 12. Test nested folder creation (folders inside folders)
        println!("📁 Testing nested folder creation...");
        let output = run_swift_command("TestCreation", "nested_folders");
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(format!("TestCreation nested_folders failed:\nStdout: {}\nStderr: {}", 
                              stdout, stderr).into());
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        println!("✅ Nested folder creation test: {}", stdout.trim());
        
        // 13. Test basic file creation
        println!("📄 Testing basic file creation...");
        let output = run_swift_command("TestCreation", "file_creation");
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(format!("TestCreation file_creation failed:\nStdout: {}\nStderr: {}", 
                              stdout, stderr).into());
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        println!("✅ Basic file creation test: {}", stdout.trim());
        
        // 14. Test multiple file types
        println!("📄 Testing multiple file types...");
        let output = run_swift_command("TestCreation", "multiple_file_types");
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(format!("TestCreation multiple_file_types failed:\nStdout: {}\nStderr: {}", 
                              stdout, stderr).into());
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        println!("✅ Multiple file types test: {}", stdout.trim());
        
        // 15. Test file creation in nested folders
        println!("📄 Testing file creation in nested folders...");
        let output = run_swift_command("TestCreation", "file_in_nested_folder");
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(format!("TestCreation file_in_nested_folder failed:\nStdout: {}\nStderr: {}", 
                              stdout, stderr).into());
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        println!("✅ File creation in nested folders test: {}", stdout.trim());
        
        // 16. Test file creation with content verification (create → download → verify)
        println!("📄 Testing file creation with content verification...");
        let output = run_swift_command("TestCreation", "file_content_verification");
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(format!("TestCreation file_content_verification failed:\nStdout: {}\nStderr: {}", 
                              stdout, stderr).into());
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        println!("✅ File creation with content verification test: {}", stdout.trim());
        
        // 17. Test file rename operations
        println!("📝 Testing file rename operations...");
        let output = run_swift_command("TestModification", "rename_file");
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(format!("TestModification rename_file failed:\nStdout: {}\nStderr: {}", 
                              stdout, stderr).into());
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        println!("✅ File rename test: {}", stdout.trim());
        
        // 18. Test nested file rename operations (dual enumeration)
        println!("📝 Testing nested file rename operations...");
        let output = run_swift_command("TestModification", "rename_file_in_folder");
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(format!("TestModification rename_file_in_folder failed:\nStdout: {}\nStderr: {}", 
                              stdout, stderr).into());
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        println!("✅ Nested file rename test: {}", stdout.trim());
        
        // 19. Test folder rename operations
        println!("📝 Testing folder rename operations...");
        let output = run_swift_command("TestModification", "rename_folder");
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(format!("TestModification rename_folder failed:\nStdout: {}\nStderr: {}", 
                              stdout, stderr).into());
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        println!("✅ Folder rename test: {}", stdout.trim());
        
        // 20. Test file move operations
        println!("📝 Testing file move operations...");
        let output = run_swift_command("TestModification", "move_file");
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(format!("TestModification move_file failed:\nStdout: {}\nStderr: {}", 
                              stdout, stderr).into());
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        println!("✅ File move test: {}", stdout.trim());
        
        // 21. Test folder-to-folder file move operations (triple enumeration)
        println!("📝 Testing folder-to-folder file move operations...");
        let output = run_swift_command("TestModification", "move_file_folder_to_folder");
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(format!("TestModification move_file_folder_to_folder failed:\nStdout: {}\nStderr: {}", 
                              stdout, stderr).into());
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        println!("✅ Folder-to-folder file move test: {}", stdout.trim());
        
        // 22. Test content update operations
        println!("📝 Testing file content update...");
        let output = run_swift_command("TestModification", "update_file_content");
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(format!("TestModification update_file_content failed:\nStdout: {}\nStderr: {}", 
                              stdout, stderr).into());
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        println!("✅ File content update test: {}", stdout.trim());
        
        // 23. Test complex move operations (move + rename)
        println!("📝 Testing complex move operations...");
        let output = run_swift_command("TestModification", "complex_move");
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(format!("TestModification complex_move failed:\nStdout: {}\nStderr: {}", 
                              stdout, stderr).into());
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        println!("✅ Complex move test: {}", stdout.trim());
        
        // 24. Test rename parent with children (hierarchy preservation)
        println!("📝 Testing rename parent with children...");
        let output = run_swift_command("TestModification", "rename_parent_with_children");
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(format!("TestModification rename_parent_with_children failed:\nStdout: {}\nStderr: {}", 
                              stdout, stderr).into());
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        println!("✅ Rename parent with children test: {}", stdout.trim());
        
        // 25. Test invalid modifications (circular reference prevention, etc.)
        println!("🚫 Testing invalid modifications...");
        let output = run_swift_command("TestModification", "invalid_modifications");
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(format!("TestModification invalid_modifications failed:\nStdout: {}\nStderr: {}", 
                              stdout, stderr).into());
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        println!("✅ Invalid modifications test: {}", stdout.trim());
        
        // 26. Test single file deletion
        println!("🗑️ Testing single file deletion...");
        let output = run_swift_command("TestDeletion", "single_file");
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(format!("TestDeletion single_file failed:\nStdout: {}\nStderr: {}", 
                              stdout, stderr).into());
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        println!("✅ Single file deletion test: {}", stdout.trim());
        
        // 27. Test single file deletion in folder
        println!("🗑️ Testing single file deletion in folder...");
        let output = run_swift_command("TestDeletion", "single_file_in_folder");
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(format!("TestDeletion single_file_in_folder failed:\nStdout: {}\nStderr: {}", 
                              stdout, stderr).into());
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        println!("✅ Single file in folder deletion test: {}", stdout.trim());
        
        // 28. Test empty folder deletion
        println!("🗑️ Testing empty folder deletion...");
        let output = run_swift_command("TestDeletion", "empty_folder");
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(format!("TestDeletion empty_folder failed:\nStdout: {}\nStderr: {}", 
                              stdout, stderr).into());
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        println!("✅ Empty folder deletion test: {}", stdout.trim());
        
        // 29. Test multiple file types deletion
        println!("🗑️ Testing multiple file types deletion...");
        let output = run_swift_command("TestDeletion", "multiple_file_types");
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(format!("TestDeletion multiple_file_types failed:\nStdout: {}\nStderr: {}", 
                              stdout, stderr).into());
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        println!("✅ Multiple file types deletion test: {}", stdout.trim());
        
        // 30. Test recursive folder deletion
        println!("🗑️ Testing recursive folder deletion...");
        let output = run_swift_command("TestDeletion", "recursive_folder");
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(format!("TestDeletion recursive_folder failed:\nStdout: {}\nStderr: {}", 
                              stdout, stderr).into());
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        println!("✅ Recursive folder deletion test: {}", stdout.trim());
        
        // 31. Test deep hierarchy deletion
        println!("🗑️ Testing deep hierarchy deletion...");
        let output = run_swift_command("TestDeletion", "deep_hierarchy");
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(format!("TestDeletion deep_hierarchy failed:\nStdout: {}\nStderr: {}", 
                              stdout, stderr).into());
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        println!("✅ Deep hierarchy deletion test: {}", stdout.trim());
        
        // 32. Test mixed content folder deletion
        println!("🗑️ Testing mixed content folder deletion...");
        let output = run_swift_command("TestDeletion", "mixed_content_folder");
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(format!("TestDeletion mixed_content_folder failed:\nStdout: {}\nStderr: {}", 
                              stdout, stderr).into());
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        println!("✅ Mixed content folder deletion test: {}", stdout.trim());
        
        // 33. Test nested recursive deletion
        println!("🗑️ Testing nested recursive deletion...");
        let output = run_swift_command("TestDeletion", "nested_recursive");
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(format!("TestDeletion nested_recursive failed:\nStdout: {}\nStderr: {}", 
                              stdout, stderr).into());
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        println!("✅ Nested recursive deletion test: {}", stdout.trim());
        
        // 34. Test non-existent item deletion error handling
        println!("🚫 Testing non-existent item deletion...");
        let output = run_swift_command("TestDeletion", "non_existent_item");
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(format!("TestDeletion non_existent_item failed:\nStdout: {}\nStderr: {}", 
                              stdout, stderr).into());
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        println!("✅ Non-existent item deletion test: {}", stdout.trim());
        
        // 35. Test root container deletion prevention
        println!("🚫 Testing root container deletion prevention...");
        let output = run_swift_command("TestDeletion", "root_container_protection");
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(format!("TestDeletion root_container_protection failed:\nStdout: {}\nStderr: {}", 
                              stdout, stderr).into());
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        println!("✅ Root container deletion prevention test: {}", stdout.trim());
        
        // 36. Test non-recursive folder deletion error
        println!("🚫 Testing non-recursive folder deletion error...");
        let output = run_swift_command("TestDeletion", "non_recursive_folder");
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(format!("TestDeletion non_recursive_folder failed:\nStdout: {}\nStderr: {}", 
                              stdout, stderr).into());
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        println!("✅ Non-recursive folder deletion test: {}", stdout.trim());
        
        Ok(())
    }.await;
    
    // 7. Always cleanup (even on failure)
    println!("🧹 Cleaning up test environment...");
    unsafe {
        std::env::remove_var("HOPNET_TEST_API_KEY");
        std::env::remove_var("HOPNET_TEST_BACKEND_URL");
    }
    cleanup(Some(backend_process));
    
    // Return the result (propagate any errors)
    result?;
    println!("✅ Integration test completed successfully");
    
    Ok(())
}

/// Helper function to run Swift test commands with standardized setup
fn run_swift_command(executable: &str, test_case: &str) -> std::process::Output {
    Command::new("swift")
        .args(&[
            "run",
            "--package-path", "apple/HopNetFileProvider",
            executable,
            test_case
        ])
        .env("HOPNET_TEST_API_KEY", std::env::var("HOPNET_TEST_API_KEY").unwrap_or_default())
        .env("HOPNET_TEST_BACKEND_URL", std::env::var("HOPNET_TEST_BACKEND_URL").unwrap_or_default())
        // Force use of system SDK instead of Nix SDK for Swift compilation
        .env("DEVELOPER_DIR", "/Applications/Xcode.app/Contents/Developer")
        .env("SDKROOT", "/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk")
        .output()
        .expect("Failed to run Swift test")
}

/// Fetch test credentials from the backend test endpoint
async fn fetch_test_credentials() -> Result<TestResponse, Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    
    // Try multiple times as backend might still be starting
    for attempt in 1..=10 {
        match client.get("http://localhost:34634/integrations/fileprovider/test").send().await {
            Ok(response) => {
                if response.status().is_success() {
                    let test_response: TestResponse = response.json().await?;
                    return Ok(test_response);
                } else if response.status() == 404 {
                    return Err("Test endpoint not available - ensure running in debug mode".into());
                }
            }
            Err(_) if attempt < 10 => {
                println!("⏳ Backend not ready, retrying... (attempt {}/10)", attempt);
                sleep(Duration::from_millis(500)).await;
                continue;
            }
            Err(e) => return Err(e.into()),
        }
    }
    
    Err("Failed to connect to backend after 10 attempts".into())
}

/// Perform initial backend setup by posting to /setup endpoint
async fn setup_backend(backend_url: &str) -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    let setup_url = format!("{}/setup", backend_url);
    
    // Use the actual InitialSetupPayload struct for type safety
    let setup_payload = InitialSetupPayload {
        username: "testuser".to_string(),
        password: "testpass".to_string(),
        node_name: "testnode".to_string(),
        ip_address: "127.0.0.1".to_string(),
        port: 34634,  // Use test port
    };
    
    let response = client
        .post(&setup_url)
        .json(&setup_payload)
        .send()
        .await?;
    
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await?;
        return Err(format!("Setup failed with status {}: {}", status, body).into());
    }
    
    Ok(())
}