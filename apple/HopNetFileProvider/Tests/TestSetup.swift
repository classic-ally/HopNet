import Foundation
import FileProvider
import HopNetFileProviderCore
import TestHelpers

@main
struct TestSetup {
    static func main() async throws {
        let args = ProcessInfo.processInfo.arguments
        let testCase = args.count > 1 ? args[1] : "health_check"
        
        print("🧪 TestSetup starting: \(testCase)")
        
        switch testCase {
        case "health_check":
            try await runHealthCheck()
        case "expect_not_ready":
            try await runHealthCheckExpecting(.notReady)
        case "expect_ready":
            try await runHealthCheckExpecting(.ready)
        case "verify_empty_root":
            try await verifyEmptyRoot()
        case "all":
            try await runHealthCheck()
        default:
            throw TestError.unknownTestCase(testCase)
        }
        
        print("✅ TestSetup passed: \(testCase)")
    }
    
    static func runHealthCheck() async throws {
        print("🔍 Running health check using actual FileProvider API client...")
        
        // 1. Load test configuration from keychain using TestHelpers
        print("📋 Loading test configuration from keychain...")
        let config = try TestHelpers.loadTestConfig()
        print("✅ Configuration loaded: \(config.baseUrl)")
        
        // 2. Create FileProvider extension instance using TestHelpers
        print("🏗️ Creating FileProvider extension instance...")
        let fileProviderExtension = try TestHelpers.createTestExtension()
        print("✅ FileProvider extension created")
        
        // 3. Use the actual API client's healthCheck method
        print("🌡️ Calling API client healthCheck() method...")
        do {
            let healthStatus = try await fileProviderExtension.apiClient.healthCheck()
            print("✅ Backend health check call successful!")
            print("   Status: \(healthStatus.rawValue)")
            
            // The Rust orchestrator will call this with different expectations:
            // - First call should expect "not_ready" (before setup)  
            // - Second call should expect "ready" (after setup)
            // For now, we just report the actual status - validation happens in Rust
            
        } catch {
            throw TestError.setupFailed("Backend health check failed: \(error)")
        }
    }
    
    static func runHealthCheckExpecting(_ expectedStatus: HealthStatus) async throws {
        print("🔍 Running health check expecting status: \(expectedStatus.rawValue)")
        
        // Create FileProvider extension using TestHelpers
        let fileProviderExtension = try TestHelpers.createTestExtension()
        
        // Call health check and validate the expected status
        do {
            let actualStatus = try await fileProviderExtension.apiClient.healthCheck()
            print("✅ Health check call successful - Status: \(actualStatus.rawValue)")
            
            guard actualStatus == expectedStatus else {
                throw TestError.assertionFailed("Expected status '\(expectedStatus.rawValue)' but got '\(actualStatus.rawValue)'")
            }
            
            print("✅ Status validation passed!")
            
        } catch let error as TestError {
            // Re-throw our test errors
            throw error
        } catch {
            throw TestError.setupFailed("Backend health check failed: \(error)")
        }
    }
    
    static func verifyEmptyRoot() async throws {
        print("📂 Verifying root container is empty after setup...")
        
        // Create FileProvider extension using TestHelpers
        let fileProviderExtension = try TestHelpers.createTestExtension()
        
        // Enumerate root container
        let rootItems = try await TestHelpers.enumerateItems(
            fileProvider: fileProviderExtension, 
            containerIdentifier: .rootContainer
        )
        
        // Verify root is empty
        guard rootItems.isEmpty else {
            throw TestError.assertionFailed("Root container should be empty after setup, but found \(rootItems.count) items")
        }
        
        print("✅ Root container is empty (0 items)")
    }
}