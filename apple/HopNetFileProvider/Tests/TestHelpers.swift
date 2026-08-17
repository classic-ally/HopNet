import Foundation
import FileProvider
import HopNetFileProviderCore
import UniformTypeIdentifiers

public struct TestHelpers {
    
    // MARK: - Test Context
    
    /// Captures initial test state and provides common test infrastructure
    public struct TestContext {
        public let fileProvider: HopNetFileProviderExtensionBase
        public let config: FileProviderConfig
        public let initialSignalCount: Int
        public let initialConsensusHeight: UInt64
        public let initialRootItemCount: Int
        
        /// Capture initial test state for standardized test setup
        public static func capture() async throws -> TestContext {
            let fileProvider = try TestHelpers.createTestExtension()
            let config = try TestHelpers.loadTestConfig()
            let initialSignalCount = try await TestHelpers.getSignalCount(config: config)
            let initialChanges = try await fileProvider.apiClient.getChanges()
            let initialRootItems = try await TestHelpers.enumerateItems(
                fileProvider: fileProvider,
                containerIdentifier: .rootContainer
            )
            
            return TestContext(
                fileProvider: fileProvider,
                config: config,
                initialSignalCount: initialSignalCount,
                initialConsensusHeight: initialChanges.current_consensus_height,
                initialRootItemCount: initialRootItems.count
            )
        }
        
        /// Capture initial state for a specific container
        public static func capture(containerIdentifier: NSFileProviderItemIdentifier) async throws -> (context: TestContext, initialContainerItemCount: Int) {
            let context = try await capture()
            let containerItems = try await TestHelpers.enumerateItems(
                fileProvider: context.fileProvider,
                containerIdentifier: containerIdentifier
            )
            return (context, containerItems.count)
        }
    }
    
    // MARK: - Test Configuration
    
    /// Test-specific keychain service to avoid conflicts with production
    static let testServiceName = "com.hopnet.desktop.fileprovider.test"
    static let testAPIKeyAccount = "api_key"
    static let testBaseURLAccount = "base_url"
    static let testTimeout: TimeInterval = 30.0
    
    // MARK: - FileProvider Extension Creation
    
    public static func createTestExtension() throws -> HopNetFileProviderExtensionBase {
        // Create a test domain
        let domain = NSFileProviderDomain(identifier: NSFileProviderDomainIdentifier("com.hopnet.test"),
                                        displayName: "Test HopNet")
        
        // Load test configuration first
        let testConfig = try loadTestConfig()
        
        // Create extension instance with test configuration using the convenience initializer
        let fileProviderExtension = HopNetFileProviderExtensionBase(domain: domain, config: testConfig)
        return fileProviderExtension
    }
    
    // MARK: - Test Configuration Loading
    
    /// Load test configuration from environment variables or keychain
    /// Checks environment variables first (works on all platforms), then falls back to keychain (macOS only)
    public static func loadTestConfig() throws -> FileProviderConfig {
        // 1. Try environment variables first (works everywhere)
        if let apiKey = ProcessInfo.processInfo.environment["HOPNET_TEST_API_KEY"],
           let backendUrl = ProcessInfo.processInfo.environment["HOPNET_TEST_BACKEND_URL"] {
            return FileProviderConfig(baseUrl: backendUrl, apiKey: apiKey)
        }
        
        // 2. Fall back to keychain (macOS only)
        #if os(macOS)
        do {
            let apiKey = try KeychainHelper.loadItem(
                service: testServiceName, 
                account: testAPIKeyAccount
            )
            let baseUrl = try KeychainHelper.loadItem(
                service: testServiceName, 
                account: testBaseURLAccount
            )
            
            return FileProviderConfig(baseUrl: baseUrl, apiKey: apiKey)
        } catch {
            throw TestError.configurationNotFound(
                "No test configuration found. Set HOPNET_TEST_API_KEY and HOPNET_TEST_BACKEND_URL environment variables, or ensure keychain is configured."
            )
        }
        #else
        throw TestError.configurationNotFound(
            "No test configuration available. Set HOPNET_TEST_API_KEY and HOPNET_TEST_BACKEND_URL environment variables."
        )
        #endif
    }
    
    // MARK: - Enumeration Helpers
    
    /// Enumerate items in a container (folder)
    public static func enumerateItems(fileProvider: HopNetFileProviderExtensionBase, 
                               containerIdentifier: NSFileProviderItemIdentifier) async throws -> [NSFileProviderItem] {
        print("🔍 Enumerating container: \(containerIdentifier.rawValue)")
        
        // Use the FileProvider extension's enumeration method
        let enumerator = try fileProvider.enumerator(for: containerIdentifier, request: NSFileProviderRequest())
        
        // Create an enumeration observer to collect items
        let items: [NSFileProviderItem] = try await withCheckedThrowingContinuation { continuation in
            
            class TestEnumerationObserver: NSObject, NSFileProviderEnumerationObserver {
                private let continuation: CheckedContinuation<[NSFileProviderItem], Error>
                private var collectedItems: [NSFileProviderItem] = []
                
                init(continuation: CheckedContinuation<[NSFileProviderItem], Error>) {
                    self.continuation = continuation
                }
                
                func didEnumerate(_ items: [NSFileProviderItem]) {
                    collectedItems.append(contentsOf: items)
                }
                
                func finishEnumerating(upTo nextPage: NSFileProviderPage?) {
                    // Return the collected NSFileProviderItems
                    continuation.resume(returning: collectedItems)
                }
                
                func finishEnumeratingWithError(_ error: Error) {
                    continuation.resume(throwing: error)
                }
            }
            
            let observer = TestEnumerationObserver(continuation: continuation)
            enumerator.enumerateItems(for: observer, startingAt: NSFileProviderPage.initialPageSortedByName as NSFileProviderPage)
        }
        
        print("✅ Found \(items.count) items in container")
        return items
    }
    
    // MARK: - Creation Helpers
    
    /// Test folder creation end-to-end with automatic verification
    /// This handles getting initial state, creating folder, and verifying all changes
    public static func testFolderCreation(folderName: String) async throws {
        print("📁 Starting comprehensive folder creation test for: \(folderName)")
        
        // Create FileProvider extension and config
        let fileProviderExtension = try createTestExtension()
        let config = try loadTestConfig()
        
        // Capture initial state
        let initialItems = try await enumerateItems(
            fileProvider: fileProviderExtension,
            containerIdentifier: .rootContainer
        )
        let initialSignalCount = try await getSignalCount(config: config)
        
        print("📋 Initial state: \(initialItems.count) items, signal count: \(initialSignalCount)")
        
        // Perform creation
        print("🏗️ Creating folder: \(folderName)")
        try await fileProviderExtension.apiClient.createItem(
            parentItemIdentifier: NSFileProviderItemIdentifier.rootContainer.rawValue,
            filename: folderName,
            fileUrl: nil
        )
        
        // Comprehensive verification (with retry for async consensus)
        let currentSignalCount = try await waitForSignalCount(config: config, expectedCount: initialSignalCount + 1)
        
        let currentItems = try await enumerateItems(
            fileProvider: fileProviderExtension,
            containerIdentifier: .rootContainer
        )
        guard currentItems.count == initialItems.count + 1 else {
            throw TestError.assertionFailed(
                "Item count should be \(initialItems.count + 1) but got \(currentItems.count)"
            )
        }
        
        try verifyItemExists(items: currentItems, filename: folderName, isFolder: true)
        
        // Version validation: Verify created folder has consensus height version
        guard let createdFolder = currentItems.first(where: { $0.filename == folderName }) else {
            throw TestError.assertionFailed("Created folder '\(folderName)' not found for version validation")
        }
        try assertVersionIsConsensusHeight(for: createdFolder)
        
        let changesResponse = try await fileProviderExtension.apiClient.getChanges()
        let folderFound = changesResponse.items.contains { change in
            change.filename == folderName
        }
        guard folderFound else {
            throw TestError.assertionFailed("Folder '\(folderName)' not found in changes API")
        }
        
        print("✅ PASS '\(folderName)': signals \(initialSignalCount)→\(currentSignalCount), items \(initialItems.count)→\(currentItems.count), enumeration+changes+version ✓")
    }
    
    /// Test nested folder creation with comprehensive verification including changes API
    public static func testNestedFolderCreation(folderName: String, 
                                               parentIdentifier: NSFileProviderItemIdentifier,
                                               parentName: String) async throws {
        print("📁 Creating nested folder '\(folderName)' in '\(parentName)'")
        
        let fileProviderExtension = try createTestExtension()
        let config = try loadTestConfig()
        
        // Get initial state of both parent folder and root (for verification)
        let initialParentItems = try await enumerateItems(
            fileProvider: fileProviderExtension,
            containerIdentifier: parentIdentifier
        )
        let initialRootItems = try await enumerateItems(
            fileProvider: fileProviderExtension,
            containerIdentifier: .rootContainer
        )
        let initialSignalCount = try await getSignalCount(config: config)
        
        // Capture parent's initial version before child creation
        let parentItem = try await getItemByIdentifier(parentIdentifier)
        let parentVersionBefore = try getConsensusHeight(from: parentItem)
        
        print("📋 Initial state - parent '\(parentName)': \(initialParentItems.count) items, root: \(initialRootItems.count) items, signals: \(initialSignalCount), parent version: \(parentVersionBefore)")
        
        // Create folder in parent
        try await fileProviderExtension.apiClient.createItem(
            parentItemIdentifier: parentIdentifier.rawValue,
            filename: folderName,
            fileUrl: nil
        )
        
        // Comprehensive verification (with retry for async consensus)
        let currentSignalCount = try await waitForSignalCount(config: config, expectedCount: initialSignalCount + 1)
        
        // Parent container should have +1 item
        let currentParentItems = try await enumerateItems(
            fileProvider: fileProviderExtension,
            containerIdentifier: parentIdentifier
        )
        guard currentParentItems.count == initialParentItems.count + 1 else {
            throw TestError.assertionFailed(
                "Parent '\(parentName)' item count should be \(initialParentItems.count + 1) but got \(currentParentItems.count)"
            )
        }
        
        // Root container should remain unchanged (nested creation doesn't affect root count)
        let currentRootItems = try await enumerateItems(
            fileProvider: fileProviderExtension,
            containerIdentifier: .rootContainer
        )
        guard currentRootItems.count == initialRootItems.count else {
            throw TestError.assertionFailed(
                "Root item count should remain \(initialRootItems.count) but got \(currentRootItems.count)"
            )
        }
        
        // Verify folder exists and has correct parent
        guard let createdFolder = currentParentItems.first(where: { $0.filename == folderName }) else {
            throw TestError.assertionFailed("Folder '\(folderName)' not found in parent '\(parentName)'")
        }
        
        guard createdFolder.parentItemIdentifier == parentIdentifier else {
            throw TestError.assertionFailed(
                "Folder '\(folderName)' has wrong parent. Expected: \(parentIdentifier.rawValue), Got: \(createdFolder.parentItemIdentifier.rawValue)"
            )
        }
        
        let isFolder = createdFolder.contentType?.conforms(to: .folder) ?? false
        guard isFolder else {
            throw TestError.assertionFailed("Created item '\(folderName)' is not a folder")
        }
        
        // Version validation: Verify created folder has consensus height version
        try assertVersionIsConsensusHeight(for: createdFolder)
        
        // Version validation: Verify parent version increased after child creation
        let parentItemAfter = try await getItemByIdentifier(parentIdentifier)
        let parentVersionAfter = try getConsensusHeight(from: parentItemAfter)
        
        guard parentVersionAfter > parentVersionBefore else {
            throw TestError.assertionFailed(
                "Parent '\(parentName)' version should increase after child creation. Before: \(parentVersionBefore), After: \(parentVersionAfter)"
            )
        }
        
        // Verify folder appears in changes API
        let changesResponse = try await fileProviderExtension.apiClient.getChanges()
        let folderFound = changesResponse.items.contains { change in
            change.filename == folderName
        }
        guard folderFound else {
            throw TestError.assertionFailed("Nested folder '\(folderName)' not found in changes API")
        }
        
        print("✅ PASS nested '\(folderName)' in '\(parentName)': signals \(initialSignalCount)→\(currentSignalCount), parent \(initialParentItems.count)→\(currentParentItems.count), root unchanged, parent version \(parentVersionBefore)→\(parentVersionAfter), parent-child+changes+version ✓")
    }
    
    /// Get folder identifier by name from root container
    public static func getFolderIdentifier(folderName: String) async throws -> NSFileProviderItemIdentifier {
        return try await getFolderIdentifier(folderName: folderName, parentIdentifier: .rootContainer)
    }
    
    /// Get folder identifier by name from specific parent container
    public static func getFolderIdentifier(folderName: String, 
                                         parentIdentifier: NSFileProviderItemIdentifier) async throws -> NSFileProviderItemIdentifier {
        let fileProviderExtension = try createTestExtension()
        
        let items = try await enumerateItems(
            fileProvider: fileProviderExtension,
            containerIdentifier: parentIdentifier
        )
        
        guard let folder = items.first(where: { $0.filename == folderName }) else {
            let parentName = parentIdentifier == .rootContainer ? "root" : parentIdentifier.rawValue
            throw TestError.assertionFailed("Folder '\(folderName)' not found in '\(parentName)'")
        }
        
        return folder.itemIdentifier
    }
    
    /// Wait for signal count to reach expected value with retries (handles async consensus)
    public static func waitForSignalCount(config: FileProviderConfig, expectedCount: Int) async throws -> Int {
        let maxRetries = 40 // 40 * 50ms = 2 seconds max
        
        for attempt in 1...maxRetries {
            let currentCount = try await getSignalCount(config: config)
            
            if currentCount >= expectedCount {
                return currentCount
            }
            
            // Wait 50ms before retrying (consensus typically takes 100-200ms)
            try await Task.sleep(nanoseconds: 50_000_000)
        }
        
        // Final attempt
        let finalCount = try await getSignalCount(config: config)
        throw TestError.timeout("Signal count did not reach \(expectedCount) within 1s. Final count: \(finalCount)")
    }
    
    /// Get current signal count from backend (unprotected test route)
    public static func getSignalCount(config: FileProviderConfig) async throws -> Int {
        let urlString = "\(config.baseUrl)/api/integrations/fileprovider/test/signals"
        
        guard let url = URL(string: urlString) else {
            throw TestError.setupFailed("Invalid signal count URL")
        }
        
        let request = URLRequest(url: url)
        
        let (data, response) = try await URLSession.shared.data(for: request)
        
        guard let httpResponse = response as? HTTPURLResponse,
              httpResponse.statusCode == 200 else {
            throw TestError.setupFailed("Failed to fetch signal count")
        }
        
        guard let signalCountString = String(data: data, encoding: .utf8),
              let signalCount = Int(signalCountString) else {
            throw TestError.setupFailed("Invalid signal count response")
        }
        
        return signalCount
    }
    
    // MARK: - Verification Helpers
    
    /// Verify that an item exists in enumeration results
    public static func verifyItemExists(items: [NSFileProviderItem], filename: String, isFolder: Bool? = nil) throws {
        guard let item = items.first(where: { $0.filename == filename }) else {
            throw TestError.assertionFailed("Item '\(filename)' not found in enumeration")
        }
        
        // Verify type if specified
        if let expectedFolder = isFolder {
            let actualIsFolder = item.contentType?.conforms(to: .folder) ?? false
            guard actualIsFolder == expectedFolder else {
                let expectedType = expectedFolder ? "folder" : "file"
                let actualType = actualIsFolder ? "folder" : "file"
                throw TestError.assertionFailed("Item '\(filename)' expected to be \(expectedType) but is \(actualType)")
            }
        }
    }
    
    /// Verify that an item does NOT exist in enumeration results
    public static func verifyItemNotExists(items: [NSFileProviderItem], filename: String) throws {
        guard !items.contains(where: { $0.filename == filename }) else {
            throw TestError.assertionFailed("Item '\(filename)' unexpectedly found in enumeration")
        }
        print("✅ Verified item does not exist: \(filename)")
    }
    
    // MARK: - Version Testing Helpers
    
    /// Version type detection for NSFileProviderItemVersion data
    public enum VersionType {
        case consensusHeight(UInt64)
        case timestamp(Int64)
        case invalid
    }
    
    /// Extract version data from NSFileProviderItem
    public static func extractVersionData(from item: NSFileProviderItem) -> (contentVersion: Data, metadataVersion: Data) {
        guard let version = item.itemVersion else {
            fatalError("Item '\(item.filename)' has no itemVersion - this should not happen in our implementation")
        }
        return (version.contentVersion, version.metadataVersion)
    }
    
    /// Determine the type of version data (consensus height vs timestamp)
    public static func getVersionType(from versionData: Data) -> VersionType {
        // Try to decode as UInt64 (consensus height)
        if versionData.count == MemoryLayout<UInt64>.size {
            let height = versionData.withUnsafeBytes { $0.load(as: UInt64.self) }
            return .consensusHeight(height)
        }
        // Try to decode as Int64 (timestamp)
        else if versionData.count == MemoryLayout<Int64>.size {
            let timestamp = versionData.withUnsafeBytes { $0.load(as: Int64.self) }
            return .timestamp(timestamp)
        }
        return .invalid
    }
    
    /// Extract consensus height from NSFileProviderItem version
    public static func getConsensusHeight(from item: NSFileProviderItem) throws -> UInt64 {
        let (contentVersion, _) = extractVersionData(from: item)
        
        switch getVersionType(from: contentVersion) {
        case .consensusHeight(let height):
            return height
        case .timestamp(_):
            throw TestError.assertionFailed("Expected consensus height but got timestamp-based version for item '\(item.filename)'")
        case .invalid:
            throw TestError.assertionFailed("Invalid version data format for item '\(item.filename)'")
        }
    }
    
    /// Assert that an item's version uses consensus height (not timestamp fallback)
    public static func assertVersionIsConsensusHeight(for item: NSFileProviderItem) throws {
        let (contentVersion, metadataVersion) = extractVersionData(from: item)
        
        // Verify content version is consensus height
        switch getVersionType(from: contentVersion) {
        case .consensusHeight(let height):
            print("✅ Content version is consensus height: \(height) for '\(item.filename)'")
        case .timestamp(_):
            throw TestError.assertionFailed("Content version should be consensus height but got timestamp for item '\(item.filename)'")
        case .invalid:
            throw TestError.assertionFailed("Invalid content version data format for item '\(item.filename)'")
        }
        
        // Verify metadata version is consensus height
        switch getVersionType(from: metadataVersion) {
        case .consensusHeight(let height):
            print("✅ Metadata version is consensus height: \(height) for '\(item.filename)'")
        case .timestamp(_):
            throw TestError.assertionFailed("Metadata version should be consensus height but got timestamp for item '\(item.filename)'")
        case .invalid:
            throw TestError.assertionFailed("Invalid metadata version data format for item '\(item.filename)'")
        }
    }
    
    /// Compare two items' versions (returns ordering)
    public static func compareVersions(item1: NSFileProviderItem, item2: NSFileProviderItem) throws -> ComparisonResult {
        let height1 = try getConsensusHeight(from: item1)
        let height2 = try getConsensusHeight(from: item2)
        
        if height1 < height2 {
            return .orderedAscending
        } else if height1 > height2 {
            return .orderedDescending
        } else {
            return .orderedSame
        }
    }
    
    /// Assert that version is greater than a baseline height
    public static func assertVersionGreaterThan(item: NSFileProviderItem, baselineHeight: UInt64) throws {
        let currentHeight = try getConsensusHeight(from: item)
        guard currentHeight > baselineHeight else {
            throw TestError.assertionFailed("Version should be greater than \(baselineHeight) but got \(currentHeight) for item '\(item.filename)'")
        }
        print("✅ Version \(currentHeight) > baseline \(baselineHeight) for '\(item.filename)'")
    }
    
    /// Get an item by its identifier using the FileProvider's item(for:) API
    public static func getItemByIdentifier(_ identifier: NSFileProviderItemIdentifier) async throws -> NSFileProviderItem {
        let fileProviderExtension = try createTestExtension()
        
        return try await withCheckedThrowingContinuation { continuation in
            _ = fileProviderExtension.item(for: identifier, request: NSFileProviderRequest()) { item, error in
                if let error = error {
                    continuation.resume(throwing: error)
                } else if let item = item {
                    continuation.resume(returning: item)
                } else {
                    continuation.resume(throwing: TestError.assertionFailed("No item returned for identifier \(identifier.rawValue)"))
                }
            }
        }
    }
    
    // MARK: - File Creation Tests
    
    /// Test file creation with comprehensive verification (enumeration + changes + version)
    /// Similar to testFolderCreation but handles file uploads with content
    public static func testFileCreation(fileName: String, content: String, parentIdentifier: NSFileProviderItemIdentifier = .rootContainer) async throws {
        let context = try await TestContext.capture()
        
        _ = try await verifyFileCreation(
            context: context,
            fileName: fileName,
            content: content,
            parentIdentifier: parentIdentifier,
            operationDescription: "file creation '\(fileName)' (\(content.count) chars)"
        )
    }
    
    /// Test file creation in nested folder with parent version tracking
    public static func testNestedFileCreation(fileName: String, content: String, parentIdentifier: NSFileProviderItemIdentifier, parentName: String) async throws {
        let context = try await TestContext.capture()
        
        _ = try await verifyFileCreation(
            context: context,
            fileName: fileName,
            content: content,
            parentIdentifier: parentIdentifier,
            operationDescription: "nested file creation '\(fileName)' in '\(parentName)' (\(content.count) chars)"
        )
    }
    
    // MARK: - File Utilities
    
    /// Create a temporary file with specified content for testing file uploads
    public static func createTemporaryFile(content: String, fileName: String) throws -> URL {
        let tempDir = FileManager.default.temporaryDirectory
        let tempFileURL = tempDir.appendingPathComponent("TestFile_\(UUID())_\(fileName)")
        
        guard let data = content.data(using: .utf8) else {
            throw TestError.setupFailed("Failed to convert content to data")
        }
        
        try data.write(to: tempFileURL)
        return tempFileURL
    }
    
    /// Create a temporary file with binary data for testing
    public static func createTemporaryFile(data: Data, fileName: String) throws -> URL {
        let tempDir = FileManager.default.temporaryDirectory
        let tempFileURL = tempDir.appendingPathComponent("TestFile_\(UUID())_\(fileName)")
        
        try data.write(to: tempFileURL)
        return tempFileURL
    }
    
    /// Generate predictable test content of specified size
    public static func generateTestContent(size: Int, pattern: String = "TestData") -> String {
        if size == 0 {
            return ""
        }
        
        let basePattern = pattern + "\n"
        let patternLength = basePattern.count
        let fullRepeats = size / patternLength
        let remainder = size % patternLength
        
        var content = String(repeating: basePattern, count: fullRepeats)
        if remainder > 0 {
            content += String(basePattern.prefix(remainder))
        }
        
        return content
    }
    
    /// Verify file content by downloading it directly via API (bypasses FileProvider system integration)
    public static func verifyFileContent(fileIdentifier: NSFileProviderItemIdentifier, expectedContent: String) async throws {
        let fileProviderExtension = try createTestExtension()
        
        // Use direct API download instead of FileProvider fetchContents to avoid system integration issues
        let downloadedURL = try await fileProviderExtension.apiClient.downloadFile(identifier: fileIdentifier.rawValue) { progress in
            // Progress callback - we don't need to do anything with it for this test
        }
        
        // Read the downloaded content
        guard let downloadedData = try? Data(contentsOf: downloadedURL),
              let downloadedContent = String(data: downloadedData, encoding: .utf8) else {
            throw TestError.assertionFailed("Failed to read downloaded file content")
        }
        
        // Compare content
        guard downloadedContent == expectedContent else {
            throw TestError.assertionFailed(
                "Content mismatch. Expected: '\(expectedContent)', Got: '\(downloadedContent)'"
            )
        }
        
        print("✅ Content verification passed for file: \(fileIdentifier.rawValue)")
        
        // Clean up downloaded file
        try? FileManager.default.removeItem(at: downloadedURL)
    }
    
    /// Test file creation with content verification (create → download → verify)
    /// This combines creation and verification in a single test
    public static func testFileCreationWithVerification(fileName: String, content: String, parentIdentifier: NSFileProviderItemIdentifier = .rootContainer) async throws {
        let context = try await TestContext.capture()
        
        _ = try await verifyFileCreation(
            context: context,
            fileName: fileName,
            content: content,
            parentIdentifier: parentIdentifier,
            shouldVerifyContent: true,
            operationDescription: "file creation with content verification '\(fileName)' (\(content.count) chars)"
        )
    }
    
    // MARK: - Modification Helper Functions
    
    /// Modify an item's metadata (rename, move, or both)
    public static func modifyItem(identifier: NSFileProviderItemIdentifier, 
                                 newFilename: String? = nil,
                                 newParent: NSFileProviderItemIdentifier? = nil) async throws {
        let fileProviderExtension = try createTestExtension()
        
        let _ = try await fileProviderExtension.apiClient.modifyItem(
            identifier: identifier.rawValue,
            filename: newFilename,
            parentItemIdentifier: newParent?.rawValue
        )
    }
    
    /// Modify an item's content (with optional metadata changes)
    public static func modifyItemWithContent(identifier: NSFileProviderItemIdentifier,
                                           contentUrl: URL,
                                           newFilename: String? = nil) async throws {
        let fileProviderExtension = try createTestExtension()
        
        let _ = try await fileProviderExtension.apiClient.modifyItemWithContent(
            identifier: identifier.rawValue,
            filename: newFilename,
            parentItemIdentifier: nil,
            contentUrl: contentUrl
        )
    }
    
    /// Download file content as a string (for text files)
    public static func downloadFileContentString(_ identifier: NSFileProviderItemIdentifier) async throws -> String {
        let fileProviderExtension = try createTestExtension()
        
        let downloadUrl = try await fileProviderExtension.apiClient.downloadFile(identifier: identifier.rawValue) { _ in }
        
        guard let data = try? Data(contentsOf: downloadUrl),
              let content = String(data: data, encoding: .utf8) else {
            throw TestError.assertionFailed("Failed to read downloaded file content as string")
        }
        
        try? FileManager.default.removeItem(at: downloadUrl)
        return content
    }
    
    /// Create a temporary file with the given content for testing
    public static func createTempFile(content: String) throws -> URL {
        let tempDir = FileManager.default.temporaryDirectory
        let tempFileURL = tempDir.appendingPathComponent("TempTestFile_\(UUID().uuidString).txt")
        
        guard let data = content.data(using: .utf8) else {
            throw TestError.setupFailed("Failed to convert content to data")
        }
        
        try data.write(to: tempFileURL)
        return tempFileURL
    }
    
    // MARK: - Change Enumeration Validation
    
    /// Validate that specific items appear in change enumeration after an operation
    public static func validateChangeEnumeration(
        initialHeight: UInt64,
        expectedChangedItems: [String],
        operationDescription: String
    ) async throws {
        let fileProviderExtension = try createTestExtension()
        let changesResponse = try await fileProviderExtension.apiClient.getChanges(
            parentPath: nil,
            sinceHeight: initialHeight
        )
        
        let changedItems = changesResponse.items.map { $0.identifier }
        
        print("🔍 Change enumeration validation for \(operationDescription):")
        print("   Expected \(expectedChangedItems.count) changed items: \(expectedChangedItems)")
        print("   Found \(changedItems.count) changed items: \(changedItems)")
        
        // Verify count matches expectation
        guard changedItems.count == expectedChangedItems.count else {
            throw TestError.assertionFailed(
                "\(operationDescription): Expected \(expectedChangedItems.count) items in change log, found \(changedItems.count). Expected: \(expectedChangedItems), Found: \(changedItems)"
            )
        }
        
        // Verify all expected items are present
        for expectedItem in expectedChangedItems {
            guard changedItems.contains(expectedItem) else {
                throw TestError.assertionFailed(
                    "\(operationDescription): Expected item '\(expectedItem)' not found in change log. Found items: \(changedItems)"
                )
            }
        }
        
        print("✅ Change enumeration validation passed: all \(expectedChangedItems.count) expected items found")
    }
    
    // MARK: - Timestamp Validation
    
    /// Validate that timestamps behave correctly for different types of modifications
    public static func validateTimestamps(
        originalItem: NSFileProviderItem,
        modifiedItem: NSFileProviderItem,
        operationType: TimestampOperationType,
        operationDescription: String
    ) throws {
        print("🕐 Timestamp validation for \(operationDescription):")
        
        // Copy dates out - NSFileProviderItem properties are apparently double-optional (Date??)
        let originalCreation: Date? = originalItem.creationDate ?? nil
        let originalModification: Date? = originalItem.contentModificationDate ?? nil
        let newCreation: Date? = modifiedItem.creationDate ?? nil
        let newModification: Date? = modifiedItem.contentModificationDate ?? nil
        
        // Validate creation dates - should NEVER change for any operation
        if let origCreate = originalCreation, let newCreate = newCreation {
            print("   Original creation: \(origCreate)")
            print("   New creation: \(newCreate)")
            
            let timeDiff = abs(origCreate.timeIntervalSince(newCreate))
            guard timeDiff < 1.0 else {
                throw TestError.assertionFailed("Creation date should never change: \(origCreate) -> \(newCreate)")
            }
        } else {
            print("   Creation dates: \(String(describing: originalCreation)) -> \(String(describing: newCreation))")
            if (originalCreation == nil) != (newCreation == nil) {
                throw TestError.assertionFailed("Creation date nullability changed unexpectedly")
            }
        }
        
        print("   Original modification: \(String(describing: originalModification))")
        print("   New modification: \(String(describing: newModification))")
        
        // Validate modification dates based on operation type
        switch operationType {
        case .fileMetadataOnly:
            // Files: contentModificationDate should NOT change for metadata-only operations (rename/move)
            // Note: Files might not have modification date if never modified
            if let origMod = originalModification, let newMod = newModification {
                let timeDiff = abs(origMod.timeIntervalSince(newMod))
                guard timeDiff < 1.0 else {
                    throw TestError.assertionFailed("File contentModificationDate should not change for metadata-only operations: \(origMod) -> \(newMod)")
                }
            } else if (originalModification == nil) != (newModification == nil) {
                throw TestError.assertionFailed("File contentModificationDate nullability should not change for metadata-only operations")
            }
            // If both are nil, that's acceptable (file never had content modifications)
            
        case .fileContentChange:
            // Files: contentModificationDate SHOULD change for content modifications
            // Note: Original file might not have a modification date if never modified before
            guard let newMod = newModification else {
                throw TestError.assertionFailed("File should have contentModificationDate after content update")
            }
            
            if let origMod = originalModification {
                // If there was an original modification date, new one should be later
                guard newMod.timeIntervalSince(origMod) > 0 else {
                    throw TestError.assertionFailed("File contentModificationDate should increase after content change: \(origMod) -> \(newMod)")
                }
                print("   ✅ Content modification date increased as expected")
            } else {
                // If no original modification date, just having one now is success
                print("   ✅ Content modification date set for first time (was nil, now \(newMod))")
            }
        }
        
        print("   ✅ Timestamp validation passed for \(operationDescription)")
    }
    
    public enum TimestampOperationType {
        case fileMetadataOnly      // File rename/move - modification date should NOT change
        case fileContentChange     // File content update - modification date SHOULD change  
    }
    
    // MARK: - Deletion Helper Functions
    
    /// Test item deletion with comprehensive verification
    public static func testItemDeletion(
        identifier: NSFileProviderItemIdentifier,
        itemName: String,
        parentIdentifier: NSFileProviderItemIdentifier,
        expectedRecursive: Bool,
        operationDescription: String
    ) async throws {
        print("📋 Testing deletion: \(operationDescription)")
        
        let fileProviderExtension = try createTestExtension()
        let config = try loadTestConfig()
        
        // Capture initial state
        let initialParentItems = try await enumerateItems(
            fileProvider: fileProviderExtension,
            containerIdentifier: parentIdentifier
        )
        let initialSignalCount = try await getSignalCount(config: config)
        let initialChanges = try await fileProviderExtension.apiClient.getChanges()
        let initialHeight = initialChanges.current_consensus_height
        
        // Perform deletion
        try await fileProviderExtension.apiClient.deleteItem(
            identifier: identifier.rawValue,
            recursive: expectedRecursive
        )
        
        // Wait for signals
        _ = try await waitForSignalCount(config: config, expectedCount: initialSignalCount + 1)
        
        // Verify item removed from enumeration
        let finalParentItems = try await enumerateItems(
            fileProvider: fileProviderExtension,
            containerIdentifier: parentIdentifier
        )
        
        guard !finalParentItems.contains(where: { $0.itemIdentifier == identifier }) else {
            throw TestError.assertionFailed("Deleted item '\(itemName)' should not appear in enumeration")
        }
        
        // Verify in changes API
        let changesSince = try await fileProviderExtension.apiClient.getChanges(parentPath: nil, sinceHeight: initialHeight)
        
        guard changesSince.deleted_identifiers.contains(identifier.rawValue) else {
            throw TestError.assertionFailed("Deleted item '\(itemName)' should appear in deleted_identifiers")
        }
        
        print("✅ Deletion verified: '\(itemName)' successfully deleted")
    }
    
    /// Test recursive folder deletion with comprehensive verification
    public static func testRecursiveFolderDeletion(
        folderIdentifier: NSFileProviderItemIdentifier,
        folderName: String,
        expectedChildCount: Int,
        operationDescription: String
    ) async throws {
        print("📋 Testing recursive deletion: \(operationDescription)")
        
        let fileProviderExtension = try createTestExtension()
        let config = try loadTestConfig()
        
        // Get all child identifiers before deletion
        let childItems = try await enumerateItems(
            fileProvider: fileProviderExtension,
            containerIdentifier: folderIdentifier
        )
        let childIdentifiers = childItems.map { $0.itemIdentifier.rawValue }
        
        // Capture initial state
        let initialSignalCount = try await getSignalCount(config: config)
        let initialHeight = try await fileProviderExtension.apiClient.getChanges().current_consensus_height
        
        // Perform recursive deletion
        try await fileProviderExtension.apiClient.deleteItem(
            identifier: folderIdentifier.rawValue,
            recursive: true
        )
        
        // Wait for signals
        _ = try await waitForSignalCount(config: config, expectedCount: initialSignalCount + 1)
        
        // Verify in changes API - folder and all children should be deleted
        let changesSince = try await fileProviderExtension.apiClient.getChanges(parentPath: nil, sinceHeight: initialHeight)
        
        guard changesSince.deleted_identifiers.contains(folderIdentifier.rawValue) else {
            throw TestError.assertionFailed("Folder '\(folderName)' should appear in deleted_identifiers")
        }
        
        for childId in childIdentifiers {
            guard changesSince.deleted_identifiers.contains(childId) else {
                throw TestError.assertionFailed("Child '\(childId)' should appear in deleted_identifiers")
            }
        }
        
        print("✅ Recursive deletion verified: '\(folderName)' + \(expectedChildCount) children deleted")
    }
    
    // MARK: - Creation Framework
    
    /// Template for creating folder hierarchies
    public struct HierarchyTemplate {
        public let levels: [String]
        public let uniqueId: String
        
        public init(levels: [String], uniqueId: String = UUID().uuidString.prefix(4).description) {
            self.levels = levels
            self.uniqueId = uniqueId
        }
    }
    
    /// Verify hierarchical folder creation with comprehensive testing
    public static func verifyHierarchyCreation(
        context: TestContext,
        hierarchies: [HierarchyTemplate],
        operationDescription: String
    ) async throws {
        print("🏗️ Creating and verifying hierarchies: \(operationDescription)")
        
        var totalFoldersCreated = 0
        
        for template in hierarchies {
            print("🧪 Creating hierarchy: \(template.levels.joined(separator: "/"))")
            
            var currentParentId: NSFileProviderItemIdentifier = .rootContainer
            var currentPath = ""
            
            for (index, levelName) in template.levels.enumerated() {
                let uniqueName = "\(levelName)_\(template.uniqueId)"
                let isRoot = (index == 0)
                
                if isRoot {
                    currentPath = uniqueName
                } else {
                    currentPath += "/\(uniqueName)"
                }
                
                print("   📁 Level \(index + 1): Creating '\(uniqueName)' in \(currentParentId == .rootContainer ? "root" : "parent")")
                
                // Create folder at this level
                if isRoot {
                    try await testFolderCreation(folderName: uniqueName)
                } else {
                    try await testNestedFolderCreation(
                        folderName: uniqueName,
                        parentIdentifier: currentParentId,
                        parentName: currentPath.components(separatedBy: "/").dropLast().joined(separator: "/")
                    )
                }
                
                // Update parent for next level
                currentParentId = try await getFolderIdentifier(
                    folderName: uniqueName,
                    parentIdentifier: currentParentId
                )
                
                totalFoldersCreated += 1
            }
        }
        
        // Comprehensive verification
        try await verifyOperation(
            context: context,
            expectedSignalIncrease: totalFoldersCreated,
            expectedRootItemCountChange: hierarchies.count, // One root folder per hierarchy
            operationDescription: "\(operationDescription) (\(totalFoldersCreated) folders in \(hierarchies.count) hierarchies)"
        )
        
        print("✅ Hierarchy creation verification passed: \(totalFoldersCreated) folders in \(hierarchies.count) hierarchies")
    }
    
    /// Consolidated file creation with comprehensive verification
    public static func verifyFileCreation(
        context: TestContext,
        fileName: String,
        content: String,
        parentIdentifier: NSFileProviderItemIdentifier = .rootContainer,
        shouldVerifyContent: Bool = false,
        operationDescription: String
    ) async throws -> NSFileProviderItem {
        print("📄 Creating and verifying file: \(operationDescription)")
        
        // Capture parent container initial state for precise verification
        let initialContainerItems = try await enumerateItems(
            fileProvider: context.fileProvider,
            containerIdentifier: parentIdentifier
        )
        
        // Capture parent version before creation (for nested files)
        let parentVersionBefore: UInt64?
        if parentIdentifier != .rootContainer {
            let parentItem = try await getItemByIdentifier(parentIdentifier)
            parentVersionBefore = try getConsensusHeight(from: parentItem)
        } else {
            parentVersionBefore = nil
        }
        
        // Create temporary file with content
        let tempFileURL = try createTemporaryFile(content: content, fileName: fileName)
        defer {
            try? FileManager.default.removeItem(at: tempFileURL)
        }
        
        // Create the file through FileProvider
        let createdFile = try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<NSFileProviderItem, Error>) in
            let itemTemplate = TestFileProviderItem(
                itemIdentifier: NSFileProviderItemIdentifier("temp-file-\(UUID())"),
                filename: fileName,
                parentItemIdentifier: parentIdentifier,
                contentType: .data
            )
            
            let progress = context.fileProvider.createItem(
                basedOn: itemTemplate,
                fields: [],
                contents: tempFileURL,
                options: [],
                request: NSFileProviderRequest()
            ) { item, fields, shouldFetchAgain, error in
                if let error = error {
                    continuation.resume(throwing: error)
                } else if let item = item {
                    continuation.resume(returning: item)
                } else {
                    continuation.resume(throwing: TestError.assertionFailed("No item returned from createItem"))
                }
            }
            
            _ = progress
        }
        
        // Comprehensive verification using existing framework
        try await verifyOperation(
            context: context,
            expectedSignalIncrease: 1,
            expectedRootItemCountChange: parentIdentifier == .rootContainer ? 1 : 0,
            operationDescription: operationDescription
        )
        
        // Verify parent container item count increased by exactly 1
        let finalContainerItems = try await enumerateItems(
            fileProvider: context.fileProvider,
            containerIdentifier: parentIdentifier
        )
        
        guard finalContainerItems.count == initialContainerItems.count + 1 else {
            throw TestError.assertionFailed(
                "Container item count should be \(initialContainerItems.count + 1) but got \(finalContainerItems.count)"
            )
        }
        
        // Verify file appears in container enumeration by identifier
        guard finalContainerItems.contains(where: { $0.itemIdentifier == createdFile.itemIdentifier }) else {
            throw TestError.assertionFailed("Created file should appear in container enumeration")
        }
        
        // Verify file appears in container enumeration by filename
        guard finalContainerItems.contains(where: { $0.filename == fileName }) else {
            throw TestError.assertionFailed("File '\(fileName)' not found in enumeration")
        }
        
        // Verify parent-child relationship
        guard createdFile.parentItemIdentifier == parentIdentifier else {
            throw TestError.assertionFailed(
                "File '\(fileName)' has wrong parent. Expected: \(parentIdentifier.rawValue), Got: \(createdFile.parentItemIdentifier.rawValue)"
            )
        }
        
        // Verify content type is file (not folder)
        let isFile = !(createdFile.contentType?.conforms(to: .folder) ?? false)
        guard isFile else {
            throw TestError.assertionFailed("Created item '\(fileName)' is not a file")
        }
        
        // Verify version is consensus height
        try assertVersionIsConsensusHeight(for: createdFile)
        
        // Verify parent version increased (for nested files)
        if let beforeVersion = parentVersionBefore {
            let parentItemAfter = try await getItemByIdentifier(parentIdentifier)
            let afterVersion = try getConsensusHeight(from: parentItemAfter)
            
            guard afterVersion > beforeVersion else {
                throw TestError.assertionFailed(
                    "Parent version should increase after file creation. Before: \(beforeVersion), After: \(afterVersion)"
                )
            }
            print("✅ Parent version tracking verified: \(beforeVersion) → \(afterVersion)")
        }
        
        // For nested operations, verify root container unchanged (critical invariant)
        if parentIdentifier != .rootContainer {
            let finalRootItems = try await enumerateItems(
                fileProvider: context.fileProvider,
                containerIdentifier: .rootContainer
            )
            
            guard finalRootItems.count == context.initialRootItemCount else {
                throw TestError.assertionFailed(
                    "Root container should remain unchanged for nested operation. Expected: \(context.initialRootItemCount), Got: \(finalRootItems.count)"
                )
            }
            print("✅ Root container unchanged verification passed (\(context.initialRootItemCount) items)")
        }
        
        // Verify appears in changes API
        let changesSince = try await context.fileProvider.apiClient.getChanges(
            parentPath: nil,
            sinceHeight: context.initialConsensusHeight
        )
        
        guard changesSince.items.contains(where: { $0.identifier == createdFile.itemIdentifier.rawValue }) else {
            throw TestError.assertionFailed("Created file should appear in changes API")
        }
        
        // Optional content verification
        if shouldVerifyContent {
            try await verifyFileContent(fileIdentifier: createdFile.itemIdentifier, expectedContent: content)
        }
        
        print("✅ File creation verification passed: '\(fileName)' in container \(parentIdentifier == .rootContainer ? "root" : parentIdentifier.rawValue)")
        return createdFile
    }
    
    // MARK: - Standardized Verification Methods
    
    /// Verify operation results with standardized checks
    public static func verifyOperation(
        context: TestContext,
        expectedSignalIncrease: Int = 1,
        expectedRootItemCountChange: Int = 0,
        operationDescription: String
    ) async throws {
        print("🔍 Verifying \(operationDescription)...")
        
        // Wait for and verify signals
        let finalSignalCount = try await waitForSignalCount(
            config: context.config,
            expectedCount: context.initialSignalCount + expectedSignalIncrease
        )
        
        // Verify root container item count if specified
        if expectedRootItemCountChange != 0 {
            let finalRootItems = try await enumerateItems(
                fileProvider: context.fileProvider,
                containerIdentifier: .rootContainer
            )
            let expectedFinalCount = context.initialRootItemCount + expectedRootItemCountChange
            guard finalRootItems.count == expectedFinalCount else {
                throw TestError.assertionFailed(
                    "\(operationDescription): Expected root item count \(expectedFinalCount), got \(finalRootItems.count)"
                )
            }
        }
        
        print("✅ \(operationDescription) verification passed: signals \(context.initialSignalCount)→\(finalSignalCount)")
    }
    
    /// Verify deletion operation with comprehensive checks
    public static func verifyDeletion(
        context: TestContext,
        deletedItems: [String],
        parentContainer: NSFileProviderItemIdentifier = .rootContainer,
        expectedContainerItemCountChange: Int? = nil,
        initialContainerItemCount: Int? = nil,  // Optional initial count for non-root containers
        operationDescription: String
    ) async throws {
        print("🔍 Verifying deletion: \(operationDescription)...")
        
        // Wait for signals
        let finalSignalCount = try await waitForSignalCount(
            config: context.config,
            expectedCount: context.initialSignalCount + 1
        )
        
        // Verify items removed from container enumeration
        let finalContainerItems = try await enumerateItems(
            fileProvider: context.fileProvider,
            containerIdentifier: parentContainer
        )
        
        for deletedItemId in deletedItems {
            guard !finalContainerItems.contains(where: { $0.itemIdentifier.rawValue == deletedItemId }) else {
                throw TestError.assertionFailed("Deleted item '\(deletedItemId)' should not appear in enumeration")
            }
        }
        
        // Verify container item count change if specified
        if let expectedChange = expectedContainerItemCountChange {
            if parentContainer == .rootContainer {
                let expectedCount = context.initialRootItemCount + expectedChange
                guard finalContainerItems.count == expectedCount else {
                    throw TestError.assertionFailed(
                        "Container item count should be \(expectedCount), got \(finalContainerItems.count)"
                    )
                }
            } else if let providedInitialCount = initialContainerItemCount {
                // Use the provided initial count for non-root containers
                let expectedCount = providedInitialCount + expectedChange
                guard finalContainerItems.count == expectedCount else {
                    throw TestError.assertionFailed(
                        "Container item count should be \(expectedCount), got \(finalContainerItems.count)"
                    )
                }
            } else {
                // Skip count verification if no initial count provided for non-root container
                print("⚠️  Skipping item count verification for non-root container (no initial count provided)")
            }
        }
        
        // Verify deletion appears in changes API
        let changesSince = try await context.fileProvider.apiClient.getChanges(
            parentPath: nil,
            sinceHeight: context.initialConsensusHeight
        )
        
        for deletedItemId in deletedItems {
            guard changesSince.deleted_identifiers.contains(deletedItemId) else {
                throw TestError.assertionFailed("Deleted item '\(deletedItemId)' should appear in deleted_identifiers")
            }
        }
        
        // Verify consensus height advanced
        guard changesSince.current_consensus_height > context.initialConsensusHeight else {
            throw TestError.assertionFailed("Consensus height should advance after deletion")
        }
        
        print("✅ Deletion verification passed: \(deletedItems.count) items deleted, signals \(context.initialSignalCount)→\(finalSignalCount)")
    }
    
    /// Verify modification operation (rename, move, content update)
    public static func verifyModification(
        context: TestContext,
        modifiedItem: NSFileProviderItem,
        expectedChangedItems: [String],
        operationDescription: String
    ) async throws {
        print("🔍 Verifying modification: \(operationDescription)...")
        
        // Wait for signals
        let finalSignalCount = try await waitForSignalCount(
            config: context.config,
            expectedCount: context.initialSignalCount + 1
        )
        
        // Verify changes appear in changes API
        let changesSince = try await context.fileProvider.apiClient.getChanges(
            parentPath: nil,
            sinceHeight: context.initialConsensusHeight
        )
        
        for expectedItemId in expectedChangedItems {
            guard changesSince.items.contains(where: { $0.identifier == expectedItemId }) else {
                throw TestError.assertionFailed("Modified item '\(expectedItemId)' should appear in changes")
            }
        }
        
        // Verify consensus height advanced
        guard changesSince.current_consensus_height > context.initialConsensusHeight else {
            throw TestError.assertionFailed("Consensus height should advance after modification")
        }
        
        print("✅ Modification verification passed: \(expectedChangedItems.count) items changed, signals \(context.initialSignalCount)→\(finalSignalCount)")
    }
    
    /// Verify file creation with content and enumeration checks
    public static func verifyFileCreation(
        context: TestContext,
        createdFile: NSFileProviderItem,
        expectedContent: String? = nil,
        parentContainer: NSFileProviderItemIdentifier = .rootContainer,
        operationDescription: String
    ) async throws {
        print("🔍 Verifying file creation: \(operationDescription)...")
        
        // Wait for signals
        let finalSignalCount = try await waitForSignalCount(
            config: context.config,
            expectedCount: context.initialSignalCount + 1
        )
        
        // Verify file appears in container enumeration
        let containerItems = try await enumerateItems(
            fileProvider: context.fileProvider,
            containerIdentifier: parentContainer
        )
        
        guard containerItems.contains(where: { $0.itemIdentifier == createdFile.itemIdentifier }) else {
            throw TestError.assertionFailed("Created file should appear in container enumeration")
        }
        
        // Verify file content if specified
        if let expectedContent = expectedContent {
            try await verifyFileContent(fileIdentifier: createdFile.itemIdentifier, expectedContent: expectedContent)
        }
        
        // Verify version is consensus height
        try assertVersionIsConsensusHeight(for: createdFile)
        
        // Verify appears in changes API
        let changesSince = try await context.fileProvider.apiClient.getChanges(
            parentPath: nil,
            sinceHeight: context.initialConsensusHeight
        )
        
        guard changesSince.items.contains(where: { $0.identifier == createdFile.itemIdentifier.rawValue }) else {
            throw TestError.assertionFailed("Created file should appear in changes API")
        }
        
        print("✅ File creation verification passed: '\(createdFile.filename)', signals \(context.initialSignalCount)→\(finalSignalCount)")
    }
    
    // MARK: - Folder Discovery Helpers
    
    /// Find folders that contain files
    public static func findFoldersWithFiles() async throws -> [(folder: NSFileProviderItem, file: NSFileProviderItem)] {
        let fileProviderExtension = try createTestExtension()
        let rootItems = try await enumerateItems(
            fileProvider: fileProviderExtension,
            containerIdentifier: .rootContainer
        )
        
        var results: [(NSFileProviderItem, NSFileProviderItem)] = []
        
        for folder in rootItems.filter({ $0.contentType == .folder }) {
            let contents = try await enumerateItems(
                fileProvider: fileProviderExtension,
                containerIdentifier: folder.itemIdentifier
            )
            
            if let file = contents.first(where: { $0.contentType != .folder }) {
                results.append((folder, file))
            }
        }
        
        return results
    }
    
    /// Find empty folders with retry logic for transient 404 errors
    public static func findEmptyFolders() async throws -> [NSFileProviderItem] {
        let fileProviderExtension = try createTestExtension()
        let rootItems = try await enumerateItems(
            fileProvider: fileProviderExtension,
            containerIdentifier: .rootContainer
        )

        var emptyFolders: [NSFileProviderItem] = []

        for folder in rootItems.filter({ $0.contentType == .folder }) {
            // Retry enumeration with exponential backoff to handle transient 404s
            // from race conditions with folder deletions between tests
            let maxRetries = 3
            var succeeded = false

            for attempt in 1...maxRetries {
                do {
                    let contents = try await enumerateItems(
                        fileProvider: fileProviderExtension,
                        containerIdentifier: folder.itemIdentifier
                    )

                    if contents.isEmpty {
                        emptyFolders.append(folder)
                    }
                    succeeded = true
                    break  // Success - exit retry loop

                } catch let error as NSFileProviderError where error.code == .noSuchItem {
                    // 404 - folder was deleted between root enumeration and this check
                    if attempt < maxRetries {
                        // Wait with exponential backoff before retrying
                        let delayMs = 50 * (1 << (attempt - 1))  // 50ms, 100ms, 200ms
                        try await Task.sleep(nanoseconds: UInt64(delayMs) * 1_000_000)
                        print("⚠️  Folder '\(folder.filename)' returned 404, retry \(attempt)/\(maxRetries)")
                    } else {
                        // Final attempt failed - skip this folder (it was probably deleted)
                        print("⚠️  Folder '\(folder.filename)' consistently returns 404, skipping (likely deleted)")
                    }
                } catch {
                    // Other errors should propagate (real bugs)
                    throw error
                }
            }
        }

        return emptyFolders
    }
    
    /// Find folders with any children
    public static func findFoldersWithChildren() async throws -> [(folder: NSFileProviderItem, childCount: Int)] {
        let fileProviderExtension = try createTestExtension()
        let rootItems = try await enumerateItems(
            fileProvider: fileProviderExtension,
            containerIdentifier: .rootContainer
        )
        
        var results: [(NSFileProviderItem, Int)] = []
        
        for folder in rootItems.filter({ $0.contentType == .folder }) {
            let contents = try await enumerateItems(
                fileProvider: fileProviderExtension,
                containerIdentifier: folder.itemIdentifier
            )
            
            if !contents.isEmpty {
                results.append((folder, contents.count))
            }
        }
        
        return results
    }
    
    /// Find folders with minimum depth (nested structure)
    public static func findFoldersWithDepth(minimumDepth: Int) async throws -> [NSFileProviderItem] {
        let fileProviderExtension = try createTestExtension()
        let rootItems = try await enumerateItems(
            fileProvider: fileProviderExtension,
            containerIdentifier: .rootContainer
        )
        
        var deepFolders: [NSFileProviderItem] = []
        
        for folder in rootItems.filter({ $0.contentType == .folder }) {
            let depth = try await calculateFolderDepth(
                folderIdentifier: folder.itemIdentifier,
                currentDepth: 0
            )
            
            if depth >= minimumDepth {
                deepFolders.append(folder)
            }
        }
        
        return deepFolders
    }
    
    /// Calculate the maximum depth of a folder structure
    private static func calculateFolderDepth(
        folderIdentifier: NSFileProviderItemIdentifier,
        currentDepth: Int
    ) async throws -> Int {
        let fileProviderExtension = try createTestExtension()
        let contents = try await enumerateItems(
            fileProvider: fileProviderExtension,
            containerIdentifier: folderIdentifier
        )
        
        let subfolders = contents.filter { $0.contentType == .folder }
        
        if subfolders.isEmpty {
            return currentDepth
        }
        
        var maxDepth = currentDepth
        for subfolder in subfolders {
            let depth = try await calculateFolderDepth(
                folderIdentifier: subfolder.itemIdentifier,
                currentDepth: currentDepth + 1
            )
            maxDepth = max(maxDepth, depth)
        }
        
        return maxDepth
    }
    
    /// Find folders with mixed content (both files and subfolders)
    public static func findFoldersWithMixedContent() async throws -> [(folder: NSFileProviderItem, fileCount: Int, subfolderCount: Int)] {
        let fileProviderExtension = try createTestExtension()
        let rootItems = try await enumerateItems(
            fileProvider: fileProviderExtension,
            containerIdentifier: .rootContainer
        )
        
        var results: [(NSFileProviderItem, Int, Int)] = []
        
        for folder in rootItems.filter({ $0.contentType == .folder }) {
            let contents = try await enumerateItems(
                fileProvider: fileProviderExtension,
                containerIdentifier: folder.itemIdentifier
            )
            
            let files = contents.filter { $0.contentType != .folder }
            let subfolders = contents.filter { $0.contentType == .folder }
            
            if !files.isEmpty && !subfolders.isEmpty {
                results.append((folder, files.count, subfolders.count))
            }
        }
        
        return results
    }
    
    /// Find complex nested folder structures
    public static func findNestedFolderStructures() async throws -> [NestedFolderStructure] {
        let fileProviderExtension = try createTestExtension()
        let rootItems = try await enumerateItems(
            fileProvider: fileProviderExtension,
            containerIdentifier: .rootContainer
        )
        
        var structures: [NestedFolderStructure] = []
        
        for folder in rootItems.filter({ $0.contentType == .folder }) {
            let descendants = try await getAllDescendantIdentifiers(folderIdentifier: folder.itemIdentifier)
            
            if descendants.count >= 3 { // At least 3 descendants for interesting structure
                structures.append(NestedFolderStructure(
                    parentFolder: folder,
                    totalDescendants: descendants.count,
                    descendantIdentifiers: descendants
                ))
            }
        }
        
        return structures
    }
    
    /// Count all descendants in a folder (recursive)
    public static func countAllDescendants(folderIdentifier: NSFileProviderItemIdentifier) async throws -> Int {
        let descendants = try await getAllDescendantIdentifiers(folderIdentifier: folderIdentifier)
        return descendants.count
    }
    
    /// Get all descendant identifiers (recursive)
    public static func getAllDescendantIdentifiers(folderIdentifier: NSFileProviderItemIdentifier) async throws -> [String] {
        let fileProviderExtension = try createTestExtension()
        let contents = try await enumerateItems(
            fileProvider: fileProviderExtension,
            containerIdentifier: folderIdentifier
        )
        
        var identifiers: [String] = contents.map { $0.itemIdentifier.rawValue }
        
        // Recursively get descendants of subfolders
        for subfolder in contents.filter({ $0.contentType == .folder }) {
            let subDescendants = try await getAllDescendantIdentifiers(
                folderIdentifier: subfolder.itemIdentifier
            )
            identifiers.append(contentsOf: subDescendants)
        }
        
        return identifiers
    }
    
    /// Structure to represent nested folder hierarchies
    public struct NestedFolderStructure {
        public let parentFolder: NSFileProviderItem
        public let totalDescendants: Int
        public let descendantIdentifiers: [String]
    }
}

/// Simple test implementation of NSFileProviderItem for creating items
class TestFileProviderItem: NSObject, NSFileProviderItem {
    let itemIdentifier: NSFileProviderItemIdentifier
    let filename: String
    let parentItemIdentifier: NSFileProviderItemIdentifier
    let contentType: UTType
    
    init(itemIdentifier: NSFileProviderItemIdentifier, filename: String, parentItemIdentifier: NSFileProviderItemIdentifier, contentType: UTType) {
        self.itemIdentifier = itemIdentifier
        self.filename = filename
        self.parentItemIdentifier = parentItemIdentifier
        self.contentType = contentType
        super.init()
    }
}

// MARK: - Test Errors

public enum TestError: Error, LocalizedError {
    case unknownTestCase(String)
    case setupFailed(String)
    case backendNotReady
    case assertionFailed(String)
    case timeout(String)
    case unexpectedResult(String)
    case configurationNotFound(String)
    
    public var errorDescription: String? {
        switch self {
        case .unknownTestCase(let testCase):
            return "Unknown test case: \(testCase)"
        case .setupFailed(let message):
            return "Test setup failed: \(message)"
        case .backendNotReady:
            return "Test backend is not ready"
        case .assertionFailed(let message):
            return "Assertion failed: \(message)"
        case .timeout(let message):
            return "Operation timed out: \(message)"
        case .unexpectedResult(let message):
            return "Unexpected result: \(message)"
        case .configurationNotFound(let message):
            return message
        }
    }
}