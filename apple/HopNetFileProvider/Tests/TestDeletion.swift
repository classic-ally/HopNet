import Foundation
import FileProvider
import HopNetFileProviderCore
import TestHelpers
import UniformTypeIdentifiers

@main
struct TestDeletion {
    static func main() async throws {
        let args = ProcessInfo.processInfo.arguments
        let testCase = args.count > 1 ? args[1] : "all"
        
        print("🧪 TestDeletion starting: \(testCase)")
        
        switch testCase {
        // Basic deletion tests (each with full verification)
        case "single_file":
            try await testSingleFileDeletion()
        case "single_file_in_folder":
            try await testSingleFileInFolderDeletion()
        case "empty_folder":
            try await testEmptyFolderDeletion()
        case "multiple_file_types":
            try await testMultipleFileTypesDeletion()
            
        // Recursive deletion tests (each with full verification)
        case "recursive_folder":
            try await testRecursiveFolderDeletion()
        case "deep_hierarchy":
            try await testDeepHierarchyDeletion()
        case "mixed_content_folder":
            try await testMixedContentFolderDeletion()
        case "nested_recursive":
            try await testNestedRecursiveDeletion()
            
        // Error handling tests
        case "non_existent_item":
            try await testNonExistentItemDeletion()
        case "root_container_protection":
            try await testRootContainerDeletionPrevention()
        case "non_recursive_folder":
            try await testNonRecursiveFolderDeletion()
            
        case "all":
            try await testSingleFileDeletion()
            try await testSingleFileInFolderDeletion()
            try await testEmptyFolderDeletion()
            try await testMultipleFileTypesDeletion()
            try await testRecursiveFolderDeletion()
            try await testDeepHierarchyDeletion()
            try await testMixedContentFolderDeletion()
            try await testNestedRecursiveDeletion()
            try await testNonExistentItemDeletion()
            try await testRootContainerDeletionPrevention()
            try await testNonRecursiveFolderDeletion()
            
        default:
            throw TestError.unknownTestCase(testCase)
        }
        
        print("✅ TestDeletion passed: \(testCase)")
    }
    
    // MARK: - Basic Deletion Tests
    
    static func testSingleFileDeletion() async throws {
        print("🗑️ Testing single file deletion (root level)...")
        
        // Capture initial test state
        let context = try await TestHelpers.TestContext.capture()
        
        // Find an existing file in the root container using existing enumeration
        let items = try await TestHelpers.enumerateItems(
            fileProvider: context.fileProvider,
            containerIdentifier: .rootContainer
        )
        
        guard let fileItem = items.first(where: { $0.contentType != UTType.folder }) else {
            throw TestError.setupFailed("No file found for single file deletion test")
        }
        
        print("🎯 Deleting file: '\(fileItem.filename)' (ID: \(fileItem.itemIdentifier.rawValue))")
        
        // Perform deletion
        try await context.fileProvider.apiClient.deleteItem(
            identifier: fileItem.itemIdentifier.rawValue,
            recursive: false
        )
        
        // Standardized verification
        try await TestHelpers.verifyDeletion(
            context: context,
            deletedItems: [fileItem.itemIdentifier.rawValue],
            expectedContainerItemCountChange: -1,
            operationDescription: "single file deletion (root level)"
        )
    }
    
    static func testSingleFileInFolderDeletion() async throws {
        print("🗑️ Testing single file deletion within folder...")
        
        // Capture initial test state
        let context = try await TestHelpers.TestContext.capture()
        
        // Find folders with files using existing finder
        let foldersWithFiles = try await TestHelpers.findFoldersWithFiles()
        guard let (folderItem, fileItem) = foldersWithFiles.first else {
            throw TestError.setupFailed("No folder with files found for nested file deletion test")
        }
        
        print("🎯 Deleting file '\(fileItem.filename)' from folder '\(folderItem.filename)'")
        
        // Capture initial folder state for count verification
        let initialFolderContents = try await TestHelpers.enumerateItems(
            fileProvider: context.fileProvider,
            containerIdentifier: folderItem.itemIdentifier
        )
        
        // Capture parent folder version before deletion (unique requirement)
        let parentVersionBefore = try TestHelpers.getConsensusHeight(from: folderItem)
        
        // Perform deletion
        try await context.fileProvider.apiClient.deleteItem(
            identifier: fileItem.itemIdentifier.rawValue,
            recursive: false
        )
        
        // Use framework for standard verifications (including consensus height check)
        try await TestHelpers.verifyDeletion(
            context: context,
            deletedItems: [fileItem.itemIdentifier.rawValue],
            parentContainer: folderItem.itemIdentifier,
            expectedContainerItemCountChange: -1,
            initialContainerItemCount: initialFolderContents.count,  // Fixed with framework update
            operationDescription: "nested file deletion"
        )
        
        // Additional unique verifications for parent folder tracking
        let changesSince = try await context.fileProvider.apiClient.getChanges(
            parentPath: nil,
            sinceHeight: context.initialConsensusHeight
        )
        
        // CRITICAL: Parent folder should appear in changes (ancestor tracking)
        guard changesSince.items.contains(where: { $0.identifier == folderItem.itemIdentifier.rawValue }) else {
            throw TestError.assertionFailed("Parent folder should appear in changes when child is deleted")
        }
        
        // Verify parent folder version increased
        let updatedParent = try await TestHelpers.getItemByIdentifier(folderItem.itemIdentifier)
        let parentVersionAfter = try TestHelpers.getConsensusHeight(from: updatedParent)
        
        guard parentVersionAfter > parentVersionBefore else {
            throw TestError.assertionFailed("Parent folder version should increase when child deleted: \(parentVersionBefore)→\(parentVersionAfter)")
        }
        
        print("✅ Nested file deletion test passed: parent version \(parentVersionBefore)→\(parentVersionAfter)")
    }
    
    static func testEmptyFolderDeletion() async throws {
        print("🗑️ Testing empty folder deletion...")
        
        // Capture initial test state  
        let context = try await TestHelpers.TestContext.capture()
        
        // Find an empty folder using existing finder
        let emptyFolders = try await TestHelpers.findEmptyFolders()
        guard let emptyFolder = emptyFolders.first else {
            throw TestError.setupFailed("No empty folder found for empty folder deletion test")
        }
        
        print("🎯 Deleting empty folder: '\(emptyFolder.filename)'")
        
        // Perform deletion (non-recursive should work for empty folder)
        try await context.fileProvider.apiClient.deleteItem(
            identifier: emptyFolder.itemIdentifier.rawValue,
            recursive: false
        )
        
        // Standardized verification
        try await TestHelpers.verifyDeletion(
            context: context,
            deletedItems: [emptyFolder.itemIdentifier.rawValue],
            expectedContainerItemCountChange: -1,
            operationDescription: "empty folder deletion"
        )
    }
    
    static func testMultipleFileTypesDeletion() async throws {
        print("🗑️ Testing deletion of various file types...")
        
        let fileProviderExtension = try TestHelpers.createTestExtension()
        let config = try TestHelpers.loadTestConfig()
        
        let rootItems = try await TestHelpers.enumerateItems(
            fileProvider: fileProviderExtension,
            containerIdentifier: .rootContainer
        )
        
        let files = rootItems.filter { $0.contentType != .folder }
        
        // Look for different file types to delete
        let testTargets = [
            ("text file", { (item: NSFileProviderItem) in item.filename.hasSuffix(".txt") }),
            ("JSON file", { (item: NSFileProviderItem) in item.filename.hasSuffix(".json") }),
            ("markdown file", { (item: NSFileProviderItem) in item.filename.hasSuffix(".md") }),
            ("Unicode file", { (item: NSFileProviderItem) in item.filename.contains("_文档") || item.filename.contains("测试") })
        ]
        
        for (fileType, predicate) in testTargets {
            if let targetFile = files.first(where: predicate) {
                print("🎯 Deleting \(fileType): '\(targetFile.filename)'")
                
                // Capture state before deletion
                let beforeCount = try await TestHelpers.enumerateItems(
                    fileProvider: fileProviderExtension,
                    containerIdentifier: .rootContainer
                ).count
                let beforeSignals = try await TestHelpers.getSignalCount(config: config)
                let beforeHeight = try await fileProviderExtension.apiClient.getChanges().current_consensus_height
                
                // Delete the file
                try await fileProviderExtension.apiClient.deleteItem(
                    identifier: targetFile.itemIdentifier.rawValue,
                    recursive: false
                )
                
                // Wait for and verify signals
                let afterSignals = try await TestHelpers.waitForSignalCount(
                    config: config,
                    expectedCount: beforeSignals + 1
                )
                
                // Verify deletion
                let afterCount = try await TestHelpers.enumerateItems(
                    fileProvider: fileProviderExtension,
                    containerIdentifier: .rootContainer
                ).count
                
                guard afterCount == beforeCount - 1 else {
                    throw TestError.assertionFailed("\(fileType) deletion failed: count should decrease")
                }
                
                // Verify in changes API
                let changes = try await fileProviderExtension.apiClient.getChanges(parentPath: nil, sinceHeight: beforeHeight)
                guard changes.deleted_identifiers.contains(targetFile.itemIdentifier.rawValue) else {
                    throw TestError.assertionFailed("\(fileType) should appear in deleted_identifiers")
                }
                
                print("✅ \(fileType.capitalized) deletion successful")
            } else {
                print("⚠️  No \(fileType) found - skipping")
            }
        }
        
        print("✅ Multiple file types deletion test passed")
    }
    
    // MARK: - Recursive Deletion Tests
    
    static func testRecursiveFolderDeletion() async throws {
        print("🗑️ Testing recursive folder deletion...")
        
        // Capture initial test state
        let context = try await TestHelpers.TestContext.capture()
        
        // Find a folder with children using existing finder
        let foldersWithChildren = try await TestHelpers.findFoldersWithChildren()
        guard let (folderItem, childCount) = foldersWithChildren.first else {
            throw TestError.setupFailed("No folder with children found for recursive deletion test")
        }
        
        print("🎯 Recursively deleting folder '\(folderItem.filename)' with \(childCount) children")
        
        // Get all child identifiers before deletion for verification
        let childItems = try await TestHelpers.enumerateItems(
            fileProvider: context.fileProvider,
            containerIdentifier: folderItem.itemIdentifier
        )
        let childIdentifiers = childItems.map { $0.itemIdentifier.rawValue }
        
        // Perform recursive deletion
        try await context.fileProvider.apiClient.deleteItem(
            identifier: folderItem.itemIdentifier.rawValue,
            recursive: true
        )
        
        // Standardized verification - parent + all children should be deleted
        let allDeletedItems = [folderItem.itemIdentifier.rawValue] + childIdentifiers
        try await TestHelpers.verifyDeletion(
            context: context,
            deletedItems: allDeletedItems,
            expectedContainerItemCountChange: -1,
            operationDescription: "recursive folder deletion (\(childCount) children)"
        )
    }
    
    static func testDeepHierarchyDeletion() async throws {
        print("🗑️ Testing deep hierarchy deletion...")
        
        // Capture initial test state
        let context = try await TestHelpers.TestContext.capture()
        
        // Find a folder with nested structure using existing finder
        let deepFolders = try await TestHelpers.findFoldersWithDepth(minimumDepth: 2)
        guard let deepFolder = deepFolders.first else {
            throw TestError.setupFailed("No deep folder structure found for deep hierarchy deletion test")
        }
        
        print("🎯 Recursively deleting deep folder structure: '\(deepFolder.filename)'")
        
        // Get all descendant identifiers using existing helper
        let descendantIdentifiers = try await TestHelpers.getAllDescendantIdentifiers(
            folderIdentifier: deepFolder.itemIdentifier
        )
        let totalDescendants = descendantIdentifiers.count
        
        // Perform recursive deletion
        try await context.fileProvider.apiClient.deleteItem(
            identifier: deepFolder.itemIdentifier.rawValue,
            recursive: true
        )
        
        // Standardized verification - parent + all descendants should be deleted
        let allDeletedItems = [deepFolder.itemIdentifier.rawValue] + descendantIdentifiers
        try await TestHelpers.verifyDeletion(
            context: context,
            deletedItems: allDeletedItems,
            expectedContainerItemCountChange: -1,
            operationDescription: "deep hierarchy deletion (\(totalDescendants) descendants)"
        )
    }
    
    static func testMixedContentFolderDeletion() async throws {
        print("🗑️ Testing mixed content folder deletion...")
        
        // Capture initial test state
        let context = try await TestHelpers.TestContext.capture()
        
        // Find a folder that contains both files and subfolders using existing finder
        let mixedFolders = try await TestHelpers.findFoldersWithMixedContent()
        guard let (folderItem, fileCount, subfolderCount) = mixedFolders.first else {
            throw TestError.setupFailed("No folder with mixed content found for mixed content deletion test")
        }
        
        print("🎯 Recursively deleting mixed folder '\(folderItem.filename)' (\(fileCount) files, \(subfolderCount) subfolders)")
        
        // Get ALL descendants for complete recursive deletion verification
        let descendantIdentifiers = try await TestHelpers.getAllDescendantIdentifiers(
            folderIdentifier: folderItem.itemIdentifier
        )
        let totalDescendants = descendantIdentifiers.count
        
        // Perform recursive deletion
        try await context.fileProvider.apiClient.deleteItem(
            identifier: folderItem.itemIdentifier.rawValue,
            recursive: true
        )
        
        // Standardized verification - parent + ALL descendants should be deleted
        let allDeletedItems = [folderItem.itemIdentifier.rawValue] + descendantIdentifiers
        try await TestHelpers.verifyDeletion(
            context: context,
            deletedItems: allDeletedItems,
            expectedContainerItemCountChange: -1,
            operationDescription: "mixed content folder deletion (\(fileCount) files, \(subfolderCount) subfolders, \(totalDescendants) total descendants)"
        )
    }
    
    static func testNestedRecursiveDeletion() async throws {
        print("🗑️ Testing nested recursive deletion (parent with multiple child folders)...")
        
        // Capture initial test state
        let context = try await TestHelpers.TestContext.capture()
        
        // Find a parent folder that has multiple child folders with their own content using existing finder
        let nestedStructures = try await TestHelpers.findNestedFolderStructures()
        guard let nestedStructure = nestedStructures.first else {
            throw TestError.setupFailed("No nested folder structure found for nested recursive deletion test")
        }
        
        print("🎯 Recursively deleting nested structure: '\(nestedStructure.parentFolder.filename)' with \(nestedStructure.totalDescendants) descendants")
        
        // Perform recursive deletion
        try await context.fileProvider.apiClient.deleteItem(
            identifier: nestedStructure.parentFolder.itemIdentifier.rawValue,
            recursive: true
        )
        
        // Standardized verification - parent + all descendants should be deleted
        let allDeletedItems = [nestedStructure.parentFolder.itemIdentifier.rawValue] + nestedStructure.descendantIdentifiers
        try await TestHelpers.verifyDeletion(
            context: context,
            deletedItems: allDeletedItems,
            expectedContainerItemCountChange: -1,
            operationDescription: "nested recursive deletion (\(nestedStructure.totalDescendants) descendants)"
        )
    }
    
    // MARK: - Error Handling Tests
    
    static func testNonExistentItemDeletion() async throws {
        print("🗑️ Testing deletion of non-existent item...")
        
        let fileProviderExtension = try TestHelpers.createTestExtension()
        let nonExistentId = "item:00000000-0000-0000-0000-000000000000"
        
        print("🎯 Attempting to delete non-existent item: \(nonExistentId)")
        
        do {
            try await fileProviderExtension.apiClient.deleteItem(
                identifier: nonExistentId,
                recursive: false
            )
            throw TestError.assertionFailed("Should have failed when deleting non-existent item")
        } catch {
            print("✅ Correctly failed when deleting non-existent item: \(error)")
        }
        
        print("✅ Non-existent item deletion test passed")
    }
    
    static func testRootContainerDeletionPrevention() async throws {
        print("🗑️ Testing root container deletion prevention...")
        
        let fileProviderExtension = try TestHelpers.createTestExtension()
        
        print("🎯 Attempting to delete root container")
        
        do {
            try await fileProviderExtension.apiClient.deleteItem(
                identifier: NSFileProviderItemIdentifier.rootContainer.rawValue,
                recursive: true
            )
            throw TestError.assertionFailed("Should have prevented root container deletion")
        } catch {
            print("✅ Correctly prevented root container deletion: \(error)")
        }
        
        print("✅ Root container deletion prevention test passed")
    }
    
    static func testNonRecursiveFolderDeletion() async throws {
        print("🗑️ Testing non-recursive deletion of non-empty folder...")
        
        let fileProviderExtension = try TestHelpers.createTestExtension()
        
        // Find a folder that has children
        let foldersWithChildren = try await TestHelpers.findFoldersWithChildren()
        guard let (folderWithChildren, childCount) = foldersWithChildren.first else {
            throw TestError.setupFailed("No folder with children found for non-recursive test")
        }
        
        print("🎯 Attempting non-recursive deletion of folder '\(folderWithChildren.filename)' with \(childCount) children")
        
        do {
            try await fileProviderExtension.apiClient.deleteItem(
                identifier: folderWithChildren.itemIdentifier.rawValue,
                recursive: false
            )
            throw TestError.assertionFailed("Should have failed when deleting non-empty folder without recursive flag")
        } catch {
            print("✅ Correctly failed non-recursive deletion of non-empty folder: \(error)")
        }
        
        print("✅ Non-recursive folder deletion test passed")
    }
}