import Foundation
import FileProvider
import HopNetFileProviderCore
import TestHelpers

@main
struct TestModification {
    static func main() async throws {
        let args = ProcessInfo.processInfo.arguments
        let testCase = args.count > 1 ? args[1] : "all"
        
        print("🧪 TestModification starting: \(testCase)")
        
        switch testCase {
        // Metadata modifications (Phase 4a)
        case "rename_file":
            try await testRenameFile()
        case "rename_file_in_folder":
            try await testRenameFileInFolder()
        case "rename_folder":
            try await testRenameFolder()
        case "move_file":
            try await testMoveFile()
        case "move_file_folder_to_folder":
            try await testMoveFileFolderToFolder()
        case "move_folder":
            try await testMoveFolder()
        case "complex_move":
            try await testComplexMove()
            
        // Content modifications (Phase 4b)
        case "update_file_content":
            try await testUpdateFileContent()
        case "update_with_rename":
            try await testUpdateContentWithRename()
            
        // Hierarchy tests
        case "rename_parent_with_children":
            try await testRenameParentWithChildren()
            
        // Error cases
        case "invalid_modifications":
            try await testInvalidModifications()
            
        case "all":
            try await testRenameFile()
            try await testRenameFileInFolder()
            try await testRenameFolder()
            try await testMoveFile()
            try await testMoveFileFolderToFolder()
            try await testMoveFolder()
            try await testComplexMove()
            try await testUpdateFileContent()
            try await testUpdateContentWithRename()
            try await testRenameParentWithChildren()
            try await testInvalidModifications()
            
        default:
            throw TestError.unknownTestCase(testCase)
        }
        
        print("✅ TestModification passed: \(testCase)")
    }
    
    static func testRenameFile() async throws {
        print("📝 Testing file rename...")
        
        // Find an existing file from TestCreation
        let items = try await TestHelpers.enumerateItems(
            fileProvider: try TestHelpers.createTestExtension(),
            containerIdentifier: .rootContainer
        )
        
        guard let fileItem = items.first(where: { $0.contentType != .folder }) else {
            throw TestError.setupFailed("No file found to rename")
        }
        
        print("🎯 Renaming file: '\(fileItem.filename)' (ID: \(fileItem.itemIdentifier.rawValue))")
        
        // Capture initial state
        let initialVersion = fileItem.itemVersion
        let config = try TestHelpers.loadTestConfig()
        let initialSignals = try await TestHelpers.getSignalCount(config: config)
        
        // Get initial consensus height for change enumeration validation
        let fileProviderExtension = try TestHelpers.createTestExtension()
        let initialChanges = try await fileProviderExtension.apiClient.getChanges()
        let initialHeight = initialChanges.current_consensus_height
        
        // Rename the file
        let newName = "Renamed_\(fileItem.filename)"
        try await TestHelpers.modifyItem(
            identifier: fileItem.itemIdentifier,
            newFilename: newName,
            newParent: nil  // Same location
        )
        
        // Verify rename
        let renamedItem = try await TestHelpers.getItemByIdentifier(fileItem.itemIdentifier)
        guard renamedItem.filename == newName else {
            throw TestError.assertionFailed("File rename failed: expected '\(newName)', got '\(renamedItem.filename)'")
        }
        
        guard renamedItem.itemIdentifier == fileItem.itemIdentifier else {
            throw TestError.assertionFailed("File identifier should not change on rename")
        }
        
        // Verify version changed (using raw version data comparison)
        let (initialContent, initialMetadata) = TestHelpers.extractVersionData(from: fileItem)
        let (newContent, newMetadata) = TestHelpers.extractVersionData(from: renamedItem)
        
        guard newContent != initialContent || newMetadata != initialMetadata else {
            throw TestError.assertionFailed("File version should change on rename")
        }
        
        // Verify timestamps: files should NOT have contentModificationDate change on metadata-only operations
        try TestHelpers.validateTimestamps(
            originalItem: fileItem,
            modifiedItem: renamedItem,
            operationType: .fileMetadataOnly,
            operationDescription: "file rename (metadata-only)"
        )
        
        // Verify signals
        let currentSignals = try await TestHelpers.getSignalCount(config: config)
        guard currentSignals > initialSignals else {
            throw TestError.assertionFailed("Signal count should increase after rename")
        }
        
        // CRITICAL: Verify change enumeration shows exactly 1 item for root-level rename
        // Root container is not tracked since it's the default enumeration target
        let expectedChangedItems = [
            fileItem.itemIdentifier.rawValue  // The renamed item itself
        ]
        
        try await TestHelpers.validateChangeEnumeration(
            initialHeight: initialHeight,
            expectedChangedItems: expectedChangedItems,
            operationDescription: "file rename (root-level)"
        )
        
        print("✅ File rename test passed: '\(fileItem.filename)' → '\(newName)'")
    }
    
    static func testRenameFileInFolder() async throws {
        print("📝 Testing file rename within folder (dual enumeration)...")
        
        // Find all folders and look for one that contains files
        let rootItems = try await TestHelpers.enumerateItems(
            fileProvider: try TestHelpers.createTestExtension(),
            containerIdentifier: .rootContainer
        )
        
        let folders = rootItems.filter { $0.contentType == .folder }
        guard !folders.isEmpty else {
            throw TestError.setupFailed("No folders found for nested file rename test")
        }
        
        // Try each folder until we find one with files
        var folderWithFile: (folder: NSFileProviderItem, file: NSFileProviderItem)?
        
        for folderItem in folders {
            print("🔍 Checking folder '\(folderItem.filename)' for files...")
            let folderContents = try await TestHelpers.enumerateItems(
                fileProvider: try TestHelpers.createTestExtension(),
                containerIdentifier: folderItem.itemIdentifier
            )
            
            if let nestedFileItem = folderContents.first(where: { $0.contentType != .folder }) {
                folderWithFile = (folder: folderItem, file: nestedFileItem)
                break
            }
        }
        
        guard let (folderItem, nestedFileItem) = folderWithFile else {
            throw TestError.setupFailed("No file found in any folder for nested file rename test")
        }
        
        print("🎯 Renaming nested file: '\(nestedFileItem.filename)' in folder '\(folderItem.filename)'")
        
        let config = try TestHelpers.loadTestConfig()
        let initialSignals = try await TestHelpers.getSignalCount(config: config)
        
        // Get initial consensus height for change enumeration validation
        let fileProviderExtension = try TestHelpers.createTestExtension()
        let initialChanges = try await fileProviderExtension.apiClient.getChanges()
        let initialHeight = initialChanges.current_consensus_height
        
        // Rename the nested file
        let newName = "renamed_nested_\(UUID().uuidString.prefix(8)).txt"
        try await TestHelpers.modifyItem(
            identifier: nestedFileItem.itemIdentifier,
            newFilename: newName,
            newParent: nil  // Keep in same folder
        )
        
        // Verify the file was renamed
        let updatedFolderContents = try await TestHelpers.enumerateItems(
            fileProvider: try TestHelpers.createTestExtension(),
            containerIdentifier: folderItem.itemIdentifier
        )
        
        guard let renamedItem = updatedFolderContents.first(where: { $0.itemIdentifier == nestedFileItem.itemIdentifier }) else {
            throw TestError.assertionFailed("File should still exist in folder after rename")
        }
        
        guard renamedItem.filename == newName else {
            throw TestError.assertionFailed("File name should be updated to '\(newName)', got '\(renamedItem.filename)'")
        }
        
        // Verify signals
        let currentSignals = try await TestHelpers.getSignalCount(config: config)
        guard currentSignals > initialSignals else {
            throw TestError.assertionFailed("Signal count should increase after nested file rename")
        }
        
        // CRITICAL: Verify change enumeration shows exactly 2 items for nested file rename  
        // (the renamed file + its parent folder)
        let expectedChangedItems = [
            nestedFileItem.itemIdentifier.rawValue,  // The renamed file itself
            folderItem.itemIdentifier.rawValue       // The parent folder (because child changed)
        ]
        
        try await TestHelpers.validateChangeEnumeration(
            initialHeight: initialHeight,
            expectedChangedItems: expectedChangedItems,
            operationDescription: "nested file rename (dual enumeration)"
        )
        
        print("✅ Nested file rename test passed: '\(nestedFileItem.filename)' → '\(newName)' in folder '\(folderItem.filename)'")
    }
    
    static func testRenameFolder() async throws {
        print("📝 Testing folder rename...")
        
        let items = try await TestHelpers.enumerateItems(
            fileProvider: try TestHelpers.createTestExtension(),
            containerIdentifier: .rootContainer
        )
        
        guard let folderItem = items.first(where: { $0.contentType == .folder }) else {
            throw TestError.setupFailed("No folder found to rename")
        }
        
        print("🎯 Renaming folder: '\(folderItem.filename)' (ID: \(folderItem.itemIdentifier.rawValue))")
        
        let initialVersion = folderItem.itemVersion
        let config = try TestHelpers.loadTestConfig()
        let initialSignals = try await TestHelpers.getSignalCount(config: config)
        
        // Rename the folder
        let newName = "RenamedFolder_\(folderItem.filename)"
        try await TestHelpers.modifyItem(
            identifier: folderItem.itemIdentifier,
            newFilename: newName,
            newParent: nil
        )
        
        // Verify rename
        let renamedFolder = try await TestHelpers.getItemByIdentifier(folderItem.itemIdentifier)
        guard renamedFolder.filename == newName else {
            throw TestError.assertionFailed("Folder rename failed: expected '\(newName)', got '\(renamedFolder.filename)'")
        }
        
        // Verify version changed
        let (initialContent, initialMetadata) = TestHelpers.extractVersionData(from: folderItem)
        let (newContent, newMetadata) = TestHelpers.extractVersionData(from: renamedFolder)
        
        guard newContent != initialContent || newMetadata != initialMetadata else {
            throw TestError.assertionFailed("Folder version should change on rename")
        }
        
        // Verify timestamps: folder rename is metadata-only unless children are added/removed
        // Simple rename doesn't change folder structure, just the name
        try TestHelpers.validateTimestamps(
            originalItem: folderItem,
            modifiedItem: renamedFolder,
            operationType: .fileMetadataOnly,  // Treat simple rename as metadata-only
            operationDescription: "folder rename (metadata-only)"
        )
        
        // Verify signals
        let currentSignals = try await TestHelpers.getSignalCount(config: config)
        guard currentSignals > initialSignals else {
            throw TestError.assertionFailed("Signal count should increase after folder rename")
        }
        
        print("✅ Folder rename test passed: '\(folderItem.filename)' → '\(newName)'")
    }
    
    static func testMoveFile() async throws {
        print("📝 Testing file move...")
        
        // Find a file and a folder
        let items = try await TestHelpers.enumerateItems(
            fileProvider: try TestHelpers.createTestExtension(),
            containerIdentifier: .rootContainer
        )
        
        guard let fileItem = items.first(where: { $0.contentType != .folder }),
              let folderItem = items.first(where: { $0.contentType == .folder }) else {
            throw TestError.setupFailed("Need both a file and folder for move test")
        }
        
        print("🎯 Moving file '\(fileItem.filename)' into folder '\(folderItem.filename)'")
        
        let initialFolderVersion = folderItem.itemVersion
        let config = try TestHelpers.loadTestConfig()
        let initialSignals = try await TestHelpers.getSignalCount(config: config)
        
        // Get initial consensus height for change enumeration validation
        let fileProviderExtension = try TestHelpers.createTestExtension()
        let initialChanges = try await fileProviderExtension.apiClient.getChanges()
        let initialHeight = initialChanges.current_consensus_height
        
        // Move file into folder
        try await TestHelpers.modifyItem(
            identifier: fileItem.itemIdentifier,
            newFilename: nil,  // Keep same name
            newParent: folderItem.itemIdentifier
        )
        
        // Verify file no longer in root
        let rootItems = try await TestHelpers.enumerateItems(
            fileProvider: try TestHelpers.createTestExtension(),
            containerIdentifier: .rootContainer
        )
        
        guard !rootItems.contains(where: { $0.itemIdentifier == fileItem.itemIdentifier }) else {
            throw TestError.assertionFailed("File should no longer be in root container after move")
        }
        
        // Verify file is in target folder
        let folderItems = try await TestHelpers.enumerateItems(
            fileProvider: try TestHelpers.createTestExtension(),
            containerIdentifier: folderItem.itemIdentifier
        )
        
        guard folderItems.contains(where: { $0.itemIdentifier == fileItem.itemIdentifier }) else {
            throw TestError.assertionFailed("File should be in target folder after move")
        }
        
        // Verify parent folder version changed
        let updatedFolder = try await TestHelpers.getItemByIdentifier(folderItem.itemIdentifier)
        let (initialFolderContent, initialFolderMetadata) = TestHelpers.extractVersionData(from: folderItem)
        let (newFolderContent, newFolderMetadata) = TestHelpers.extractVersionData(from: updatedFolder)
        
        guard newFolderContent != initialFolderContent || newFolderMetadata != initialFolderMetadata else {
            throw TestError.assertionFailed("Parent folder version should change when child moved into it")
        }
        
        // Verify signals
        let currentSignals = try await TestHelpers.getSignalCount(config: config)
        guard currentSignals > initialSignals else {
            throw TestError.assertionFailed("Signal count should increase after file move")
        }
        
        // CRITICAL: Verify change enumeration shows exactly 2 items for reparenting from root
        // Root container not tracked (default enumeration target), only new parent + moved item
        let expectedChangedItems = [
            fileItem.itemIdentifier.rawValue,  // The moved item itself
            folderItem.itemIdentifier.rawValue  // New parent (folder) gained a child
        ]
        
        try await TestHelpers.validateChangeEnumeration(
            initialHeight: initialHeight,
            expectedChangedItems: expectedChangedItems,
            operationDescription: "file move (from root to folder)"
        )
        
        print("✅ File move test passed: '\(fileItem.filename)' moved into '\(folderItem.filename)'")
    }
    
    static func testMoveFileFolderToFolder() async throws {
        print("📝 Testing file move from folder to folder (triple enumeration)...")
        
        // Find two folders that each contain files
        let rootItems = try await TestHelpers.enumerateItems(
            fileProvider: try TestHelpers.createTestExtension(),
            containerIdentifier: .rootContainer
        )
        
        let folders = rootItems.filter { $0.contentType == .folder }
        guard folders.count >= 2 else {
            throw TestError.setupFailed("Need at least 2 folders for folder-to-folder move test")
        }
        
        // Find source folder with a file
        var sourceConfig: (folder: NSFileProviderItem, file: NSFileProviderItem)?
        
        for folderItem in folders {
            let folderContents = try await TestHelpers.enumerateItems(
                fileProvider: try TestHelpers.createTestExtension(),
                containerIdentifier: folderItem.itemIdentifier
            )
            
            if let fileItem = folderContents.first(where: { $0.contentType != .folder }) {
                sourceConfig = (folder: folderItem, file: fileItem)
                break
            }
        }
        
        guard let (sourceFolderItem, fileItem) = sourceConfig else {
            throw TestError.setupFailed("No file found in any folder for folder-to-folder move test")
        }
        
        // Find an empty target folder (different from source)
        var targetFolderItem: NSFileProviderItem?
        
        for folder in folders where folder.itemIdentifier != sourceFolderItem.itemIdentifier {
            let contents = try await TestHelpers.enumerateItems(
                fileProvider: try TestHelpers.createTestExtension(),
                containerIdentifier: folder.itemIdentifier
            )
            if contents.isEmpty {
                targetFolderItem = folder
                print("✅ Found empty target folder: '\(folder.filename)'")
                break
            }
        }
        
        guard let targetFolder = targetFolderItem else {
            throw TestError.setupFailed("Need an empty target folder for folder-to-folder move test")
        }
        
        print("🎯 Moving file '\(fileItem.filename)' from '\(sourceFolderItem.filename)' to '\(targetFolder.filename)'")
        
        let config = try TestHelpers.loadTestConfig()
        let initialSignals = try await TestHelpers.getSignalCount(config: config)
        
        // Get initial consensus height for change enumeration validation
        let fileProviderExtension = try TestHelpers.createTestExtension()
        let initialChanges = try await fileProviderExtension.apiClient.getChanges()
        let initialHeight = initialChanges.current_consensus_height
        
        // Move file from source folder to target folder
        try await TestHelpers.modifyItem(
            identifier: fileItem.itemIdentifier,
            newFilename: nil,  // Keep same name
            newParent: targetFolder.itemIdentifier
        )
        
        // Verify file no longer in source folder
        let sourceContents = try await TestHelpers.enumerateItems(
            fileProvider: try TestHelpers.createTestExtension(),
            containerIdentifier: sourceFolderItem.itemIdentifier
        )
        
        guard !sourceContents.contains(where: { $0.itemIdentifier == fileItem.itemIdentifier }) else {
            throw TestError.assertionFailed("File should no longer be in source folder after move")
        }
        
        // Verify file is now in target folder
        let targetContents = try await TestHelpers.enumerateItems(
            fileProvider: try TestHelpers.createTestExtension(),
            containerIdentifier: targetFolder.itemIdentifier
        )
        
        guard targetContents.contains(where: { $0.itemIdentifier == fileItem.itemIdentifier }) else {
            throw TestError.assertionFailed("File should be in target folder after move")
        }
        
        // Verify timestamps: files should NOT change contentModificationDate on moves (metadata-only)
        let movedItem = try await TestHelpers.getItemByIdentifier(fileItem.itemIdentifier)
        try TestHelpers.validateTimestamps(
            originalItem: fileItem,
            modifiedItem: movedItem,
            operationType: .fileMetadataOnly,
            operationDescription: "file move (metadata-only)"
        )
        
        // Note: Folder modification dates are computed from their contents' modification dates.
        // Moving files doesn't change the files' modification times, only their location.
        // Therefore folder modification dates may increase, decrease, or stay the same
        // depending on the relative modification times of files being moved.
        // We skip timestamp validation for folders as any result is valid.
        
        // Verify signals
        let currentSignals = try await TestHelpers.getSignalCount(config: config)
        guard currentSignals > initialSignals else {
            throw TestError.assertionFailed("Signal count should increase after folder-to-folder move")
        }
        
        // CRITICAL: Verify change enumeration shows exactly 3 items for folder-to-folder move
        // (moved item + old parent folder + new parent folder)
        let expectedChangedItems = [
            fileItem.itemIdentifier.rawValue,        // The moved item itself
            sourceFolderItem.itemIdentifier.rawValue, // Old parent folder (lost a child)
            targetFolder.itemIdentifier.rawValue      // New parent folder (gained a child)
        ]
        
        try await TestHelpers.validateChangeEnumeration(
            initialHeight: initialHeight,
            expectedChangedItems: expectedChangedItems,
            operationDescription: "file move (folder to folder)"
        )
        
        print("✅ Folder-to-folder file move test passed: '\(fileItem.filename)' moved from '\(sourceFolderItem.filename)' to '\(targetFolder.filename)'")
    }
    
    static func testMoveFolder() async throws {
        print("📝 Testing folder move...")
        
        // Find two folders - one to move and one to move it into
        let items = try await TestHelpers.enumerateItems(
            fileProvider: try TestHelpers.createTestExtension(),
            containerIdentifier: .rootContainer
        )
        
        let folders = items.filter { $0.contentType == .folder }
        guard folders.count >= 2 else {
            throw TestError.setupFailed("Need at least 2 folders for move test")
        }
        
        let sourceFolder = folders[0]
        let targetFolder = folders[1]
        
        print("🎯 Moving folder '\(sourceFolder.filename)' into '\(targetFolder.filename)'")
        
        let initialTargetVersion = targetFolder.itemVersion
        let config = try TestHelpers.loadTestConfig()
        let initialSignals = try await TestHelpers.getSignalCount(config: config)
        
        // Move source folder into target folder
        try await TestHelpers.modifyItem(
            identifier: sourceFolder.itemIdentifier,
            newFilename: nil,
            newParent: targetFolder.itemIdentifier
        )
        
        // Verify source folder no longer in root
        let rootItems = try await TestHelpers.enumerateItems(
            fileProvider: try TestHelpers.createTestExtension(),
            containerIdentifier: .rootContainer
        )
        
        guard !rootItems.contains(where: { $0.itemIdentifier == sourceFolder.itemIdentifier }) else {
            throw TestError.assertionFailed("Source folder should no longer be in root after move")
        }
        
        // Verify source folder is in target folder
        let targetFolderItems = try await TestHelpers.enumerateItems(
            fileProvider: try TestHelpers.createTestExtension(),
            containerIdentifier: targetFolder.itemIdentifier
        )
        
        guard targetFolderItems.contains(where: { $0.itemIdentifier == sourceFolder.itemIdentifier }) else {
            throw TestError.assertionFailed("Source folder should be in target folder after move")
        }
        
        // Verify target folder version changed
        let updatedTargetFolder = try await TestHelpers.getItemByIdentifier(targetFolder.itemIdentifier)
        let (initialContent, initialMetadata) = TestHelpers.extractVersionData(from: targetFolder)
        let (newContent, newMetadata) = TestHelpers.extractVersionData(from: updatedTargetFolder)
        
        guard newContent != initialContent || newMetadata != initialMetadata else {
            throw TestError.assertionFailed("Target folder version should change when folder moved into it")
        }
        
        // Verify signals
        let currentSignals = try await TestHelpers.getSignalCount(config: config)
        guard currentSignals > initialSignals else {
            throw TestError.assertionFailed("Signal count should increase after folder move")
        }
        
        print("✅ Folder move test passed: '\(sourceFolder.filename)' moved into '\(targetFolder.filename)'")
    }
    
    static func testComplexMove() async throws {
        print("📝 Testing complex move (move + rename)...")
        
        // Find a file and folder
        let items = try await TestHelpers.enumerateItems(
            fileProvider: try TestHelpers.createTestExtension(),
            containerIdentifier: .rootContainer
        )
        
        guard let fileItem = items.first(where: { $0.contentType != .folder }),
              let folderItem = items.first(where: { $0.contentType == .folder }) else {
            throw TestError.setupFailed("Need file and folder for complex move test")
        }
        
        print("🎯 Moving and renaming file '\(fileItem.filename)' into folder '\(folderItem.filename)'")
        
        let config = try TestHelpers.loadTestConfig()
        let initialSignals = try await TestHelpers.getSignalCount(config: config)
        
        // Move and rename simultaneously
        let newName = "MovedAndRenamed_\(fileItem.filename)"
        try await TestHelpers.modifyItem(
            identifier: fileItem.itemIdentifier,
            newFilename: newName,
            newParent: folderItem.itemIdentifier
        )
        
        // Verify both changes applied
        let movedItem = try await TestHelpers.getItemByIdentifier(fileItem.itemIdentifier)
        guard movedItem.filename == newName else {
            throw TestError.assertionFailed("File should be renamed: expected '\(newName)', got '\(movedItem.filename)'")
        }
        
        guard movedItem.parentItemIdentifier == folderItem.itemIdentifier else {
            throw TestError.assertionFailed("File should be moved to target folder")
        }
        
        // Verify file is in target folder with new name
        let folderItems = try await TestHelpers.enumerateItems(
            fileProvider: try TestHelpers.createTestExtension(),
            containerIdentifier: folderItem.itemIdentifier
        )
        
        guard folderItems.contains(where: { $0.itemIdentifier == fileItem.itemIdentifier && $0.filename == newName }) else {
            throw TestError.assertionFailed("File should be in target folder with new name")
        }
        
        // Verify signals
        let currentSignals = try await TestHelpers.getSignalCount(config: config)
        guard currentSignals > initialSignals else {
            throw TestError.assertionFailed("Signal count should increase after complex move")
        }
        
        print("✅ Complex move test passed: '\(fileItem.filename)' → '\(newName)' in '\(folderItem.filename)'")
    }
    
    static func testUpdateFileContent() async throws {
        print("📝 Testing file content update...")
        
        // Find a text file (or any file we can update)
        let items = try await TestHelpers.enumerateItems(
            fileProvider: try TestHelpers.createTestExtension(),
            containerIdentifier: .rootContainer
        )
        
        guard let fileItem = items.first(where: { 
            $0.contentType != .folder && $0.filename.hasSuffix(".txt")
        }) else {
            throw TestError.setupFailed("No text file found for content update")
        }
        
        print("🎯 Updating content of file: '\(fileItem.filename)'")
        
        // Download original content for comparison
        let originalContent = try await TestHelpers.downloadFileContentString(fileItem.itemIdentifier)
        let initialVersion = fileItem.itemVersion
        let config = try TestHelpers.loadTestConfig()
        let initialSignals = try await TestHelpers.getSignalCount(config: config)
        
        // Prepare new content
        let newContent = "Updated content at \(Date())\nOriginal content: \(originalContent)\nOriginal size: \(originalContent.count) characters"
        let tempFile = try TestHelpers.createTempFile(content: newContent)
        
        // Update file content
        try await TestHelpers.modifyItemWithContent(
            identifier: fileItem.itemIdentifier,
            contentUrl: tempFile
        )
        
        // Verify content changed
        let updatedContent = try await TestHelpers.downloadFileContentString(fileItem.itemIdentifier)
        guard updatedContent == newContent else {
            throw TestError.assertionFailed("Content update failed: expected '\(newContent)', got '\(updatedContent)'")
        }
        
        // Verify version changed
        let updatedItem = try await TestHelpers.getItemByIdentifier(fileItem.itemIdentifier)
        let (initialContent, initialMetadata) = TestHelpers.extractVersionData(from: fileItem)
        let (newVersionContent, newVersionMetadata) = TestHelpers.extractVersionData(from: updatedItem)
        
        guard newVersionContent != initialContent || newVersionMetadata != initialMetadata else {
            throw TestError.assertionFailed("File version should change on content update")
        }
        
        // Verify timestamps: files SHOULD have contentModificationDate change on content updates
        try TestHelpers.validateTimestamps(
            originalItem: fileItem,
            modifiedItem: updatedItem,
            operationType: .fileContentChange,
            operationDescription: "file content update"
        )
        
        // Verify signals
        let currentSignals = try await TestHelpers.getSignalCount(config: config)
        guard currentSignals > initialSignals else {
            throw TestError.assertionFailed("Signal count should increase after content update")
        }
        
        // Cleanup temp file
        try FileManager.default.removeItem(at: tempFile)
        
        print("✅ Content update test passed: '\(fileItem.filename)' content updated")
    }
    
    static func testUpdateContentWithRename() async throws {
        print("📝 Testing content update with rename...")
        
        // Find a text file
        let items = try await TestHelpers.enumerateItems(
            fileProvider: try TestHelpers.createTestExtension(),
            containerIdentifier: .rootContainer
        )
        
        guard let fileItem = items.first(where: { 
            $0.contentType != .folder && $0.filename.hasSuffix(".txt")
        }) else {
            throw TestError.setupFailed("No text file found for content update with rename")
        }
        
        print("🎯 Updating content and renaming file: '\(fileItem.filename)'")
        
        let config = try TestHelpers.loadTestConfig()
        let initialSignals = try await TestHelpers.getSignalCount(config: config)
        
        // Prepare new content and name
        let newContent = "Content updated with rename at \(Date())"
        let newName = "UpdatedAndRenamed_\(fileItem.filename)"
        let tempFile = try TestHelpers.createTempFile(content: newContent)
        
        // Update content and rename
        try await TestHelpers.modifyItemWithContent(
            identifier: fileItem.itemIdentifier,
            contentUrl: tempFile,
            newFilename: newName
        )
        
        // Verify both changes applied
        let updatedItem = try await TestHelpers.getItemByIdentifier(fileItem.itemIdentifier)
        guard updatedItem.filename == newName else {
            throw TestError.assertionFailed("File should be renamed: expected '\(newName)', got '\(updatedItem.filename)'")
        }
        
        let updatedContent = try await TestHelpers.downloadFileContentString(fileItem.itemIdentifier)
        guard updatedContent == newContent else {
            throw TestError.assertionFailed("Content should be updated: expected '\(newContent)', got '\(updatedContent)'")
        }
        
        // Verify signals
        let currentSignals = try await TestHelpers.getSignalCount(config: config)
        guard currentSignals > initialSignals else {
            throw TestError.assertionFailed("Signal count should increase after content update with rename")
        }
        
        // Cleanup temp file
        try FileManager.default.removeItem(at: tempFile)
        
        print("✅ Content update with rename test passed: '\(fileItem.filename)' → '\(newName)' with new content")
    }
    
    static func testRenameParentWithChildren() async throws {
        print("📝 Testing rename parent with children (hierarchy preservation)...")
        
        // Find a parent folder that has children (created by TestCreation)
        let rootItems = try await TestHelpers.enumerateItems(
            fileProvider: try TestHelpers.createTestExtension(),
            containerIdentifier: .rootContainer
        )
        
        // Look for a folder that has children
        var parentWithChildren: NSFileProviderItem? = nil
        var childrenCount = 0
        
        for folder in rootItems.filter({ $0.contentType == .folder }) {
            let children = try await TestHelpers.enumerateItems(
                fileProvider: try TestHelpers.createTestExtension(),
                containerIdentifier: folder.itemIdentifier
            )
            
            if !children.isEmpty {
                parentWithChildren = folder
                childrenCount = children.count
                print("🎯 Found parent folder '\(folder.filename)' with \(childrenCount) children")
                break
            }
        }
        
        guard let parentFolder = parentWithChildren else {
            throw TestError.setupFailed("No parent folder with children found - TestCreation may not have run nested folder tests")
        }
        
        let config = try TestHelpers.loadTestConfig()
        let initialSignals = try await TestHelpers.getSignalCount(config: config)
        
        // Get the children before rename for comparison
        print("🔍 Enumerating children before rename...")
        let childrenBefore = try await TestHelpers.enumerateItems(
            fileProvider: try TestHelpers.createTestExtension(),
            containerIdentifier: parentFolder.itemIdentifier
        )
        
        print("📋 Children before rename:")
        for child in childrenBefore {
            let type = child.contentType == .folder ? "📁" : "📄"
            print("   \(type) \(child.filename) (ID: \(child.itemIdentifier.rawValue))")
        }
        
        // Rename the parent folder
        let originalName = parentFolder.filename
        let newParentName = "Renamed_\(originalName)"
        
        print("🎯 Renaming parent '\(originalName)' → '\(newParentName)'")
        
        try await TestHelpers.modifyItem(
            identifier: parentFolder.itemIdentifier,
            newFilename: newParentName,
            newParent: nil
        )
        
        // Wait for rename operation to complete
        let afterRenameSignals = try await TestHelpers.waitForSignalCount(config: config, expectedCount: initialSignals + 1)
        
        // Verify parent was renamed in root enumeration
        print("🔍 Verifying parent rename in root container...")
        let rootItemsAfter = try await TestHelpers.enumerateItems(
            fileProvider: try TestHelpers.createTestExtension(),
            containerIdentifier: .rootContainer
        )
        
        guard let renamedParent = rootItemsAfter.first(where: { $0.itemIdentifier == parentFolder.itemIdentifier }) else {
            throw TestError.assertionFailed("Renamed parent folder not found in root container")
        }
        
        guard renamedParent.filename == newParentName else {
            throw TestError.assertionFailed("Parent name not changed: expected '\(newParentName)', got '\(renamedParent.filename)'")
        }
        
        print("✅ Parent successfully renamed: '\(originalName)' → '\(newParentName)'")
        
        // CRITICAL TEST: Verify all children are still accessible under renamed parent
        print("🔍 Verifying children still accessible under renamed parent...")
        let childrenAfter = try await TestHelpers.enumerateItems(
            fileProvider: try TestHelpers.createTestExtension(),
            containerIdentifier: parentFolder.itemIdentifier  // Same identifier, parent was just renamed
        )
        
        guard childrenAfter.count == childrenBefore.count else {
            throw TestError.assertionFailed("Child count mismatch after parent rename: expected \(childrenBefore.count), got \(childrenAfter.count)")
        }
        
        print("📋 Children after rename:")
        for child in childrenAfter {
            let type = child.contentType == .folder ? "📁" : "📄"
            print("   \(type) \(child.filename) (ID: \(child.itemIdentifier.rawValue))")
        }
        
        // Verify each child from before is still present after
        for childBefore in childrenBefore {
            guard let childAfter = childrenAfter.first(where: { $0.itemIdentifier == childBefore.itemIdentifier }) else {
                throw TestError.assertionFailed("Child '\(childBefore.filename)' missing after parent rename")
            }
            
            // Verify child properties unchanged
            guard childAfter.filename == childBefore.filename else {
                throw TestError.assertionFailed("Child filename changed: '\(childBefore.filename)' → '\(childAfter.filename)'")
            }
            
            guard childAfter.parentItemIdentifier == parentFolder.itemIdentifier else {
                throw TestError.assertionFailed("Child '\(childAfter.filename)' has wrong parent identifier after rename")
            }
            
            // If it's a file, verify content is still accessible
            if childAfter.contentType != .folder {
                print("🔍 Verifying content accessibility for '\(childAfter.filename)'...")
                let content = try await TestHelpers.downloadFileContentString(childAfter.itemIdentifier)
                guard !content.isEmpty else {
                    throw TestError.assertionFailed("File '\(childAfter.filename)' content empty/inaccessible after parent rename")
                }
                print("✅ File content still accessible (\(content.count) chars)")
            }
        }
        
        print("✅ All children still accessible with correct relationships")
        
        // Verify signals incremented correctly
        guard afterRenameSignals > initialSignals else {
            throw TestError.assertionFailed("Signal count should increase after parent rename")
        }
        
        print("✅ Rename parent with children test passed:")
        print("   📁 Parent: '\(originalName)' → '\(newParentName)'")
        print("   👥 Children: \(childrenCount) items all preserved")
        print("   🔄 All relationships maintained, content accessible")
    }
    
    static func testInvalidModifications() async throws {
        print("📝 Testing invalid modification scenarios...")
        
        // Test 1: Try to move to non-existent parent
        let items = try await TestHelpers.enumerateItems(
            fileProvider: try TestHelpers.createTestExtension(),
            containerIdentifier: .rootContainer
        )
        
        guard let fileItem = items.first(where: { $0.contentType != .folder }) else {
            throw TestError.setupFailed("No file found for invalid modification tests")
        }
        
        print("🎯 Testing move to non-existent parent...")
        
        // Try to move to a non-existent parent
        let fakeParentId = NSFileProviderItemIdentifier("item:00000000-0000-0000-0000-000000000000")
        
        do {
            try await TestHelpers.modifyItem(
                identifier: fileItem.itemIdentifier,
                newFilename: nil,
                newParent: fakeParentId
            )
            throw TestError.assertionFailed("Should have failed when moving to non-existent parent")
        } catch {
            print("✅ Correctly failed when moving to non-existent parent: \(error)")
        }
        
        // Test 2: Try to rename with invalid characters (if backend validates this)
        print("🎯 Testing rename with potentially problematic name...")
        
        // Try to rename to a name with path separators
        let problematicName = "file/with/slashes"
        
        do {
            try await TestHelpers.modifyItem(
                identifier: fileItem.itemIdentifier,
                newFilename: problematicName,
                newParent: nil
            )
            // This might succeed depending on backend validation
            print("⚠️  Rename with slashes succeeded - backend allows this")
        } catch {
            print("✅ Correctly failed when renaming with problematic characters: \(error)")
        }
        
        // Test 3: Circular reference prevention - try to move folder into itself
        print("🎯 Testing circular reference prevention (folder into itself)...")
        
        guard let folderItem = items.first(where: { $0.contentType == .folder }) else {
            throw TestError.setupFailed("No folder found for circular reference test")
        }
        
        do {
            try await TestHelpers.modifyItem(
                identifier: folderItem.itemIdentifier,
                newFilename: nil,
                newParent: folderItem.itemIdentifier  // Try to move folder into itself
            )
            throw TestError.assertionFailed("Should have prevented moving folder into itself")
        } catch TestError.assertionFailed(let message) {
            // Re-throw our assertion failures
            throw TestError.assertionFailed(message)
        } catch {
            print("✅ Correctly prevented moving folder into itself: \(error)")
        }
        
        // Test 4: Circular reference prevention - try to move parent into child
        print("🎯 Testing circular reference prevention (parent into child)...")
        
        // Find a folder with children
        var parentWithChild: (parent: NSFileProviderItem, child: NSFileProviderItem)?
        
        for folder in items.filter({ $0.contentType == .folder }) {
            let children = try await TestHelpers.enumerateItems(
                fileProvider: try TestHelpers.createTestExtension(),
                containerIdentifier: folder.itemIdentifier
            )
            
            if let childFolder = children.first(where: { $0.contentType == .folder }) {
                parentWithChild = (parent: folder, child: childFolder)
                break
            }
        }
        
        if let (parentFolder, childFolder) = parentWithChild {
            print("   Found parent '\(parentFolder.filename)' with child folder '\(childFolder.filename)'")
            
            do {
                try await TestHelpers.modifyItem(
                    identifier: parentFolder.itemIdentifier,
                    newFilename: nil,
                    newParent: childFolder.itemIdentifier  // Try to move parent into its child
                )
                throw TestError.assertionFailed("Should have prevented moving parent into its child")
            } catch TestError.assertionFailed(let message) {
                // Re-throw our assertion failures
                throw TestError.assertionFailed(message)
            } catch {
                print("✅ Correctly prevented moving parent into child: \(error)")
            }
        } else {
            print("⚠️  No nested folder structure found to test parent-into-child prevention")
        }
        
        // Test 5: Circular reference prevention - try to move into deeper descendant
        print("🎯 Testing circular reference prevention (into deeper descendant)...")
        
        // Find a folder with grandchildren
        var ancestorWithDescendant: (ancestor: NSFileProviderItem, descendant: NSFileProviderItem)?
        
        for folder in items.filter({ $0.contentType == .folder }) {
            let children = try await TestHelpers.enumerateItems(
                fileProvider: try TestHelpers.createTestExtension(),
                containerIdentifier: folder.itemIdentifier
            )
            
            for childFolder in children.filter({ $0.contentType == .folder }) {
                let grandchildren = try await TestHelpers.enumerateItems(
                    fileProvider: try TestHelpers.createTestExtension(),
                    containerIdentifier: childFolder.itemIdentifier
                )
                
                if let grandchildFolder = grandchildren.first(where: { $0.contentType == .folder }) {
                    ancestorWithDescendant = (ancestor: folder, descendant: grandchildFolder)
                    break
                }
            }
            
            if ancestorWithDescendant != nil { break }
        }
        
        if let (ancestorFolder, descendantFolder) = ancestorWithDescendant {
            print("   Found ancestor '\(ancestorFolder.filename)' with descendant folder")
            
            do {
                try await TestHelpers.modifyItem(
                    identifier: ancestorFolder.itemIdentifier,
                    newFilename: nil,
                    newParent: descendantFolder.itemIdentifier  // Try to move ancestor into descendant
                )
                throw TestError.assertionFailed("Should have prevented moving ancestor into descendant")
            } catch TestError.assertionFailed(let message) {
                // Re-throw our assertion failures
                throw TestError.assertionFailed(message)
            } catch {
                print("✅ Correctly prevented moving ancestor into descendant: \(error)")
            }
        } else {
            print("⚠️  No deep folder structure found to test ancestor-into-descendant prevention")
        }
        
        print("✅ Invalid modification tests completed")
    }
}