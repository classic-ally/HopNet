import Foundation
import FileProvider
import HopNetFileProviderCore
import TestHelpers

@main
struct TestCreation {
    static func main() async throws {
        let args = ProcessInfo.processInfo.arguments
        let testCase = args.count > 1 ? args[1] : "folder_creation"
        
        print("🧪 TestCreation starting: \(testCase)")
        
        switch testCase {
        case "folder_creation":
            try await testFolderCreation()
        case "multiple_folder_names":
            try await testMultipleFolderNames()
        case "nested_folders":
            try await testNestedFolders()
        case "file_creation":
            try await testFileCreation()
        case "multiple_file_types":
            try await testMultipleFileTypes()
        case "file_in_nested_folder":
            try await testFileInNestedFolder()
        case "file_content_verification":
            try await testFileContentVerification()
        case "all":
            try await testFolderCreation()
            try await testMultipleFolderNames()
            try await testNestedFolders()
            try await testFileCreation()
            try await testMultipleFileTypes()
            try await testFileInNestedFolder()
            try await testFileContentVerification()
        default:
            throw TestError.unknownTestCase(testCase)
        }
        
        print("✅ TestCreation passed: \(testCase)")
    }
    
    static func testFolderCreation() async throws {
        print("📁 Testing basic folder creation...")
        
        let folderName = "TestFolder_\(UUID().uuidString.prefix(8))"
        try await TestHelpers.testFolderCreation(folderName: folderName)
        
        print("✅ Basic folder creation test passed")
    }
    
    static func testMultipleFolderNames() async throws {
        print("📁 Testing multiple folder name variations...")
        
        let testNames = [
            "SimpleFolder",
            "Folder-With-Hyphens", 
            "Folder_With_Underscores",
            "Folder.With.Dots",
            "Folder With Spaces",
            "文件夹Test",  // Unicode test
            "TestПапка",   // Cyrillic test
            "Folder123",
            "📁TestFolder", // Emoji test
            "🎯ProjectFolder🚀", // Multiple emojis
            "Folder📊Data" // Emoji in middle
        ]
        
        for folderName in testNames {
            let uniqueName = "\(folderName)_\(UUID().uuidString.prefix(4))"
            print("🧪 Testing folder name: '\(uniqueName)'")
            try await TestHelpers.testFolderCreation(folderName: uniqueName)
        }
        
        // Add some files directly in a few of these folders for modification tests
        let foldersWithFiles = ["SimpleFolder", "Folder With Spaces", "Folder123"]
        for baseName in foldersWithFiles {
            let uniqueName = "\(baseName)_\(UUID().uuidString.prefix(4))"
            print("🧪 Creating folder with direct file: '\(uniqueName)'")
            
            try await TestHelpers.testFolderCreation(folderName: uniqueName)
            let folderId = try await TestHelpers.getFolderIdentifier(folderName: uniqueName)
            
            // Add a file directly in this folder
            let fileName = "file_in_\(baseName.replacingOccurrences(of: " ", with: "_")).txt"
            let content = "File directly in folder: \(uniqueName)\nFilename: \(fileName)\nCreated: \(Date())"
            
            try await TestHelpers.testNestedFileCreation(
                fileName: fileName,
                content: content,
                parentIdentifier: folderId,
                parentName: uniqueName
            )
        }
        
        print("✅ Multiple folder names test passed")
    }
    
    static func testNestedFolders() async throws {
        print("📁 Testing nested folder creation...")
        
        let context = try await TestHelpers.TestContext.capture()
        
        let hierarchyTemplates = [
            TestHelpers.HierarchyTemplate(levels: ["ProjectAlpha", "src", "components", "ui"]),
            TestHelpers.HierarchyTemplate(levels: ["ProjectBeta", "docs", "specs"]),
            TestHelpers.HierarchyTemplate(levels: ["DataFolder", "raw", "processed"]),
            TestHelpers.HierarchyTemplate(levels: ["WorkSpace", "projects", "active", "current"]),
            TestHelpers.HierarchyTemplate(levels: ["Archives", "2024", "Q1"]),
            TestHelpers.HierarchyTemplate(levels: ["Testing", "integration", "scenarios"])
        ]
        
        try await TestHelpers.verifyHierarchyCreation(
            context: context,
            hierarchies: hierarchyTemplates,
            operationDescription: "nested folder creation (multiple hierarchies)"
        )
        
        print("✅ Nested folder creation test passed")
    }
    
    // MARK: - File Creation Tests
    
    static func testFileCreation() async throws {
        print("📄 Testing basic file creation...")
        
        let fileName = "TestFile_\(UUID().uuidString.prefix(8)).txt"
        let testContent = "Hello, World! This is a test file created by the FileProvider test suite.\nContent timestamp: \(Date())"
        
        try await TestHelpers.testFileCreation(fileName: fileName, content: testContent)
        
        print("✅ Basic file creation test passed")
    }
    
    static func testMultipleFileTypes() async throws {
        print("📄 Testing multiple file types...")
        
        let testFiles = [
            ("simple.txt", "Simple text file content."),
            ("empty.txt", ""), // Empty file
            ("unicode_文档.txt", "Unicode content: 测试文档 🚀📁💫"), // Unicode filename and content
            ("data.json", "{\"test\": true, \"value\": 42}"), // JSON content
            ("markdown.md", "# Test Markdown\n\nThis is **bold** and *italic*."), // Markdown
            ("large.txt", TestHelpers.generateTestContent(size: 10000, pattern: "LargeFileData")) // ~10KB file
        ]
        
        for (fileName, content) in testFiles {
            let uniqueName = "\(fileName.replacingOccurrences(of: ".", with: "_\(UUID().uuidString.prefix(4)).")).\(fileName.split(separator: ".").last ?? "txt")"
            print("🧪 Testing file type: '\(uniqueName)' (content: \(content.count) chars)")
            try await TestHelpers.testFileCreation(fileName: uniqueName, content: content)
        }
        
        print("✅ Multiple file types test passed")
    }
    
    static func testFileInNestedFolder() async throws {
        print("📄 Testing file creation in nested folders...")
        
        // Create multiple nested structures with different file configurations
        let nestedConfigs: [(String, String, [String])] = [
            ("DevProjects", "Frontend", ["app.js", "styles.css"]),      // 2-level with multiple files
            ("Documents", "Reports", ["summary.txt"]),                  // 2-level with single file  
            ("MediaLibrary", "Photos", ["vacation.jpg", "family.png"]), // 2-level with media files
            ("CodeBase", "Utils", ["helpers.swift"]),                   // 2-level with code file
            ("Resources", "Templates", ["template.md", "config.json"]), // 2-level with config files
        ]
        
        // ALSO create some direct files in top-level folders for modification tests
        let directFileConfigs: [(String, [String])] = [
            ("DirectTestFolder", ["direct_file.txt", "readme.md"]),
            ("QuickTestFolder", ["test.json"]),
            ("SimpleTestFolder", ["data.csv", "notes.txt"]),
        ]
        
        // Create folders with direct files (not nested)
        for (folderBase, fileNames) in directFileConfigs {
            let uniqueId = UUID().uuidString.prefix(4)
            let folderName = "\(folderBase)_\(uniqueId)"
            
            print("🧪 Creating folder with direct files: \(folderName)")
            
            // Create parent folder
            try await TestHelpers.testFolderCreation(folderName: folderName)
            let folderId = try await TestHelpers.getFolderIdentifier(folderName: folderName)
            
            // Create files directly in this folder
            for fileName in fileNames {
                let content = "Direct file: \(fileName)\nFolder: \(folderName)\nCreated: \(Date())"
                
                try await TestHelpers.testNestedFileCreation(
                    fileName: fileName,
                    content: content,
                    parentIdentifier: folderId,
                    parentName: folderName
                )
            }
        }
        
        // CREATE MIXED CONTENT FOLDERS (files + subfolders in same directory) for deletion tests
        let mixedContentConfigs: [(String, [String], [String])] = [
            ("MixedContentFolder", ["document.txt", "notes.md"], ["SubfolderA", "SubfolderB"]),
            ("WorkspaceRoot", ["config.json", "readme.txt"], ["src", "docs", "tests"]),
            ("ProjectFiles", ["package.json", "license.txt"], ["lib", "assets"]),
        ]
        
        for (folderBase, fileNames, subfolderNames) in mixedContentConfigs {
            let uniqueId = UUID().uuidString.prefix(4)
            let folderName = "\(folderBase)_\(uniqueId)"
            
            print("🧪 Creating mixed content folder: \(folderName) (files + subfolders)")
            
            // Create parent folder
            try await TestHelpers.testFolderCreation(folderName: folderName)
            let folderId = try await TestHelpers.getFolderIdentifier(folderName: folderName)
            
            // Create files directly in this folder
            for fileName in fileNames {
                let content = "Mixed content file: \(fileName)\nParent: \(folderName)\nType: File\nCreated: \(Date())"
                
                try await TestHelpers.testNestedFileCreation(
                    fileName: fileName,
                    content: content,
                    parentIdentifier: folderId,
                    parentName: folderName
                )
            }
            
            // Create subfolders in this same folder
            for subfolderName in subfolderNames {
                let fullSubfolderName = "\(subfolderName)_\(uniqueId)"
                
                try await TestHelpers.testNestedFolderCreation(
                    folderName: fullSubfolderName,
                    parentIdentifier: folderId,
                    parentName: folderName
                )
            }
        }
        
        for (parentBase, childBase, fileNames) in nestedConfigs {
            let uniqueId = UUID().uuidString.prefix(4)
            let parentName = "\(parentBase)_\(uniqueId)"
            let childName = "\(childBase)_\(uniqueId)"
            
            print("🧪 Creating nested file structure: \(parentName)/\(childName)")
            
            // Create parent folder
            try await TestHelpers.testFolderCreation(folderName: parentName)
            let parentId = try await TestHelpers.getFolderIdentifier(folderName: parentName)
            
            // Create child folder
            try await TestHelpers.testNestedFolderCreation(
                folderName: childName,
                parentIdentifier: parentId,
                parentName: parentName
            )
            let childId = try await TestHelpers.getFolderIdentifier(
                folderName: childName,
                parentIdentifier: parentId
            )
            
            // Create files in the child folder
            for fileName in fileNames {
                let content = "File: \(fileName)\nPath: \(parentName)/\(childName)/\(fileName)\nCreated: \(Date())"
                
                try await TestHelpers.testNestedFileCreation(
                    fileName: fileName,
                    content: content,
                    parentIdentifier: childId,
                    parentName: "\(parentName)/\(childName)"
                )
            }
        }
        
        // Create some deeper 3-level structures with files for comprehensive testing
        let deepConfigs: [(String, String, String, [String])] = [
            ("Enterprise", "Projects", "Mobile", ["app.swift", "tests.swift"]),
            ("Research", "Data", "Analysis", ["results.csv", "summary.txt"]),
        ]
        
        for (level1, level2, level3, fileNames) in deepConfigs {
            let uniqueId = UUID().uuidString.prefix(4)
            let parentName = "\(level1)_\(uniqueId)"
            let childName = "\(level2)_\(uniqueId)"
            let grandchildName = "\(level3)_\(uniqueId)"
            
            print("🧪 Creating deep file structure: \(parentName)/\(childName)/\(grandchildName)")
            
            // Create 3-level hierarchy
            try await TestHelpers.testFolderCreation(folderName: parentName)
            let parentId = try await TestHelpers.getFolderIdentifier(folderName: parentName)
            
            try await TestHelpers.testNestedFolderCreation(
                folderName: childName,
                parentIdentifier: parentId,
                parentName: parentName
            )
            let childId = try await TestHelpers.getFolderIdentifier(
                folderName: childName,
                parentIdentifier: parentId
            )
            
            try await TestHelpers.testNestedFolderCreation(
                folderName: grandchildName,
                parentIdentifier: childId,
                parentName: "\(parentName)/\(childName)"
            )
            let grandchildId = try await TestHelpers.getFolderIdentifier(
                folderName: grandchildName,
                parentIdentifier: childId
            )
            
            // Create files in the deepest level
            for fileName in fileNames {
                let content = "Deep file: \(fileName)\nPath: \(parentName)/\(childName)/\(grandchildName)/\(fileName)\nLevel: 3"
                
                try await TestHelpers.testNestedFileCreation(
                    fileName: fileName,
                    content: content,
                    parentIdentifier: grandchildId,
                    parentName: "\(parentName)/\(childName)/\(grandchildName)"
                )
            }
        }
        
        print("✅ Nested file creation test passed")
    }
    
    static func testFileContentVerification() async throws {
        print("📄 Testing file creation with content verification...")
        
        let testFiles = [
            ("verification_simple.txt", "Simple content for verification test."),
            ("verification_unicode.txt", "Unicode: 测试文档 🎯🚀📁 emoji test"),
            ("verification_json.json", "{\"message\":\"This is JSON content\",\"test\":true,\"number\":42}"),
            ("verification_large.txt", TestHelpers.generateTestContent(size: 50000, pattern: "VerificationData")), // ~50KB
            ("verification_empty.txt", ""), // Empty file - test last to avoid breaking other tests
            ("verification_multiline.md", """
            # Multiline Content Test
            
            This file contains multiple lines with various formatting:
            
            - **Bold text**
            - *Italic text*
            - `Code snippets`
            
            ## Code Block:
            ```swift
            let message = "Hello, World!"
            print(message)
            ```
            
            ## Unicode Test
            测试中文内容 🌟✨💫
            
            End of file.
            """)
        ]
        
        for (fileName, content) in testFiles {
            let uniqueName = "\(fileName.replacingOccurrences(of: ".", with: "_\(UUID().uuidString.prefix(4)).")).\(fileName.split(separator: ".").last ?? "txt")"
            print("🧪 Testing content verification for: '\(uniqueName)' (content: \(content.count) chars)")
            
            try await TestHelpers.testFileCreationWithVerification(fileName: uniqueName, content: content)
        }
        
        print("✅ File content verification test passed")
    }
}