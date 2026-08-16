/*
HopNet FileProvider Extension Implementation
*/

import FileProvider
import Foundation
import UniformTypeIdentifiers
import os.log

open class HopNetFileProviderExtensionBase: NSObject, NSFileProviderReplicatedExtension {
    let logger = Logger(subsystem: "com.hopnet.desktop.fileprovider", category: "extension")
    
    @objc(_initializedByViewServices)
    static let _initializedByViewServices = true
    
    // This runs when the class is first loaded by the runtime (before any init)
    static let classLoadLogger: Void = {
        // Removed debug print - use NSLog for class loading events
        NSLog("🚀 HopNetFileProviderExtension class loaded!")
        os_log("🚀 HopNetFileProviderExtension class loaded!", type: .debug)
    }()
    
    public let domain: NSFileProviderDomain
    public var manager: NSFileProviderManager
    public let apiClient: HopNetApiClient
    
    required public init(domain: NSFileProviderDomain) {
        self.domain = domain
        self.manager = NSFileProviderManager(for: domain)!
        
        // Load configuration from keychain (stored by Rust main app)
        do {
            let config = try FileProviderConfig.loadFromKeychain()
            self.apiClient = HopNetApiClient(config: config)
            logger.debug("✅ Loaded FileProvider configuration from keychain")
        } catch {
            logger.error("❌ Failed to load FileProvider configuration from keychain: \(error)")
            // Fallback to default config for development
            let config = FileProviderConfig(
                baseUrl: "http://localhost:34632",
                apiKey: "development-key"
            )
            self.apiClient = HopNetApiClient(config: config)
        }
        
        super.init()
        
        logger.debug("🎯 HopNetFileProviderExtension initialized for domain: \(domain.identifier.rawValue, privacy: .public)")
        // Moved to logger.debug below
    }
    
    /// Convenience initializer for testing with custom configuration
    public init(domain: NSFileProviderDomain, config: FileProviderConfig) {
        self.domain = domain
        self.manager = NSFileProviderManager(for: domain)!
        self.apiClient = HopNetApiClient(config: config)
        
        super.init()
        
        logger.debug("🎯 HopNetFileProviderExtension initialized for domain: \(domain.identifier.rawValue, privacy: .public) with custom config")
        // Moved to logger.debug below
    }
    
    public func invalidate() {
        logger.debug("🎯 HopNetFileProviderExtension invalidated")
        // Moved to logger.debug below
    }
    
    // MARK: - NSFileProviderEnumerating
    
    public func enumerator(for containerItemIdentifier: NSFileProviderItemIdentifier, request: NSFileProviderRequest) throws -> NSFileProviderEnumerator {
        logger.debug("🎯 enumerator requested for container: \(containerItemIdentifier.rawValue)")
        // Moved to logger.debug below
        
        return HopNetEnumerator(containerItemIdentifier: containerItemIdentifier, apiClient: apiClient)
    }
    
    // MARK: - NSFileProviderReplicatedExtension
    
    public func item(for identifier: NSFileProviderItemIdentifier, request: NSFileProviderRequest, completionHandler: @escaping (NSFileProviderItem?, Error?) -> Void) -> Progress {
        logger.debug("🎯 item requested for identifier: \(identifier.rawValue)")
        // Moved to logger.debug below
        
        let progress = Progress()
        
        // Handle special root container case
        if identifier == NSFileProviderItemIdentifier.rootContainer {
            // Create a synthetic root item
            let rootItem = FileProviderItem(
                identifier: NSFileProviderItemIdentifier.rootContainer.rawValue,
                filename: "HopNet",
                parent_item_identifier: NSFileProviderItemIdentifier.rootContainer.rawValue,
                item_type: .folder,
                file_size: nil,  // Folders don't have size
                creation_date: nil,  // Root container doesn't have timestamps
                content_modification_date: nil,  // Root container doesn't have timestamps
                modification_height: nil  // Root container doesn't have modification height
            )
            let providerItem = HopNetFileProviderItem(apiItem: rootItem)
            completionHandler(providerItem, nil)
            return progress
        }
        
        // Fetch item metadata from backend
        Task {
            do {
                let apiItem = try await apiClient.getItem(identifier: identifier.rawValue)
                let providerItem = HopNetFileProviderItem(apiItem: apiItem)
                logger.debug("✅ Successfully retrieved item: \(identifier.rawValue)")
                completionHandler(providerItem, nil)
            } catch let error as ApiError {
                logger.error("❌ API error during item lookup: \(error)")
                
                // Map API errors to appropriate FileProvider errors
                let fileProviderError: Error
                switch error {
                case .notFound:
                    fileProviderError = NSFileProviderError(.noSuchItem)
                case .unauthorized:
                    fileProviderError = NSFileProviderError(.notAuthenticated)
                case .upgradeRequired:
                    fileProviderError = NSFileProviderError(
                        .notAuthenticated,
                        userInfo: [
                            NSLocalizedDescriptionKey: "HopNet Update Required",
                            NSLocalizedRecoverySuggestionErrorKey: "Update the HopNet app to continue syncing files."
                        ]
                    )
                case .notReady:
                    fileProviderError = NSFileProviderError(
                        .notAuthenticated,
                        userInfo: [
                            NSLocalizedDescriptionKey: "HopNet Setup Required",
                            NSLocalizedRecoverySuggestionErrorKey: "Please sign in to the HopNet desktop app before accessing files."
                        ]
                    )
                case .serverError(_):
                    fileProviderError = NSFileProviderError(.serverUnreachable)
                default:
                    fileProviderError = NSFileProviderError(.serverUnreachable)
                }
                
                completionHandler(nil, fileProviderError)
                
            } catch {
                logger.error("❌ Unexpected error during item lookup: \(error)")
                completionHandler(nil, NSFileProviderError(.serverUnreachable))
            }
        }
        
        return progress
    }
    
    public func fetchContents(for itemIdentifier: NSFileProviderItemIdentifier, version requestedVersion: NSFileProviderItemVersion?, request: NSFileProviderRequest, completionHandler: @escaping (URL?, NSFileProviderItem?, Error?) -> Void) -> Progress {
        logger.debug("🎯 fetchContents requested for identifier: \(itemIdentifier.rawValue)")
        // Moved to logger.debug below
        
        let progress = Progress()
        // Don't set totalUnitCount until we know the actual file size
        
        // Only handle item identifiers - but need to check if it's a file
        guard itemIdentifier.rawValue.hasPrefix("item:") else {
            logger.error("❌ fetchContents called for non-item identifier: \(itemIdentifier.rawValue)")
            completionHandler(nil, nil, NSFileProviderError(.noSuchItem))
            return progress
        }
        
        Task {
            do {
                // First, get the current item metadata
                // Don't set completedUnitCount until we have totalUnitCount
                
                let currentItem = try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<HopNetFileProviderItem, Error>) in
                    let _ = self.item(for: itemIdentifier, request: request) { item, error in
                        if let error = error {
                            continuation.resume(throwing: error)
                        } else if let item = item as? HopNetFileProviderItem {
                            continuation.resume(returning: item)
                        } else {
                            continuation.resume(throwing: NSFileProviderError(.noSuchItem))
                        }
                    }
                }
                
                // Only fetch contents for files, not folders
                guard currentItem.contentType != .folder else {
                    logger.error("❌ fetchContents called for folder: \(itemIdentifier.rawValue)")
                    completionHandler(nil, nil, NSFileProviderError(.noSuchItem))
                    return
                }
                
                // Download file content to temporary location
                let urlSessionTempUrl = try await apiClient.downloadFile(identifier: itemIdentifier.rawValue) { downloadProgress in
                    // Don't update progress until we have totalUnitCount set
                }
                
                // Check URLSession temp file BEFORE moving
                logger.debug("🔍 BEFORE MOVE - URLSession temp exists: \(FileManager.default.fileExists(atPath: urlSessionTempUrl.path))")
                if FileManager.default.fileExists(atPath: urlSessionTempUrl.path) {
                    do {
                        let preSize = try FileManager.default.attributesOfItem(atPath: urlSessionTempUrl.path)[.size] as? UInt64 ?? 0
                        logger.debug("🔍 BEFORE MOVE - URLSession temp size: \(preSize) bytes")
                        
                        // Try to read first few bytes to verify content
                        let testData = try Data(contentsOf: urlSessionTempUrl, options: [.mappedIfSafe])
                        logger.debug("🔍 BEFORE MOVE - Content length: \(testData.count), first 16 bytes: \(testData.prefix(16).map { String(format: "%02x", $0) }.joined())")
                    } catch {
                        logger.debug("🔍 BEFORE MOVE - Error reading URLSession temp: \(error)")
                    }
                }
                
                // Move the file to FileProvider temp directory for system access
                let fileProviderTempDir = try manager.temporaryDirectoryURL()
                let tempFileUrl = fileProviderTempDir.appendingPathComponent("hopnet-\(UUID().uuidString)")
                
                logger.debug("🔍 Moving from URLSession temp: \(urlSessionTempUrl.path)")
                logger.debug("🔍 Moving to FileProvider temp: \(tempFileUrl.path)")
                
                try FileManager.default.moveItem(at: urlSessionTempUrl, to: tempFileUrl)
                
                // Set file permissions to allow system access (644 = rw-r--r--)
                try FileManager.default.setAttributes([
                    .posixPermissions: 0o644
                ], ofItemAtPath: tempFileUrl.path)
                
                // Check FileProvider temp file AFTER moving
                logger.debug("🔍 AFTER MOVE - FileProvider temp exists: \(FileManager.default.fileExists(atPath: tempFileUrl.path))")
                if FileManager.default.fileExists(atPath: tempFileUrl.path) {
                    do {
                        let postSize = try FileManager.default.attributesOfItem(atPath: tempFileUrl.path)[.size] as? UInt64 ?? 0
                        logger.debug("🔍 AFTER MOVE - FileProvider temp size: \(postSize) bytes")
                        
                        // Try to read first few bytes to verify content
                        let testData = try Data(contentsOf: tempFileUrl, options: [.mappedIfSafe])
                        logger.debug("🔍 AFTER MOVE - Content length: \(testData.count), first 16 bytes: \(testData.prefix(16).map { String(format: "%02x", $0) }.joined())")
                        
                        // Check file permissions for system access
                        let attributes = try FileManager.default.attributesOfItem(atPath: tempFileUrl.path)
                        let permissions = attributes[.posixPermissions] as? NSNumber ?? 0
                        logger.debug("🔍 AFTER MOVE - File permissions: \(String(format: "%o", permissions.intValue), privacy: .public)")
                    } catch {
                        logger.debug("🔍 AFTER MOVE - Error reading FileProvider temp: \(error)")
                    }
                }
                
                // Get the actual file size from the downloaded file and set progress accordingly
                var fileSize: NSNumber? = nil
                if FileManager.default.fileExists(atPath: tempFileUrl.path) {
                    do {
                        let attributes = try FileManager.default.attributesOfItem(atPath: tempFileUrl.path)
                        if let size = attributes[.size] as? UInt64 {
                            fileSize = NSNumber(value: size)
                            logger.debug("Downloaded file size: \(size) bytes")
                            
                            // Now set the progress to reflect the actual file size
                            progress.totalUnitCount = Int64(size)
                            progress.completedUnitCount = Int64(size)  // File is fully downloaded
                        }
                    } catch {
                        logger.warning("Could not get file size: \(error)")
                    }
                }
                
                // Create updated item with downloaded state and file size
                let updatedItem = currentItem.asDownloaded(withSize: fileSize)
                
                // Debug: Verify item consistency for FileProvider system
                logger.debug("🔍 ITEM VERIFICATION:")
                logger.debug("🔍 Requested identifier: \(itemIdentifier.rawValue, privacy: .public)")
                logger.debug("🔍 Returned item identifier: \(updatedItem.itemIdentifier.rawValue, privacy: .public)")
                logger.debug("🔍 Identifiers match: \(updatedItem.itemIdentifier == itemIdentifier, privacy: .public)")
                logger.debug("🔍 Item isDownloaded: \(updatedItem.isDownloaded, privacy: .public)")
                logger.debug("🔍 Item isMostRecentVersionDownloaded: \(updatedItem.isMostRecentVersionDownloaded, privacy: .public)")
                logger.debug("🔍 Item documentSize: \(updatedItem.documentSize?.description ?? "nil", privacy: .public)")
                logger.debug("🔍 Item filename: \(updatedItem.filename, privacy: .public)")
                
                // Force log the URL we're returning to FileProvider
                logger.debug("🎯 RETURNING to FileProvider: \(tempFileUrl.path, privacy: .public)")
                logger.debug("🎯 Returning path to FileProvider: \(tempFileUrl.path, privacy: .public)")
                logger.debug("🎯 Returning URL to FileProvider: \(tempFileUrl, privacy: .public)")
                
                logger.debug("✅ Successfully fetched contents for identifier: \(itemIdentifier.rawValue, privacy: .public)")
                
                // Add domain verification
                logger.debug("🔍 DOMAIN INFO:")
                logger.debug("🔍 Domain identifier: \(self.domain.identifier.rawValue, privacy: .public)")
                logger.debug("🔍 Domain displayName: \(self.domain.displayName, privacy: .public)")
                
                completionHandler(tempFileUrl, updatedItem, nil)
                
            } catch let error as ApiError {
                logger.error("❌ API error during fetchContents: \(error)")
                
                // Map API errors to appropriate FileProvider errors
                let fileProviderError: Error
                switch error {
                case .notFound:
                    fileProviderError = NSFileProviderError(.noSuchItem)
                case .unauthorized:
                    fileProviderError = NSFileProviderError(.notAuthenticated)
                case .upgradeRequired:
                    fileProviderError = NSFileProviderError(
                        .notAuthenticated,
                        userInfo: [
                            NSLocalizedDescriptionKey: "HopNet Update Required",
                            NSLocalizedRecoverySuggestionErrorKey: "Update the HopNet app to continue syncing files."
                        ]
                    )
                case .notReady:
                    fileProviderError = NSFileProviderError(
                        .notAuthenticated,
                        userInfo: [
                            NSLocalizedDescriptionKey: "HopNet Setup Required",
                            NSLocalizedRecoverySuggestionErrorKey: "Please sign in to the HopNet desktop app before downloading files."
                        ]
                    )
                case .serverError(_):
                    fileProviderError = NSFileProviderError(.serverUnreachable)
                default:
                    fileProviderError = NSFileProviderError(.serverUnreachable)
                }
                
                completionHandler(nil, nil, fileProviderError)
                
            } catch {
                logger.error("❌ Unexpected error during fetchContents: \(error)")
                completionHandler(nil, nil, NSFileProviderError(.serverUnreachable))
            }
        }
        
        return progress
    }
    
    public func createItem(basedOn itemTemplate: NSFileProviderItem, fields: NSFileProviderItemFields, contents url: URL?, options: NSFileProviderCreateItemOptions, request: NSFileProviderRequest, completionHandler: @escaping (NSFileProviderItem?, NSFileProviderItemFields, Bool, Error?) -> Void) -> Progress {
        logger.debug("🎯 createItem requested: \(itemTemplate.filename)")
        
        let progress = Progress()
        
        Task {
            do {
                // Create item via API using parent identifier directly
                try await apiClient.createItem(
                    parentItemIdentifier: itemTemplate.parentItemIdentifier.rawValue,
                    filename: itemTemplate.filename,
                    fileUrl: url
                )
                
                // For files, we need to fetch the actual item from the server to get the real data_block_id
                // For folders, we can construct the identifier directly
                let createdItem: NSFileProviderItem
                if url != nil { // This is a file upload
                    // Fetch the actual file from the server to get the real identifier
                    let parentIdentifier = getParentIdentifierForEnumeration(itemTemplate.parentItemIdentifier)
                    createdItem = try await fetchCreatedFile(parentIdentifier: parentIdentifier, filename: itemTemplate.filename)
                } else { // This is a folder creation
                    let parentIdentifier = getParentIdentifierForEnumeration(itemTemplate.parentItemIdentifier)
                    createdItem = try await createFolderResponse(
                        parentIdentifier: parentIdentifier,
                        filename: itemTemplate.filename
                    )
                }
                
                logger.debug("✅ Successfully created item: \(itemTemplate.filename)")
                completionHandler(createdItem, [], false, nil)
                
            } catch let error as ApiError {
                logger.error("❌ API error during item creation: \(error)")
                
                let fileProviderError: Error
                switch error {
                case .serverError(let message) where message.contains("already exists"):
                    fileProviderError = NSFileProviderError(.filenameCollision)
                case .unauthorized:
                    fileProviderError = NSFileProviderError(.notAuthenticated)
                case .upgradeRequired:
                    fileProviderError = NSFileProviderError(.notAuthenticated)
                case .notReady:
                    fileProviderError = NSFileProviderError(.serverUnreachable)
                default:
                    fileProviderError = NSFileProviderError(.cannotSynchronize)
                }
                
                completionHandler(nil, [], false, fileProviderError)
            } catch {
                logger.error("❌ Unexpected error during item creation: \(error)")
                completionHandler(nil, [], false, NSFileProviderError(.cannotSynchronize))
            }
        }
        
        return progress
    }
    
    private func getPathFromIdentifier(_ identifier: NSFileProviderItemIdentifier) async throws -> String {
        if identifier == .rootContainer {
            return "/"
        }
        
        if identifier.rawValue.starts(with: "item:") {
            // For unified item: identifiers, we need to get the item metadata first
            // This will require a separate API call to get the path
            throw ApiError.parseError("Path extraction from item: identifiers requires API call")
        }
        
        // Note: Old folder: format is no longer supported with unified identifiers
        throw ApiError.parseError("Cannot determine path for identifier: \(identifier.rawValue)")
    }
    
    /// Helper function to get parent identifier for enumeration
    /// No longer needs to convert to paths - we can use identifiers directly!
    private func getParentIdentifierForEnumeration(_ identifier: NSFileProviderItemIdentifier) -> String {
        return identifier.rawValue
    }
    
    private func fetchCreatedFile(parentIdentifier: String, filename: String) async throws -> NSFileProviderItem {
        // Enumerate the parent folder to find the newly created file
        let response = try await apiClient.enumerate(parentItemIdentifier: parentIdentifier)
        
        // Find the file we just created by filename
        guard let createdFile = response.items.first(where: { $0.filename == filename && $0.item_type == .file }) else {
            throw ApiError.notFound
        }
        
        // Return the file with its real data_block_id identifier
        return HopNetFileProviderItem(apiItem: createdFile)
    }
    
    private func createFolderResponse(parentIdentifier: String, filename: String) async throws -> NSFileProviderItem {
        // For folders, we need to use the encrypted path identifier from the backend
        // This ensures consistency with how the database generates folder identifiers during enumeration
        
        // Call the backend to get the encrypted path for this folder
        // Since we just created it, enumerate the parent to find it with encrypted identifier
        let response = try await apiClient.enumerate(parentItemIdentifier: parentIdentifier)
        
        // Find the folder we just created by filename
        guard let createdFolder = response.items.first(where: { $0.filename == filename && $0.item_type == .folder }) else {
            throw ApiError.notFound
        }
        
        // Return the folder with its backend-generated encrypted identifier
        return HopNetFileProviderItem(apiItem: createdFolder)
    }
    
    public func modifyItem(_ item: NSFileProviderItem, baseVersion version: NSFileProviderItemVersion, changedFields: NSFileProviderItemFields, contents newContents: URL?, options: NSFileProviderModifyItemOptions, request: NSFileProviderRequest, completionHandler: @escaping (NSFileProviderItem?, NSFileProviderItemFields, Bool, Error?) -> Void) -> Progress {
        logger.debug("🎯 modifyItem requested for: \(item.filename, privacy: .public)")
        logger.debug("🎯 Changed fields: \(String(describing: changedFields), privacy: .public)")
        
        let progress = Progress()
        
        // Check if content modification is requested (Phase 4b)
        if let newContentsUrl = newContents {
            logger.debug("📝 Content modification requested for item: \(item.itemIdentifier.rawValue)")
            Task {
                do {
                    // Use modifyItemWithContent for content updates
                    let response = try await apiClient.modifyItemWithContent(
                        identifier: item.itemIdentifier.rawValue,
                        filename: changedFields.contains(.filename) ? item.filename : nil,
                        parentItemIdentifier: changedFields.contains(.parentItemIdentifier) ? item.parentItemIdentifier.rawValue : nil,
                        contentUrl: newContentsUrl,
                        progressHandler: { progressValue in
                            progress.completedUnitCount = Int64(progressValue * 100)
                            progress.totalUnitCount = 100
                        }
                    )
                    
                    // Fetch the updated item from the server
                    let updatedApiItem = try await apiClient.getItem(identifier: response.new_identifier)
                    let updatedProviderItem = HopNetFileProviderItem(apiItem: updatedApiItem)
                    
                    logger.debug("✅ Successfully modified item content: \(response.new_identifier)")
                    completionHandler(updatedProviderItem, [], false, nil)
                } catch let error as ApiError {
                    logger.error("❌ API error during item content modification: \(error, privacy: .public)")
                    
                    let fileProviderError: Error
                    switch error {
                    case .serverError(let message) where message.contains("conflict") || message.contains("already exists"):
                        fileProviderError = NSFileProviderError(.filenameCollision)
                    case .serverError(let message) where message.contains("not found"):
                        fileProviderError = NSFileProviderError(.noSuchItem)
                    case .notFound:
                        fileProviderError = NSFileProviderError(.noSuchItem)
                    case .unauthorized:
                        fileProviderError = NSFileProviderError(.notAuthenticated)
                    case .upgradeRequired:
                        fileProviderError = NSFileProviderError(.notAuthenticated)
                    case .notReady:
                        fileProviderError = NSFileProviderError(.serverUnreachable)
                    case .serverError(let message) where message.contains("not yet implemented"):
                        fileProviderError = NSFileProviderError(.providerDomainTemporarilyUnavailable)
                    case .serverError(let message) where message.contains("too large"):
                        fileProviderError = NSFileProviderError(.insufficientQuota)
                    default:
                        fileProviderError = NSFileProviderError(.cannotSynchronize)
                    }
                    
                    completionHandler(nil, [], false, fileProviderError)
                } catch {
                    logger.error("❌ Unexpected error during item content modification: \(error, privacy: .public)")
                    logger.error("❌ Error type: \(type(of: error), privacy: .public)")
                    logger.error("❌ Error localized description: \(error.localizedDescription, privacy: .public)")
                    completionHandler(nil, [], false, NSFileProviderError(.cannotSynchronize))
                }
            }
            return progress
        }
        
        Task {
            do {
                var newFilename: String?
                var newParentItemIdentifier: String?
                
                // Check what fields changed
                if changedFields.contains(.filename) {
                    newFilename = item.filename
                    logger.debug("🔄 Filename change requested: \(item.filename)")
                }
                
                if changedFields.contains(.parentItemIdentifier) {
                    // Check if this is a move to trash (which we don't support)
                    if item.parentItemIdentifier == NSFileProviderItemIdentifier.trashContainer {
                        logger.debug("🗑️ Move to trash requested - not supported")
                        completionHandler(
                            nil, 
                            [], 
                            false,
                            NSError(domain: NSCocoaErrorDomain, code: NSFeatureUnsupportedError, userInfo: [
                                NSLocalizedDescriptionKey: "Trashing not supported",
                                NSLocalizedRecoverySuggestionErrorKey: "Use permanent deletion instead."
                            ])
                        )
                        return
                    }
                    
                    newParentItemIdentifier = item.parentItemIdentifier.rawValue
                    logger.debug("🔄 Parent change requested: \(item.parentItemIdentifier.rawValue)")
                }
                
                // If no supported changes requested, return success with no changes
                if newFilename == nil && newParentItemIdentifier == nil {
                    logger.debug("ℹ️ No supported changes requested")
                    completionHandler(item, [], false, nil)
                    return
                }
                
                // Call API to modify the item
                let response = try await apiClient.modifyItem(
                    identifier: item.itemIdentifier.rawValue,
                    filename: newFilename,
                    parentItemIdentifier: newParentItemIdentifier
                )
                
                // Fetch the updated item using the new identifier from the response
                let updatedApiItem = try await apiClient.getItem(identifier: response.new_identifier)
                let updatedItem = HopNetFileProviderItem(apiItem: updatedApiItem)
                
                logger.debug("✅ Successfully modified item: \(item.filename) -> new identifier: \(response.new_identifier)")
                
                // Check if the identifier changed (especially for folder renames)
                let identifierChanged = item.itemIdentifier.rawValue != response.new_identifier
                
                if identifierChanged {
                    logger.debug("🔄 Item identifier changed: \(item.itemIdentifier.rawValue) -> \(response.new_identifier)")
                    
                    // Signal the manager to refresh after successful modification
                    // This ensures the UI updates to remove the old identifier and show the new one
                    Task {
                        try? await Task.sleep(nanoseconds: 100_000_000) // 100ms delay
                        do {
                            // Signal both the parent container and working set for refresh
                            try await self.manager.signalEnumerator(for: updatedItem.parentItemIdentifier)
                            try await self.manager.signalEnumerator(for: .workingSet)
                            logger.debug("✅ Signaled enumerators for refresh after identifier change")
                        } catch {
                            logger.warning("⚠️ Failed to signal enumerators after identifier change: \(error)")
                        }
                    }
                }
                
                completionHandler(updatedItem, [], false, nil)
                
            } catch let error as ApiError {
                logger.error("❌ API error during item modification: \(error)")
                
                let fileProviderError: Error
                switch error {
                case .serverError(let message) where message.contains("conflict") || message.contains("already exists"):
                    fileProviderError = NSFileProviderError(.filenameCollision)
                case .serverError(let message) where message.contains("not found"):
                    fileProviderError = NSFileProviderError(.noSuchItem)
                case .unauthorized:
                    fileProviderError = NSFileProviderError(.notAuthenticated)
                case .upgradeRequired:
                    fileProviderError = NSFileProviderError(.notAuthenticated)
                case .notReady:
                    fileProviderError = NSFileProviderError(.serverUnreachable)
                case .serverError(let message) where message.contains("not yet implemented"):
                    fileProviderError = NSFileProviderError(.providerDomainTemporarilyUnavailable)
                default:
                    fileProviderError = NSFileProviderError(.cannotSynchronize)
                }
                
                completionHandler(nil, [], false, fileProviderError)
            } catch {
                logger.error("❌ Unexpected error during item modification: \(error)")
                completionHandler(nil, [], false, NSFileProviderError(.cannotSynchronize))
            }
        }
        
        return progress
    }
    
    public func deleteItem(identifier: NSFileProviderItemIdentifier, baseVersion version: NSFileProviderItemVersion, options: NSFileProviderDeleteItemOptions, request: NSFileProviderRequest, completionHandler: @escaping (Error?) -> Void) -> Progress {
        logger.debug("🎯 deleteItem requested for identifier: \(identifier.rawValue)")
        logger.debug("🎯 Delete options: recursive=\(options.contains(.recursive))")
        // Moved to logger.debug below
        
        let progress = Progress()
        progress.totalUnitCount = 100
        
        // Cannot delete root container
        if identifier == NSFileProviderItemIdentifier.rootContainer {
            let error = NSFileProviderError(.deletionRejected)
            completionHandler(error)
            return progress
        }
        
        Task {
            do {
                // Get item info to check if it's a folder
                let apiItem = try await apiClient.getItem(identifier: identifier.rawValue)
                let item = HopNetFileProviderItem(apiItem: apiItem)
                
                // Check if this is a folder and whether recursive deletion is needed
                let isFolder = item.contentType == .folder
                if isFolder && !options.contains(.recursive) {
                    // For non-recursive folder deletion, we should check if it's empty
                    // Since we don't have a direct way to check emptiness from the identifier alone,
                    // we'll let the backend handle this validation
                    logger.debug("🎯 Non-recursive folder deletion requested, backend will validate emptiness")
                }
                
                // Update progress
                progress.completedUnitCount = 20
                
                // Call API to delete the item with recursive flag
                let recursive = options.contains(.recursive)
                try await apiClient.deleteItem(identifier: identifier.rawValue, recursive: recursive)
                
                // Update progress
                progress.completedUnitCount = 80
                
                // Signal the manager to refresh after successful deletion
                // This ensures the UI updates to reflect the deletion
                Task {
                    try? await Task.sleep(nanoseconds: 100_000_000) // 100ms delay
                    do {
                        try await self.manager.signalEnumerator(for: .workingSet)
                    } catch {
                        self.logger.warning("Failed to signal working set after deletion: \(error)")
                    }
                }
                
                // Update progress to complete
                progress.completedUnitCount = 100
                
                logger.debug("✅ Successfully deleted item: \(identifier.rawValue)")
                completionHandler(nil)
                
            } catch let error as ApiError {
                logger.error("❌ API error during deletion: \(error)")
                
                // Map API errors to appropriate FileProvider errors
                let fileProviderError: Error
                switch error {
                case .notFound:
                    fileProviderError = NSFileProviderError(.noSuchItem)
                case .unauthorized:
                    fileProviderError = NSFileProviderError(.notAuthenticated)
                case .upgradeRequired:
                    fileProviderError = NSFileProviderError(
                        .notAuthenticated,
                        userInfo: [
                            NSLocalizedDescriptionKey: "HopNet Update Required",
                            NSLocalizedRecoverySuggestionErrorKey: "Update the HopNet app to continue syncing files."
                        ]
                    )
                case .notReady:
                    fileProviderError = NSFileProviderError(
                        .notAuthenticated,
                        userInfo: [
                            NSLocalizedDescriptionKey: "HopNet Setup Required",
                            NSLocalizedRecoverySuggestionErrorKey: "Please sign in to the HopNet desktop app before deleting files."
                        ]
                    )
                case .serverError(let message):
                    if message.contains("Cannot delete root") {
                        fileProviderError = NSFileProviderError(.deletionRejected)
                    } else if message.contains("Invalid identifier") {
                        fileProviderError = NSFileProviderError(.noSuchItem)
                    } else if message.contains("Folder not empty") {
                        fileProviderError = NSFileProviderError(.directoryNotEmpty)
                    } else {
                        fileProviderError = NSFileProviderError(.serverUnreachable)
                    }
                default:
                    fileProviderError = NSFileProviderError(.serverUnreachable)
                }
                
                completionHandler(fileProviderError)
                
            } catch {
                logger.error("❌ Unexpected error during deletion: \(error)")
                completionHandler(NSFileProviderError(.serverUnreachable))
            }
        }
        
        return progress
    }
    
    public func importDidFinish(completionHandler: @escaping () -> Void) {
        logger.debug("🎯 importDidFinish called")
        // Moved to logger.debug below
        completionHandler()
    }
}

// MARK: - Simple Enumerator

class HopNetEnumerator: NSObject, NSFileProviderEnumerator {
    let containerItemIdentifier: NSFileProviderItemIdentifier
    let apiClient: HopNetApiClient
    let logger = Logger(subsystem: "com.hopnet.desktop.fileprovider", category: "enumerator")
    
    init(containerItemIdentifier: NSFileProviderItemIdentifier, apiClient: HopNetApiClient) {
        self.containerItemIdentifier = containerItemIdentifier
        self.apiClient = apiClient
        super.init()
        logger.debug("🎯 HopNetEnumerator created for container: \(containerItemIdentifier.rawValue)")
        logger.debug("🎯 Root container identifier is: \(NSFileProviderItemIdentifier.rootContainer.rawValue)")
        // Moved to logger.debug below
        // Moved to logger.debug below
    }
    
    func invalidate() {
        logger.debug("🎯 HopNetEnumerator invalidated")
        // Moved to logger.debug below
    }
    
    /// Extract path from container identifier (ported from Rust version)
    private func extractPath() -> String? {
        let identifier = containerItemIdentifier.rawValue
        
        if identifier == NSFileProviderItemIdentifier.rootContainer.rawValue || identifier == "root" {
            return "/"
        } else if identifier.hasPrefix("item:") {
            // For unified item: identifiers, we can't extract path directly
            // This would require an API call to get metadata
            return nil
        }
        
        // Note: Old folder: format is no longer supported with unified identifiers
        return nil
    }
    
    func enumerateItems(for observer: NSFileProviderEnumerationObserver, startingAt page: NSFileProviderPage) {
        logger.debug("🎯 enumerateItems called for container: \(self.containerItemIdentifier.rawValue)")
        // Moved to logger.debug below
        
        let pageToken = page.rawValue.isEmpty ? nil : String(data: page.rawValue, encoding: .utf8)
        
        logger.debug("🎯 Using identifier-based enumeration: \(self.containerItemIdentifier.rawValue)")
        // Moved to logger.debug below
        
        Task {
            do {
                // Normalize container identifier - only send identifiers the backend understands
                let normalizedIdentifier: String
                if self.containerItemIdentifier == NSFileProviderItemIdentifier.rootContainer {
                    normalizedIdentifier = self.containerItemIdentifier.rawValue
                } else if self.containerItemIdentifier.rawValue.starts(with: "item:") {
                    normalizedIdentifier = self.containerItemIdentifier.rawValue
                } else {
                    // Unknown identifier types (like WorkingSet) get crushed to root
                    logger.debug("🎯 Normalizing unknown identifier '\(self.containerItemIdentifier.rawValue)' to root container")
                    // Moved to logger.debug below
                    normalizedIdentifier = NSFileProviderItemIdentifier.rootContainer.rawValue
                }
                
                let response = try await self.apiClient.enumerate(parentItemIdentifier: normalizedIdentifier, pageToken: pageToken)
                logger.debug("Successfully enumerated \(response.items.count) items")
                
                // Convert FileProviderItem to NSFileProviderItem objects
                let providerItems = response.items.map { HopNetFileProviderItem(apiItem: $0) }
                
                let nextPage = response.next_page.map { 
                    NSFileProviderPage($0.data(using: .utf8) ?? Data()) 
                }
                
                observer.didEnumerate(providerItems)
                observer.finishEnumerating(upTo: nextPage)
            } catch {
                logger.error("Enumeration failed: \(error)")
                
                // Handle specific API errors with appropriate FileProvider errors
                if let apiError = error as? ApiError {
                    switch apiError {
                    case .upgradeRequired:
                        let updateError = NSFileProviderError(
                            .notAuthenticated,
                            userInfo: [
                                NSLocalizedDescriptionKey: "HopNet Update Required",
                                NSLocalizedRecoverySuggestionErrorKey: "Update the HopNet app to continue syncing files."
                            ]
                        )
                        observer.finishEnumeratingWithError(updateError)
                        return
                    case .notReady:
                        let setupError = NSFileProviderError(
                            .notAuthenticated,
                            userInfo: [
                                NSLocalizedDescriptionKey: "HopNet Setup Required",
                                NSLocalizedRecoverySuggestionErrorKey: "Please sign in to the HopNet desktop app before accessing files."
                            ]
                        )
                        observer.finishEnumeratingWithError(setupError)
                        return
                    case .unauthorized:
                        let authError = NSFileProviderError(
                            .notAuthenticated,
                            userInfo: [
                                NSLocalizedDescriptionKey: "HopNet Authorization Required",
                                NSLocalizedRecoverySuggestionErrorKey: "Please check your authentication in the HopNet desktop app."
                            ]
                        )
                        observer.finishEnumeratingWithError(authError)
                        return
                    case .notFound:
                        let notFoundError = NSFileProviderError(
                            .noSuchItem,
                            userInfo: [
                                NSLocalizedDescriptionKey: "Folder Not Found",
                                NSLocalizedRecoverySuggestionErrorKey: "This folder may have been moved or deleted."
                            ]
                        )
                        observer.finishEnumeratingWithError(notFoundError)
                        return
                    case .network(_):
                        let networkError = NSFileProviderError(
                            .serverUnreachable,
                            userInfo: [
                                NSLocalizedDescriptionKey: "HopNet Unavailable",
                                NSLocalizedRecoverySuggestionErrorKey: "Please check that the HopNet desktop app is running."
                            ]
                        )
                        observer.finishEnumeratingWithError(networkError)
                        return
                    case .invalidUrl:
                        let appNotRunningError = NSFileProviderError(
                            .serverUnreachable,
                            userInfo: [
                                NSLocalizedDescriptionKey: "HopNet App Required",
                                NSLocalizedRecoverySuggestionErrorKey: "Please launch the HopNet desktop app to access your files."
                            ]
                        )
                        observer.finishEnumeratingWithError(appNotRunningError)
                        return
                    default:
                        break
                    }
                }
                
                // Default error handling for other cases
                let defaultError = NSFileProviderError(
                    .serverUnreachable,
                    userInfo: [
                        NSLocalizedDescriptionKey: "HopNet Connection Error",
                        NSLocalizedRecoverySuggestionErrorKey: "Please ensure the HopNet desktop app is running and try again."
                    ]
                )
                observer.finishEnumeratingWithError(defaultError)
            }
        }
    }
    
    func enumerateChanges(for observer: NSFileProviderChangeObserver, from anchor: NSFileProviderSyncAnchor) {
        logger.debug("🎯 enumerateChanges called")
        // Moved to logger.debug below
        
        let path = extractPath() ?? "/"
        
        // Parse consensus height from sync anchor (default to 0 for first run)
        let sinceHeight = parseConsensusHeight(from: anchor)
        logger.debug("🎯 Enumerating changes since consensus height: \(sinceHeight)")
        
        Task {
            do {
                // Get changes since the given consensus height
                let response = try await apiClient.getChanges(parentPath: path, sinceHeight: sinceHeight)
                
                var updatedItems: [NSFileProviderItem] = []
                
                // Convert response items to FileProvider items
                for item in response.items {
                    let providerItem = HopNetFileProviderItem(apiItem: item)
                    updatedItems.append(providerItem)
                }
                
                // Report updated/added items to the system
                if !updatedItems.isEmpty {
                    observer.didUpdate(updatedItems)
                }
                
                // Report deleted items to the system
                if !response.deleted_identifiers.isEmpty {
                    let deletedIdentifiers = response.deleted_identifiers.map { NSFileProviderItemIdentifier($0) }
                    observer.didDeleteItems(withIdentifiers: deletedIdentifiers)
                }
                
                // Create new sync anchor with current consensus height
                let newAnchor = createSyncAnchor(from: response.current_consensus_height)
                observer.finishEnumeratingChanges(upTo: newAnchor, moreComing: false)
                
                logger.debug("🎯 Successfully enumerated \(updatedItems.count) updates and \(response.deleted_identifiers.count) deletions, new consensus height: \(response.current_consensus_height)")
                
            } catch {
                logger.error("Failed to enumerate changes: \(error)")
                observer.finishEnumeratingWithError(NSFileProviderError(.serverUnreachable))
            }
        }
    }
    
    /// Parse consensus height from sync anchor data
    private func parseConsensusHeight(from anchor: NSFileProviderSyncAnchor) -> UInt64 {
        let data = anchor.rawValue
        guard let heightString = String(data: data, encoding: .utf8),
              let height = UInt64(heightString) else {
            // Return 0 for first run or invalid anchor
            logger.debug("🎯 Using default consensus height 0 (first run or invalid anchor)")
            return 0
        }
        return height
    }
    
    /// Create sync anchor from consensus height
    private func createSyncAnchor(from height: UInt64) -> NSFileProviderSyncAnchor {
        let heightString = String(height)
        let heightData = heightString.data(using: .utf8)!
        return NSFileProviderSyncAnchor(heightData)
    }
    
    func currentSyncAnchor(completionHandler: @escaping (NSFileProviderSyncAnchor?) -> Void) {
        logger.debug("🎯 currentSyncAnchor called")
        // Moved to logger.debug below
        
        let path = extractPath() ?? "/"
        
        Task {
            do {
                // Get current consensus height from backend by doing an enumerate call
                let response = try await apiClient.enumerate(parentPath: path, pageToken: nil)
                
                // Use consensus height as sync anchor
                let heightString = String(response.current_consensus_height)
                let heightData = heightString.data(using: .utf8)!
                let anchor = NSFileProviderSyncAnchor(heightData)
                
                logger.debug("🎯 Created sync anchor with consensus height: \(response.current_consensus_height)")
                completionHandler(anchor)
                
            } catch {
                logger.error("Failed to get current sync anchor: \(error)")
                completionHandler(nil)
            }
        }
    }
}

// MARK: - Extensions

extension Data {
    init?(hexString: String) {
        let length = hexString.count
        guard length % 2 == 0 else { return nil }
        
        var data = Data()
        data.reserveCapacity(length / 2)
        
        var index = hexString.startIndex
        while index < hexString.endIndex {
            let nextIndex = hexString.index(index, offsetBy: 2)
            let byteString = String(hexString[index..<nextIndex])
            guard let byte = UInt8(byteString, radix: 16) else { return nil }
            data.append(byte)
            index = nextIndex
        }
        
        self = data
    }
}