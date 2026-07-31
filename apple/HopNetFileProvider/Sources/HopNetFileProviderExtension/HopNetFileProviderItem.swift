/*
HopNet FileProvider Item
Minimal NSFileProviderItem implementation to get enumeration working
*/

import FileProvider
import Foundation
import UniformTypeIdentifiers

/// Minimal wrapper to convert FileProviderItem to NSFileProviderItem
public class HopNetFileProviderItem: NSObject, NSFileProviderItem {
    private let apiItem: FileProviderItem
    private let _isDownloaded: Bool
    private let _documentSize: NSNumber?
    
    public init(apiItem: FileProviderItem, isDownloaded: Bool = false, documentSize: NSNumber? = nil) {
        self.apiItem = apiItem
        self._isDownloaded = isDownloaded
        // If documentSize not provided, try to parse from apiItem.file_size
        if let documentSize = documentSize {
            self._documentSize = documentSize
        } else if let fileSizeStr = apiItem.file_size, let fileSize = UInt64(fileSizeStr) {
            self._documentSize = NSNumber(value: fileSize)
        } else {
            self._documentSize = nil
        }
        super.init()
    }
    
    public func asDownloaded(withSize size: NSNumber? = nil) -> HopNetFileProviderItem {
        return HopNetFileProviderItem(apiItem: self.apiItem, isDownloaded: true, documentSize: size ?? self._documentSize)
    }
    
    // MARK: - Required NSFileProviderItem Properties
    
    public var itemIdentifier: NSFileProviderItemIdentifier {
        return NSFileProviderItemIdentifier(apiItem.identifier)
    }
    
    public var parentItemIdentifier: NSFileProviderItemIdentifier {
        return NSFileProviderItemIdentifier(apiItem.parent_item_identifier)
    }
    
    public var filename: String {
        return apiItem.filename
    }
    
    public var contentType: UTType {
        return apiItem.item_type == .folder ? .folder : .data
    }
    
    public var capabilities: NSFileProviderItemCapabilities {
        switch apiItem.item_type {
        case .folder:
            return [.allowsReading, .allowsContentEnumerating, .allowsDeleting, .allowsAddingSubItems, .allowsWriting, .allowsRenaming, .allowsReparenting]
        case .file:
            return [.allowsReading, .allowsDeleting, .allowsWriting, .allowsRenaming, .allowsReparenting]
        }
    }
    
    // MARK: - Optional Properties (minimal defaults)
    
    public var documentSize: NSNumber? {
        if apiItem.item_type == .folder {
            return NSNumber(value: 0)
        } else {
            return _documentSize  // Return actual size if we have it, nil if unknown
        }
    }
    
    // ItemVersion is required - use consensus height from backend for proper versioning
    public var itemVersion: NSFileProviderItemVersion {
        let versionData: Data
        
        if let modHeight = apiItem.modification_height {
            // Convert the consensus height to Data safely
            var height = modHeight
            versionData = Data(bytes: &height, count: MemoryLayout<UInt64>.size)
        } else {
            // Generate timestamp-based version when modification height unavailable
            let timestamp = Date().timeIntervalSince1970
            var timestampInt = Int64(timestamp * 1000) // Use milliseconds for precision
            versionData = Data(bytes: &timestampInt, count: MemoryLayout<Int64>.size)
        }
        
        return NSFileProviderItemVersion(contentVersion: versionData, metadataVersion: versionData)
    }
    
    public var isUploaded: Bool { return true }
    public var isUploading: Bool { return false }
    public var uploadingError: Error? { return nil }
    public var isDownloaded: Bool { return _isDownloaded }
    public var isDownloading: Bool { return false }
    public var downloadingError: Error? { return nil }
    public var isMostRecentVersionDownloaded: Bool { return _isDownloaded }
    
    // MARK: - Enhanced Metadata (Phase 3)
    
    public var creationDate: Date? {
        return parseISODate(apiItem.creation_date)
    }
    
    public var contentModificationDate: Date? {
        return parseISODate(apiItem.content_modification_date)
    }
    
    // Helper function to parse ISO 8601 date strings
    private func parseISODate(_ dateString: String?) -> Date? {
        guard let dateString = dateString else { return nil }
        
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        
        // Try with fractional seconds first
        if let date = formatter.date(from: dateString) {
            return date
        }
        
        // Fallback to without fractional seconds
        formatter.formatOptions = [.withInternetDateTime]
        return formatter.date(from: dateString)
    }
}