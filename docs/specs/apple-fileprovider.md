# RFC-009: Apple FileProvider Integration

## Abstract

This RFC specifies the integration of Apple's FileProvider framework with HopNet, enabling native macOS Finder and iOS Files app access to distributed HopNet storage. The implementation provides a FileProvider extension that communicates with the main HopNet process via scoped HTTP APIs, maintaining security and consensus integrity while delivering seamless native OS integration with read, write, and sync capabilities.

## Motivation

### Why FileProvider Integration?

1. **Native User Experience**: Users expect files to appear in Finder/Files app alongside local storage
2. **Workflow Integration**: Enable drag-and-drop, Quick Look, and standard file operations
3. **Application Compatibility**: Allow any macOS/iOS app to open HopNet files directly
4. **Enterprise Adoption**: File servers that don't appear in Finder are non-starters for most organizations

### Design Goals

- Maintain HopNet's consensus-based architecture (no direct database writes from extension)
- Preserve file identity across renames and moves using stable identifiers
- Support future thin client model for iOS devices
- Minimize implementation complexity for MVP while enabling future enhancements
- Ensure secure separation between FileProvider extension and main application

## Architecture

### Process Isolation Model

Apple's FileProvider framework mandates process isolation for security and reliability:

```
System Process Tree:
├── Finder.app / Files.app (file access requests)
├── fileproviderd (Apple's coordination daemon)
│   └── HopNetFileProvider.appex (extension process)
└── HopNet.app (main application with consensus)
```

Key implications:
- FileProvider extension runs independently of main HopNet app
- Extension can operate when main app is closed (essential for Finder integration)
- No shared memory or direct function calls between processes
- Perfect foundation for future iOS thin client architecture

### Communication Architecture

```
┌─────────────────┐    HTTP API     ┌──────────────────┐
│ FileProvider    │◄─────────────►│ HopNet Main      │
│ Extension       │                 │ Process          │
│ (Swift Binary)  │                 │ (Consensus Node) │
└─────────────────┘                 └──────────────────┘
        │                                    │
        ▼                                    ▼
┌─────────────────┐                 ┌──────────────────┐
│ macOS Finder    │                 │ Fragment Storage │
│ Files.app       │                 │ Network Ops      │
└─────────────────┘                 └──────────────────┘
```

Benefits:
- All consensus operations remain in main process
- FileProvider acts as a requesting layer
- API design enables future remote endpoint support for iOS thin clients
- Clean separation of concerns

## Implementation Strategy

### Technology Stack

**FileProvider Extension:**
- **Language**: Swift (Native implementation for better Apple framework integration)
- **Communication**: URLSession for HTTP requests
- **Authentication**: Device token stored in macOS Keychain (see [RFC-012](device-token-sessions.md))
- **Bundle**: Swift Package Manager project compiled into `.appex` bundle

**Main HopNet Application:**
- **API Routes**: `/integrations/fileprovider/*` endpoints (implemented)
- **Authentication**: Device token authentication via `device_token_auth_middleware` (see [RFC-012](device-token-sessions.md))
- **File Assembly**: Fragment assembly integrated with download endpoint

### Stable File Identity (Implemented)

Leverages HopNet's existing database schema for stable identifiers:

```swift
// File identification strategy (implemented)
switch itemType {
case .file:
    // Files: Use stable data_block_id (persists across renames)
    itemIdentifier = "file:\(dataBlockId)"
case .folder:
    // Folders: Use hex-encoded path
    itemIdentifier = "folder:\(hexEncodedPath)"
case .rootContainer:
    itemIdentifier = NSFileProviderItemIdentifier.rootContainer
}
```

This approach:
- ✅ Files maintain identity when renamed (data_block_id unchanged)
- ✅ No additional database schema changes required
- ✅ Works with existing HopNet file operations
- ✅ Clean separation between files and folders
- ✅ Folder identifiers use encrypted paths for consistency with database queries

### API Design (Implemented)

**Implemented Endpoints:**
```rust
// File/folder metadata ✅
GET /integrations/fileprovider/item?identifier={identifier}
Response: FileProviderItem { identifier, filename, parent, type, file_size, creation_date, content_modification_date, modification_height }

// Directory enumeration ✅ (includes timestamps)
GET /integrations/fileprovider/enumerate?parent_path={path}&page={token}
Response: { items: [FileProviderItem], next_page: Option<String>, current_consensus_height }

// File download (assembled from fragments) ✅
GET /integrations/fileprovider/download?identifier={identifier}
Response: Binary stream of assembled file content

// Health check ✅
GET /integrations/fileprovider/health
Response: { status: "ready" | "not_ready" }

// Incremental sync ✅ (includes timestamps)
GET /integrations/fileprovider/changes?parent_path={path}&since_height={height}
Response: { items: [FileProviderItem], deleted_identifiers: [String], current_consensus_height }

// Delete operation ✅
DELETE /integrations/fileprovider/delete
Body: { identifier: String, recursive: bool }
Response: 200 OK or error
```

// Create item ✅ 
POST /integrations/fileprovider/create
Body: multipart/form-data with path field and optional file field

// Modify item ✅ (Phase 4a complete - metadata only, Phase 4b - content)
PUT /integrations/fileprovider/modify
Body: { identifier, new_filename?, new_parent?, content? }
Response: 200 OK or error

**Authentication (Implemented — migrated to device tokens in [RFC-012](device-token-sessions.md)):**
- Device token stored in macOS Keychain ✅
- Extension reads token from keychain at initialization ✅
- Authenticated via `device_token_auth_middleware` with session bootstrap ✅
- Restricted to `/integrations/fileprovider/*` routes only ✅

## NSFileProviderReplicatedExtension Implementation

### Core Protocol Methods (Swift Implementation)

**Current Implementation Status:**

```swift
// ✅ IMPLEMENTED - Core read operations
class HopNetFileProviderExtension: NSFileProviderReplicatedExtension {
    
    // ✅ Fetch metadata for specific file/folder
    func item(for identifier: NSFileProviderItemIdentifier) -> NSFileProviderItem {
        let item = apiClient.getItem(identifier: identifier.rawValue)
        return HopNetFileProviderItem(apiItem: item)
    }
    
    // ✅ List contents of a folder with pagination
    func enumerator(for container: NSFileProviderItemIdentifier) -> NSFileProviderEnumerator {
        return HopNetEnumerator(containerItemIdentifier: container, apiClient: apiClient)
    }
    
    // ✅ Download file content with progress tracking
    func fetchContents(for identifier: NSFileProviderItemIdentifier) -> Progress {
        let progress = Progress()
        Task {
            let tempUrl = try await apiClient.downloadFile(identifier: identifier.rawValue)
            // Move to FileProvider temp directory and return
        }
        return progress
    }
    
    // ✅ Delete items with recursive support
    func deleteItem(identifier: NSFileProviderItemIdentifier) -> Progress {
        let progress = Progress()
        Task {
            try await apiClient.deleteItem(identifier: identifier.rawValue, recursive: options.contains(.recursive))
        }
        return progress
    }
    
    // ✅ Create files and folders with multipart upload
    func createItem(basedOn itemTemplate: NSFileProviderItem) -> Progress {
        let progress = Progress()
        Task {
            let parentPath = try await getPathFromIdentifier(itemTemplate.parentItemIdentifier)
            try await apiClient.createItem(parentPath: parentPath, filename: itemTemplate.filename, fileUrl: url)
            // Fetch actual item from backend to get correct encrypted identifiers
        }
        return progress
    }
    
    // ✅ IMPLEMENTED - Metadata modifications (rename/move)  
    func modifyItem(_ item: NSFileProviderItem) -> Progress {
        let progress = Progress()
        Task {
            // Phase 4a: Handle rename and move operations
            if item.parentItemIdentifier == NSFileProviderItemIdentifier.trashContainer {
                // Reject trashing with NSFeatureUnsupportedError
                completionHandler(nil, [], false, NSError(...))
                return
            }
            
            let newPath = try await getPathFromIdentifier(item.parentItemIdentifier)
            try await apiClient.modifyItem(identifier: item.itemIdentifier.rawValue, 
                                         newPath: newPath, newFilename: item.filename)
            
            // Signal enumerator changes and return updated item
        }
        return progress
    }
}
```

**NSFileProviderItem Implementation (Current):**
```swift
class HopNetFileProviderItem: NSFileProviderItem {
    // ✅ Required properties
    var itemIdentifier: NSFileProviderItemIdentifier  // "file:uuid" or "folder:hex(encrypted_path)"
    var filename: String                              // Decrypted filename
    var parentItemIdentifier: NSFileProviderItemIdentifier
    var contentType: UTType                          // .folder or .data
    var capabilities: NSFileProviderItemCapabilities // Reading, deleting, writing
    var documentSize: NSNumber?                      // File size in bytes
    var itemVersion: NSFileProviderItemVersion       // Consensus height-based version tracking
    
    // ✅ Download state tracking
    var isDownloaded: Bool
    var isMostRecentVersionDownloaded: Bool
    
    // ✅ Enhanced metadata properties (Phase 3)
    var creationDate: Date?               // From UUIDv7 timestamp extraction
    var contentModificationDate: Date?    // From modified_at or fallback to creation_date
    
    // ❌ Missing properties (Future phases)
    // var lastUsedDate: Date?           // For working set
    // var childItemCount: NSNumber?     // For folders
    // var typeIdentifier: String        // Specific file types
    // var favoriteRank: NSNumber?       // For starred items
}
```

### Consensus Height-Based Versioning (COMPLETED ✅)

**Problem**: FileProvider requires proper versioning via `NSFileProviderItemVersion` for change detection and sync operations. Initial implementation used static identifiers that never changed, breaking incremental sync.

**Solution**: Implement consensus height-based versioning that leverages HopNet's modification tracking system.

#### Implementation Details:

**Backend Enhancement**:
```rust
// Added modification_height field to all FileProvider database structures
pub struct FileProviderItemData {
    // ... existing fields ...
    pub modification_height: Option<i32>, // Consensus height when item was last modified
}
```

All FileProvider database queries now JOIN with `modification_log` table to provide the consensus height when each item was last modified:
```sql
LEFT JOIN (
    SELECT inode_id, MAX(modified_at_height) as modified_at_height
    FROM modification_log WHERE owner_id = ?
    GROUP BY inode_id
) ml ON i.id = ml.inode_id
```

**Swift Implementation**:
```swift
// HopNetFileProviderItem.swift - Enhanced itemVersion property
public var itemVersion: NSFileProviderItemVersion {
    let versionData: Data
    
    if let modHeight = apiItem.modification_height {
        // Convert consensus height to Data safely
        var height = modHeight
        versionData = Data(bytes: &height, count: MemoryLayout<Int32>.size)
    } else {
        // Timestamp fallback for items without modification height
        let timestamp = Date().timeIntervalSince1970
        var timestampInt = Int64(timestamp * 1000) // milliseconds for precision
        versionData = Data(bytes: &timestampInt, count: MemoryLayout<Int64>.size)
    }
    
    return NSFileProviderItemVersion(contentVersion: versionData, metadataVersion: versionData)
}
```

**Key Benefits**:
- ✅ **Monotonic Versioning**: Consensus heights always increase, ensuring proper change detection
- ✅ **Consistency**: All instances of HopNet report identical versions for the same items
- ✅ **Efficient Sync**: FileProvider can use consensus height for precise incremental enumeration
- ✅ **Parent Updates**: When child items are modified, parent folder versions automatically update
- ✅ **Safe Fallback**: Timestamp-based versioning when consensus height unavailable (though this should be rare)

**Testing Integration**:
Comprehensive test suite validates version behavior:
- Version consistency across all created items
- Parent version changes when children are modified
- No timestamp fallback in normal operations (all items should have consensus height versions)
- Version data format validation (Int32 for consensus heights vs Int64 for timestamps)

### File Assembly Integration

Leverage existing HopNet fragment assembly:

```rust
// In main HopNet process
async fn download_fileprovider_file(identifier: String) -> Result<impl Stream<Item = Bytes>> {
    let data_block_id = identifier.strip_prefix("file:")?;
    
    // Use existing fragment discovery and assembly
    let fragments = discover_fragments(data_block_id).await?;
    let assembled_stream = assemble_fragments_stream(fragments).await?;
    
    Ok(assembled_stream)
}
```

## Error Handling

### Main HopNet Process Unavailable

```rust
// Graceful degradation when HopNet is not running
impl HopNetFileProvider {
    async fn handle_api_error(&self, error: reqwest::Error) -> NSError {
        if error.is_connect() {
            NSError::new(
                NSFileProviderErrorDomain,
                NSFileProviderErrorServerUnreachable,
                "HopNet is not running. Please launch HopNet to access your files."
            )
        } else {
            NSError::new(
                NSFileProviderErrorDomain, 
                NSFileProviderErrorSyncAnchorExpired,
                "Unable to communicate with HopNet"
            )
        }
    }
}
```

### Progress Reporting

```rust
// Fragment-level progress (no schema changes needed)
fn fetch_contents_with_progress(request: NSFileProviderRequest) {
    let progress = request.progress();
    
    // During fragment assembly
    for (completed, fragment) in fragments.enumerate() {
        fetch_fragment(fragment)?;
        progress.completed_unit_count = completed as i64;
        progress.total_unit_count = fragments.len() as i64;
    }
}
```

## Domain Registration and User Experience

### Domain Configuration

```rust
// FileProvider domain represents HopNet in Finder sidebar
let domain = NSFileProviderDomain::new(
    "com.hopnet.fileprovider",
    "HopNet"  // Display name in Finder
);

// Registration options:
// MVP: Manual button in HopNet settings
// Future: Automatic after node setup completion
```

### User Interaction Flow

1. **Initial Setup**: User clicks "Enable Finder Integration" in HopNet settings
2. **System Prompt**: macOS asks permission to add FileProvider
3. **Finder Integration**: "HopNet" appears in Finder sidebar
4. **File Access**: Users browse/open files through standard Finder operations
5. **Background Operation**: FileProvider continues working when HopNet app is closed

## Implementation Phases

### Phase 1: Read Operations (COMPLETED ✅)
- [x] Core FileProvider extension in Swift
- [x] HTTP API endpoints in main HopNet process
- [x] Scoped authentication via device tokens (migrated from API keys in RFC-012)
- [x] File enumeration with pagination
- [x] File metadata retrieval
- [x] File download with fragment assembly
- [x] Delete operations with recursive support
- [x] Incremental sync with consensus height tracking
- [x] Basic error handling and API status checks

### Phase 2: Create Operations (COMPLETED ✅)
- [x] Implement `createItem()` method
  - [x] Create new files with content upload via multipart form data
  - [x] Create new folders using path-based approach
  - [x] Handle file upload via consensus (wraps existing post_files endpoint)
- [x] Add POST `/integrations/fileprovider/create` endpoint
- [x] Fragment creation and distribution for new files (reuses existing upload logic)
- [x] Fix folder identifier consistency (use encrypted paths from backend)
- [x] Add .allowsWriting capability to enable file/folder creation
- [ ] Progress reporting during upload (basic implementation)

### Phase 3: Enhanced Metadata (COMPLETED ✅)
- [x] Add creation and modification dates from database
  - [x] Extract creation dates from UUIDv7 timestamps using `uuid_extract_timestamp()`
  - [x] Use `modified_at` from data_blocks table with fallback to creation date
  - [x] Fixed CustomDateTime deserialization to handle DuckDB native timestamps
  - [x] Added ISO 8601 timestamp formatting for Swift Date parsing
  - [x] Resolved NULL handling to prevent epoch-based timestamps
- [ ] Add last used date tracking for working set
- [ ] Add child item count for folders  
- [ ] Improve type identifier detection based on file extensions
- [ ] Add favorite rank support for starred items
- [ ] Enhance contentType with specific UTTypes

### Phase 4a: Metadata Modification (COMPLETED ✅)
- [x] Implement `modifyItem()` method for metadata-only changes
  - [x] Rename files and folders with proper identifier handling
  - [x] Move items between folders with path updates
  - [x] **NEW**: Unified modification log for comprehensive change tracking
- [x] Add PUT `/integrations/fileprovider/modify` endpoint
- [x] Authentication header fix (Bearer token format)
- [x] Trash detection with NSFeatureUnsupportedError
- [x] **NEW**: Replace deletion_log with modification_log for all file system changes
- [x] **NEW**: Efficient single-query change enumeration using LEFT JOIN pattern

#### Unified Change Tracking Implementation:

**Problem Solved**: Previous system only tracked content changes via `placement_height`, missing file/folder moves, renames, and metadata changes without content updates.

**Solution**: Comprehensive `modification_log` table tracking all file system operations:
```sql
CREATE TABLE modification_log (
    inode_id           UUID NOT NULL,
    owner_id           INTEGER NOT NULL,
    modified_at_height INTEGER NOT NULL,
    PRIMARY KEY (inode_id, modified_at_height)
);
```

**Key Improvements**:
- All consensus handlers (insert, modify, delete) log changes to modification_log
- Single efficient query determines both existing items and deleted items
- Deleted items properly appear in `deleted_identifiers` array
- No longer depends on complex placement_height logic for change detection

### Phase 4b: Content Modification (COMPLETED ✅)

**Architecture Decision**: Create new data blocks for modified content while maintaining stable file identifiers.

#### Core Changes:

1. **Database Schema Evolution**:
   - [x] ~~Add `id` column (UUIDv7) to `inodes` table~~ (already exists, leveraged existing schema)
   - [x] **COMPLETED**: Replace `deletion_log` with `modification_log` for unified change tracking
   - [x] **COMPLETED**: Track all file operations (create, modify, move, delete) in single table
   - [x] **COMPLETED**: UUIDv7 timestamps provide intrinsic creation/modification dates

2. **Identifier Strategy**:
   - [x] **COMPLETED**: Unified `item:{inode_id}` identifiers for files and folders
   - [x] **COMPLETED**: Inode ID remains stable across renames, moves, and content updates
   - [ ] Leverage UUIDv7 for intrinsic timestamp information:
     - `inode.id` timestamp = creation time (for both files and folders)
     - `data_block.id` timestamp = modification time (files only)

3. **Content Update Flow**:
   - [x] **COMPLETED**: `modifyItem()` HTTP endpoint handles multipart file uploads
   - [x] **COMPLETED**: Multipart upload processing with Reed-Solomon encoding
   - [x] **COMPLETED**: Create new data_block with new fragments (full file replacement)  
   - [x] **COMPLETED**: Update inode's `data_id` to point to new data_block via consensus
   - [x] **COMPLETED**: Original data_block becomes orphaned (cleaned up by maintenance)

4. **Consensus Operations**:
   - [x] **COMPLETED**: `modify_item` database function accepts:
     - `new_data_block_id: Option<CustomUUID>`
     - `new_data_record: Option<DataRecord>`
   - [x] **COMPLETED**: Atomic inode update in transaction handler
   - [x] **COMPLETED**: Last-write-wins semantics via consensus transaction ordering

5. **API Endpoints**:
   - [x] **COMPLETED**: PUT `/integrations/fileprovider/modify` accepts multipart content
   - [x] **COMPLETED**: Reuses existing file processing pipeline from creation logic
   - [x] **COMPLETED**: Returns updated metadata with new modification timestamp

#### Design Rationale:

- **Why new data blocks**: Preserves sharing semantics where multiple users can reference same content
- **Why full file replacement**: Reed-Solomon encoding requires complete re-encoding anyway
- **Why inode.id**: Provides stable identity that survives all file operations
- **Future optimization**: Delta encoding can be added later without changing architecture

#### Testing Considerations:
- Large file modifications (up to 5GB limit)
- Rapid successive updates to same file  
- Concurrent modifications from multiple FileProvider instances
- Combined operations (rename + content update)

#### Recursive Folder Modification Dates (COMPLETED ✅)
**Enhancement**: Folders now display the most recent modification date from any descendant (files or subfolders).

**Implementation**: Enhanced all FileProvider database queries to compute recursive modification dates:
```sql
CASE 
    WHEN i.data_id IS NOT NULL THEN uuid_extract_timestamp(i.data_id)  -- Files
    WHEN i.type = 'folder' THEN (
        SELECT MAX(uuid_extract_timestamp(COALESCE(child.data_id, child.id)))
        FROM inodes child
        WHERE child.owner_id = i.owner_id
          AND child.path LIKE i.path || '/%'
    )  -- Folders: most recent child modification
    ELSE NULL
END as content_modification_date
```

**Benefits**:
- Intuitive folder timestamps matching standard filesystem behavior
- FileProvider automatically inherits this for native macOS integration
- No schema changes required

#### Parent Folder Change Inclusion (COMPLETED ✅)
**Enhancement**: Change queries now include ALL ancestor folders when children are modified.

**Completed Implementation**: Full ancestor folder modification logging with comprehensive path tracking:
- Enhanced `log_modification()` to accept `old_path` and `new_path` parameters
- Added `get_all_ancestor_folders()` helper using efficient single-query path matching (`? LIKE path || '/%'`)
- Automatically logs ALL ancestor folders for create, delete, move, and modify operations
- Uses existing `old_parent_id` column for tracking parent relationships at modification time

**Key Features**:
- **File creation** (`/a/b/c/file.txt`): Logs file + `/a/b/c` + `/a/b` + `/a`
- **File deletion** (`/x/y/file.txt`): Logs file + `/x/y` + `/x` 
- **File moves** (`/a/b/file.txt` → `/x/y/file.txt`): Logs file + old ancestors (`/a/b`, `/a`) + new ancestors (`/x/y`, `/x`)
- **Shared ancestors**: Handled gracefully with `INSERT OR IGNORE` for duplicate entries

**Implementation**:
```rust
/// Enhanced log_modification with automatic ancestor logging
pub fn log_modification(
    tx: &Transaction,
    inode_id: CustomUUID,
    owner_id: i32,
    old_parent_id: Option<CustomUUID>,
    old_path: Option<&str>,  // Path BEFORE modification
    new_path: Option<&str>,  // Path AFTER modification  
    modification_height: i32,
) -> Result<(), DatabaseError>

/// Efficient single-query ancestor discovery
fn get_all_ancestor_folders(tx: &Transaction, path: &str, owner_id: i32) -> Result<Vec<CustomUUID>, DatabaseError> {
    // SELECT id FROM inodes WHERE owner_id = ? AND type = 'folder' AND ? LIKE path || '/%'
}
```

**Benefits**:
- ✅ Guarantees ALL ancestor folders appear for ANY child operation (including deletes/moves)
- ✅ Complete ancestor chain tracking for deep hierarchies (e.g., `/grandparent/parent/child.txt` logs all three levels)
- ✅ Root enumeration automatically shows all affected ancestor folders without query changes
- ✅ Single efficient query per path using SQL pattern matching
- ✅ No schema changes required - leverages existing `modification_log` structure

### Phase 5: Advanced Features
- [ ] Working set enumeration (recent files)
- [ ] Thumbnail generation and caching
- [ ] File eviction support (`evictItem()`)
- [ ] Materialized state management
- [ ] Conflict resolution UI
- [ ] File coordination improvements

### Phase 6: iOS Thin Client Foundation
- [ ] Remote endpoint configuration
- [ ] Enhanced authentication for remote access
- [ ] iOS Files app compatibility testing
- [ ] Documentation for thin client deployment

## Security Considerations

### Authentication Security
- FileProvider extension never has access to consensus private keys
- Device token authentication with per-user session bootstrap (see [RFC-012](device-token-sessions.md))
- Device token stored in secure macOS Keychain
- Main HopNet process validates all operations through consensus

### Process Isolation Benefits
- Extension crash cannot affect main HopNet consensus operations
- Malicious extension cannot perform admin operations
- Clean separation between UI layer and consensus layer

### Future Remote Access
- Foundation for secure iOS thin client architecture
- Remote endpoint authentication can use OAuth2/mTLS

## Compatibility Notes

- **macOS 13.0+**: Required for NSFileProviderReplicatedExtension
- **iOS 16.0+**: When iOS support is added
- **Xcode Project**: Extension bundle must be signed with FileProvider entitlement
- **Rust Toolchain**: Uses objc2 crate ecosystem for Objective-C bridging

## Outstanding Considerations

### Consensus Timing Race Condition
On non-leader nodes, FileProvider operations may experience a brief inconsistency window where:
1. API returns 200 OK immediately after forwarding transaction to leader
2. FileProvider queries for changes before local consensus processing completes
3. Changes appear missing until the Lock phase QC arrives and triggers `signal_fileprovider_refresh()`

**Current Mitigation**: The existing `signal_fileprovider_refresh()` mechanism provides eventual consistency by notifying FileProvider to re-enumerate after transaction processing completes.

**Future Enhancement**: A subscription mechanism tied to block commitment (tolerant of both leader and follower scenarios) could ensure stronger consistency between transactions marked as accepted by FileProvider and those actually committed by the backend.

## Future Enhancements

### Multi-Device Sync
```rust
// iOS device configuration
FileProviderConfig {
    endpoint: "https://home-server.local:8080",  // Remote HopNet node
    auth_method: AuthMethod::OAuth2,
    cache_policy: CachePolicy::MetadataOnly,
}
```

### Advanced Features
- Real-time sync notifications
- Thumbnail generation and caching
- Spotlight search integration
- Quick Actions and context menus
- Collaborative editing support

### Phase 5: Streaming File Upload Support
- **Current Limitation**: 5GB body limit on FileProvider routes for large file uploads
- **Enhancement**: Implement streaming upload support to eliminate arbitrary file size limits
- **Benefits**: Enable upload of files larger than available RAM, better memory efficiency
- **Implementation**: Replace multipart body parsing with streaming chunk processing

## Conclusion

This FileProvider integration provides HopNet with native macOS/iOS file system integration while maintaining the security and architectural integrity of the consensus-based system. The HTTP API approach creates a clean separation of concerns and establishes the foundation for future thin client deployments, enabling HopNet to scale from personal use to enterprise environments with familiar, native file access patterns.