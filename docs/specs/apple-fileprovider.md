# RFC-009: Apple FileProvider Integration

## Abstract

This RFC specifies the integration of Apple's FileProvider framework with HopNet, enabling native macOS Finder and iOS Files app access to distributed HopNet storage. The implementation provides a read-only FileProvider extension that communicates with the main HopNet process via scoped HTTP APIs, maintaining security and consensus integrity while delivering seamless native OS integration.

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
│ (Rust Binary)   │                 │ (Consensus Node) │
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
- **Language**: Rust with `objc2-file-provider` crate
- **Communication**: HTTP client using `reqwest`
- **Authentication**: Scoped API key for FileProvider endpoints only
- **Binary**: Separate Rust binary compiled into `.appex` bundle

**Main HopNet Application:**
- **New API Routes**: `/integrations/fileprovider/*` endpoints
- **Authentication**: Generate FileProvider-specific API key at startup
- **File Assembly**: Extend existing fragment assembly for FileProvider requests

### Stable File Identity

Leverage HopNet's existing database schema for stable identifiers:

```rust
// File identification strategy
match inode.data_id {
    Some(data_block_id) => {
        // Files: Use stable data_block_id (persists across renames)
        itemIdentifier = format!("file:{}", data_block_id)
    }
    None => {
        // Folders: Use base64-encoded encrypted path
        itemIdentifier = format!("folder:{}", base64_encode(encrypted_path))
    }
}
```

This approach:
- ✅ Files maintain identity when renamed (data_block_id unchanged)
- ✅ No additional database schema changes required
- ✅ Works with existing HopNet file operations
- ✅ Clean separation between files and folders

### API Design

**Core Endpoints:**
```rust
// File/folder metadata
GET /integrations/fileprovider/item/{identifier}
Response: FileProviderItem { identifier, filename, parent, type, ... }

// Directory enumeration
GET /integrations/fileprovider/enumerate?parent={identifier}&page={token}
Response: { items: [FileProviderItem], next_page: Option<String> }

// File download (assembled from fragments)
GET /integrations/fileprovider/download/{identifier}
Response: Binary stream of assembled file content

// Health check
GET /integrations/fileprovider/health
Response: { status: "ready" | "not_ready" }
```

**Authentication:**
- FileProvider-scoped API key generated at HopNet startup
- Stored in macOS Keychain for FileProvider extension access
- Restricted to `/integrations/fileprovider/*` routes only (prevents privilege escalation)

## NSFileProviderReplicatedExtension Implementation

### Core Protocol Methods

**MVP Implementation (Read-Only):**

```rust
impl NSFileProviderReplicatedExtension for HopNetFileProvider {
    // Fetch metadata for specific file/folder
    fn item(identifier: NSFileProviderItemIdentifier) -> Result<NSFileProviderItem> {
        let response: FileProviderItem = self.client
            .get(format!("{}/api/fileprovider/item/{}", self.base_url, identifier))
            .bearer_auth(&self.api_key)
            .send()?
            .json()?;
        
        Ok(HopNetItem::from(response))
    }
    
    // List contents of a folder
    fn enumerator(container: NSFileProviderItemIdentifier) -> NSFileProviderEnumerator {
        HopNetEnumerator::new(container, self.client.clone())
    }
    
    // Download file content
    fn fetch_contents(
        identifier: NSFileProviderItemIdentifier,
        version: NSFileProviderItemVersion,
        request: NSFileProviderRequest,
        completion: |URL, NSFileProviderItem, NSError|
    ) {
        tokio::spawn(async move {
            // Stream file from HopNet to FileProvider's URL
            let mut response = self.client
                .get(format!("{}/api/fileprovider/download/{}", self.base_url, identifier))
                .send()
                .await?;
            
            let mut file = File::create(request.url())?;
            while let Some(chunk) = response.chunk().await? {
                file.write_all(&chunk)?;
            }
            
            completion(request.url(), item, nil);
        });
    }
}
```

**NSFileProviderItem Implementation:**
```rust
struct HopNetItem {
    item_identifier: String,        // "file:uuid" or "folder:base64path"
    filename: String,               // Decrypted filename from API
    parent_item_identifier: String, // Parent folder identifier
    type_identifier: String,        // UTI (public.folder, public.data, etc.)
    content_modification_date: Date, // From data_blocks.modified_at
    capabilities: NSFileProviderItemCapabilities, // .allowsReading for MVP
}
```

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

### Phase 1: Read-Only MVP (2-3 weeks)
- [ ] Core FileProvider extension with `objc2-file-provider`
- [ ] HTTP API endpoints in main HopNet process
- [ ] Scoped authentication with API keys
- [ ] File enumeration and metadata retrieval
- [ ] File download with fragment assembly
- [ ] Manual domain registration

### Phase 2: Enhanced Functionality (1-2 weeks)
- [ ] Progress reporting during file assembly
- [ ] Error handling and retry logic
- [ ] Working sets support (recents, favorites)
- [ ] Basic thumbnail support

### Phase 3: Write Operations (2-3 weeks)
- [ ] File upload through FileProvider
- [ ] File/folder creation and deletion
- [ ] Move and rename operations
- [ ] Conflict resolution

### Phase 4: iOS Thin Client Foundation (1-2 weeks)
- [ ] Remote endpoint configuration
- [ ] Enhanced authentication for remote access
- [ ] iOS Files app compatibility testing
- [ ] Documentation for thin client deployment

## Security Considerations

### Authentication Security
- FileProvider extension never has access to consensus private keys
- API key scoped to read-only FileProvider operations only
- Key stored in secure macOS Keychain
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

## Conclusion

This FileProvider integration provides HopNet with native macOS/iOS file system integration while maintaining the security and architectural integrity of the consensus-based system. The HTTP API approach creates a clean separation of concerns and establishes the foundation for future thin client deployments, enabling HopNet to scale from personal use to enterprise environments with familiar, native file access patterns.