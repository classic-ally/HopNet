import Foundation
import Photos
import PhotoIngressKit

// Phase 2 vertical slice: ingest one asset end-to-end. These subcommands are
// slice scaffolding — this executable becomes the LaunchAgent daemon shell in
// later phases; the real user-facing CLI is the Rust ingress-cli (Phase 6).
//   photo-ingress setup  --data-dir D --blob-root B [--shared-blob-root S]
//   photo-ingress ingest --data-dir D <local_id>

setvbuf(stdout, nil, _IOLBF, 0)

func fail(_ msg: String) -> Never {
    FileHandle.standardError.write(Data(("error: " + msg + "\n").utf8))
    exit(1)
}

func flagValue(_ args: [String], _ name: String) -> String? {
    guard let i = args.firstIndex(of: name), i + 1 < args.count else { return nil }
    return args[i + 1]
}

let args = Array(CommandLine.arguments.dropFirst())
guard let command = args.first else {
    print("usage: photo-ingress setup --data-dir D --blob-root B [--shared-blob-root S]")
    print("       photo-ingress ingest --data-dir D <local_id>")
    exit(2)
}
guard let dataDir = flagValue(args, "--data-dir") else {
    fail("--data-dir is required (deliberately no default — keeps slice state away from production paths)")
}

// The PH resource types the daemon archives (spec mapping table), original first.
let originalTypes: Set<Int32> = [1, 2]
let archivedTypes: Set<Int32> = [1, 2, 5, 6, 9, 7, 4, 10]

func describe(_ outcome: FfiWriteOutcome, label: String) {
    print("  [\(label)] hash=\(outcome.contentHash.prefix(16))… size=\(outcome.sizeBytes) " +
          "ext=\(outcome.ext) deduped=\(outcome.deduped)")
    print("           blob=\(outcome.blobPath)")
    if let sidecar = outcome.sidecarPath {
        print("           photo COMPLETE — sidecar=\(sidecar)")
    }
}

func runSetup() throws {
    guard let blobRoot = flagValue(args, "--blob-root") else { fail("--blob-root is required") }
    let session = try IngressSession(dataDir: dataDir)
    try session.addLibrary(
        libraryId: "personal", displayName: "Personal", blobRoot: blobRoot, scope: .personal)
    print("configured library 'personal' → \(blobRoot)")
    if let shared = flagValue(args, "--shared-blob-root") {
        try session.addLibrary(
            libraryId: "shared", displayName: "Shared Library", blobRoot: shared, scope: .shared)
        print("configured library 'shared' → \(shared)")
    }
    print("state: \(dataDir)/state.db")
}

func runIngest() throws {
    guard let localId = args.last, !localId.hasPrefix("--"), args.count >= 4 else {
        fail("usage: photo-ingress ingest --data-dir D <local_id>")
    }
    try ensureAuthorized()
    let session = try IngressSession(dataDir: dataDir)
    let asset = try fetchAsset(localId: localId)
    let descriptor = try extractDescriptor(asset: asset)
    print("asset \(localId)")
    print("  cloud_id=\(descriptor.cloudId ?? "none") scope=\(descriptor.scope) " +
          "media=\(descriptor.mediaType) resources=\(descriptor.resources.count)")

    switch try session.ingestDescriptor(desc: descriptor) {
    case .alreadyKnown(let photoId, let metadataChanged, let scopeChanged):
        print("resolution: ALREADY KNOWN photo_id=\(photoId) " +
              "metadata_changed=\(metadataChanged) scope_changed=\(scopeChanged)")
        print("nothing to stream — fast path, zero bytes downloaded")
        return
    case .unmappedScope(let photoId):
        print("resolution: UNMAPPED SCOPE photo_id=\(photoId)")
        print("run setup with the appropriate blob root to bind this scope")
        return
    case .needsOriginal:
        print("resolution: NEW — streaming resources")
    }

    let resources = PHAssetResource.assetResources(for: asset)
    guard let original = resources.first(where: { originalTypes.contains(Int32($0.type.rawValue)) })
    else { fail("asset has no original resource") }

    // Original first: identity rules 2a–2c key on its hash.
    let sink = try session.beginOriginal(desc: descriptor)
    let outcome = try streamResource(original, into: sink)
    print("  identity: \(outcome.resolutionKind) photo_id=\(outcome.photoId)")
    describe(outcome, label: "original type=\(original.type.rawValue)")

    // Remaining archivable resources.
    for resource in resources {
        let rawType = Int32(resource.type.rawValue)
        guard archivedTypes.contains(rawType), !originalTypes.contains(rawType) else { continue }
        let sink = try session.beginResource(
            photoId: outcome.photoId,
            phResourceType: rawType,
            uti: resource.uniformTypeIdentifier,
            originalFilename: resource.originalFilename)
        let res = try streamResource(resource, into: sink)
        describe(res, label: "resource type=\(rawType)")
    }
    print("done")
}

// Never block the main thread on PhotoKit (spike lesson): work on a
// background queue, main thread services the main dispatch queue.
DispatchQueue.global().async {
    do {
        switch command {
        case "setup": try runSetup()
        case "ingest": try runIngest()
        default: fail("unknown command \(command)")
        }
        exit(0)
    } catch {
        fail(String(describing: error))
    }
}
dispatchMain()
