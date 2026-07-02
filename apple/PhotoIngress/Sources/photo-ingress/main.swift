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
    case .adopted(let photoId):
        print("resolution: ADOPTED photo_id=\(photoId) — pending rows now drain-eligible")
        print("run `photo-ingress drain` to materialize")
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

func intFlag(_ name: String, default def: UInt64) -> UInt64 {
    flagValue(args, name).flatMap { UInt64($0) } ?? def
}

/// Enumerate real assets (default fetch: personal + Shared Photo Library,
/// legacy shared albums excluded for free) and mint pending rows. No bytes.
func runSeed() throws {
    let limit = Int(intFlag("--limit", default: 25))
    let mediaFilter = flagValue(args, "--media")  // image|video|live
    try ensureAuthorized()
    let session = try IngressSession(dataDir: dataDir)

    var minted = 0, known = 0, adopted = 0, unmapped = 0, errors = 0
    var seen = 0
    let all = PHAsset.fetchAssets(with: nil)
    all.enumerateObjects { asset, _, stop in
        if seen >= limit { stop.pointee = true; return }
        if let f = mediaFilter {
            let isLive = asset.mediaSubtypes.contains(.photoLive)
            let matches = switch f {
            case "image": asset.mediaType == .image && !isLive
            case "video": asset.mediaType == .video
            case "live": isLive
            default: true
            }
            if !matches { return }
        }
        seen += 1
        do {
            let desc = try extractDescriptor(asset: asset)
            switch try session.seedDescriptor(desc: desc) {
            case .mintedPending: minted += 1
            case .alreadyKnown: known += 1
            case .adopted: adopted += 1
            case .unmapped: unmapped += 1
            }
        } catch {
            errors += 1
            print("  seed error \(asset.localIdentifier): \(error)")
        }
    }
    print("seeded \(seen): minted=\(minted) known=\(known) adopted=\(adopted) " +
          "unmapped=\(unmapped) errors=\(errors)")
}

func runDrain() throws {
    try ensureAuthorized()
    let session = try IngressSession(dataDir: dataDir)
    let fetcher = PhotoKitFetcher()

    // SIGTERM/SIGINT → cooperative cancellation: PhotoKit requests cancelled,
    // admission stops, rows stay untouched.
    signal(SIGTERM, SIG_IGN)
    signal(SIGINT, SIG_IGN)
    let makeHandler = { (sig: Int32) -> DispatchSourceSignal in
        let src = DispatchSource.makeSignalSource(signal: sig, queue: .global())
        src.setEventHandler {
            print("signal received — cancelling drain (rows stay pending)")
            fetcher.cancelAll()
            session.cancelDrain()
        }
        src.resume()
        return src
    }
    let sigterm = makeHandler(SIGTERM)
    let sigint = makeHandler(SIGINT)
    defer { sigterm.cancel(); sigint.cancel() }

    let options = FfiDrainOptions(
        fetchConcurrency: UInt32(intFlag("--fetch-concurrency", default: 4)),
        retryCap: Int64(intFlag("--retry-cap", default: 5)),
        retryBaseSecs: intFlag("--retry-base-secs", default: 30),
        retryMaxSecs: intFlag("--retry-max-secs", default: 3600),
        reserveFloorGib: intFlag("--reserve-floor-gib", default: 10),
        pressurePauseSecs: intFlag("--pressure-pause-secs", default: 60),
        storagePollSecs: intFlag("--storage-poll-secs", default: 15)
    )
    let report = try session.drain(fetcher: fetcher, options: options)
    print("drain report:")
    print("  photos completed:     \(report.photosCompleted)")
    print("  resources written:    \(report.resourcesWritten) (\(report.bytesWritten) bytes)")
    print("  resources deduped:    \(report.resourcesDeduped)")
    print("  late-binding merges:  \(report.lateBindingMerges)")
    print("  swept partial temps:  \(report.sweptPartials)")
    print("  pauses:               \(report.pauses)")
    print("  awaiting retry:       \(report.awaitingRetry)" +
          (report.earliestNextRetryAt.map { " (earliest \($0))" } ?? ""))
    print("  gave up:              \(report.gaveUp)")
}

/// The continuous daemon (spec §Process model): change observer + startup and
/// periodic reconciliation scans feeding the never-exiting Rust loop.
func runDaemon() throws {
    try ensureAuthorized()
    let session = try IngressSession(dataDir: dataDir)
    let fetcher = PhotoKitFetcher()
    let retryCap = Int64(intFlag("--retry-cap", default: 5))
    let scanQueue = DispatchQueue(label: "photo-ingress.scan")

    let scanLimit = flagValue(args, "--scan-limit").flatMap { Int($0) }
    let runScanLogged = { (label: String) in
        do {
            let s = try runScan(session: session, retryCap: retryCap, limit: scanLimit)
            print("scan (\(label)): probed=\(s.probed) needed_full=\(s.neededFull) " +
                  "deletions=\(s.deletionsSynthesized) gave_up_reset=\(s.gaveUpReset)" +
                  (s.synthesisSkipped ? " SYNTHESIS SKIPPED (zero enumeration)" : ""))
        } catch {
            // Includes "a scan is already active" — a periodic tick landing
            // mid-scan just skips.
            print("scan (\(label)) skipped/failed: \(error)")
        }
    }

    // Observer registers BEFORE the startup scan: the overlap window closes
    // via idempotent duplicate delivery; an unobserved window does not.
    // A non-incremental (full reload) change falls back to a rescan.
    let observer = LibraryObserver(session: session) {
        scanQueue.async { runScanLogged("full-reload") }
    }
    observer.start()
    scanQueue.async { runScanLogged("startup") }

    let scanInterval = intFlag("--scan-interval-secs", default: 3600)
    let timer = DispatchSource.makeTimerSource(queue: scanQueue)
    timer.schedule(deadline: .now() + Double(scanInterval), repeating: Double(scanInterval))
    timer.setEventHandler { runScanLogged("periodic") }
    timer.resume()

    signal(SIGTERM, SIG_IGN)
    signal(SIGINT, SIG_IGN)
    let makeHandler = { (sig: Int32) -> DispatchSourceSignal in
        let src = DispatchSource.makeSignalSource(signal: sig, queue: .global())
        src.setEventHandler {
            print("signal received — stopping daemon (rows stay pending)")
            fetcher.cancelAll()
            session.cancelDrain()
        }
        src.resume()
        return src
    }
    let sigterm = makeHandler(SIGTERM)
    let sigint = makeHandler(SIGINT)
    defer {
        sigterm.cancel()
        sigint.cancel()
        timer.cancel()
        observer.stop()
    }

    let options = FfiDaemonOptions(
        fetchConcurrency: UInt32(intFlag("--fetch-concurrency", default: 4)),
        retryCap: retryCap,
        retryBaseSecs: intFlag("--retry-base-secs", default: 30),
        retryMaxSecs: intFlag("--retry-max-secs", default: 3600),
        reserveFloorGib: intFlag("--reserve-floor-gib", default: 10),
        pressurePauseSecs: intFlag("--pressure-pause-secs", default: 60),
        storagePollSecs: intFlag("--storage-poll-secs", default: 15)
    )
    print("daemon running (scan interval \(scanInterval)s) — SIGTERM/SIGINT to stop")
    let report = try session.runDaemon(fetcher: fetcher, options: options)
    print("daemon report:")
    print("  events applied:       \(report.eventsApplied) (deferred \(report.eventsDeferred))")
    print("  deletions:            \(report.deletions)")
    print("  restores:             \(report.restores)")
    print("  transitions:          \(report.transitions)")
    print("  resources reopened:   \(report.resourcesReopened)")
    print("  photos completed:     \(report.drain.photosCompleted)")
    print("  resources written:    \(report.drain.resourcesWritten) (\(report.drain.bytesWritten) bytes)")
    print("  resources deduped:    \(report.drain.resourcesDeduped)")
    print("  swept partial temps:  \(report.drain.sweptPartials)")
    print("  pauses:               \(report.drain.pauses)")
    print("  awaiting retry:       \(report.drain.awaitingRetry)" +
          (report.drain.earliestNextRetryAt.map { " (earliest \($0))" } ?? ""))
    print("  gave up:              \(report.drain.gaveUp)")
}

// Never block the main thread on PhotoKit (spike lesson): work on a
// background queue, main thread services the main dispatch queue.
DispatchQueue.global().async {
    do {
        switch command {
        case "setup": try runSetup()
        case "ingest": try runIngest()
        case "seed": try runSeed()
        case "drain": try runDrain()
        case "daemon": try runDaemon()
        default: fail("unknown command \(command)")
        }
        exit(0)
    } catch {
        fail(String(describing: error))
    }
}
dispatchMain()
