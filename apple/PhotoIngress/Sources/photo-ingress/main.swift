import Foundation
import Photos
import PhotoIngressKit

// PhotoKit-side executable: the daemon shell plus the PhotoKit-dependent
// scaffolding subcommands. Everything that does NOT need PhotoKit — status,
// fsck, recover, library configuration — lives in the Rust ingress-cli.
//   photo-ingress setup  --data-dir D    (data dir + Photos authorization)
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
// Zero arguments = the bundled LaunchAgent invocation (belt-and-braces for
// BundleProgram argv semantics) — run the daemon.
let command = args.first ?? "daemon"

// Default mirrors ingress-cli's canonical data dir. The bundled LaunchAgent
// plist cannot expand `~`, so the daemon must self-default; the explicit
// flag remains for dev/soak isolation.
let dataDir = flagValue(args, "--data-dir")
    ?? FileManager.default.homeDirectoryForCurrentUser
        .appendingPathComponent(".local/share/hopnet-photo-ingress").path

// The bundled plist passes --log-to-data-dir: launchd's StandardOutPath
// cannot be user-relative, so the daemon owns its log file instead.
if args.contains("--log-to-data-dir") {
    try? FileManager.default.createDirectory(atPath: dataDir, withIntermediateDirectories: true)
    let logPath = dataDir + "/daemon.log"
    freopen(logPath, "a", stdout)
    freopen(logPath, "a", stderr)
    setvbuf(stdout, nil, _IOLBF, 0)
}

// The PH resource types the daemon archives (spec mapping table), original first.
let originalTypes: Set<Int32> = [1, 2]
let archivedTypes: Set<Int32> = [1, 2, 5, 6, 9, 7, 4, 10]

func describe(_ outcome: FfiWriteOutcome, label: String) {
    print("  [\(label)] hash=\(outcome.contentHash.prefix(16))… size=\(outcome.sizeBytes) " +
          "ext=\(outcome.ext) deduped=\(outcome.deduped)")
    print("           blob=\(outcome.blobPath)")
    if outcome.photoCompleted {
        print("           photo COMPLETE — descriptor persisted=\(outcome.descriptorPersisted)")
    }
}

// Library configuration moved to the Rust CLI (Phase 6): setup only
// creates the data dir + state.db and walks the Photos authorization
// prompt — the two things that need this process (PhotoKit entitlement).
func runSetup() throws {
    _ = try IngressSession(dataDir: dataDir)
    print("state: \(dataDir)/state.db")
    try ensureAuthorized()
    print("Photos authorization: granted")
    print("next: configure libraries with")
    print("  ingress-cli --data-dir \(dataDir) library add --scope personal")
    print("  ingress-cli --data-dir \(dataDir) library add --scope shared")
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
        print("bind this scope with: ingress-cli library add --scope shared")
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

func printCleanup(_ c: FfiCleanupReport, indent: String = "  ") {
    print("\(indent)photos hard-deleted:  \(c.photosHardDeleted)")
    print("\(indent)blob files deleted:   \(c.blobFilesDeleted)")
    print("\(indent)log rows pruned:      \(c.logRowsPruned)")
    print("\(indent)spool evicted:        \(c.spoolEvicted)")
}

/// One-shot lifecycle run. No PhotoKit involvement — needs no authorization;
/// errors if the daemon is running (shared exclusive lock).
func runCleanup() throws {
    let session = try IngressSession(dataDir: dataDir)
    let report = try session.cleanup(options: FfiCleanupOptions(
        logRetentionDays: Int64(intFlag("--log-retention-days", default: 180)),
        hardDeleteBatch: UInt32(intFlag("--hard-delete-batch", default: 500))
    ))
    print("cleanup report:")
    printCleanup(report)
}

/// The continuous daemon (spec §Process model): change observer + startup and
/// periodic reconciliation scans feeding the never-exiting Rust loop.
func runDaemon() throws {
    try ensureAuthorized()
    let session = try IngressSession(dataDir: dataDir)

    // Startup library ensure: the personal library needs no configuration
    // (the spool is data-dir-derived), so it is created unconditionally
    // when absent — this must precede the startup scan so seeded assets
    // route into the library instead of minting unmapped rows. A failure
    // (e.g. ingress-cli holds the run lock) aborts startup — the run lock
    // would block the daemon loop anyway; launchd retries.
    switch try session.ensurePersonalLibrary() {
    case .created(let libraryId):
        print("personal library created: \(libraryId)")
    case .alreadyExists:
        break
    }

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

    // Publish credentials: explicit flags (dev/soak) win over the keychain
    // service the HopNet app provisions. Neither present = ingest-only.
    let flagUrl = flagValue(args, "--node-url")
    let flagToken = flagValue(args, "--device-token")
    let keychain = (flagUrl == nil || flagToken == nil) ? PublishCredentials.load() : nil
    let nodeUrl = flagUrl ?? keychain?.baseUrl
    let deviceToken = flagToken ?? keychain?.deviceToken
    let publishing = nodeUrl != nil && deviceToken != nil

    let options = FfiDaemonOptions(
        fetchConcurrency: UInt32(intFlag("--fetch-concurrency", default: 4)),
        retryCap: retryCap,
        retryBaseSecs: intFlag("--retry-base-secs", default: 30),
        retryMaxSecs: intFlag("--retry-max-secs", default: 3600),
        reserveFloorGib: intFlag("--reserve-floor-gib", default: 10),
        pressurePauseSecs: intFlag("--pressure-pause-secs", default: 60),
        storagePollSecs: intFlag("--storage-poll-secs", default: 15),
        cleanupIntervalSecs: intFlag("--cleanup-interval-secs", default: 3600),
        publishNodeUrl: publishing ? nodeUrl : nil,
        publishDeviceToken: publishing ? deviceToken : nil,
        publishIntervalSecs: intFlag("--publish-interval-secs", default: 60)
    )
    if publishing {
        // Device id only — the secret half of the token stays out of logs.
        print("publishing to \(nodeUrl!) (device \(deviceToken!.prefix(while: { $0 != "." })))")
    } else {
        print("publishing OFF — no --node-url/--device-token and no keychain " +
              "credentials (\(PublishCredentials.service))")
    }
    // Flags pin credentials (dev/soak); only a fully keychain-sourced daemon
    // re-reads them after unreachable passes (ephemeral-port healing).
    let credentialsProvider: PublishCredentialsProvider? =
        (flagUrl == nil && flagToken == nil) ? KeychainCredentialsProvider() : nil
    print("daemon running (scan interval \(scanInterval)s) — SIGTERM/SIGINT to stop")
    let report = try session.runDaemon(
        fetcher: fetcher, options: options, credentialsProvider: credentialsProvider)
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
    print("  lifecycle:")
    printCleanup(report.cleanup, indent: "    ")
    if publishing {
        print("  publish:")
        print("    published:          \(report.publish.published) " +
              "(already \(report.publish.alreadyPublished), adopted \(report.publish.adopted))")
        print("    failed:             \(report.publish.failed) " +
              "(gave up \(report.publish.gaveUp), missing descriptor \(report.publish.missingDescriptor), " +
              "spool evicted \(report.publish.evictedBlobs))")
        if report.publish.parked {
            print("    PARKED — node unreachable at last pass")
        }
        if report.publish.parkedResponsibility {
            print("    PARKED — not the responsible ingress device")
        }
    }
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
        case "cleanup": try runCleanup()
        default: fail("unknown command \(command)")
        }
        exit(0)
    } catch {
        fail(String(describing: error))
    }
}
dispatchMain()
