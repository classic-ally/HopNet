import Foundation
import Photos

// PhotoKit spike. Subcommands, one per spec assumption:
//   basic      - authorization + counts (cycle 1)
//   cloudids   - batch PHCloudIdentifier mapping at full-library scale
//   scopes     - per-asset library scope detection (sourceType distribution)
//   resources  - resource enumeration for interesting asset categories
//   filesize   - undocumented fileSize KVC availability across resources

func elapsed(_ start: DispatchTime) -> String {
    let ms = Double(DispatchTime.now().uptimeNanoseconds - start.uptimeNanoseconds) / 1_000_000
    return ms >= 1000 ? String(format: "%.2fs", ms / 1000) : String(format: "%.1fms", ms)
}

func ensureAuthorized() {
    let status = PHPhotoLibrary.authorizationStatus(for: .readWrite)
    if status == .authorized { return }
    guard status == .notDetermined else {
        print("FAIL: authorization status \(status.rawValue), not authorized")
        exit(1)
    }
    let sema = DispatchSemaphore(value: 0)
    var granted: PHAuthorizationStatus = .notDetermined
    PHPhotoLibrary.requestAuthorization(for: .readWrite) { s in granted = s; sema.signal() }
    print("waiting for TCC prompt…")
    sema.wait()
    guard granted == .authorized else {
        print("FAIL: not authorized (\(granted.rawValue))")
        exit(1)
    }
}

func allAssets() -> PHFetchResult<PHAsset> {
    PHAsset.fetchAssets(with: nil)
}

func allLocalIdentifiers() -> [String] {
    let all = allAssets()
    var ids: [String] = []
    ids.reserveCapacity(all.count)
    all.enumerateObjects { asset, _, _ in ids.append(asset.localIdentifier) }
    return ids
}

// -- basic -----------------------------------------------------------------

func cmdBasic() {
    let t0 = DispatchTime.now()
    let all = allAssets()
    print("total assets: \(all.count) (fetch: \(elapsed(t0)))")
    for (label, type) in [("images", PHAssetMediaType.image), ("videos", .video)] {
        let r = PHAsset.fetchAssets(with: type, options: nil)
        print("\(label): \(r.count)")
    }
}

// -- cloudids ---------------------------------------------------------------

func cmdCloudIds() {
    let tIds = DispatchTime.now()
    let ids = allLocalIdentifiers()
    print("collected \(ids.count) local ids (\(elapsed(tIds)))")

    // Full-library batch mapping: the spec's fast path assumes this is cheap
    // (or at least amortizable). Measure in one shot, then in chunks.
    let tMap = DispatchTime.now()
    let mappings = PHPhotoLibrary.shared().cloudIdentifierMappings(forLocalIdentifiers: ids)
    print("batch mapping ALL \(ids.count): \(elapsed(tMap))")

    var ok = 0, failed = 0
    var errorSamples: [String] = []
    for (_, result) in mappings {
        switch result {
        case .success: ok += 1
        case .failure(let e):
            failed += 1
            if errorSamples.count < 3 { errorSamples.append("\(e)") }
        }
    }
    print("mapped: \(ok), failed: \(failed)")
    for e in errorSamples { print("  error sample: \(e)") }

    // Chunked timing (the shape a reconciliation scan would actually use)
    let chunk = Array(ids.prefix(1000))
    let tChunk = DispatchTime.now()
    _ = PHPhotoLibrary.shared().cloudIdentifierMappings(forLocalIdentifiers: chunk)
    print("batch mapping 1000: \(elapsed(tChunk))")

    // Reverse direction: cloud -> local (recovery re-linking path)
    let someCloudIds = mappings.values.compactMap { try? $0.get() }.prefix(1000)
    let tRev = DispatchTime.now()
    let rev = PHPhotoLibrary.shared().localIdentifierMappings(for: Array(someCloudIds))
    var revOk = 0
    for (_, r) in rev where (try? r.get()) != nil { revOk += 1 }
    print("reverse mapping 1000: \(elapsed(tRev)), ok: \(revOk)")

    // Stringification round-trip (what we'd store in state.db)
    if let first = someCloudIds.first {
        let s = first.stringValue
        let back = PHCloudIdentifier(stringValue: s)
        print("stringValue round-trip: len=\(s.count) equal=\(back == first)")
        print("  sample (truncated): \(s.prefix(40))…")
    }
}

// -- scopes ------------------------------------------------------------------

func cmdScopes() {
    // Probe: what distinguishes personal vs iCloud Shared Photo Library assets?
    // Candidate 1: PHAsset.sourceType (OptionSet)
    let all = allAssets()
    var histogram: [UInt: Int] = [:]
    let t0 = DispatchTime.now()
    all.enumerateObjects { asset, _, _ in
        histogram[asset.sourceType.rawValue, default: 0] += 1
    }
    print("sourceType histogram over \(all.count) assets (\(elapsed(t0))):")
    for (raw, count) in histogram.sorted(by: { $0.key < $1.key }) {
        print("  rawValue=\(raw): \(count)")
    }
    print("  (typeUserLibrary=\(PHAssetSourceType.typeUserLibrary.rawValue)," +
          " typeCloudShared=\(PHAssetSourceType.typeCloudShared.rawValue)," +
          " typeiTunesSynced=\(PHAssetSourceType.typeiTunesSynced.rawValue))")
}

// -- scopes2: alternate fetch paths for iCloud Shared Photo Library ----------

func cmdScopes2() {
    // Path A: explicit includeAssetSourceTypes
    for (label, types) in [
        ("cloudShared only", PHAssetSourceType.typeCloudShared),
        ("userLibrary+cloudShared", [.typeUserLibrary, .typeCloudShared]),
    ] as [(String, PHAssetSourceType)] {
        let opts = PHFetchOptions()
        opts.includeAssetSourceTypes = types
        let t = DispatchTime.now()
        let r = PHAsset.fetchAssets(with: opts)
        print("fetch [\(label)]: \(r.count) assets (\(elapsed(t)))")
        var histogram: [UInt: Int] = [:]
        r.enumerateObjects { asset, idx, stop in
            if idx >= 20000 { stop.pointee = true; return }
            histogram[asset.sourceType.rawValue, default: 0] += 1
        }
        print("  sourceType histogram (first 20k): \(histogram)")
    }

    // Path B: smart-album sweep — look for a shared-library collection
    print("smart albums:")
    let albums = PHAssetCollection.fetchAssetCollections(
        with: .smartAlbum, subtype: .any, options: nil)
    albums.enumerateObjects { coll, _, _ in
        let count = PHAsset.fetchAssets(in: coll, options: nil).count
        print("  subtype=\(coll.assetCollectionSubtype.rawValue) " +
              "title=\(coll.localizedTitle ?? "nil") assets=\(count)")
    }
}

// -- scopes3: distinguish Shared Photo Library from legacy shared albums -----

func cmdScopes3() {
    // Legacy iCloud Shared Albums live in .albumCloudShared collections.
    // If their assets also report sourceType=typeCloudShared, the binary
    // sourceType signal conflates two features and we need collection
    // membership to disambiguate.
    let shared = PHAssetCollection.fetchAssetCollections(
        with: .album, subtype: .albumCloudShared, options: nil)
    print("legacy shared albums: \(shared.count)")
    shared.enumerateObjects { coll, _, _ in
        let assets = PHAsset.fetchAssets(in: coll, options: nil)
        var histogram: [UInt: Int] = [:]
        assets.enumerateObjects { a, _, _ in
            histogram[a.sourceType.rawValue, default: 0] += 1
        }
        print("  title=\(coll.localizedTitle ?? "nil") assets=\(assets.count) " +
              "sourceTypes=\(histogram)")
    }
}

// -- scopes4: hunt the per-asset Shared Photo Library discriminator ----------

func cmdScopes4() {
    // SPL assets appear in default fetches with sourceType=typeUserLibrary,
    // indistinguishable via public API. List PHAsset's runtime properties and
    // sample promising ones across the library, looking for a value that
    // splits ~26k personal / ~10k shared.
    var propCount: UInt32 = 0
    var names: [String] = []
    if let props = class_copyPropertyList(PHAsset.self, &propCount) {
        for i in 0..<Int(propCount) {
            names.append(String(cString: property_getName(props[i])))
        }
        free(props)
    }
    print("PHAsset runtime properties (\(names.count)):")
    print("  \(names.sorted().joined(separator: ", "))")

    let interesting = names.filter { n in
        let l = n.lowercased()
        return l.contains("scope") || l.contains("shared") || l.contains("owner")
            || l.contains("syndicat") || l.contains("cloud")
    }
    print("candidate keys: \(interesting)")

    guard !interesting.isEmpty else { return }
    let all = allAssets()
    var histograms: [String: [String: Int]] = [:]
    let sampleStride = max(1, all.count / 4000)
    all.enumerateObjects { asset, idx, _ in
        guard idx % sampleStride == 0 else { return }
        for key in interesting {
            let v = asset.value(forKey: key)
            let desc = v.map { String(describing: $0) } ?? "nil"
            histograms[key, default: [:]][String(desc.prefix(60)), default: 0] += 1
        }
    }
    for (key, hist) in histograms.sorted(by: { $0.key < $1.key }) {
        // Only print keys whose values actually vary — constants can't discriminate
        if hist.count > 1 {
            print("VARIES \(key): \(hist)")
        } else {
            print("const  \(key): \(hist.first.map { "\($0.key)" } ?? "-")")
        }
    }
}

// -- scopes5: verify participatesInLibraryScope is the SPL discriminator -----

func cmdScopes5() {
    // Expected from Photos.app: shared = 9,871 photos + 269 videos = 10,140
    let all = allAssets()
    var inScope = 0, notInScope = 0, byType: [Int: Int] = [:]
    let t0 = DispatchTime.now()
    all.enumerateObjects { asset, _, _ in
        if (asset.value(forKey: "participatesInLibraryScope") as? Bool) == true {
            inScope += 1
            byType[asset.mediaType.rawValue, default: 0] += 1
        } else {
            notInScope += 1
        }
    }
    print("participatesInLibraryScope over \(all.count) assets (\(elapsed(t0))):")
    print("  true:  \(inScope)  (by mediaType: \(byType))")
    print("  false: \(notInScope)")
    print("  expected shared: 10140 (9871 photos + 269 videos)")

    // Cross-check: legacy shared-album assets must NOT participate
    let opts = PHFetchOptions()
    opts.includeAssetSourceTypes = [.typeCloudShared]
    let legacy = PHAsset.fetchAssets(with: opts)
    var legacyInScope = 0
    legacy.enumerateObjects { asset, _, _ in
        if (asset.value(forKey: "participatesInLibraryScope") as? Bool) == true {
            legacyInScope += 1
        }
    }
    print("legacy shared-album assets with participatesInLibraryScope=true: " +
          "\(legacyInScope) / \(legacy.count) (expect 0)")
}

// -- resources ----------------------------------------------------------------

func describeResources(_ asset: PHAsset, label: String) {
    print("\(label): local_id=\(asset.localIdentifier)")
    print("  mediaType=\(asset.mediaType.rawValue) subtypes=\(asset.mediaSubtypes.rawValue) " +
          "burst=\(asset.representsBurst) burstId=\(asset.burstIdentifier ?? "nil")")
    for r in PHAssetResource.assetResources(for: asset) {
        print("  resource: type=\(r.type.rawValue) uti=\(r.uniformTypeIdentifier) " +
              "filename=\(r.originalFilename)")
    }
}

func cmdResources() {
    let all = allAssets()

    var livePhoto: PHAsset?
    var burst: PHAsset?
    var edited: PHAsset?
    var rawPlusJpeg: PHAsset?
    var plainVideo: PHAsset?

    let t0 = DispatchTime.now()
    all.enumerateObjects { asset, _, stop in
        if livePhoto == nil, asset.mediaSubtypes.contains(.photoLive) { livePhoto = asset }
        if burst == nil, asset.representsBurst { burst = asset }
        if plainVideo == nil, asset.mediaType == .video { plainVideo = asset }
        if edited == nil || rawPlusJpeg == nil {
            let types = Set(PHAssetResource.assetResources(for: asset).map(\.type))
            if edited == nil, types.contains(.adjustmentData) { edited = asset }
            if rawPlusJpeg == nil, types.contains(.alternatePhoto) { rawPlusJpeg = asset }
        }
        if livePhoto != nil, burst != nil, edited != nil, rawPlusJpeg != nil, plainVideo != nil {
            stop.pointee = true
        }
    }
    print("category scan: \(elapsed(t0))")

    for (label, asset) in [("LIVE PHOTO", livePhoto), ("BURST", burst), ("EDITED", edited),
                           ("RAW+JPEG", rawPlusJpeg), ("VIDEO", plainVideo)] {
        if let a = asset { describeResources(a, label: label) } else { print("\(label): none found") }
    }
    print("  (PHAssetResourceType: photo=1 video=2 audio=3 alternatePhoto=4 fullSizePhoto=5 " +
          "fullSizeVideo=6 adjustmentData=7 adjustmentBasePhoto=8 pairedVideo=9 …)")
}

// -- filesize --------------------------------------------------------------------

func cmdFileSize() {
    // The undocumented KVC keys the spec's storage-aware admission leans on.
    // Sample across the library (stride, not prefix, to hit old + new assets).
    let all = allAssets()
    let sampleCount = 500
    let stride = max(1, all.count / sampleCount)

    var nonzero = 0, zero = 0, missing = 0, resTotal = 0
    var locallyAvailableTrue = 0, locallyAvailableFalse = 0, locallyAvailableMissing = 0
    let t0 = DispatchTime.now()
    var i = 0
    all.enumerateObjects { asset, idx, _ in
        guard idx % stride == 0 else { return }
        i += 1
        for r in PHAssetResource.assetResources(for: asset) {
            resTotal += 1
            if let size = r.value(forKey: "fileSize") as? Int64 {
                if size > 0 { nonzero += 1 } else { zero += 1 }
            } else {
                missing += 1
            }
            if let avail = r.value(forKey: "locallyAvailable") as? Bool {
                if avail { locallyAvailableTrue += 1 } else { locallyAvailableFalse += 1 }
            } else {
                locallyAvailableMissing += 1
            }
        }
    }
    print("sampled \(i) assets, \(resTotal) resources (\(elapsed(t0)))")
    print("fileSize:          nonzero=\(nonzero) zero=\(zero) missing=\(missing)")
    print("locallyAvailable:  true=\(locallyAvailableTrue) false=\(locallyAvailableFalse) " +
          "missing=\(locallyAvailableMissing)")
    print("NOTE: correlate locallyAvailable=false with fileSize to check the iCloud-remote case")
}

// -- stream: pull one iCloud-remote original ---------------------------------

func cmdStream() {
    // Find a remote (not locally available) image original and stream it.
    let all = allAssets()
    var target: (PHAsset, PHAssetResource)?
    all.enumerateObjects { asset, _, stop in
        guard asset.mediaType == .image else { return }
        for r in PHAssetResource.assetResources(for: asset) where r.type == .photo {
            if (r.value(forKey: "locallyAvailable") as? Bool) == false {
                target = (asset, r)
                stop.pointee = true
            }
        }
    }
    guard let (asset, resource) = target else {
        print("no remote original found (library fully materialized?)")
        return
    }
    let expected = resource.value(forKey: "fileSize") as? Int64 ?? -1
    print("target: \(asset.localIdentifier) file=\(resource.originalFilename) " +
          "expectedSize=\(expected)")

    let opts = PHAssetResourceRequestOptions()
    opts.isNetworkAccessAllowed = true
    var chunks = 0
    var bytes: Int64 = 0
    var minChunk = Int.max
    var maxChunk = 0
    var progressReports: [Double] = []
    opts.progressHandler = { p in progressReports.append(p) }

    let sema = DispatchSemaphore(value: 0)
    let t0 = DispatchTime.now()
    PHAssetResourceManager.default().requestData(
        for: resource, options: opts,
        dataReceivedHandler: { data in
            chunks += 1
            bytes += Int64(data.count)
            minChunk = min(minChunk, data.count)
            maxChunk = max(maxChunk, data.count)
        },
        completionHandler: { error in
            if let e = error {
                print("FAILED after \(elapsed(t0)): \(e)")
            } else {
                print("complete: \(bytes) bytes in \(chunks) chunks (\(elapsed(t0)))")
                print("  chunk sizes: min=\(minChunk) max=\(maxChunk) " +
                      "avg=\(chunks > 0 ? bytes / Int64(chunks) : 0)")
                print("  progress reports: \(progressReports.count) " +
                      "(first=\(progressReports.first ?? -1), last=\(progressReports.last ?? -1))")
                print("  size match: expected=\(expected) actual=\(bytes) " +
                      "equal=\(expected == bytes)")
            }
            sema.signal()
        })
    sema.wait()

    // Fallback probe: writeData(for:toFile:) — different code path internally
    let tmp = FileManager.default.temporaryDirectory
        .appendingPathComponent("spike-\(UUID().uuidString).bin")
    let opts2 = PHAssetResourceRequestOptions()
    opts2.isNetworkAccessAllowed = true
    let sema2 = DispatchSemaphore(value: 0)
    let t1 = DispatchTime.now()
    PHAssetResourceManager.default().writeData(for: resource, toFile: tmp, options: opts2) { error in
        if let e = error {
            print("writeData FAILED after \(elapsed(t1)): \(e)")
        } else {
            let size = (try? FileManager.default.attributesOfItem(atPath: tmp.path)[.size] as? Int64) ?? nil
            print("writeData complete: \(size ?? -1) bytes (\(elapsed(t1)))")
            try? FileManager.default.removeItem(at: tmp)
        }
        sema2.signal()
    }
    sema2.wait()

    // Third probe: PHImageManager full-size image data (yet another path)
    let opts3 = PHImageRequestOptions()
    opts3.isNetworkAccessAllowed = true
    opts3.deliveryMode = .highQualityFormat
    opts3.progressHandler = { p, e, _, _ in
        if let e = e { print("imageManager progress error: \(e)") }
    }
    let sema3 = DispatchSemaphore(value: 0)
    let t2 = DispatchTime.now()
    PHImageManager.default().requestImageDataAndOrientation(for: asset, options: opts3) { data, uti, _, info in
        let cloud = (info?[PHImageResultIsInCloudKey] as? Bool) ?? false
        let err = info?[PHImageErrorKey]
        print("imageManager: bytes=\(data?.count ?? -1) uti=\(uti ?? "nil") " +
              "inCloud=\(cloud) error=\(err.map { "\($0)" } ?? "none") (\(elapsed(t2)))")
        sema3.signal()
    }
    sema3.wait()
}

// -- stream2: requestData across several remote assets (rule out per-asset) --

func cmdStream2() {
    let all = allAssets()
    var targets: [(PHAsset, PHAssetResource)] = []
    all.enumerateObjects { asset, idx, stop in
        guard asset.mediaType == .image else { return }
        // sample from different eras of the library
        guard idx % 5000 == 0 || targets.isEmpty else { return }
        for r in PHAssetResource.assetResources(for: asset) where r.type == .photo {
            if (r.value(forKey: "locallyAvailable") as? Bool) == false {
                targets.append((asset, r))
                break
            }
        }
        if targets.count >= 5 { stop.pointee = true }
    }
    print("testing \(targets.count) remote assets")
    for (asset, resource) in targets {
        let opts = PHAssetResourceRequestOptions()
        opts.isNetworkAccessAllowed = true
        var bytes: Int64 = 0
        let sema = DispatchSemaphore(value: 0)
        let t0 = DispatchTime.now()
        PHAssetResourceManager.default().requestData(
            for: resource, options: opts,
            dataReceivedHandler: { data in bytes += Int64(data.count) },
            completionHandler: { error in
                let created = asset.creationDate.map {
                    ISO8601DateFormatter().string(from: $0)
                } ?? "nil"
                if let e = error as NSError? {
                    print("  FAIL \(resource.originalFilename) (created \(created)): " +
                          "domain=\(e.domain) code=\(e.code)")
                } else {
                    print("  OK   \(resource.originalFilename) (created \(created)): \(bytes) bytes " +
                          "(\(elapsed(t0)))")
                }
                sema.signal()
            })
        sema.wait()
    }
}

// -- observe: change observer granularity -------------------------------------

final class SpikeObserver: NSObject, PHPhotoLibraryChangeObserver {
    var fetchResult: PHFetchResult<PHAsset>
    let started = DispatchTime.now()

    override init() {
        fetchResult = allAssets()
        super.init()
    }

    func stamp() -> String { elapsed(started) }

    func photoLibraryDidChange(_ changeInstance: PHChange) {
        print("[\(stamp())] photoLibraryDidChange fired")
        guard let details = changeInstance.changeDetails(for: fetchResult) else {
            print("  no changeDetails for our fetchResult (unrelated change)")
            return
        }
        print("  hasIncrementalChanges=\(details.hasIncrementalChanges)")
        if details.hasIncrementalChanges {
            let inserted = details.insertedObjects
            let removed = details.removedObjects
            let changed = details.changedObjects
            print("  inserted=\(inserted.count) removed=\(removed.count) " +
                  "changed=\(changed.count) hasMoves=\(details.hasMoves)")
            for a in inserted { print("    + \(a.localIdentifier) \(a.mediaType.rawValue)") }
            for a in removed { print("    - \(a.localIdentifier)") }
            for a in changed {
                let res = PHAssetResource.assetResources(for: a).map { $0.type.rawValue }
                print("    ~ \(a.localIdentifier) favorite=\(a.isFavorite) " +
                      "hidden=\(a.isHidden) modified=\(a.modificationDate.map { ISO8601DateFormatter().string(from: $0) } ?? "nil") " +
                      "resourceTypes=\(res)")
            }
        } else {
            print("  NON-INCREMENTAL: full reload required")
        }
        fetchResult = details.fetchResultAfterChanges
    }
}

let spikeObserver = SpikeObserver()

func cmdObserve() {
    PHPhotoLibrary.shared().register(spikeObserver)
    print("observing \(spikeObserver.fetchResult.count) assets — perform actions in Photos.app now")
    print("(Ctrl-C or kill to stop)")
    DispatchSemaphore(value: 0).wait()  // block forever; observer callbacks arrive on their own queue
}

// -- main -------------------------------------------------------------------------

// PhotoKit may deliver some completion handlers on the main queue; never block
// the main thread. Probe work runs on a background queue, main thread services
// the main dispatch queue via dispatchMain().
ensureAuthorized()
let cmd = CommandLine.arguments.count > 1 ? CommandLine.arguments[1] : "basic"
DispatchQueue.global().async {
    switch cmd {
    case "basic": cmdBasic()
    case "cloudids": cmdCloudIds()
    case "scopes": cmdScopes()
    case "scopes2": cmdScopes2()
    case "scopes3": cmdScopes3()
    case "scopes4": cmdScopes4()
    case "scopes5": cmdScopes5()
    case "resources": cmdResources()
    case "filesize": cmdFileSize()
    case "stream": cmdStream()
case "stream2": cmdStream2()
case "observe": cmdObserve()
    default:
        print("unknown subcommand: \(cmd)")
        print("usage: photokit-spike [basic|cloudids|scopes|resources|filesize|stream]")
        exit(2)
    }
    print("DONE: \(cmd)")
    exit(0)
}
dispatchMain()
