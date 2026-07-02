import Foundation
import Photos

public enum ExtractionError: Error, CustomStringConvertible {
    case notAuthorized
    case assetNotFound(String)
    case scopeUnavailable(String)

    public var description: String {
        switch self {
        case .notAuthorized:
            return "Photos access not authorized"
        case .assetNotFound(let id):
            return "no PHAsset for local identifier \(id)"
        case .scopeUnavailable(let id):
            return "participatesInLibraryScope returned nil for \(id) — cannot classify scope, refusing to default to personal"
        }
    }
}

public func ensureAuthorized() throws {
    let status = PHPhotoLibrary.authorizationStatus(for: .readWrite)
    if status == .authorized { return }
    guard status == .notDetermined else { throw ExtractionError.notAuthorized }
    let sema = DispatchSemaphore(value: 0)
    var granted: PHAuthorizationStatus = .notDetermined
    PHPhotoLibrary.requestAuthorization(for: .readWrite) { s in
        granted = s
        sema.signal()
    }
    print("waiting for Photos authorization prompt…")
    sema.wait()
    guard granted == .authorized else { throw ExtractionError.notAuthorized }
}

public func fetchAsset(localId: String) throws -> PHAsset {
    let result = PHAsset.fetchAssets(withLocalIdentifiers: [localId], options: nil)
    guard let asset = result.firstObject else {
        throw ExtractionError.assetNotFound(localId)
    }
    return asset
}

private let isoFormatter: ISO8601DateFormatter = {
    let f = ISO8601DateFormatter()
    f.formatOptions = [.withInternetDateTime]
    return f
}()

private func mediaSubtypeFlags(_ asset: PHAsset) -> [String] {
    var flags: [String] = []
    let st = asset.mediaSubtypes
    if st.contains(.photoHDR) { flags.append("hdr") }
    if st.contains(.photoScreenshot) { flags.append("screenshot") }
    if st.contains(.photoPanorama) { flags.append("panorama") }
    if st.contains(.videoHighFrameRate) { flags.append("slomo") }
    if st.contains(.videoTimelapse) { flags.append("timelapse") }
    if st.contains(.videoScreenRecording) { flags.append("screen_recording") }
    if st.contains(.photoDepthEffect) { flags.append("portrait") }
    return flags
}

/// Build the FFI descriptor for one asset. Spike-verified field sources:
/// cloud id via batch mapping, scope via the `participatesInLibraryScope`
/// KVC property (nil = hard error, never default to personal — spec
/// §Library scope detection), sizes via the `fileSize` KVC key.
public func extractDescriptor(asset: PHAsset) throws -> FfiAssetDescriptor {
    // Cloud identifier (single-asset batch mapping).
    let mappings = PHPhotoLibrary.shared()
        .cloudIdentifierMappings(forLocalIdentifiers: [asset.localIdentifier])
    let cloudId: String? = mappings[asset.localIdentifier].flatMap { try? $0.get().stringValue }
    return try extractDescriptor(asset: asset, cloudId: cloudId)
}

/// Overload taking a pre-mapped cloud identifier — the reconciliation scan
/// batches `cloudIdentifierMappings` in 1k chunks and must not pay a
/// per-asset mapping call on the NeedsFull path.
public func extractDescriptor(asset: PHAsset, cloudId: String?) throws -> FfiAssetDescriptor {
    // Library scope: private KVC property, verified exact in the spike.
    guard let inScope = asset.value(forKey: "participatesInLibraryScope") as? Bool else {
        throw ExtractionError.scopeUnavailable(asset.localIdentifier)
    }
    let scope: FfiLibraryScope = inScope ? .shared : .personal

    let isLive = asset.mediaSubtypes.contains(.photoLive)
    let mediaType: FfiMediaType =
        asset.mediaType == .video ? .video : (isLive ? .livePhoto : .image)

    let resources = PHAssetResource.assetResources(for: asset).map { r in
        FfiResourceDescriptor(
            phResourceType: Int32(r.type.rawValue),
            uti: r.uniformTypeIdentifier,
            originalFilename: r.originalFilename,
            expectedSize: (r.value(forKey: "fileSize") as? Int64).map { UInt64($0) },
            locallyAvailable: r.value(forKey: "locallyAvailable") as? Bool
        )
    }

    let capture = FfiCaptureMetadata(
        // NOTE: PHAsset.creationDate is an absolute instant; PhotoKit exposes
        // no capture-time UTC offset. Serialized as UTC for the slice; the
        // EXIF pass (Phase 4 metadata extraction) can refine this.
        capturedAt: asset.creationDate.map { isoFormatter.string(from: $0) },
        pixelWidth: UInt32(asset.pixelWidth),
        pixelHeight: UInt32(asset.pixelHeight),
        orientation: nil,  // EXIF orientation comes with the Phase 4 metadata pass
        durationMs: asset.duration > 0 ? UInt64(asset.duration * 1000) : nil,
        camera: nil,       // camera make/model requires EXIF; Phase 4
        location: asset.location.map {
            FfiLocation(lat: $0.coordinate.latitude, lon: $0.coordinate.longitude)
        }
    )

    return FfiAssetDescriptor(
        localId: asset.localIdentifier,
        cloudId: cloudId,
        scope: scope,
        mediaType: mediaType,
        mediaSubtypes: mediaSubtypeFlags(asset),
        assetModifiedAt: asset.modificationDate.map { isoFormatter.string(from: $0) },
        favorite: asset.isFavorite,
        burst: asset.representsBurst
            ? asset.burstIdentifier.map { FfiBurstInfo(burstIdentifier: $0, isPick: true) }
            : nil,
        capture: capture,
        resources: resources
    )
}
