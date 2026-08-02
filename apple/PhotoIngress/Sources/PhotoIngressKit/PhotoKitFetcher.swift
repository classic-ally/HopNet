import AppKit
import Foundation
import ImageIO
import Photos
import UniformTypeIdentifiers

/// The daemon-side implementation of the Rust scheduler's fetch callbacks.
/// Blocking by contract; the scheduler invokes it from worker threads only.
public final class PhotoKitFetcher: PhotoResourceFetcher {
    private let lock = NSLock()
    private var inflight: Set<PHAssetResourceDataRequestID> = []
    private var cancelled = false

    public init() {}

    /// SIGTERM path: cancel every inflight PhotoKit data request so shutdown
    /// doesn't wait for downloads delivering into poisoned sinks. Rendition
    /// requests are synchronous and uncancellable — the pre-check in
    /// fetchRendition bounds the wait to one local, fast request.
    public func cancelAll() {
        lock.lock()
        cancelled = true
        let ids = inflight
        inflight.removeAll()
        lock.unlock()
        for id in ids {
            PHAssetResourceManager.default().cancelDataRequest(id)
        }
    }

    public func descriptorFor(localId: String) throws -> FfiAssetDescriptor {
        do {
            return try extractDescriptor(asset: try fetchAsset(localId: localId))
        } catch let e as ExtractionError {
            switch e {
            case .assetNotFound:
                throw FfiError.AssetUnavailable(msg: String(describing: e))
            default:
                throw FfiError.FetchTransient(msg: String(describing: e))
            }
        }
    }

    public func fetchResource(request: FfiFetchRequest, sink: ChunkSink) throws {
        if lock.withLock({ cancelled }) { throw FfiError.Cancelled }
        guard let asset = try? fetchAsset(localId: request.localId) else {
            throw FfiError.AssetUnavailable(msg: "no PHAsset for \(request.localId)")
        }
        // Thumbnail sentinels have no backing PHAssetResource — they are
        // PHImageManager renditions.
        if request.phResourceType == phSentinelThumbnailSmall
            || request.phResourceType == phSentinelThumbnailMedium {
            try fetchRendition(
                asset: asset,
                small: request.phResourceType == phSentinelThumbnailSmall,
                sink: sink)
            return
        }
        guard let resource = PHAssetResource.assetResources(for: asset)
            .first(where: { Int32($0.type.rawValue) == request.phResourceType })
        else {
            throw FfiError.AssetUnavailable(
                msg: "resource type \(request.phResourceType) not on \(request.localId)")
        }

        let options = PHAssetResourceRequestOptions()
        options.isNetworkAccessAllowed = true

        let sema = DispatchSemaphore(value: 0)
        var writeError: Error?
        var fetchError: Error?

        let id = PHAssetResourceManager.default().requestData(
            for: resource,
            options: options,
            dataReceivedHandler: { data in
                guard writeError == nil else { return }  // failing; drain quietly
                do { try sink.write(chunk: data) } catch { writeError = error }
            },
            completionHandler: { error in
                fetchError = error
                sema.signal()
            }
        )
        lock.withLock { _ = inflight.insert(id) }
        // Poll-wait: cancelDataRequest does not reliably invoke the
        // completion handler (observed hang under SIGTERM), so a cancelled
        // fetch must be able to abandon the wait itself. Post-abandon chunk
        // deliveries hit a consumed sink and error harmlessly.
        var abandoned = false
        while sema.wait(timeout: .now() + 1.0) == .timedOut {
            if lock.withLock({ cancelled }) {
                abandoned = true
                break
            }
        }
        lock.withLock { _ = inflight.remove(id) }
        if abandoned {
            throw FfiError.Cancelled
        }

        // Write errors (already-classified FfiError, e.g. Cancelled) take
        // precedence: a PhotoKit "cancelled" completion caused by our own
        // sink failure must not masquerade as a PhotoKit problem.
        if let e = writeError { throw classifyFetchError(e) }
        if let e = fetchError { throw classifyFetchError(e) }
        // Success: return WITHOUT finishing — commit control stays in Rust.
    }

    /// Fetch a JPEG rendition (~256px small / ~1024px medium). Primary:
    /// PHImageManager.requestImage in SYNCHRONOUS mode — async delivery was
    /// observed returning nil-image/nil-error in the daemon (non-app)
    /// context; synchronous delivers on this already-blocking worker thread
    /// with no runloop dependency. Fallback: full image data (the request
    /// path the PhotoKit spike verified in a CLI context) downscaled via
    /// ImageIO. Cancellation is a pre-check only — renditions are local and
    /// fast, so SIGTERM waits at most one in-flight rendition.
    private func fetchRendition(asset: PHAsset, small: Bool, sink: ChunkSink) throws {
        if lock.withLock({ cancelled }) { throw FfiError.Cancelled }
        let edge: CGFloat = small ? 256 : 1024

        let options = PHImageRequestOptions()
        options.deliveryMode = .highQualityFormat  // single, final delivery
        options.resizeMode = .exact
        options.isNetworkAccessAllowed = true
        options.isSynchronous = true

        var result: NSImage?
        var info: [AnyHashable: Any]?
        PHImageManager.default().requestImage(
            for: asset,
            targetSize: CGSize(width: edge, height: edge),
            contentMode: .aspectFit,
            options: options
        ) { image, requestInfo in
            // Synchronous + .highQualityFormat should deliver exactly one
            // final image; skip any degraded delivery rather than archive it.
            if (requestInfo?[PHImageResultIsDegradedKey] as? Bool) == true { return }
            result = image
            info = requestInfo
        }

        if let error = info?[PHImageErrorKey] as? NSError {
            throw classifyFetchError(error)
        }
        if let image = result,
           let cg = image.cgImage(forProposedRect: nil, context: nil, hints: nil) {
            try sink.write(chunk: encodeJPEG(cg))
            return
        }
        try fetchRenditionViaData(asset: asset, edge: edge, sink: sink, primaryInfo: info)
        // Success: return WITHOUT finishing — commit control stays in Rust.
    }

    /// Fallback rendition path: full image data via
    /// requestImageDataAndOrientation (spike-proven in the CLI context),
    /// downscaled with CGImageSource thumbnailing (native HEIC decode).
    private func fetchRenditionViaData(
        asset: PHAsset, edge: CGFloat, sink: ChunkSink, primaryInfo: [AnyHashable: Any]?
    ) throws {
        let options = PHImageRequestOptions()
        options.isNetworkAccessAllowed = true
        options.isSynchronous = true

        var data: Data?
        var info: [AnyHashable: Any]?
        PHImageManager.default().requestImageDataAndOrientation(for: asset, options: options) {
            d, _, _, i in
            data = d
            info = i
        }
        if let error = info?[PHImageErrorKey] as? NSError {
            throw classifyFetchError(error)
        }
        guard let d = data,
              let source = CGImageSourceCreateWithData(d as CFData, nil),
              let cg = CGImageSourceCreateThumbnailAtIndex(source, 0, [
                  kCGImageSourceCreateThumbnailFromImageAlways: true,
                  kCGImageSourceThumbnailMaxPixelSize: edge,
                  kCGImageSourceCreateThumbnailWithTransform: true,
              ] as CFDictionary)
        else {
            throw FfiError.FetchTransient(
                msg: "rendition failed both paths; primary info=\(String(describing: primaryInfo)) "
                    + "fallback data=\(data.map { String($0.count) } ?? "nil") "
                    + "info=\(String(describing: info))")
        }
        try sink.write(chunk: encodeJPEG(cg))
    }
}

/// Encode a CGImage as JPEG via ImageIO.
func encodeJPEG(_ image: CGImage, quality: Double = 0.8) throws -> Data {
    let data = NSMutableData()
    guard let dest = CGImageDestinationCreateWithData(
        data, UTType.jpeg.identifier as CFString, 1, nil)
    else {
        throw FfiError.FetchTransient(msg: "CGImageDestination create failed")
    }
    CGImageDestinationAddImage(
        dest, image,
        [kCGImageDestinationLossyCompressionQuality: quality] as CFDictionary)
    guard CGImageDestinationFinalize(dest) else {
        throw FfiError.FetchTransient(msg: "JPEG encode finalize failed")
    }
    return data as Data
}

extension NSLock {
    func withLock<T>(_ body: () -> T) -> T {
        lock()
        defer { unlock() }
        return body()
    }
}
