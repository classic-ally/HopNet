import Foundation
import Photos

/// The daemon-side implementation of the Rust scheduler's fetch callbacks.
/// Blocking by contract; the scheduler invokes it from worker threads only.
public final class PhotoKitFetcher: PhotoResourceFetcher {
    private let lock = NSLock()
    private var inflight: Set<PHAssetResourceDataRequestID> = []
    private var cancelled = false

    public init() {}

    /// SIGTERM path: cancel every inflight PhotoKit data request so shutdown
    /// doesn't wait for downloads delivering into poisoned sinks.
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
}

extension NSLock {
    func withLock<T>(_ body: () -> T) -> T {
        lock()
        defer { unlock() }
        return body()
    }
}
