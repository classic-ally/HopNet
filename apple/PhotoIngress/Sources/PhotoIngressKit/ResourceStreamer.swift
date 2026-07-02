import Foundation
import Photos

public enum StreamError: Error, CustomStringConvertible {
    case photoKit(String)
    case sinkWrite(String)

    public var description: String {
        switch self {
        case .photoKit(let msg): return "PhotoKit fetch failed: \(msg)"
        case .sinkWrite(let msg): return "sink write failed: \(msg)"
        }
    }
}

/// Pump one PHAssetResource's bytes into a ChunkSink and finish it.
///
/// Chunks are written synchronously inside `dataReceivedHandler` — PhotoKit's
/// delivery queue blocks on our disk write, which is natural backpressure
/// and preserves ordering with zero queue machinery. (If delivery ever moves
/// to the main queue, switch to a serial background queue + semaphore.)
///
/// Never call from the main thread.
public func streamResource(
    _ resource: PHAssetResource,
    into sink: ChunkSink,
    networkAllowed: Bool = true
) throws -> FfiWriteOutcome {
    let options = PHAssetResourceRequestOptions()
    options.isNetworkAccessAllowed = networkAllowed

    let sema = DispatchSemaphore(value: 0)
    var writeError: Error?
    var fetchError: Error?

    PHAssetResourceManager.default().requestData(
        for: resource,
        options: options,
        dataReceivedHandler: { data in
            guard writeError == nil else { return }  // already failing; drain
            do {
                try sink.write(chunk: data)
            } catch {
                writeError = error
            }
        },
        completionHandler: { error in
            fetchError = error
            sema.signal()
        }
    )
    sema.wait()

    if let e = fetchError {
        sink.abort()
        throw StreamError.photoKit(String(describing: e))
    }
    if let e = writeError {
        sink.abort()
        throw StreamError.sinkWrite(String(describing: e))
    }
    return try sink.finish()
}
