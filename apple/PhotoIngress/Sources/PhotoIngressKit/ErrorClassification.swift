import Foundation
import Photos

/// Classify a PhotoKit/Foundation error into the FFI error taxonomy the Rust
/// scheduler dispositions on (spec §Failure Handling).
public func classifyFetchError(_ error: Error) -> FfiError {
    // Errors that are already classified (e.g. a sink write that failed with
    // Cancelled) pass through untouched.
    if let ffi = error as? FfiError { return ffi }

    let ns = error as NSError
    // Spike-verified: CloudPhotoLibraryErrorDomain 1005 = local disk
    // pressure; cloudphotod refuses downloads below a headroom threshold.
    if ns.domain == "CloudPhotoLibraryErrorDomain" && ns.code == 1005 {
        return .LocalDiskPressure
    }
    if ns.domain == NSCocoaErrorDomain && ns.code == NSUserCancelledError {
        return .Cancelled
    }
    if ns.domain == "PHPhotosErrorDomain" && ns.code == PHPhotosError.userCancelled.rawValue {
        return .Cancelled
    }
    return .FetchTransient(msg: String(describing: error))
}
