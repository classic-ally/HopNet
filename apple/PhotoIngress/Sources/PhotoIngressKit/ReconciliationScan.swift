import Foundation
import Photos

public enum ScanError: Error, CustomStringConvertible {
    case notAuthorized
    case extraction(String, Error)

    public var description: String {
        switch self {
        case .notAuthorized:
            return "refusing to scan without full Photos authorization — an empty fetch must never drive deletion synthesis"
        case .extraction(let id, let err):
            return "scan extraction failed for \(id): \(err)"
        }
    }
}

/// One full reconciliation scan (spec §Discovery: startup + periodic):
/// enumerate every asset, probe it lightly (identity + scope + modification
/// date — NO per-asset resource enumeration, the expensive PhotoKit call),
/// push full descriptors only for probe misses, then close the scan
/// (offline-deletion synthesis + gave-up reset happen Rust-side).
///
/// Throws if a scan is already active (`beginScan` errors) — periodic timers
/// simply skip that tick.
///
/// `limit` bounds the enumeration (testing against scratch data-dirs on a
/// full-size library); `enumerated` is then the probed count, so deletion
/// synthesis stays scoped to what was actually looked at.
public func runScan(session: IngressSession, retryCap: Int64, limit: Int? = nil) throws
    -> FfiScanSummary
{
    guard PHPhotoLibrary.authorizationStatus(for: .readWrite) == .authorized else {
        throw ScanError.notAuthorized
    }

    try session.beginScan()
    do {
        let fetch = PHAsset.fetchAssets(with: nil)
        let bound = limit.map { min($0, fetch.count) } ?? fetch.count

        // Batch the cloud-identifier mapping (spike: ~39ms per 1k chunk;
        // per-asset calls would dominate the scan).
        var localIds: [String] = []
        localIds.reserveCapacity(bound)
        fetch.enumerateObjects { asset, index, stop in
            if index >= bound { stop.pointee = true; return }
            localIds.append(asset.localIdentifier)
        }
        var cloudIds: [String: String] = [:]
        var i = 0
        while i < localIds.count {
            let chunk = Array(localIds[i..<min(i + 1000, localIds.count)])
            let mappings = PHPhotoLibrary.shared().cloudIdentifierMappings(forLocalIdentifiers: chunk)
            for (local, result) in mappings {
                if let cloud = try? result.get().stringValue {
                    cloudIds[local] = cloud
                }
            }
            i += 1000
        }

        let isoFormatter = ISO8601DateFormatter()
        isoFormatter.formatOptions = [.withInternetDateTime]

        var pending: [FfiAssetDescriptor] = []
        var failure: Error?
        fetch.enumerateObjects { asset, index, stop in
            if index >= bound { stop.pointee = true; return }
            do {
                guard let inScope = asset.value(forKey: "participatesInLibraryScope") as? Bool
                else { throw ExtractionError.scopeUnavailable(asset.localIdentifier) }
                let probe = FfiScanProbe(
                    localId: asset.localIdentifier,
                    cloudId: cloudIds[asset.localIdentifier],
                    scope: inScope ? .shared : .personal,
                    assetModifiedAt: asset.modificationDate.map { isoFormatter.string(from: $0) }
                )
                if case .needsFull = try session.scanAsset(probe: probe) {
                    pending.append(try extractDescriptor(
                        asset: asset, cloudId: cloudIds[asset.localIdentifier]))
                    if pending.count >= 200 {
                        try session.observeDescriptors(descs: pending)
                        pending.removeAll(keepingCapacity: true)
                    }
                }
            } catch {
                failure = ScanError.extraction(asset.localIdentifier, error)
                stop.pointee = true
            }
        }
        if let failure { throw failure }
        if !pending.isEmpty {
            try session.observeDescriptors(descs: pending)
        }

        return try session.finishScan(enumerated: UInt64(bound), retryCap: retryCap)
    } catch {
        // An incomplete seen set must never drive deletion synthesis.
        session.abortScan()
        throw error
    }
}
