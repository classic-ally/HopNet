import Foundation
import Photos

/// PhotoKit change observer → daemon inbox (spec §Discovery: observer events).
///
/// Spike-verified behavior this leans on: every user action arrives as
/// incremental per-asset diffs; deletions are `removed`; restores arrive as
/// `inserted` with the same identity (the Rust side's match precedence turns
/// them back into restores); moves between libraries are `changed` with a
/// flipped scope; frequent unrelated notifications carry no changeDetails and
/// must be ignored silently.
public final class LibraryObserver: NSObject, PHPhotoLibraryChangeObserver {
    private let session: IngressSession
    /// Fired when PhotoKit reports a non-incremental change (full reload) —
    /// per-asset diffing is impossible, so the owner triggers a rescan.
    private let onFullReload: () -> Void
    /// Serializes handling; PhotoKit's callback queue must never be blocked
    /// by descriptor extraction.
    private let queue = DispatchQueue(label: "photo-ingress.observer")
    private var fetchResult: PHFetchResult<PHAsset>?

    public init(session: IngressSession, onFullReload: @escaping () -> Void) {
        self.session = session
        self.onFullReload = onFullReload
    }

    /// Register BEFORE the startup scan — the missed window closes through
    /// idempotent duplicate delivery, an unobserved one does not.
    public func start() {
        fetchResult = PHAsset.fetchAssets(with: nil)
        PHPhotoLibrary.shared().register(self)
    }

    public func stop() {
        PHPhotoLibrary.shared().unregisterChangeObserver(self)
    }

    public func photoLibraryDidChange(_ changeInstance: PHChange) {
        queue.async { self.handle(changeInstance) }
    }

    private func handle(_ change: PHChange) {
        guard let fetchResult,
              let details = change.changeDetails(for: fetchResult)
        else { return }  // collection-level noise: tolerate silently (spike)
        self.fetchResult = details.fetchResultAfterChanges

        guard details.hasIncrementalChanges else {
            onFullReload()
            return
        }

        var descriptors: [FfiAssetDescriptor] = []
        for asset in details.insertedObjects + details.changedObjects {
            do {
                descriptors.append(try extractDescriptor(asset: asset))
            } catch ExtractionError.scopeUnavailable(let id) {
                // Fail loud (spec §Library scope detection): silently
                // guessing a scope would route bytes to the wrong ACL domain.
                FileHandle.standardError.write(Data(
                    "fatal: scope unavailable for \(id) — stopping\n".utf8))
                exit(1)
            } catch {
                print("observer: skipping \(asset.localIdentifier): \(error)")
            }
        }
        if !descriptors.isEmpty {
            do { try session.observeDescriptors(descs: descriptors) }
            catch { print("observer: push failed: \(error)") }
        }

        let removed = details.removedObjects.map(\.localIdentifier)
        if !removed.isEmpty {
            session.observeRemoved(localIds: removed)
        }
    }
}
