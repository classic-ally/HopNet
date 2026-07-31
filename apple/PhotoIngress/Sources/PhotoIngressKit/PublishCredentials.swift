import Foundation
import Security

/// HopNet publish credentials for the daemon's publish tick: the node base
/// URL and an RFC-012 device token, provisioned into the keychain by the
/// HopNet app (`ensure_photo_ingress_device_token`, mirroring the
/// FileProvider pattern). Credential storage is a platform concern — the
/// Rust core receives the values via `FfiDaemonOptions` and never touches
/// the keychain itself.
public enum PublishCredentials {
    public static let service = "com.hopnet.desktop.photo-ingress"

    public struct Credentials {
        public let baseUrl: String
        public let deviceToken: String
    }

    /// Load both items; nil when either is absent (publishing off — the
    /// daemon runs ingest-only until the app provisions the token).
    public static func load() -> Credentials? {
        guard let token = loadItem(account: "api_key"),
              let url = loadItem(account: "base_url") else {
            return nil
        }
        return Credentials(baseUrl: url, deviceToken: token)
    }

    /// Library provisioning values the app stores alongside the token
    /// (enablement flow): the personal library's blob root and the optional
    /// remote sidecar root. Absent until the user enables photo ingress.
    public static func loadBlobRoot() -> String? {
        loadItem(account: "blob_root")
    }

    public static func loadSidecarRootRemote() -> String? {
        loadItem(account: "sidecar_root_remote")
    }

    private static func loadItem(account: String) -> String? {
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: account,
            kSecReturnData as String: true,
            kSecMatchLimit as String: kSecMatchLimitOne,
        ]
        var result: AnyObject?
        guard SecItemCopyMatching(query as CFDictionary, &result) == errSecSuccess,
              let data = result as? Data,
              let string = String(data: data, encoding: .utf8) else {
            return nil
        }
        return string
    }
}

/// The daemon's live credential source: re-queried by the Rust publish loop
/// after an unreachable pass (`RefreshingPublisher`) so a GUI relaunch on a
/// new ephemeral port heals without a daemon restart. Cheap SecItem read.
public final class KeychainCredentialsProvider: PublishCredentialsProvider {
    public init() {}

    public func current() -> FfiPublishCredentials? {
        guard let creds = PublishCredentials.load() else { return nil }
        return FfiPublishCredentials(nodeUrl: creds.baseUrl, deviceToken: creds.deviceToken)
    }
}
