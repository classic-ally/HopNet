/*
HopNet API Client
Swift implementation of the API client based on the Rust version
*/

import Foundation
import Security
import os.log

// MARK: - Configuration

public struct FileProviderConfig {
    public let baseUrl: String
    public let apiKey: String
    
    public init(baseUrl: String, apiKey: String) {
        self.baseUrl = baseUrl
        self.apiKey = apiKey
    }
    
    /// Load configuration from keychain (matches Rust keychain constants)
    public static func loadFromKeychain() throws -> FileProviderConfig {
        let logger = Logger(subsystem: "com.hopnet.desktop.fileprovider", category: "config")
        
        logger.debug("Attempting to load API key from keychain...")
        let apiKey = try KeychainHelper.loadItem(service: "com.hopnet.desktop.fileprovider", account: "api_key")
        logger.debug("✅ Successfully loaded API key from keychain")
        
        logger.debug("Attempting to load base URL from keychain...")
        let baseUrl = try KeychainHelper.loadItem(service: "com.hopnet.desktop.fileprovider", account: "base_url")
        logger.debug("✅ Successfully loaded base URL from keychain: \(baseUrl)")
        
        return FileProviderConfig(baseUrl: baseUrl, apiKey: apiKey)
    }
}

// MARK: - API Errors

public enum ApiError: Error, CustomStringConvertible {
    case network(Error)
    case notFound
    case unauthorized
    case notReady
    case serverError(String)
    case parseError(String)
    case invalidUrl
    
    public var description: String {
        switch self {
        case .network(let error):
            return "Network error: \(error.localizedDescription)"
        case .notFound:
            return "Resource not found"
        case .unauthorized:
            return "Unauthorized - check API key"
        case .notReady:
            return "HopNet not ready - sign in to main app required"
        case .serverError(let message):
            return "Server error: \(message)"
        case .parseError(let message):
            return "Parse error: \(message)"
        case .invalidUrl:
            return "Invalid URL"
        }
    }
}

// MARK: - API Client

public class HopNetApiClient {
    private let config: FileProviderConfig
    private let session: URLSession
    private let logger = Logger(subsystem: "com.hopnet.desktop.fileprovider", category: "api")
    
    public init(config: FileProviderConfig) {
        self.config = config
        self.session = URLSession.shared
    }
    
    // MARK: - Health Check
    
    public func healthCheck() async throws -> HealthStatus {
        let urlString = "\(config.baseUrl)/api/integrations/fileprovider/health"
        logger.debug("Checking health at: \(urlString)")
        
        guard let url = URL(string: urlString) else {
            throw ApiError.invalidUrl
        }
        
        var request = URLRequest(url: url)
        request.setValue("Bearer \(config.apiKey)", forHTTPHeaderField: "Authorization")
        
        do {
            let (data, response) = try await session.data(for: request)
            
            guard let httpResponse = response as? HTTPURLResponse else {
                throw ApiError.serverError("Invalid response type")
            }
            
            switch httpResponse.statusCode {
            case 200:
                let healthResponse = try JSONDecoder().decode(HealthResponse.self, from: data)
                return healthResponse.status
            case 401:
                throw ApiError.unauthorized
            case 428:
                throw ApiError.notReady
            default:
                throw ApiError.serverError("Unexpected status: \(httpResponse.statusCode)")
            }
        } catch let error as DecodingError {
            throw ApiError.parseError(error.localizedDescription)
        } catch let error as ApiError {
            throw error
        } catch {
            throw ApiError.network(error)
        }
    }
    
    // MARK: - Enumerate
    
    public func enumerate(parentPath: String? = nil, pageToken: String? = nil) async throws -> EnumerateResponse {
        let urlString = "\(config.baseUrl)/api/integrations/fileprovider/enumerate"
        
        guard var urlComponents = URLComponents(string: urlString) else {
            throw ApiError.invalidUrl
        }
        
        // Add query parameters
        var queryItems: [URLQueryItem] = []
        if let path = parentPath {
            queryItems.append(URLQueryItem(name: "parent_path", value: path))
        }
        if let page = pageToken {
            queryItems.append(URLQueryItem(name: "page", value: page))
        }
        urlComponents.queryItems = queryItems.isEmpty ? nil : queryItems
        
        guard let url = urlComponents.url else {
            throw ApiError.invalidUrl
        }
        
        var request = URLRequest(url: url)
        request.setValue("Bearer \(config.apiKey)", forHTTPHeaderField: "Authorization")
        
        logger.debug("Enumerating folder with URL: \(url)")
        
        do {
            let (data, response) = try await session.data(for: request)
            
            guard let httpResponse = response as? HTTPURLResponse else {
                throw ApiError.serverError("Invalid response type")
            }
            
            switch httpResponse.statusCode {
            case 200:
                let enumerateResponse = try JSONDecoder().decode(EnumerateResponse.self, from: data)
                return enumerateResponse
            case 404:
                throw ApiError.notFound
            case 401:
                throw ApiError.unauthorized
            case 428:
                throw ApiError.notReady
            default:
                throw ApiError.serverError("Enumerate failed: \(httpResponse.statusCode)")
            }
        } catch let error as DecodingError {
            throw ApiError.parseError(error.localizedDescription)
        } catch let error as ApiError {
            throw error
        } catch {
            throw ApiError.network(error)
        }
    }
    
    // Identifier-based enumerate function for FileProvider
    public func enumerate(parentItemIdentifier: String, pageToken: String? = nil) async throws -> EnumerateResponse {
        let urlString = "\(config.baseUrl)/api/integrations/fileprovider/enumerate"
        
        guard var urlComponents = URLComponents(string: urlString) else {
            throw ApiError.invalidUrl
        }
        
        // Add query parameters
        var queryItems: [URLQueryItem] = []
        queryItems.append(URLQueryItem(name: "parent_item_identifier", value: parentItemIdentifier))
        if let page = pageToken {
            queryItems.append(URLQueryItem(name: "page", value: page))
        }
        urlComponents.queryItems = queryItems
        
        guard let url = urlComponents.url else {
            throw ApiError.invalidUrl
        }
        
        var request = URLRequest(url: url)
        request.setValue("Bearer \(config.apiKey)", forHTTPHeaderField: "Authorization")
        
        logger.debug("Enumerating folder with identifier: \(parentItemIdentifier)")
        
        do {
            let (data, response) = try await session.data(for: request)
            
            guard let httpResponse = response as? HTTPURLResponse else {
                throw ApiError.serverError("Invalid response type")
            }
            
            switch httpResponse.statusCode {
            case 200:
                let enumerateResponse = try JSONDecoder().decode(EnumerateResponse.self, from: data)
                return enumerateResponse
            case 401:
                throw ApiError.unauthorized
            case 404:
                throw ApiError.notFound
            case 428:
                throw ApiError.notReady
            default:
                throw ApiError.serverError("Enumerate failed: \(httpResponse.statusCode)")
            }
        } catch let error as ApiError {
            throw error
        } catch {
            throw ApiError.network(error)
        }
    }
    
    // MARK: - Changes
    
    public func getChanges(parentPath: String? = nil, sinceHeight: Int32? = nil) async throws -> ChangesResponse {
        let urlString = "\(config.baseUrl)/api/integrations/fileprovider/changes"
        
        guard var urlComponents = URLComponents(string: urlString) else {
            throw ApiError.invalidUrl
        }
        
        // Add query parameters
        var queryItems: [URLQueryItem] = []
        if let path = parentPath {
            queryItems.append(URLQueryItem(name: "parent_path", value: path))
        }
        if let height = sinceHeight {
            queryItems.append(URLQueryItem(name: "since_height", value: String(height)))
        }
        urlComponents.queryItems = queryItems.isEmpty ? nil : queryItems
        
        guard let url = urlComponents.url else {
            throw ApiError.invalidUrl
        }
        
        var request = URLRequest(url: url)
        request.setValue("Bearer \(config.apiKey)", forHTTPHeaderField: "Authorization")
        
        logger.debug("Getting changes with URL: \(url)")
        
        do {
            let (data, response) = try await session.data(for: request)
            
            guard let httpResponse = response as? HTTPURLResponse else {
                throw ApiError.serverError("Invalid response type")
            }
            
            switch httpResponse.statusCode {
            case 200:
                let changesResponse = try JSONDecoder().decode(ChangesResponse.self, from: data)
                return changesResponse
            case 404:
                throw ApiError.notFound
            case 401:
                throw ApiError.unauthorized
            case 428:
                throw ApiError.notReady
            default:
                throw ApiError.serverError("Changes request failed: \(httpResponse.statusCode)")
            }
        } catch let error as DecodingError {
            throw ApiError.parseError(error.localizedDescription)
        } catch let error as ApiError {
            throw error
        } catch {
            throw ApiError.network(error)
        }
    }
    
    // MARK: - Delete
    
    public func deleteItem(identifier: String, recursive: Bool) async throws {
        let urlString = "\(config.baseUrl)/api/integrations/fileprovider/delete"
        logger.debug("Deleting item with identifier: \(identifier), recursive: \(recursive)")
        
        guard let url = URL(string: urlString) else {
            throw ApiError.invalidUrl
        }
        
        // Create the delete request body
        let deleteRequest = DeleteItemRequest(
            identifier: identifier,
            recursive: recursive
        )
        
        var request = URLRequest(url: url)
        request.httpMethod = "DELETE"
        request.setValue("Bearer \(config.apiKey)", forHTTPHeaderField: "Authorization")
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        
        // Encode the request body
        do {
            request.httpBody = try JSONEncoder().encode(deleteRequest)
        } catch {
            throw ApiError.parseError("Failed to encode delete request")
        }
        
        do {
            let (_, response) = try await session.data(for: request)
            
            guard let httpResponse = response as? HTTPURLResponse else {
                throw ApiError.serverError("Invalid response type")
            }
            
            switch httpResponse.statusCode {
            case 200:
                logger.info("Successfully deleted item: \(identifier)")
                return
            case 400:
                throw ApiError.serverError("Invalid identifier format")
            case 401:
                throw ApiError.unauthorized
            case 403:
                throw ApiError.serverError("Cannot delete root container")
            case 404:
                throw ApiError.notFound
            case 409:
                throw ApiError.serverError("Folder not empty")
            case 428:
                throw ApiError.notReady
            default:
                throw ApiError.serverError("Delete failed: \(httpResponse.statusCode)")
            }
        } catch let error as ApiError {
            throw error
        } catch {
            throw ApiError.network(error)
        }
    }
    
    // MARK: - Download
    
    public func downloadFile(identifier: String, progressHandler: @escaping (Double) -> Void = { _ in }) async throws -> URL {
        let urlString = "\(config.baseUrl)/api/integrations/fileprovider/download"
        
        guard var urlComponents = URLComponents(string: urlString) else {
            throw ApiError.invalidUrl
        }
        
        // Use generated DownloadQuery type for consistency
        let downloadQuery = DownloadQuery(identifier: identifier)
        urlComponents.queryItems = [URLQueryItem(name: "identifier", value: downloadQuery.identifier)]
        
        guard let url = urlComponents.url else {
            throw ApiError.invalidUrl
        }
        
        var request = URLRequest(url: url)
        request.setValue("Bearer \(config.apiKey)", forHTTPHeaderField: "Authorization")
        
        logger.debug("Downloading file with identifier: \(identifier)")
        
        do {
            // Use URLSession download task for progress tracking and streaming to disk
            let (tempUrl, response) = try await session.download(for: request)
            
            guard let httpResponse = response as? HTTPURLResponse else {
                throw ApiError.serverError("Invalid response type")
            }
            
            logger.debug("Download response status: \(httpResponse.statusCode)")
            logger.debug("Download temp URL: \(tempUrl)")
            
            // Check if downloaded file exists and has content
            let downloadedFileExists = FileManager.default.fileExists(atPath: tempUrl.path)
            logger.debug("Downloaded file exists: \(downloadedFileExists)")
            
            if downloadedFileExists {
                do {
                    let fileSize = try FileManager.default.attributesOfItem(atPath: tempUrl.path)[.size] as? UInt64 ?? 0
                    logger.debug("Downloaded file size: \(fileSize) bytes")
                } catch {
                    logger.error("Failed to get file size: \(error)")
                }
            }
            
            switch httpResponse.statusCode {
            case 200:
                // Return URLSession temp file - extension will move it to FileProvider temp dir
                logger.debug("Returning URLSession temp file: \(tempUrl)")
                
                // Force log the actual path (bypass privacy redaction)
                logger.debug("🔍 URLSession temp path: \(tempUrl.path, privacy: .public)")
                
                // Check HTTP Content-Length header vs actual file size
                let contentLength = httpResponse.expectedContentLength
                logger.debug("🌐 HTTP Content-Length header: \(contentLength) bytes")
                
                // Verify the URLSession temp file
                let fileExists = FileManager.default.fileExists(atPath: tempUrl.path)
                logger.debug("🔍 URLSession file exists: \(fileExists)")
                
                if fileExists {
                    do {
                        let fileSize = try FileManager.default.attributesOfItem(atPath: tempUrl.path)[.size] as? UInt64 ?? 0
                        logger.debug("🔍 URLSession file size on disk: \(fileSize) bytes")
                        logger.info("Successfully downloaded \(fileSize) bytes to URLSession temp: \(tempUrl.path)")
                    } catch {
                        logger.error("Failed to verify URLSession temp file size: \(error)")
                    }
                }
                
                return tempUrl
            case 404:
                throw ApiError.notFound
            case 401:
                throw ApiError.unauthorized
            case 428:
                throw ApiError.notReady
            default:
                throw ApiError.serverError("Download failed: \(httpResponse.statusCode)")
            }
        } catch let error as ApiError {
            throw error
        } catch {
            throw ApiError.network(error)
        }
    }
    
    // MARK: - Create
    
    public func createItem(parentItemIdentifier: String, filename: String, fileUrl: URL? = nil) async throws {
        let urlString = "\(config.baseUrl)/api/integrations/fileprovider/create"
        logger.debug("Creating item at: \(urlString)")
        
        guard let url = URL(string: urlString) else {
            throw ApiError.invalidUrl
        }
        
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("Bearer \(config.apiKey)", forHTTPHeaderField: "Authorization")
        
        // Create multipart form data
        let boundary = "Boundary-\(UUID().uuidString)"
        request.setValue("multipart/form-data; boundary=\(boundary)", forHTTPHeaderField: "Content-Type")
        
        let httpBody = createMultipartBody(
            boundary: boundary,
            parentItemIdentifier: parentItemIdentifier,
            filename: filename,
            fileUrl: fileUrl
        )
        request.httpBody = httpBody
        
        logger.debug("Creating item: \(filename) in \(parentItemIdentifier)")
        
        do {
            let (_, response) = try await session.data(for: request)
            
            guard let httpResponse = response as? HTTPURLResponse else {
                throw ApiError.serverError("Invalid response type")
            }
            
            switch httpResponse.statusCode {
            case 201:
                logger.info("Successfully created item: \(filename)")
                return
            case 400:
                throw ApiError.serverError("Invalid request format")
            case 401:
                throw ApiError.unauthorized
            case 409:
                throw ApiError.serverError("Item already exists")
            case 428:
                throw ApiError.notReady
            default:
                throw ApiError.serverError("Create failed: \(httpResponse.statusCode)")
            }
        } catch let error as ApiError {
            throw error
        } catch {
            throw ApiError.network(error)
        }
    }
    
    private func createMultipartBody(boundary: String, parentItemIdentifier: String, filename: String, fileUrl: URL?) -> Data {
        var body = Data()
        
        // Add parent_item_identifier field
        body.append("--\(boundary)\r\n")
        body.append("Content-Disposition: form-data; name=\"parent_item_identifier\"\r\n\r\n")
        body.append("\(parentItemIdentifier)\r\n")
        
        // Add file field if URL provided, otherwise add folder_name for folder creation
        if let fileUrl = fileUrl {
            do {
                let fileData = try Data(contentsOf: fileUrl)
                let fileSize = fileData.count
                
                body.append("--\(boundary)\r\n")
                body.append("Content-Disposition: form-data; name=\"file_\(fileSize)\"; filename=\"\(filename)\"\r\n")
                body.append("Content-Type: application/octet-stream\r\n\r\n")
                body.append(fileData)
                body.append("\r\n")
            } catch {
                logger.error("Failed to read file data from \(fileUrl): \(error)")
            }
        } else {
            // Add folder_name field for folder creation
            body.append("--\(boundary)\r\n")
            body.append("Content-Disposition: form-data; name=\"folder_name\"\r\n\r\n")
            body.append("\(filename)\r\n")
        }
        
        // End boundary
        body.append("--\(boundary)--\r\n")
        
        return body
    }
    
    // MARK: - Modify
    
    public func modifyItem(identifier: String, filename: String? = nil, parentItemIdentifier: String? = nil) async throws -> ModifyItemResponse {
        let urlString = "\(config.baseUrl)/api/integrations/fileprovider/modify"
        logger.debug("Modifying item with identifier: \(identifier)")
        
        guard let url = URL(string: urlString) else {
            throw ApiError.invalidUrl
        }
        
        var request = URLRequest(url: url)
        request.httpMethod = "PATCH"
        request.setValue("Bearer \(config.apiKey)", forHTTPHeaderField: "Authorization")
        
        // Create multipart form data
        let boundary = "Boundary-\(UUID().uuidString)"
        request.setValue("multipart/form-data; boundary=\(boundary)", forHTTPHeaderField: "Content-Type")
        
        let httpBody = createModifyMultipartBody(
            boundary: boundary,
            identifier: identifier,
            filename: filename,
            parentItemIdentifier: parentItemIdentifier
        )
        request.httpBody = httpBody
        
        logger.debug("Modifying item: \(identifier)")
        
        do {
            let (data, response) = try await session.data(for: request)
            
            guard let httpResponse = response as? HTTPURLResponse else {
                throw ApiError.serverError("Invalid response type")
            }
            
            switch httpResponse.statusCode {
            case 200:
                logger.info("Successfully modified item: \(identifier)")
                // Parse the response to get the new identifier
                let modifyResponse = try JSONDecoder().decode(ModifyItemResponse.self, from: data)
                return modifyResponse
            case 400:
                throw ApiError.serverError("Invalid request format")
            case 401:
                throw ApiError.unauthorized
            case 404:
                throw ApiError.serverError("Item not found")
            case 409:
                throw ApiError.serverError("Naming conflict - item already exists at target location")
            case 428:
                throw ApiError.notReady
            case 501:
                throw ApiError.serverError("Content modification not yet implemented")
            default:
                throw ApiError.serverError("Modify failed: \(httpResponse.statusCode)")
            }
        } catch let error as ApiError {
            throw error
        } catch {
            throw ApiError.network(error)
        }
    }
    
    private func createModifyMultipartBody(boundary: String, identifier: String, filename: String?, parentItemIdentifier: String?) -> Data {
        var body = Data()
        
        // Add identifier field (required)
        body.append("--\(boundary)\r\n")
        body.append("Content-Disposition: form-data; name=\"identifier\"\r\n\r\n")
        body.append("\(identifier)\r\n")
        
        // Add filename field if provided
        if let filename = filename {
            body.append("--\(boundary)\r\n")
            body.append("Content-Disposition: form-data; name=\"filename\"\r\n\r\n")
            body.append("\(filename)\r\n")
        }
        
        // Add parent item identifier field if provided
        if let parentItemIdentifier = parentItemIdentifier {
            body.append("--\(boundary)\r\n")
            body.append("Content-Disposition: form-data; name=\"parent_item_identifier\"\r\n\r\n")
            body.append("\(parentItemIdentifier)\r\n")
        }
        
        // End boundary
        body.append("--\(boundary)--\r\n")
        
        return body
    }
    
    /// Phase 4b: Modify item with content update
    public func modifyItemWithContent(identifier: String, filename: String? = nil, parentItemIdentifier: String? = nil, contentUrl: URL, progressHandler: @escaping (Double) -> Void = { _ in }) async throws -> ModifyItemResponse {
        let urlString = "\(config.baseUrl)/api/integrations/fileprovider/modify"
        logger.debug("Modifying item with content: \(identifier)")
        
        guard let url = URL(string: urlString) else {
            throw ApiError.invalidUrl
        }
        
        var request = URLRequest(url: url)
        request.httpMethod = "PATCH"
        request.setValue("Bearer \(config.apiKey)", forHTTPHeaderField: "Authorization")
        
        // Create multipart form data with file content
        let boundary = "Boundary-\(UUID().uuidString)"
        request.setValue("multipart/form-data; boundary=\(boundary)", forHTTPHeaderField: "Content-Type")
        
        let httpBody = try createModifyWithContentMultipartBody(
            boundary: boundary,
            identifier: identifier,
            filename: filename,
            parentItemIdentifier: parentItemIdentifier,
            contentUrl: contentUrl
        )
        request.httpBody = httpBody
        
        logger.info("Modifying item with content: \(identifier)")
        logger.info("Request URL: \(request.url?.absoluteString ?? "nil")")
        logger.info("Request method: \(request.httpMethod ?? "nil")")
        logger.info("Request body size: \(request.httpBody?.count ?? 0) bytes")
        logger.info("Content-Type: \(request.value(forHTTPHeaderField: "Content-Type") ?? "nil")")
        
        do {
            let (data, response) = try await session.data(for: request)
            
            guard let httpResponse = response as? HTTPURLResponse else {
                logger.error("Invalid response type received")
                throw ApiError.serverError("Invalid response type")
            }
            
            logger.info("Response received, status: \(httpResponse.statusCode)")
            logger.info("Response headers: \(httpResponse.allHeaderFields)")
            
            switch httpResponse.statusCode {
            case 200:
                logger.info("Successfully modified item with content: \(identifier)")
                let modifyResponse = try JSONDecoder().decode(ModifyItemResponse.self, from: data)
                return modifyResponse
            case 400:
                throw ApiError.serverError("Invalid identifier or content format")
            case 401:
                throw ApiError.unauthorized
            case 404:
                throw ApiError.serverError("Item not found")
            case 413:
                throw ApiError.serverError("File too large")
            default:
                throw ApiError.serverError("HTTP \(httpResponse.statusCode)")
            }
        } catch {
            logger.error("Failed to modify item with content: \(error, privacy: .public)")
            logger.error("Error type: \(type(of: error), privacy: .public)")
            if let urlError = error as? URLError {
                logger.error("URLError code: \(urlError.code.rawValue, privacy: .public)")
                logger.error("URLError description: \(urlError.localizedDescription, privacy: .public)")
            }
            throw error
        }
    }
    
    private func createModifyWithContentMultipartBody(boundary: String, identifier: String, filename: String?, parentItemIdentifier: String?, contentUrl: URL) throws -> Data {
        var body = Data()
        
        // Add identifier field (required)
        body.append("--\(boundary)\r\n")
        body.append("Content-Disposition: form-data; name=\"identifier\"\r\n\r\n")
        body.append("\(identifier)\r\n")
        
        // Add filename field if provided
        if let filename = filename {
            body.append("--\(boundary)\r\n")
            body.append("Content-Disposition: form-data; name=\"filename\"\r\n\r\n")
            body.append("\(filename)\r\n")
        }
        
        // Add parent item identifier field if provided
        if let parentItemIdentifier = parentItemIdentifier {
            body.append("--\(boundary)\r\n")
            body.append("Content-Disposition: form-data; name=\"parent_item_identifier\"\r\n\r\n")
            body.append("\(parentItemIdentifier)\r\n")
        }
        
        // Add file content with size-based field name (same pattern as createItem)
        let fileData = try Data(contentsOf: contentUrl)
        let fileSize = fileData.count
        let filename = contentUrl.lastPathComponent
        body.append("--\(boundary)\r\n")
        body.append("Content-Disposition: form-data; name=\"file_\(fileSize)\"; filename=\"\(filename)\"\r\n")
        body.append("Content-Type: application/octet-stream\r\n\r\n")
        body.append(fileData)
        body.append("\r\n")
        
        // Close boundary
        body.append("--\(boundary)--\r\n")
        
        return body
    }
    
    // MARK: - Item
    
    public func getItem(identifier: String) async throws -> FileProviderItem {
        let urlString = "\(config.baseUrl)/api/integrations/fileprovider/item"
        
        guard var urlComponents = URLComponents(string: urlString) else {
            throw ApiError.invalidUrl
        }
        
        // Use generated ItemQuery type for consistency
        let itemQuery = ItemQuery(identifier: identifier)
        urlComponents.queryItems = [URLQueryItem(name: "identifier", value: itemQuery.identifier)]
        
        guard let url = urlComponents.url else {
            throw ApiError.invalidUrl
        }
        
        var request = URLRequest(url: url)
        request.setValue("Bearer \(config.apiKey)", forHTTPHeaderField: "Authorization")
        
        logger.debug("Getting item with identifier: \(identifier)")
        
        do {
            let (data, response) = try await session.data(for: request)
            
            guard let httpResponse = response as? HTTPURLResponse else {
                throw ApiError.serverError("Invalid response type")
            }
            
            switch httpResponse.statusCode {
            case 200:
                let item = try JSONDecoder().decode(FileProviderItem.self, from: data)
                logger.debug("Successfully retrieved item: \(identifier)")
                return item
            case 404:
                throw ApiError.notFound
            case 401:
                throw ApiError.unauthorized
            case 428:
                throw ApiError.notReady
            default:
                throw ApiError.serverError("Item lookup failed: \(httpResponse.statusCode)")
            }
        } catch let error as DecodingError {
            throw ApiError.parseError(error.localizedDescription)
        } catch let error as ApiError {
            throw error
        } catch {
            throw ApiError.network(error)
        }
    }
}

// MARK: - Data Extension

extension Data {
    mutating func append(_ string: String) {
        if let data = string.data(using: .utf8) {
            append(data)
        }
    }
    
    init?(hex: String) {
        let len = hex.count / 2
        var data = Data(capacity: len)
        var index = hex.startIndex
        for _ in 0..<len {
            let nextIndex = hex.index(index, offsetBy: 2)
            if let b = UInt8(hex[index..<nextIndex], radix: 16) {
                data.append(b)
            } else {
                return nil
            }
            index = nextIndex
        }
        self = data
    }
    
    func hexEncodedString() -> String {
        return map { String(format: "%02hhx", $0) }.joined()
    }
}

// MARK: - Keychain Helper

/// Helper for reading from keychain (matches Rust implementation)
public struct KeychainHelper {
    
    public enum KeychainError: Error, CustomStringConvertible {
        case itemNotFound
        case invalidData
        case securityError(OSStatus)
        
        public var description: String {
            switch self {
            case .itemNotFound:
                return "Keychain item not found"
            case .invalidData:
                return "Invalid keychain data"
            case .securityError(let status):
                return "Security framework error: \(status)"
            }
        }
    }
    
    /// Load a generic password from the keychain
    public static func loadItem(service: String, account: String) throws -> String {
        let logger = Logger(subsystem: "com.hopnet.desktop.fileprovider", category: "keychain")
        
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: account,
            kSecReturnData as String: true,
            kSecMatchLimit as String: kSecMatchLimitOne
        ]
        
        logger.debug("Keychain query for service: \(service), account: \(account)")
        
        var dataTypeRef: AnyObject?
        let status = SecItemCopyMatching(query as CFDictionary, &dataTypeRef)
        
        logger.debug("Keychain query status: \(status)")
        
        guard status == errSecSuccess else {
            if status == errSecItemNotFound {
                logger.error("Keychain item not found for service: \(service), account: \(account)")
                throw KeychainError.itemNotFound
            } else {
                logger.error("Keychain security error: \(status) for service: \(service), account: \(account)")
                throw KeychainError.securityError(status)
            }
        }
        
        guard let data = dataTypeRef as? Data,
              let string = String(data: data, encoding: .utf8) else {
            logger.error("Invalid keychain data for service: \(service), account: \(account)")
            throw KeychainError.invalidData
        }
        
        logger.debug("Successfully retrieved keychain item for service: \(service), account: \(account)")
        return string
    }
}