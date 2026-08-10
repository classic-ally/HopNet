package app.hopnet.drive.data

import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale

/**
 * Represents a single API call made by the Files app to our DocumentsProvider.
 */
data class ApiCallLog(
    val timestamp: Long = System.currentTimeMillis(),
    val method: String,
    val parameters: Map<String, String?> = emptyMap(),
    val result: String? = null,
    val error: String? = null,
    val responseJson: String? = null
) {
    val formattedTime: String
        get() = SimpleDateFormat("HH:mm:ss.SSS", Locale.US).format(Date(timestamp))

    val parametersString: String
        get() = if (parameters.isEmpty()) {
            "(none)"
        } else {
            parameters.entries.joinToString(", ") { "${it.key}=${it.value ?: "null"}" }
        }
}

/**
 * Lightweight response types for serialization in logs.
 * These mirror the document structure but are separate to avoid circular dependencies.
 */
@Serializable
data class LoggedDocument(
    val id: String,
    val name: String,
    val mimeType: String,
    val size: Long,
    val lastModified: Long,
    val parentId: String?
)

@Serializable
data class LoggedRoot(
    val rootId: String,
    val title: String,
    val summary: String,
    val documentId: String
)

/**
 * Repository for storing and observing API call logs.
 * Singleton to be shared between DocumentsProvider and UI.
 */
object LogRepository {
    private const val MAX_LOGS = 100

    private val logs = mutableListOf<ApiCallLog>()
    private val changeListeners = mutableListOf<() -> Unit>()

    private val json = Json { prettyPrint = true }

    fun addChangeListener(listener: () -> Unit) {
        changeListeners.add(listener)
    }

    fun removeChangeListener(listener: () -> Unit) {
        changeListeners.remove(listener)
    }

    private fun notifyChange() {
        changeListeners.forEach { it() }
    }

    /**
     * Log an API call.
     */
    fun log(
        method: String,
        parameters: Map<String, String?> = emptyMap(),
        result: String? = null,
        error: String? = null,
        responseJson: String? = null
    ) {
        synchronized(logs) {
            logs.add(0, ApiCallLog(
                method = method,
                parameters = parameters,
                result = result,
                error = error,
                responseJson = responseJson
            ))
            // Keep only the most recent logs
            while (logs.size > MAX_LOGS) {
                logs.removeLast()
            }
        }
        notifyChange()
    }

    /**
     * Get all logs (most recent first).
     */
    fun getLogs(): List<ApiCallLog> = synchronized(logs) { logs.toList() }

    /**
     * Clear all logs.
     */
    fun clear() {
        synchronized(logs) { logs.clear() }
        notifyChange()
    }

    // Convenience methods for common operations

    fun logQueryRoots(roots: List<LoggedRoot>) {
        log(
            method = "queryRoots",
            result = "${roots.size} root(s)",
            responseJson = json.encodeToString(roots)
        )
    }

    fun logQueryDocument(documentId: String, document: LoggedDocument?) {
        log(
            method = "queryDocument",
            parameters = mapOf("documentId" to documentId),
            result = if (document != null) "found" else "not found",
            responseJson = document?.let { json.encodeToString(it) }
        )
    }

    fun logQueryChildDocuments(parentId: String, children: List<LoggedDocument>) {
        log(
            method = "queryChildDocuments",
            parameters = mapOf("parentId" to parentId),
            result = "${children.size} children",
            responseJson = json.encodeToString(children)
        )
    }

    fun logOpenDocument(documentId: String, mode: String, document: LoggedDocument?) {
        log(
            method = "openDocument",
            parameters = mapOf("documentId" to documentId, "mode" to mode),
            result = if (document != null) "opened" else "not found",
            responseJson = document?.let { json.encodeToString(it) }
        )
    }

    fun logCreateDocument(parentId: String, name: String, mimeType: String, created: LoggedDocument) {
        log(
            method = "createDocument",
            parameters = mapOf(
                "parentId" to parentId,
                "name" to name,
                "mimeType" to mimeType
            ),
            result = "id=${created.id}",
            responseJson = json.encodeToString(created)
        )
    }

    fun logDeleteDocument(documentId: String, deleted: LoggedDocument?) {
        log(
            method = "deleteDocument",
            parameters = mapOf("documentId" to documentId),
            result = "deleted",
            responseJson = deleted?.let { json.encodeToString(it) }
        )
    }

    fun logRenameDocument(documentId: String, newName: String, renamed: LoggedDocument) {
        log(
            method = "renameDocument",
            parameters = mapOf("documentId" to documentId, "newName" to newName),
            result = "newId=${renamed.id}",
            responseJson = json.encodeToString(renamed)
        )
    }

    fun logIsChildDocument(parentId: String, documentId: String, isChild: Boolean) {
        log(
            method = "isChildDocument",
            parameters = mapOf("parentId" to parentId, "documentId" to documentId),
            result = isChild.toString()
        )
    }

    fun logError(method: String, error: String) {
        log(method = method, error = error)
    }
}

/**
 * Extension to convert HopNetDocument to LoggedDocument for logging.
 */
fun HopNetDocument.toLogged() = LoggedDocument(
    id = id,
    name = name,
    mimeType = mimeType,
    size = size,
    lastModified = lastModified,
    parentId = parentId
)
