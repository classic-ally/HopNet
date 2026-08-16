package app.hopnet.drive.data

import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale

/**
 * One entry in the in-app request log — now fed by the HTTP interceptor
 * (net/PinnedTls.kt), so the log shows the real traffic between this app
 * and the paired node.
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
 * Repository for storing and observing the request log.
 * Singleton shared between the provider, the transport, and the UI.
 */
object LogRepository {
    private const val MAX_LOGS = 100

    private val logs = mutableListOf<ApiCallLog>()
    private val changeListeners = mutableListOf<() -> Unit>()

    fun addChangeListener(listener: () -> Unit) {
        changeListeners.add(listener)
    }

    fun removeChangeListener(listener: () -> Unit) {
        changeListeners.remove(listener)
    }

    private fun notifyChange() {
        changeListeners.forEach { it() }
    }

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
}
