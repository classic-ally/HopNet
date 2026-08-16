package app.hopnet.drive.data

import android.content.Context
import android.util.Log
import app.hopnet.drive.net.UpgradeRequiredResponse
import app.hopnet.drive.net.formatVersionCode
import app.hopnet.drive.ui.UpgradeNotifier

private const val TAG = "HopDriveUpgrade"

/**
 * Sticky upgrade-required episode (RFC-023). Raised by the transport on a
 * parsed 426, cleared by the next successful device-token request — so a
 * node rollback or an app upgrade self-heals without user action. An
 * episode is loud exactly once: the warning log and the system
 * notification fire on the null → active transition only; repeat 426s
 * while active update the values silently (mirror of the mount watcher's
 * loud-once handler, hopnet-mount/src/watch.rs).
 */
object UpgradeState {

    data class Info(val surface: String, val minClient: Int, val nodeVersion: Int)

    @Volatile
    var current: Info? = null
        private set

    private val listeners = mutableListOf<() -> Unit>()

    fun addListener(listener: () -> Unit) = synchronized(listeners) {
        listeners.add(listener)
    }

    fun removeListener(listener: () -> Unit) = synchronized(listeners) {
        listeners.remove(listener)
    }

    private fun notifyListeners() {
        synchronized(listeners) { listeners.toList() }.forEach { it() }
    }

    /**
     * Record a version rejection. Context may be null (direct-constructed
     * clients in JVM tests) — then the episode is state-only, no
     * notification.
     */
    fun raise(context: Context?, payload: UpgradeRequiredResponse) {
        val info = Info(payload.surface, payload.minClient, payload.nodeVersion)
        val started: Boolean
        synchronized(this) {
            started = current == null
            current = info
        }
        if (started) {
            Log.w(
                TAG,
                "node ${formatVersionCode(info.nodeVersion)} requires client >= " +
                    "${formatVersionCode(info.minClient)} on ${info.surface} — " +
                    "holding until the app is updated"
            )
            if (context != null) UpgradeNotifier.show(context, info)
            notifyListeners()
        }
    }

    /** End the episode; called from every successful transport response. */
    fun clear(context: Context?) {
        if (current == null) return // fast path: the common no-episode case
        val ended: Boolean
        synchronized(this) {
            ended = current != null
            current = null
        }
        if (ended) {
            if (context != null) UpgradeNotifier.cancel(context)
            notifyListeners()
        }
    }

    /** Test hook: drop state without notifying anyone. */
    internal fun resetForTest() {
        synchronized(this) { current = null }
    }
}
