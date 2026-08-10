package app.hopnet.drive.data

import android.content.Context
import android.provider.DocumentsContract
import app.hopnet.drive.HopDriveProvider

data class Pairing(
    val host: String,
    val port: Int,
    /** 64 lower-hex chars; the node cert's SPKI SHA-256 the transport pins. */
    val spki: String,
    /** Device token {device_id}.{secret_hex}; sent as the Bearer credential. */
    val token: String,
) {
    val baseUrl: String get() = "https://$host:$port"
    val deviceId: String get() = token.substringBefore('.')
}

/**
 * Persisted pairing in app-private SharedPreferences.
 *
 * Not Keystore-wrapped in v1: the app sandbox gates the file,
 * allowBackup=false keeps it out of device backups, and the token is
 * revocable server-side from the node's device list. Keystore wrapping
 * is listed as future hardening in docs/specs/pinned-https.md.
 */
object PairingStore {
    private const val PREFS = "pairing"

    @Volatile
    private var cached: Pairing? = null

    @Volatile
    private var loaded = false

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

    @Synchronized
    fun load(context: Context): Pairing? {
        if (loaded) return cached
        val prefs = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
        val host = prefs.getString("host", null)
        val port = prefs.getInt("port", 0)
        val spki = prefs.getString("spki", null)
        val token = prefs.getString("token", null)
        cached = if (host != null && port != 0 && spki != null && token != null) {
            Pairing(host, port, spki, token)
        } else {
            null
        }
        loaded = true
        return cached
    }

    @Synchronized
    fun save(context: Context, pairing: Pairing) {
        context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
            .edit()
            .putString("host", pairing.host)
            .putInt("port", pairing.port)
            .putString("spki", pairing.spki.lowercase())
            .putString("token", pairing.token)
            .apply()
        cached = pairing
        loaded = true
        notifyRootsChanged(context)
        notifyListeners()
    }

    @Synchronized
    fun clear(context: Context) {
        context.getSharedPreferences(PREFS, Context.MODE_PRIVATE).edit().clear().apply()
        cached = null
        loaded = true
        notifyRootsChanged(context)
        notifyListeners()
    }

    /** The SAF root appears/disappears with the pairing. */
    private fun notifyRootsChanged(context: Context) {
        context.contentResolver.notifyChange(
            DocumentsContract.buildRootsUri(HopDriveProvider.AUTHORITY),
            null
        )
    }
}
