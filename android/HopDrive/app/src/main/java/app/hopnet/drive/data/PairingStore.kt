package app.hopnet.drive.data

import android.app.KeyguardManager
import android.content.Context
import android.provider.DocumentsContract
import android.security.keystore.KeyGenParameterSpec
import android.security.keystore.KeyPermanentlyInvalidatedException
import android.security.keystore.KeyProperties
import android.security.keystore.StrongBoxUnavailableException
import android.util.Base64
import android.util.Log
import app.hopnet.drive.HopDriveProvider
import java.security.KeyStore
import java.security.UnrecoverableKeyException
import javax.crypto.AEADBadTagException
import javax.crypto.Cipher
import javax.crypto.KeyGenerator
import javax.crypto.SecretKey
import javax.crypto.spec.GCMParameterSpec

private const val TAG = "HopDrivePairing"

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
 * Envelope encryption for the device token: a non-exportable AES-256-GCM
 * key generated inside the Android Keystore (StrongBox when available)
 * wraps the token, and only the ciphertext touches disk. The key is
 * unlock-bound (setUnlockedDeviceRequired), so a locked device refuses to
 * decrypt — [unwrap] must distinguish that transient refusal from
 * permanent key loss (lock-screen removal, keystore corruption), which is
 * unrecoverable and demands a re-pair.
 */
private object TokenVault {
    private const val ALIAS = "pairing-token-wrap"
    private const val PROVIDER = "AndroidKeyStore"
    private const val TRANSFORM = "AES/GCM/NoPadding"
    private const val GCM_TAG_BITS = 128

    sealed interface UnwrapResult {
        data class Ok(val token: String) : UnwrapResult
        /** Keystore unavailable right now (e.g. device locked); retry later. */
        object Transient : UnwrapResult
        /** The wrapping key or ciphertext is gone for good; re-pair required. */
        object Invalidated : UnwrapResult
    }

    private fun keyStore(): KeyStore =
        KeyStore.getInstance(PROVIDER).apply { load(null) }

    private fun obtainKey(): SecretKey {
        (keyStore().getKey(ALIAS, null) as? SecretKey)?.let { return it }
        return try {
            generateKey(strongBox = true)
        } catch (e: StrongBoxUnavailableException) {
            Log.i(TAG, "StrongBox unavailable; using TEE-backed key")
            generateKey(strongBox = false)
        }
    }

    private fun generateKey(strongBox: Boolean): SecretKey {
        val spec = KeyGenParameterSpec.Builder(
            ALIAS,
            KeyProperties.PURPOSE_ENCRYPT or KeyProperties.PURPOSE_DECRYPT
        )
            .setBlockModes(KeyProperties.BLOCK_MODE_GCM)
            .setEncryptionPaddings(KeyProperties.ENCRYPTION_PADDING_NONE)
            .setKeySize(256)
            .setUnlockedDeviceRequired(true)
            .setIsStrongBoxBacked(strongBox)
            .build()
        return KeyGenerator.getInstance(KeyProperties.KEY_ALGORITHM_AES, PROVIDER)
            .apply { init(spec) }
            .generateKey()
    }

    /** base64(iv):base64(ciphertext). Throws if the keystore is unusable. */
    fun wrap(token: String): String {
        val cipher = Cipher.getInstance(TRANSFORM)
        cipher.init(Cipher.ENCRYPT_MODE, obtainKey())
        val ciphertext = cipher.doFinal(token.toByteArray(Charsets.UTF_8))
        return Base64.encodeToString(cipher.iv, Base64.NO_WRAP) + ":" +
            Base64.encodeToString(ciphertext, Base64.NO_WRAP)
    }

    fun unwrap(context: Context, stored: String): UnwrapResult {
        val parts = stored.split(':')
        if (parts.size != 2) return UnwrapResult.Invalidated
        return try {
            // Ciphertext without its key means the key was destroyed (or the
            // prefs were restored onto a device that never had it).
            val key = keyStore().getKey(ALIAS, null) as? SecretKey
                ?: return UnwrapResult.Invalidated
            val iv = Base64.decode(parts[0], Base64.NO_WRAP)
            val ciphertext = Base64.decode(parts[1], Base64.NO_WRAP)
            val cipher = Cipher.getInstance(TRANSFORM)
            cipher.init(Cipher.DECRYPT_MODE, key, GCMParameterSpec(GCM_TAG_BITS, iv))
            UnwrapResult.Ok(String(cipher.doFinal(ciphertext), Charsets.UTF_8))
        } catch (e: KeyPermanentlyInvalidatedException) {
            Log.w(TAG, "wrapping key permanently invalidated: $e")
            UnwrapResult.Invalidated
        } catch (e: AEADBadTagException) {
            Log.w(TAG, "token ciphertext failed authentication: $e")
            UnwrapResult.Invalidated
        } catch (e: UnrecoverableKeyException) {
            Log.w(TAG, "wrapping key unrecoverable: $e")
            UnwrapResult.Invalidated
        } catch (e: Exception) {
            // Anything unclassified is treated as transient — never
            // destructive. The common case is the unlock-bound key refusing
            // while the device is locked.
            val locked = (context.getSystemService(Context.KEYGUARD_SERVICE) as KeyguardManager)
                .isDeviceLocked
            Log.d(TAG, "transient unwrap failure (deviceLocked=$locked): $e")
            UnwrapResult.Transient
        }
    }

    fun deleteKey() {
        runCatching { keyStore().deleteEntry(ALIAS) }
    }
}

/**
 * Persisted pairing in app-private SharedPreferences, with the device
 * token Keystore-wrapped ([TokenVault]) — only ciphertext is on disk.
 * Host/port/SPKI stay plaintext: none is a secret (the pin is the node's
 * public identity), and keeping them readable lets the pairing screen
 * prefill a re-pair after key invalidation. allowBackup=false keeps the
 * file out of device backups, and the token stays revocable server-side
 * from the node's device list.
 */
object PairingStore {
    private const val PREFS = "pairing"
    private const val KEY_TOKEN_LEGACY = "token"
    private const val KEY_TOKEN_ENC = "token_enc"
    private const val KEY_INVALIDATED = "token_invalidated"

    /** Endpoint details surviving a token invalidation, for re-pair prefill. */
    data class Endpoint(val host: String, val port: Int, val spki: String)

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

        var tokenEnc = prefs.getString(KEY_TOKEN_ENC, null)
        if (tokenEnc == null) {
            val legacy = prefs.getString(KEY_TOKEN_LEGACY, null)
            if (legacy != null) {
                // v1 stored the token in plaintext; wrap it on first sight.
                try {
                    tokenEnc = TokenVault.wrap(legacy)
                    prefs.edit()
                        .putString(KEY_TOKEN_ENC, tokenEnc)
                        .remove(KEY_TOKEN_LEGACY)
                        .apply()
                    Log.i(TAG, "migrated plaintext token to Keystore-wrapped storage")
                } catch (e: Exception) {
                    // Keystore unusable right now: serve the plaintext token
                    // and leave it in place; `loaded` stays false so the
                    // migration is retried on the next load.
                    Log.d(TAG, "token migration deferred: $e")
                    cached = if (host != null && port != 0 && spki != null) {
                        Pairing(host, port, spki, legacy)
                    } else {
                        null
                    }
                    return cached
                }
            }
        }

        var token: String? = null
        var invalidatedNow = false
        if (tokenEnc != null) {
            when (val result = TokenVault.unwrap(context, tokenEnc)) {
                is TokenVault.UnwrapResult.Ok -> token = result.token
                TokenVault.UnwrapResult.Transient -> {
                    // `loaded` stays false → the next provider call retries.
                    cached = null
                    return null
                }
                TokenVault.UnwrapResult.Invalidated -> {
                    // Keep host/port/spki so the UI can prefill the re-pair.
                    prefs.edit()
                        .remove(KEY_TOKEN_ENC)
                        .putBoolean(KEY_INVALIDATED, true)
                        .apply()
                    invalidatedNow = true
                }
            }
        }

        cached = if (host != null && port != 0 && spki != null && token != null) {
            Pairing(host, port, spki, token)
        } else {
            null
        }
        loaded = true
        if (invalidatedNow) {
            notifyRootsChanged(context)
            notifyListeners()
        }
        return cached
    }

    @Synchronized
    fun save(context: Context, pairing: Pairing) {
        context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
            .edit()
            .putString("host", pairing.host)
            .putInt("port", pairing.port)
            .putString("spki", pairing.spki.lowercase())
            .putString(KEY_TOKEN_ENC, TokenVault.wrap(pairing.token))
            .remove(KEY_TOKEN_LEGACY)
            .remove(KEY_INVALIDATED)
            .apply()
        cached = pairing
        loaded = true
        notifyRootsChanged(context)
        notifyListeners()
    }

    @Synchronized
    fun clear(context: Context) {
        context.getSharedPreferences(PREFS, Context.MODE_PRIVATE).edit().clear().apply()
        TokenVault.deleteKey()
        cached = null
        loaded = true
        app.hopnet.drive.net.WatchLoop.stop()
        notifyRootsChanged(context)
        notifyListeners()
    }

    /** True after [load] found the wrapped token permanently undecryptable. */
    fun wasInvalidated(context: Context): Boolean =
        context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
            .getBoolean(KEY_INVALIDATED, false)

    /** The endpoint left behind by an invalidated token, if complete. */
    fun invalidatedRemnant(context: Context): Endpoint? {
        val prefs = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
        if (!prefs.getBoolean(KEY_INVALIDATED, false)) return null
        val host = prefs.getString("host", null) ?: return null
        val port = prefs.getInt("port", 0)
        val spki = prefs.getString("spki", null) ?: return null
        if (port == 0) return null
        return Endpoint(host, port, spki)
    }

    /** Test hook: drop the in-memory cache so the next [load] re-reads disk. */
    @Synchronized
    internal fun dropMemoryCache() {
        cached = null
        loaded = false
    }

    /** The SAF root appears/disappears with the pairing. */
    private fun notifyRootsChanged(context: Context) {
        context.contentResolver.notifyChange(
            DocumentsContract.buildRootsUri(HopDriveProvider.AUTHORITY),
            null
        )
    }
}
