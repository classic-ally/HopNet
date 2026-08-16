package app.hopnet.drive

import android.content.Context
import android.content.SharedPreferences
import android.util.Base64
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import app.hopnet.drive.data.Pairing
import app.hopnet.drive.data.PairingStore
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

/**
 * Keystore-wrapped pairing storage. Instrumented because the Android
 * Keystore has no JVM implementation. The permanent-invalidation cause
 * itself (lock-screen removal) cannot be simulated in a test, but its
 * handling path is reachable by planting undecryptable ciphertext.
 */
@RunWith(AndroidJUnit4::class)
class PairingStoreTest {

    private lateinit var context: Context

    private fun prefs(): SharedPreferences =
        context.getSharedPreferences("pairing", Context.MODE_PRIVATE)

    @Before
    fun reset() {
        context = InstrumentationRegistry.getInstrumentation().targetContext
        PairingStore.clear(context)
        PairingStore.dropMemoryCache()
    }

    @After
    fun cleanUp() {
        PairingStore.clear(context)
    }

    // Impact: guards the at-rest secrecy contract of the pairing store —
    // the bearer token must never be recoverable from the prefs file alone.
    // Should: round-trip a saved pairing through the Keystore-wrapped store.
    // Should not: leave the token readable as plaintext anywhere in the prefs.
    @Test
    fun roundTripsWithoutPlaintextToken() {
        val secret = "device42.feedfacecafesecret"
        PairingStore.save(context, Pairing("10.0.0.2", 34632, "cd".repeat(32), secret))

        PairingStore.dropMemoryCache()
        val loaded = PairingStore.load(context)
        assertNotNull("pairing lost across a reload", loaded)
        assertEquals(secret, loaded!!.token)

        assertNull("plaintext token key present", prefs().getString("token", null))
        val wrapped = prefs().getString("token_enc", null)
        assertNotNull("wrapped token missing", wrapped)
        assertFalse("token visible in ciphertext", wrapped!!.contains("feedfacecafesecret"))
    }

    // Impact: existing installs pair before this feature existed; losing
    // their pairing on upgrade would silently unmount the SAF root.
    // Should: migrate a legacy plaintext token to wrapped form on first load.
    // Should not: keep the plaintext copy after migration.
    @Test
    fun migratesLegacyPlaintextToken() {
        prefs().edit()
            .putString("host", "10.0.0.1")
            .putInt("port", 34632)
            .putString("spki", "ab".repeat(32))
            .putString("token", "device7.1egacysecret")
            .commit()
        PairingStore.dropMemoryCache()

        val pairing = PairingStore.load(context)
        assertNotNull("legacy pairing not loaded", pairing)
        assertEquals("device7.1egacysecret", pairing!!.token)
        assertNull("plaintext survived migration", prefs().getString("token", null))
        assertNotNull("wrapped token missing after migration", prefs().getString("token_enc", null))
    }

    // Impact: guards the recovery path for keystore loss — the app must
    // fail to an explicit re-pair state, never crash or retry forever.
    // Should: treat undecryptable ciphertext as permanent invalidation,
    // dropping the wrapped token and flagging the re-pair prompt while
    // keeping the endpoint details for prefill.
    @Test
    fun undecryptableCiphertextFlagsInvalidation() {
        PairingStore.save(context, Pairing("10.0.0.3", 34632, "ef".repeat(32), "device9.abc"))
        val garbage = Base64.encodeToString(ByteArray(12), Base64.NO_WRAP) + ":" +
            Base64.encodeToString(ByteArray(32), Base64.NO_WRAP)
        prefs().edit().putString("token_enc", garbage).commit()
        PairingStore.dropMemoryCache()

        assertNull("undecryptable token yielded a pairing", PairingStore.load(context))
        assertTrue("invalidation not flagged", PairingStore.wasInvalidated(context))
        assertNull("dead ciphertext kept", prefs().getString("token_enc", null))
        val remnant = PairingStore.invalidatedRemnant(context)
        assertEquals("10.0.0.3", remnant?.host)

        // A fresh pairing clears the invalidated state.
        PairingStore.save(context, Pairing("10.0.0.3", 34632, "ef".repeat(32), "device9.def"))
        assertFalse(PairingStore.wasInvalidated(context))
    }
}
