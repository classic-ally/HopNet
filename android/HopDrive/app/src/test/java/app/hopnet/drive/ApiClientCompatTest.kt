package app.hopnet.drive

import app.hopnet.drive.data.Pairing
import app.hopnet.drive.data.UpgradeState
import app.hopnet.drive.net.ApiClient
import app.hopnet.drive.net.CLIENT_VERSION_HEADER
import app.hopnet.drive.net.NodeHttpException
import app.hopnet.drive.net.UpgradeRequiredException
import java.security.MessageDigest
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.tls.HandshakeCertificates
import okhttp3.tls.HeldCertificate
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test

/**
 * RFC-023 client-compat transport tests against a TLS MockWebServer whose
 * SPKI pin is computed from its generated certificate — the real pinned
 * client, real handshake, fake node.
 */
class ApiClientCompatTest {

    private lateinit var server: MockWebServer
    private lateinit var client: ApiClient

    private val upgradeBody =
        """{"surface":"/integrations/documentprovider","min_client":20990100,"node_version":20990101}"""

    @Before
    fun start() {
        UpgradeState.resetForTest()
        val cert = HeldCertificate.Builder()
            .addSubjectAlternativeName("localhost")
            .build()
        val pin = MessageDigest.getInstance("SHA-256")
            .digest(cert.certificate.publicKey.encoded)
            .joinToString("") { "%02x".format(it) }
        server = MockWebServer()
        server.useHttps(
            HandshakeCertificates.Builder()
                .heldCertificate(cert)
                .build()
                .sslSocketFactory(),
            false
        )
        server.start()
        client = ApiClient(Pairing("localhost", server.port, pin, "device.secret"))
    }

    @After
    fun stop() {
        server.shutdown()
        UpgradeState.resetForTest()
    }

    private fun ok(body: String): MockResponse =
        MockResponse().setResponseCode(200).setBody(body)

    // Impact: the node's gate rejects header-less requests outright — a
    // single missed attachment point re-breaks the whole app.
    // Should: send the client version header on every device-token request.
    // Should: send it on the SSE watch stream too, which bypasses execute().
    // Should not: vary by endpoint or method.
    @Test
    fun versionHeaderOnEveryRequest() {
        server.enqueue(ok("""{"items":[]}"""))
        server.enqueue(ok("""{"total_bytes":1,"used_bytes":0}"""))
        server.enqueue(ok(""))
        server.enqueue(ok(""))

        client.enumerate(null)
        client.statfs()
        client.delete("some-id", recursive = false)
        client.openWatch().close()

        val expected = BuildConfig.HOPNET_CLIENT_VERSION_CODE.toString()
        repeat(4) {
            val request = server.takeRequest()
            assertEquals(expected, request.getHeader(CLIENT_VERSION_HEADER))
            assertEquals("Bearer device.secret", request.getHeader("Authorization"))
        }
    }

    // Impact: this is the client half of the RFC-023 wire contract — a
    // shape drift between compat.rs and Compat.kt strands the app silently.
    // Should: parse a 426 body into the typed exception's fields.
    // Should: hold the sticky upgrade state while rejected.
    // Should: clear the state on the next successful request.
    @Test
    fun upgradeRequiredIsTypedAndSticky() {
        server.enqueue(MockResponse().setResponseCode(426).setBody(upgradeBody))

        val e = assertThrows(UpgradeRequiredException::class.java) {
            client.enumerate(null)
        }
        assertEquals("/integrations/documentprovider", e.surface)
        assertEquals(20990100, e.minClient)
        assertEquals(20990101, e.nodeVersion)
        assertEquals(20990100, UpgradeState.current?.minClient)

        server.enqueue(ok("""{"items":[]}"""))
        client.enumerate(null)
        assertNull(UpgradeState.current)
    }

    // Should: parse a 426 from the watch-stream open, which has its own
    // error path outside execute().
    @Test
    fun upgradeRequiredParsesOnWatchStream() {
        server.enqueue(MockResponse().setResponseCode(426).setBody(upgradeBody))

        val e = assertThrows(UpgradeRequiredException::class.java) {
            client.openWatch()
        }
        assertEquals(20990100, e.minClient)
        assertEquals("/integrations/documentprovider", UpgradeState.current?.surface)
    }

    // Should: degrade an unparseable 426 body to the generic HTTP error.
    // Should not: raise the upgrade state from a body it could not parse.
    @Test
    fun malformed426BodyDegradesToGenericError() {
        server.enqueue(MockResponse().setResponseCode(426).setBody("upgrade"))

        val e = assertThrows(NodeHttpException::class.java) {
            client.enumerate(null)
        }
        assertTrue(e !is UpgradeRequiredException)
        assertEquals(426, e.code)
        assertNull(UpgradeState.current)
    }

    // Should: keep the one-shot 503 overload retry working unchanged.
    // Should not: involve the upgrade state in overload shedding.
    @Test
    fun overloadRetryIsUntouched() {
        server.enqueue(
            MockResponse().setResponseCode(503).setHeader("Retry-After", "1")
        )
        server.enqueue(ok("""{"items":[]}"""))

        assertEquals(0, client.enumerate(null).size)
        assertEquals(2, server.requestCount)
        assertNull(UpgradeState.current)
    }
}
