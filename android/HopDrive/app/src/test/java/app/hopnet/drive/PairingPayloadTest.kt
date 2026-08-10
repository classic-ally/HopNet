package app.hopnet.drive

import app.hopnet.drive.ui.parsePairingPayload
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class PairingPayloadTest {

    private val spki = "a".repeat(64)
    private val token = "0199aaaa-0000-7000-8000-000000000000.deadbeef"

    // Should: accept a v1 hopnet-device payload and carry every field through.
    @Test
    fun acceptsValidPayload() {
        val payload = parsePairingPayload(
            """{"v":1,"kind":"hopnet-device","host":"192.168.1.20","port":34632,
               "spki":"$spki","token":"$token"}"""
        ).getOrThrow()
        assertEquals("192.168.1.20", payload.host)
        assertEquals(34632, payload.port)
        assertEquals(spki, payload.spki)
        assertEquals(token, payload.token)
    }

    // Should: accept a host-less payload (QR rendered from a loopback
    // browser) so the UI can fall back to manual host entry.
    @Test
    fun acceptsPayloadWithoutHost() {
        val payload = parsePairingPayload(
            """{"v":1,"kind":"hopnet-device","port":34632,"spki":"$spki","token":"$token"}"""
        ).getOrThrow()
        assertNull(payload.host)
    }

    // Should not: pair from foreign or malformed codes — wrong kind, wrong
    // version, truncated fingerprint, or non-JSON input.
    @Test
    fun rejectsForeignAndMalformedPayloads() {
        val cases = listOf(
            """{"v":1,"kind":"other-app","port":1,"spki":"$spki","token":"$token"}""",
            """{"v":2,"kind":"hopnet-device","port":1,"spki":"$spki","token":"$token"}""",
            """{"v":1,"kind":"hopnet-device","port":1,"spki":"abc123","token":"$token"}""",
            """{"v":1,"kind":"hopnet-device","port":0,"spki":"$spki","token":"$token"}""",
            """{"v":1,"kind":"hopnet-device","port":1,"spki":"$spki","token":"no-dot"}""",
            "https://example.com/not-a-payload",
        )
        cases.forEach { case ->
            assertTrue("should reject: $case", parsePairingPayload(case).isFailure)
        }
    }
}
