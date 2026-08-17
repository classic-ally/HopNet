package app.hopnet.drive

import app.hopnet.drive.net.formatVersionCode
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

/**
 * The CalVer parse itself lives in build.gradle.kts (a malformed workspace
 * version fails the build); these tests pin the invariants of its output.
 */
class VersionCodeTest {

    // Impact: the compiled-in identity is what every node gate compares —
    // an encoding drift from common/src/version.rs misclassifies the app.
    // Should: decode the BuildConfig code to a plausible year/month/counter.
    @Test
    fun buildConfigCodeDecodesToCalVer() {
        val code = BuildConfig.HOPNET_CLIENT_VERSION_CODE
        val year = code / 10_000
        val month = (code / 100) % 100
        val counter = code % 100
        assertTrue(year in 2000..9999)
        assertTrue(month in 1..12)
        assertTrue(counter in 0..99)
    }

    // Should: render the code back to exactly the workspace version string.
    @Test
    fun formattedCodeMatchesWorkspaceVersion() {
        assertEquals(
            BuildConfig.HOPNET_CLIENT_VERSION_NAME,
            formatVersionCode(BuildConfig.HOPNET_CLIENT_VERSION_CODE)
        )
    }

    // Should: order codes the same way the calendar orders releases.
    @Test
    fun codeOrderingMatchesCalendarOrdering() {
        assertEquals("2026.8.4", formatVersionCode(20260804))
        assertTrue(20260804 > 20260799) // 2026.8.x sorts after 2026.7.x
        assertTrue(20270101 > 20261299) // year rolls over above any month
    }
}
