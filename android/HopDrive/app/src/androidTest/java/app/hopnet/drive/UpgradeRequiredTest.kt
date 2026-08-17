package app.hopnet.drive

import android.app.NotificationManager
import android.content.Context
import androidx.compose.ui.test.assertIsDisplayed
import androidx.compose.ui.test.junit4.createAndroidComposeRule
import androidx.compose.ui.test.onNodeWithText
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import androidx.test.rule.GrantPermissionRule
import app.hopnet.drive.data.LogRepository
import app.hopnet.drive.data.Pairing
import app.hopnet.drive.data.PairingStore
import app.hopnet.drive.data.UpgradeState
import app.hopnet.drive.net.ApiClient
import app.hopnet.drive.net.UpgradeRequiredException
import app.hopnet.drive.net.WatchLoop
import app.hopnet.drive.ui.UpgradeNotifier
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Assume.assumeTrue
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith

/**
 * End-to-end RFC-023 coverage against a LIVE node whose surface minimum is
 * raised above this build (HOPNET_MIN_CLIENT_OVERRIDE on the node side).
 *
 * Needs the LiveNodeTest args (`host/port/spki/token` — the current-version
 * node, for recovery assertions) plus the min-raised node:
 *
 *   -Pandroid.testInstrumentationRunnerArguments.upgradedHost=10.0.2.2
 *   -Pandroid.testInstrumentationRunnerArguments.upgradedPort=<https port>
 *   -Pandroid.testInstrumentationRunnerArguments.upgradedSpki=<64 hex>
 *   -Pandroid.testInstrumentationRunnerArguments.upgradedToken=<device_id>.<secret>
 *
 * The whole class is skipped (not failed) when the upgraded* args are
 * absent, so a manual single-node run still works.
 */
@RunWith(AndroidJUnit4::class)
class UpgradeRequiredTest {

    @get:Rule
    val composeRule = createAndroidComposeRule<MainActivity>()

    @get:Rule
    val notificationPermission: GrantPermissionRule =
        GrantPermissionRule.grant(android.Manifest.permission.POST_NOTIFICATIONS)

    private lateinit var context: Context
    private lateinit var upgradedPairing: Pairing
    private var currentPairing: Pairing? = null

    @Before
    fun pairAgainstUpgradedNode() {
        context = InstrumentationRegistry.getInstrumentation().targetContext
        val args = InstrumentationRegistry.getArguments()
        val host = args.getString("upgradedHost")
        assumeTrue("upgraded-node instrumentation args absent — skipping", host != null)
        upgradedPairing = Pairing(
            host!!,
            args.getString("upgradedPort")?.toInt()
                ?: error("missing instrumentation arg: upgradedPort"),
            args.getString("upgradedSpki") ?: error("missing instrumentation arg: upgradedSpki"),
            args.getString("upgradedToken") ?: error("missing instrumentation arg: upgradedToken"),
        )
        currentPairing = args.getString("host")?.let { currentHost ->
            Pairing(
                currentHost,
                args.getString("port")!!.toInt(),
                args.getString("spki")!!,
                args.getString("token")!!,
            )
        }
        UpgradeState.resetForTest()
        LogRepository.clear()
        UpgradeNotifier.cancel(context)
        PairingStore.save(context, upgradedPairing)
    }

    @After
    fun cleanup() {
        WatchLoop.stop()
        UpgradeState.resetForTest()
        UpgradeNotifier.cancel(context)
    }

    private fun triggerRejection(): UpgradeRequiredException =
        assertThrows(UpgradeRequiredException::class.java) {
            ApiClient.forContext(context)!!.enumerate(null)
        }

    private fun awaitNotificationCount(expected: Int, timeoutMs: Long = 5_000) {
        val manager = context.getSystemService(NotificationManager::class.java)
        val deadline = System.currentTimeMillis() + timeoutMs
        var count = -1
        while (System.currentTimeMillis() < deadline) {
            count = manager.activeNotifications.count {
                it.id == UpgradeNotifier.NOTIFICATION_ID
            }
            if (count == expected) return
            Thread.sleep(250)
        }
        assertEquals(expected, count)
    }

    // Impact: proves the real node's version gate and the app's parser
    // agree end-to-end over pinned TLS — the full RFC-023 contract.
    // Should: reject this build with a typed exception naming both versions.
    // Should: hold the sticky upgrade state while rejected.
    @Test
    fun liveGateRejectionIsTypedAndSticky() {
        val e = triggerRejection()
        assertTrue(e.minClient > BuildConfig.HOPNET_CLIENT_VERSION_CODE)
        assertTrue(e.nodeVersion > 0)
        assertTrue(e.surface.contains("documentprovider"))
        assertEquals(e.minClient, UpgradeState.current?.minClient)
    }

    // Should: surface the upgrade episode as a visible in-app banner.
    @Test
    fun upgradeBannerRenders() {
        triggerRejection()
        composeRule.waitUntil(5_000) { UpgradeState.current != null }
        composeRule.onNodeWithText("Upgrade required", substring = true).assertIsDisplayed()
    }

    // Should: hold the watch loop at max backoff once the node says the
    // build is too old.
    // Should not: hammer a node that already rejected this version.
    @Test
    fun watchLoopParksInsteadOfSpinning() {
        WatchLoop.touch(context)
        val deadline = System.currentTimeMillis() + 10_000
        while (UpgradeState.current == null && System.currentTimeMillis() < deadline) {
            Thread.sleep(250)
        }
        assertTrue("watch loop never hit the gate", UpgradeState.current != null)

        fun watchAttempts() = LogRepository.getLogs().count { it.method.contains("/watch") }
        val before = watchAttempts()
        Thread.sleep(8_000)
        // Parked backoff is 30s; a spinning 1s→2s→4s loop would add ~4.
        assertTrue(
            "watch loop kept retrying while parked",
            watchAttempts() - before <= 2
        )
    }

    // Should: post exactly one notification per upgrade episode.
    // Should: clear the state and the notification on the next successful
    // request (recovery via the current-version node).
    @Test
    fun notificationIsOncePerEpisodeAndClearsOnRecovery() {
        assumeTrue("current-node instrumentation args absent", currentPairing != null)

        triggerRejection()
        awaitNotificationCount(1)
        triggerRejection() // same episode — must not re-notify or duplicate
        awaitNotificationCount(1)

        PairingStore.save(context, currentPairing!!)
        ApiClient.forContext(context)!!.enumerate(null) // succeeding is the recovery
        assertNull(UpgradeState.current)
        awaitNotificationCount(0)
    }
}
