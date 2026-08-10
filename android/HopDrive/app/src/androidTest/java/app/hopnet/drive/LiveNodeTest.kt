package app.hopnet.drive

import android.content.ContentResolver
import android.content.Context
import android.database.Cursor
import android.net.Uri
import android.provider.DocumentsContract
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import app.hopnet.drive.data.Pairing
import app.hopnet.drive.data.PairingStore
import app.hopnet.drive.net.ROOT_ID
import java.io.FileInputStream
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

/**
 * End-to-end coverage of the documents provider against a LIVE node.
 *
 * Pairing comes from instrumentation args (same-UID access bypasses the
 * MANAGE_DOCUMENTS gate):
 *
 *   ./gradlew connectedDebugAndroidTest \
 *     -Pandroid.testInstrumentationRunnerArguments.host=10.0.2.2 \
 *     -Pandroid.testInstrumentationRunnerArguments.port=34700 \
 *     -Pandroid.testInstrumentationRunnerArguments.spki=<64 hex> \
 *     -Pandroid.testInstrumentationRunnerArguments.token=<device_id>.<secret>
 *
 * Mutations ride the strict-wait mount surface, so every effect is
 * asserted immediately after the call — deliberately no polling.
 */
@RunWith(AndroidJUnit4::class)
class LiveNodeTest {

    private lateinit var context: Context
    private lateinit var resolver: ContentResolver
    private val authority = HopDriveProvider.AUTHORITY

    @Before
    fun pair() {
        context = InstrumentationRegistry.getInstrumentation().targetContext
        val args = InstrumentationRegistry.getArguments()
        val host = args.getString("host") ?: error("missing instrumentation arg: host")
        val port = args.getString("port")?.toInt() ?: error("missing instrumentation arg: port")
        val spki = args.getString("spki") ?: error("missing instrumentation arg: spki")
        val token = args.getString("token") ?: error("missing instrumentation arg: token")
        PairingStore.save(context, Pairing(host, port, spki, token))
        resolver = context.contentResolver
    }

    private fun docUri(documentId: String): Uri =
        DocumentsContract.buildDocumentUri(authority, documentId)

    private fun childrenUri(parentId: String): Uri =
        DocumentsContract.buildChildDocumentsUri(authority, parentId)

    private fun childNames(parentId: String): Map<String, String> {
        val names = mutableMapOf<String, String>()
        resolver.query(childrenUri(parentId), null, null, null, null)!!.use { cursor ->
            assertNoError(cursor)
            while (cursor.moveToNext()) {
                val id = cursor.getString(
                    cursor.getColumnIndexOrThrow(DocumentsContract.Document.COLUMN_DOCUMENT_ID)
                )
                val name = cursor.getString(
                    cursor.getColumnIndexOrThrow(DocumentsContract.Document.COLUMN_DISPLAY_NAME)
                )
                names[name] = id
            }
        }
        return names
    }

    private fun assertNoError(cursor: Cursor) {
        val error = cursor.extras?.getString(DocumentsContract.EXTRA_ERROR)
        if (error != null) {
            throw AssertionError("provider returned EXTRA_ERROR: $error")
        }
    }

    /** Bounded wait for the proxy fd's release-time upload to land. */
    private fun awaitContent(uri: Uri, expected: ByteArray, timeoutMs: Long = 10_000) {
        val deadline = System.currentTimeMillis() + timeoutMs
        var last = ""
        while (System.currentTimeMillis() < deadline) {
            last = resolver.openInputStream(uri)!!.use { String(it.readBytes()) }
            if (last == String(expected)) return
            Thread.sleep(250)
        }
        assertEquals(String(expected), last)
    }

    // Impact: this is the only path that proves remote-origin changes
    // reach the Files app without user action.
    // Should: surface a mutation made outside the provider as a change
    // notification on the affected children URI.
    @Test
    fun remoteChangeNotifiesChildrenUri() {
        // A query starts the watch loop (provider touch) and anchors it.
        childNames(ROOT_ID)

        val changed = java.util.concurrent.CountDownLatch(1)
        val observer = object : android.database.ContentObserver(null) {
            override fun onChange(selfChange: Boolean) {
                changed.countDown()
            }
        }
        resolver.registerContentObserver(childrenUri(ROOT_ID), false, observer)
        try {
            // Mutate BEHIND the provider's back: a raw ApiClient call can't
            // trigger the provider's own notifyChange — only the watch
            // loop's delta sync can surface it.
            val client = app.hopnet.drive.net.ApiClient.forContext(context)!!
            val name = "watch-probe-${System.currentTimeMillis()}"
            val item = client.createFolder(null, name)
            try {
                assertTrue(
                    "no change notification within 15s of a remote mutation",
                    changed.await(15, java.util.concurrent.TimeUnit.SECONDS)
                )
                assertTrue(
                    "remote folder not visible after notification",
                    name in childNames(ROOT_ID)
                )
            } finally {
                client.delete(item.id!!, recursive = false)
            }
        } finally {
            resolver.unregisterContentObserver(observer)
        }
    }

    // Should: expose exactly one root once paired.
    @Test
    fun rootIsExposedWhenPaired() {
        resolver.query(
            DocumentsContract.buildRootsUri(authority), null, null, null, null
        )!!.use { cursor ->
            assertEquals(1, cursor.count)
            assertTrue(cursor.moveToFirst())
            val title = cursor.getString(
                cursor.getColumnIndexOrThrow(DocumentsContract.Root.COLUMN_TITLE)
            )
            assertEquals("Hop Drive", title)
        }
    }

    // Impact: this is the full SAF lifecycle a Files-app user exercises;
    // each mutation's effect must be visible IMMEDIATELY because the mount
    // surface is strict-wait.
    // Should: create a folder and a file, round-trip written bytes through
    // open/read including a seek, rename, move into the folder, and delete
    // — with every step visible to the next without polling.
    // Should not: leave any test artifacts behind on the node.
    @Test
    fun fullLifecycleAgainstLiveNode() {
        val stamp = System.currentTimeMillis()
        val folderName = "e2e-folder-$stamp"
        val fileName = "e2e-file-$stamp.txt"
        val content = "hop drive end to end $stamp".toByteArray()

        // Create folder + file at root.
        val folderUri = DocumentsContract.createDocument(
            resolver, docUri(ROOT_ID), DocumentsContract.Document.MIME_TYPE_DIR, folderName
        )
        assertNotNull("folder create returned null", folderUri)
        val folderId = DocumentsContract.getDocumentId(folderUri!!)

        val fileUri = DocumentsContract.createDocument(
            resolver, docUri(ROOT_ID), "text/plain", fileName
        )
        assertNotNull("file create returned null", fileUri)
        val fileId = DocumentsContract.getDocumentId(fileUri!!)

        var rootChildren = childNames(ROOT_ID)
        assertTrue("folder missing after create", folderName in rootChildren)
        assertTrue("file missing after create", fileName in rootChildren)

        // Write bytes, read them back, and read again from an offset (the
        // proxy fd turns the seek into a ranged download). The writer's
        // close() returns BEFORE the proxy's onRelease runs the upload on
        // its handler thread, so visibility needs a short bounded wait —
        // this is the local fd-release flush window, not consensus lag
        // (the upload itself is strict-wait once it starts).
        resolver.openOutputStream(fileUri, "w")!!.use { it.write(content) }
        awaitContent(fileUri, content)
        resolver.openFileDescriptor(fileUri, "r")!!.use { pfd ->
            FileInputStream(pfd.fileDescriptor).use { input ->
                input.channel.position(8)
                val tail = input.readBytes()
                assertEquals(String(content.copyOfRange(8, content.size)), String(tail))
            }
        }

        // Rename (inode id is stable, so the uri stays valid).
        val renamed = "renamed-$stamp.txt"
        DocumentsContract.renameDocument(resolver, fileUri, renamed)
        rootChildren = childNames(ROOT_ID)
        assertTrue("renamed file missing", renamed in rootChildren)
        assertTrue("old name still present", fileName !in rootChildren)

        // Move into the folder.
        DocumentsContract.moveDocument(resolver, fileUri, docUri(ROOT_ID), docUri(folderId))
        assertTrue("file not in folder after move", renamed in childNames(folderId))
        assertTrue("file still at root after move", renamed !in childNames(ROOT_ID))

        // Recursive delete cleans everything up.
        DocumentsContract.deleteDocument(resolver, docUri(folderId))
        rootChildren = childNames(ROOT_ID)
        assertTrue("folder still present after delete", folderName !in rootChildren)
        val orphan = runCatching { resolver.query(docUri(fileId), null, null, null, null)?.use { it.count } }
        assertTrue("moved file survived recursive delete", orphan.getOrNull() != 1)
    }
}
