package app.hopnet.drive

import android.database.Cursor
import android.database.MatrixCursor
import android.os.Bundle
import android.os.CancellationSignal
import android.os.Handler
import android.os.HandlerThread
import android.os.ParcelFileDescriptor
import android.os.storage.StorageManager
import android.provider.DocumentsContract
import android.provider.DocumentsContract.Document
import android.provider.DocumentsContract.Root
import android.provider.DocumentsProvider
import android.util.Log
import app.hopnet.drive.data.PairingStore
import app.hopnet.drive.net.ApiClient
import app.hopnet.drive.net.DpItem
import app.hopnet.drive.net.MIME_TYPE_DIR
import app.hopnet.drive.net.NodeHttpException
import app.hopnet.drive.net.ROOT_ID
import app.hopnet.drive.net.ReadProxyCallback
import app.hopnet.drive.net.WatchLoop
import app.hopnet.drive.net.WriteProxyCallback
import java.io.File
import java.io.FileNotFoundException
import java.io.IOException

/**
 * Live SAF provider for a paired HopNet node.
 *
 * Reads ride the DocumentProvider surface; every mutation rides the mount
 * surface, whose responses arrive only after the consensus transaction is
 * decided AND applied on the serving node — so each SAF call returns with
 * the authoritative post-mutation state and no convergence polling exists
 * anywhere. All calls are synchronous on SAF's binder threads; file I/O
 * runs on a per-open HandlerThread (never the main looper).
 */
class HopDriveProvider : DocumentsProvider() {

    companion object {
        private const val TAG = "HopDriveProvider"

        /** Matches the manifest's `${applicationId}.documents` placeholder. */
        val AUTHORITY = BuildConfig.APPLICATION_ID + ".documents"

        private const val WRITE_TEMP_PREFIX = "hopdrive-write-"
        private const val WRITE_TEMP_RETENTION_MS = 7L * 24 * 60 * 60 * 1000
        private const val MAX_NAME_MANGLE_ATTEMPTS = 32
        private const val MAX_PARENT_WALK_DEPTH = 64

        private val DEFAULT_ROOT_PROJECTION = arrayOf(
            Root.COLUMN_ROOT_ID,
            Root.COLUMN_MIME_TYPES,
            Root.COLUMN_FLAGS,
            Root.COLUMN_ICON,
            Root.COLUMN_TITLE,
            Root.COLUMN_SUMMARY,
            Root.COLUMN_DOCUMENT_ID,
            Root.COLUMN_AVAILABLE_BYTES,
        )

        private val DEFAULT_DOCUMENT_PROJECTION = arrayOf(
            Document.COLUMN_DOCUMENT_ID,
            Document.COLUMN_MIME_TYPE,
            Document.COLUMN_DISPLAY_NAME,
            Document.COLUMN_LAST_MODIFIED,
            Document.COLUMN_FLAGS,
            Document.COLUMN_SIZE,
        )

    }

    override fun onCreate(): Boolean {
        val cache = context?.cacheDir
        Thread {
            // Prune stale write temps, but keep recent ones — a failed
            // release-upload deliberately leaves its temp for post-mortem.
            val cutoff = System.currentTimeMillis() - WRITE_TEMP_RETENTION_MS
            cache?.listFiles()?.forEach { file ->
                if (file.name.startsWith(WRITE_TEMP_PREFIX) && file.lastModified() < cutoff) {
                    file.delete()
                }
            }
        }.start()
        return true
    }

    // --- roots ---

    override fun queryRoots(projection: Array<out String>?): Cursor {
        WatchLoop.touch(context!!)
        val result = MatrixCursor(projection ?: DEFAULT_ROOT_PROJECTION)
        val pairing = PairingStore.load(context!!) ?: return result
        val client = ApiClient.forContext(context!!) ?: return result

        // Best-effort capacity; never blocks the root row (quick timeout,
        // and a single-node dev mesh legitimately reports total 0).
        val availableBytes = try {
            val statfs = client.statfs()
            if (statfs.totalBytes > 0) statfs.totalBytes - statfs.usedBytes else null
        } catch (e: Exception) {
            Log.d(TAG, "statfs unavailable: $e")
            null
        }

        result.newRow().apply {
            add(Root.COLUMN_ROOT_ID, ROOT_ID)
            add(Root.COLUMN_MIME_TYPES, "*/*")
            add(
                Root.COLUMN_FLAGS,
                Root.FLAG_SUPPORTS_CREATE or Root.FLAG_SUPPORTS_IS_CHILD
            )
            add(Root.COLUMN_ICON, R.mipmap.ic_launcher)
            add(Root.COLUMN_TITLE, "Hop Drive")
            add(Root.COLUMN_SUMMARY, "${pairing.host}:${pairing.port}")
            add(Root.COLUMN_DOCUMENT_ID, ROOT_ID)
            if (availableBytes != null) {
                add(Root.COLUMN_AVAILABLE_BYTES, availableBytes)
            }
        }
        return result
    }

    // --- queries ---

    override fun queryDocument(documentId: String, projection: Array<out String>?): Cursor {
        WatchLoop.touch(context!!)
        val result = MatrixCursor(projection ?: DEFAULT_DOCUMENT_PROJECTION)
        if (documentId == ROOT_ID) {
            result.newRow().apply {
                add(Document.COLUMN_DOCUMENT_ID, ROOT_ID)
                add(Document.COLUMN_MIME_TYPE, Document.MIME_TYPE_DIR)
                add(Document.COLUMN_DISPLAY_NAME, "Hop Drive")
                add(Document.COLUMN_LAST_MODIFIED, System.currentTimeMillis())
                add(Document.COLUMN_FLAGS, Document.FLAG_DIR_SUPPORTS_CREATE)
                add(Document.COLUMN_SIZE, 0)
            }
            return result
        }
        return withQueryErrors(result) {
            val item = requireClient().item(documentId)
            WatchLoop.parentMap[item.id] = item.parentId ?: ROOT_ID
            addItemRow(result, item)
            result
        }
    }

    override fun queryChildDocuments(
        parentDocumentId: String,
        projection: Array<out String>?,
        sortOrder: String?,
    ): Cursor {
        WatchLoop.touch(context!!)
        val result = MatrixCursor(projection ?: DEFAULT_DOCUMENT_PROJECTION)
        result.setNotificationUri(
            context?.contentResolver,
            DocumentsContract.buildChildDocumentsUri(AUTHORITY, parentDocumentId)
        )
        return withQueryErrors(result) {
            val items = requireClient().enumerate(apiParent(parentDocumentId))
            items.forEach { item ->
                WatchLoop.parentMap[item.id] = parentDocumentId
                addItemRow(result, item)
            }
            result
        }
    }

    override fun isChildDocument(parentDocumentId: String, documentId: String): Boolean {
        var current: String? = documentId
        repeat(MAX_PARENT_WALK_DEPTH) {
            if (current == null || current == ROOT_ID) {
                return parentDocumentId == ROOT_ID && current == ROOT_ID
            }
            if (current == parentDocumentId) return true
            current = WatchLoop.parentMap[current] ?: try {
                val item = requireClient().item(current!!)
                (item.parentId ?: ROOT_ID).also { WatchLoop.parentMap[current!!] = it }
            } catch (e: Exception) {
                Log.w(TAG, "isChildDocument lookup failed for $current", e)
                return false
            }
        }
        return false
    }

    // --- open ---

    override fun openDocument(
        documentId: String,
        mode: String,
        signal: CancellationSignal?,
    ): ParcelFileDescriptor {
        WatchLoop.touch(context!!)
        val client = requireClient()
        val item = try {
            client.item(documentId, signal)
        } catch (e: NodeHttpException) {
            throw FileNotFoundException("Document $documentId: HTTP ${e.code}")
        } catch (e: IOException) {
            throw FileNotFoundException("Node unreachable: $e")
        }
        WatchLoop.parentMap[item.id] = item.parentId ?: ROOT_ID

        val storageManager = context!!.getSystemService(StorageManager::class.java)
        val accessMode = ParcelFileDescriptor.parseMode(mode)
        val thread = HandlerThread("hopdrive-io-${documentId.take(8)}").apply { start() }

        val callback = if (mode == "r") {
            ReadProxyCallback(client, documentId, item.size, thread)
        } else {
            val temp = File(
                context!!.cacheDir,
                "$WRITE_TEMP_PREFIX${documentId.take(8)}-${System.nanoTime()}.tmp"
            )
            // "w"/"wt"/"rwt" truncate; "rw"/"wa" must start from current bytes.
            val preserveExisting = !mode.contains('t') && mode != "w"
            if (preserveExisting && item.size > 0) {
                try {
                    client.download(documentId, 0, signal).use { response ->
                        temp.outputStream().use { out ->
                            response.body!!.byteStream().copyTo(out)
                        }
                    }
                } catch (e: IOException) {
                    thread.quitSafely()
                    temp.delete()
                    throw FileNotFoundException("Prefill for $mode failed: $e")
                }
            }
            val parentDocId = WatchLoop.parentMap[documentId] ?: ROOT_ID
            WriteProxyCallback(client, documentId, temp, thread) {
                notifyChildrenChanged(parentDocId)
                context?.contentResolver?.notifyChange(
                    DocumentsContract.buildDocumentUri(AUTHORITY, documentId),
                    null
                )
            }
        }

        return storageManager.openProxyFileDescriptor(
            accessMode,
            callback,
            Handler(thread.looper)
        )
    }

    // --- mutations (mount surface, strict-wait) ---

    override fun createDocument(
        parentDocumentId: String,
        mimeType: String,
        displayName: String,
    ): String {
        val client = requireClient()
        val parent = apiParent(parentDocumentId)
        val isFolder = mimeType == Document.MIME_TYPE_DIR

        var lastConflict: NodeHttpException? = null
        for (attempt in 0 until MAX_NAME_MANGLE_ATTEMPTS) {
            val name = mangleName(displayName, attempt)
            try {
                val item = if (isFolder) {
                    client.createFolder(parent, name)
                } else {
                    val empty = File.createTempFile(WRITE_TEMP_PREFIX, ".empty", context!!.cacheDir)
                    try {
                        client.createFile(parent, name, empty)
                    } finally {
                        empty.delete()
                    }
                }
                val id = item.id ?: throw FileNotFoundException("create returned no id")
                WatchLoop.parentMap[id] = parentDocumentId
                notifyChildrenChanged(parentDocumentId)
                return id
            } catch (e: NodeHttpException) {
                if (e.code == 409) {
                    lastConflict = e
                    continue
                }
                throw FileNotFoundException("create '$displayName': HTTP ${e.code}")
            } catch (e: IOException) {
                throw FileNotFoundException("create '$displayName': $e")
            }
        }
        throw FileNotFoundException("create '$displayName': every candidate name taken ($lastConflict)")
    }

    override fun deleteDocument(documentId: String) {
        val client = requireClient()
        val parentDocId = WatchLoop.parentMap[documentId]
            ?: runCatching { client.item(documentId).parentId ?: ROOT_ID }.getOrNull()
            ?: ROOT_ID
        try {
            client.delete(documentId, recursive = true)
        } catch (e: NodeHttpException) {
            throw FileNotFoundException("delete $documentId: HTTP ${e.code}")
        } catch (e: IOException) {
            throw FileNotFoundException("delete $documentId: $e")
        }
        WatchLoop.parentMap.remove(documentId)
        notifyChildrenChanged(parentDocId)
    }

    override fun renameDocument(documentId: String, displayName: String): String? {
        val client = requireClient()
        try {
            val item = client.rename(documentId, displayName)
            val parentDocId = item.parentId ?: ROOT_ID
            WatchLoop.parentMap[documentId] = parentDocId
            notifyChildrenChanged(parentDocId)
        } catch (e: NodeHttpException) {
            throw FileNotFoundException("rename $documentId: HTTP ${e.code}")
        } catch (e: IOException) {
            throw FileNotFoundException("rename $documentId: $e")
        }
        // Inode ids are stable across rename.
        return null
    }

    override fun moveDocument(
        sourceDocumentId: String,
        sourceParentDocumentId: String,
        targetParentDocumentId: String,
    ): String {
        val client = requireClient()
        try {
            client.move(sourceDocumentId, apiParent(targetParentDocumentId))
        } catch (e: NodeHttpException) {
            throw FileNotFoundException("move $sourceDocumentId: HTTP ${e.code}")
        } catch (e: IOException) {
            throw FileNotFoundException("move $sourceDocumentId: $e")
        }
        WatchLoop.parentMap[sourceDocumentId] = targetParentDocumentId
        notifyChildrenChanged(sourceParentDocumentId)
        notifyChildrenChanged(targetParentDocumentId)
        return sourceDocumentId
    }

    // --- helpers ---

    /** ROOT_ID is a client-side sentinel; the API expresses root as absence. */
    private fun apiParent(parentDocumentId: String): String? =
        if (parentDocumentId == ROOT_ID) null else parentDocumentId

    private fun requireClient(): ApiClient =
        ApiClient.forContext(context!!)
            ?: throw FileNotFoundException("Hop Drive is not paired")

    /**
     * Transient failures surface through EXTRA_ERROR so DocumentsUI shows
     * a banner instead of treating the root as broken.
     */
    private fun withQueryErrors(cursor: MatrixCursor, block: () -> Cursor): Cursor {
        val message = try {
            return block()
        } catch (e: NodeHttpException) {
            when (e.code) {
                401 -> "Pairing rejected — re-pair in the Hop Drive app"
                426 -> "Update the Hop Drive app — this node requires a newer version"
                428 -> "Node is locked — sign in on the node once"
                else -> "Node error (HTTP ${e.code})"
            }
        } catch (e: FileNotFoundException) {
            e.message ?: "Not paired"
        } catch (e: IOException) {
            "Node unreachable"
        }
        cursor.extras = Bundle().apply {
            putString(DocumentsContract.EXTRA_ERROR, message)
        }
        return cursor
    }

    private fun addItemRow(cursor: MatrixCursor, item: DpItem) {
        val isDirectory = item.mimeType == MIME_TYPE_DIR
        var flags = Document.FLAG_SUPPORTS_DELETE or
            Document.FLAG_SUPPORTS_RENAME or
            Document.FLAG_SUPPORTS_MOVE
        flags = if (isDirectory) {
            flags or Document.FLAG_DIR_SUPPORTS_CREATE
        } else {
            flags or Document.FLAG_SUPPORTS_WRITE
        }
        cursor.newRow().apply {
            add(Document.COLUMN_DOCUMENT_ID, item.id)
            add(Document.COLUMN_MIME_TYPE, item.mimeType)
            add(Document.COLUMN_DISPLAY_NAME, item.name)
            add(Document.COLUMN_LAST_MODIFIED, item.lastModified)
            add(Document.COLUMN_FLAGS, flags)
            add(Document.COLUMN_SIZE, item.size)
        }
    }

    private fun notifyChildrenChanged(parentDocumentId: String) {
        context?.contentResolver?.notifyChange(
            DocumentsContract.buildChildDocumentsUri(AUTHORITY, parentDocumentId),
            null
        )
    }

    /** SAF-conventional collision handling: `report.txt` → `report (1).txt`. */
    private fun mangleName(displayName: String, attempt: Int): String {
        if (attempt == 0) return displayName
        val dot = displayName.lastIndexOf('.')
        return if (dot > 0) {
            "${displayName.substring(0, dot)} ($attempt)${displayName.substring(dot)}"
        } else {
            "$displayName ($attempt)"
        }
    }
}
