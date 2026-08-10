package app.hopnet.drive

import android.database.Cursor
import android.database.MatrixCursor
import android.os.CancellationSignal
import android.os.Handler
import android.os.Looper
import android.os.ParcelFileDescriptor
import android.os.storage.StorageManager
import android.provider.DocumentsContract.Document
import android.provider.DocumentsContract.Root
import android.provider.DocumentsProvider
import android.util.Log
import app.hopnet.drive.data.ContentStorage
import app.hopnet.drive.data.DocumentRepository
import app.hopnet.drive.data.HopNetDocument
import app.hopnet.drive.data.LogRepository
import app.hopnet.drive.data.LoggedDocument
import app.hopnet.drive.data.LoggedRoot
import app.hopnet.drive.data.toLogged

/**
 * DocumentsProvider implementation for HopNet distributed storage.
 *
 * This provider exposes HopNet files through Android's Storage Access Framework,
 * allowing any app to browse and open files from HopNet via the system Files app.
 *
 * Data is persisted to a JSON file which can be inspected to understand
 * the API contract needed from the real HopNet backend.
 */
class HopDriveProvider : DocumentsProvider() {

    companion object {
        private const val TAG = "HopDriveProvider"

        /** Matches the manifest's ${applicationId}.documents placeholder. */
        val AUTHORITY = BuildConfig.APPLICATION_ID + ".documents"

        // Column projections
        private val DEFAULT_ROOT_PROJECTION = arrayOf(
            Root.COLUMN_ROOT_ID,
            Root.COLUMN_MIME_TYPES,
            Root.COLUMN_FLAGS,
            Root.COLUMN_ICON,
            Root.COLUMN_TITLE,
            Root.COLUMN_SUMMARY,
            Root.COLUMN_DOCUMENT_ID,
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

    private lateinit var repository: DocumentRepository
    private lateinit var contentStorage: ContentStorage

    override fun onCreate(): Boolean {
        repository = DocumentRepository.getInstance(context!!)
        contentStorage = ContentStorage.getInstance(context!!)
        Log.d(TAG, "Provider created, data file: ${repository.getFilePath()}")
        return true
    }

    /**
     * Returns the root(s) for this provider.
     * A root represents a top-level storage location (like "Hop Drive" in the Files app sidebar).
     */
    override fun queryRoots(projection: Array<out String>?): Cursor {
        Log.d(TAG, "queryRoots called")
        val result = MatrixCursor(projection ?: DEFAULT_ROOT_PROJECTION)

        val root = LoggedRoot(
            rootId = HopNetDocument.ROOT_ID,
            title = "Hop Drive",
            summary = "Distributed Storage",
            documentId = HopNetDocument.ROOT_ID
        )

        result.newRow().apply {
            add(Root.COLUMN_ROOT_ID, root.rootId)
            add(Root.COLUMN_MIME_TYPES, "*/*")
            add(Root.COLUMN_FLAGS,
                Root.FLAG_SUPPORTS_CREATE or
                Root.FLAG_SUPPORTS_IS_CHILD
            )
            add(Root.COLUMN_ICON, R.mipmap.ic_launcher)
            add(Root.COLUMN_TITLE, root.title)
            add(Root.COLUMN_SUMMARY, root.summary)
            add(Root.COLUMN_DOCUMENT_ID, root.documentId)
        }

        LogRepository.logQueryRoots(listOf(root))

        return result
    }

    /**
     * Returns metadata for a single document.
     */
    override fun queryDocument(documentId: String, projection: Array<out String>?): Cursor {
        Log.d(TAG, "queryDocument: $documentId")
        val result = MatrixCursor(projection ?: DEFAULT_DOCUMENT_PROJECTION)

        val loggedDoc: LoggedDocument?
        if (documentId == HopNetDocument.ROOT_ID) {
            // Root document
            val now = System.currentTimeMillis()
            result.newRow().apply {
                add(Document.COLUMN_DOCUMENT_ID, HopNetDocument.ROOT_ID)
                add(Document.COLUMN_MIME_TYPE, Document.MIME_TYPE_DIR)
                add(Document.COLUMN_DISPLAY_NAME, "Hop Drive")
                add(Document.COLUMN_LAST_MODIFIED, now)
                add(Document.COLUMN_FLAGS, Document.FLAG_DIR_SUPPORTS_CREATE)
                add(Document.COLUMN_SIZE, 0)
            }
            loggedDoc = LoggedDocument(
                id = HopNetDocument.ROOT_ID,
                name = "Hop Drive",
                mimeType = Document.MIME_TYPE_DIR,
                size = 0,
                lastModified = now,
                parentId = null
            )
        } else {
            val doc = repository.getDocument(documentId)
            loggedDoc = doc?.toLogged()
            doc?.let { addDocumentRow(result, it) }
        }

        LogRepository.logQueryDocument(documentId, loggedDoc)
        return result
    }

    /**
     * Returns the children of a folder document.
     */
    override fun queryChildDocuments(
        parentDocumentId: String,
        projection: Array<out String>?,
        sortOrder: String?
    ): Cursor {
        Log.d(TAG, "queryChildDocuments: parent=$parentDocumentId")
        val result = MatrixCursor(projection ?: DEFAULT_DOCUMENT_PROJECTION)

        val children = repository.getChildren(parentDocumentId)
        children.forEach { doc ->
            addDocumentRow(result, doc)
        }

        LogRepository.logQueryChildDocuments(parentDocumentId, children.map { it.toLogged() })

        // Enable auto-refresh when data changes
        result.setNotificationUri(context?.contentResolver,
            android.provider.DocumentsContract.buildChildDocumentsUri(
                AUTHORITY, parentDocumentId
            )
        )

        return result
    }

    /**
     * Opens a document for reading (or writing, based on mode).
     * Uses ProxyFileDescriptor for efficient streaming without temp files.
     */
    override fun openDocument(
        documentId: String,
        mode: String,
        signal: CancellationSignal?
    ): ParcelFileDescriptor {
        Log.d(TAG, "openDocument: $documentId, mode=$mode")

        val doc = repository.getDocument(documentId)
        LogRepository.logOpenDocument(documentId, mode, doc?.toLogged())

        if (doc == null) {
            throw java.io.FileNotFoundException("Document $documentId not found")
        }

        val storageManager = context!!.getSystemService(StorageManager::class.java)
        val accessMode = ParcelFileDescriptor.parseMode(mode)
        val handler = Handler(Looper.getMainLooper())

        val parentId = doc.parentId

        val callback = MockProxyCallback(
            documentId = documentId,
            repository = repository,
            contentStorage = contentStorage,
            mode = accessMode
        ) {
            // onRelease callback - notify content resolver of changes
            Log.d(TAG, "Document closed: $documentId")
            context?.contentResolver?.notifyChange(
                android.provider.DocumentsContract.buildDocumentUri(
                    AUTHORITY, documentId
                ), null
            )
            // Also notify parent's child list so Files app refreshes the size
            if (parentId != null) {
                context?.contentResolver?.notifyChange(
                    android.provider.DocumentsContract.buildChildDocumentsUri(
                        AUTHORITY, parentId
                    ), null
                )
            }
        }

        return storageManager.openProxyFileDescriptor(accessMode, callback, handler)
    }

    /**
     * Create a new document (file or folder).
     */
    override fun createDocument(
        parentDocumentId: String,
        mimeType: String,
        displayName: String
    ): String {
        Log.d(TAG, "createDocument: parent=$parentDocumentId, name=$displayName, type=$mimeType")

        val newId = repository.createDocument(parentDocumentId, displayName, mimeType)

        // Get the created document for logging
        val created = repository.getDocument(newId)!!
        LogRepository.logCreateDocument(parentDocumentId, displayName, mimeType, created.toLogged())

        // Notify content resolver
        context?.contentResolver?.notifyChange(
            android.provider.DocumentsContract.buildChildDocumentsUri(
                AUTHORITY, parentDocumentId
            ), null
        )

        Log.d(TAG, "Created document: $newId")
        return newId
    }

    /**
     * Delete a document.
     */
    override fun deleteDocument(documentId: String) {
        Log.d(TAG, "deleteDocument: $documentId")

        val doc = repository.getDocument(documentId)
        val loggedDoc = doc?.toLogged()
        val parentId = doc?.parentId

        repository.deleteDocument(documentId)

        LogRepository.logDeleteDocument(documentId, loggedDoc)

        // Notify content resolver
        if (parentId != null) {
            context?.contentResolver?.notifyChange(
                android.provider.DocumentsContract.buildChildDocumentsUri(
                    AUTHORITY, parentId
                ), null
            )
        }

        Log.d(TAG, "Deleted document: $documentId")
    }

    /**
     * Rename a document.
     */
    override fun renameDocument(documentId: String, displayName: String): String {
        Log.d(TAG, "renameDocument: $documentId -> $displayName")

        val newId = repository.renameDocument(documentId, displayName)

        // Get the renamed document for logging
        val renamed = repository.getDocument(newId)!!
        LogRepository.logRenameDocument(documentId, displayName, renamed.toLogged())

        if (renamed.parentId != null) {
            context?.contentResolver?.notifyChange(
                android.provider.DocumentsContract.buildChildDocumentsUri(
                    AUTHORITY, renamed.parentId
                ), null
            )
        }

        Log.d(TAG, "Renamed document: $newId")
        return newId
    }

    /**
     * Check if a document is a descendant of another.
     */
    override fun isChildDocument(parentDocumentId: String, documentId: String): Boolean {
        var currentId: String? = documentId
        while (currentId != null && currentId != HopNetDocument.ROOT_ID) {
            if (currentId == parentDocumentId) return true
            currentId = repository.getDocument(currentId)?.parentId
        }
        val result = parentDocumentId == HopNetDocument.ROOT_ID && currentId == HopNetDocument.ROOT_ID
        LogRepository.logIsChildDocument(parentDocumentId, documentId, result)
        return result
    }

    /**
     * Helper to add a document row to a cursor.
     */
    private fun addDocumentRow(cursor: MatrixCursor, doc: HopNetDocument) {
        var flags = 0
        if (doc.isDirectory) {
            flags = flags or Document.FLAG_DIR_SUPPORTS_CREATE
        } else {
            flags = flags or Document.FLAG_SUPPORTS_DELETE
            flags = flags or Document.FLAG_SUPPORTS_WRITE
            flags = flags or Document.FLAG_SUPPORTS_RENAME
        }
        // Folders can also be deleted and renamed
        if (doc.isDirectory) {
            flags = flags or Document.FLAG_SUPPORTS_DELETE
            flags = flags or Document.FLAG_SUPPORTS_RENAME
        }

        cursor.newRow().apply {
            add(Document.COLUMN_DOCUMENT_ID, doc.id)
            add(Document.COLUMN_MIME_TYPE, doc.mimeType)
            add(Document.COLUMN_DISPLAY_NAME, doc.name)
            add(Document.COLUMN_LAST_MODIFIED, doc.lastModified)
            add(Document.COLUMN_FLAGS, flags)
            add(Document.COLUMN_SIZE, doc.size)
        }
    }
}
