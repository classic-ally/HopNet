package app.hopnet.drive.data

import android.content.Context
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import java.io.File
import java.util.UUID

/**
 * Repository for managing HopNet document metadata with JSON file persistence.
 * Binary content is stored separately via ContentStorage.
 *
 * The JSON file can be inspected to understand the data contract
 * that the real HopNet API will need to provide.
 */
class DocumentRepository private constructor(private val context: Context) {

    companion object {
        private const val FILENAME = "hopnet_documents.json"

        @Volatile
        private var instance: DocumentRepository? = null

        fun getInstance(context: Context): DocumentRepository {
            return instance ?: synchronized(this) {
                instance ?: DocumentRepository(context.applicationContext).also { instance = it }
            }
        }
    }

    private val contentStorage: ContentStorage by lazy { ContentStorage.getInstance(context) }

    private val json = Json {
        prettyPrint = true
        encodeDefaults = true
    }

    private val file: File
        get() = File(context.filesDir, FILENAME)

    private var store: DocumentStore = loadOrCreateDefault()

    /** Listeners notified when data changes */
    private val changeListeners = mutableListOf<() -> Unit>()

    fun addChangeListener(listener: () -> Unit) {
        changeListeners.add(listener)
    }

    fun removeChangeListener(listener: () -> Unit) {
        changeListeners.remove(listener)
    }

    private fun notifyChange() {
        changeListeners.forEach { it() }
    }

    /**
     * Load from JSON file, or create default mock data if file doesn't exist.
     */
    private fun loadOrCreateDefault(): DocumentStore {
        return if (file.exists()) {
            try {
                json.decodeFromString<DocumentStore>(file.readText())
            } catch (e: Exception) {
                createDefaultStore().also { save(it) }
            }
        } else {
            createDefaultStore().also { save(it) }
        }
    }

    /**
     * Create empty initial store.
     */
    private fun createDefaultStore(): DocumentStore {
        return DocumentStore(mutableListOf())
    }

    /**
     * Save current store to JSON file.
     */
    private fun save(store: DocumentStore = this.store) {
        file.writeText(json.encodeToString(store))
    }

    /**
     * Reload from disk (useful if file was modified externally).
     */
    fun reload() {
        store = loadOrCreateDefault()
        notifyChange()
    }

    /**
     * Reset to default mock data.
     */
    fun reset() {
        contentStorage.clearAll()
        store = createDefaultStore()
        save()
        notifyChange()
    }

    // --- Read operations ---

    fun getDocument(id: String): HopNetDocument? = store.findById(id)

    fun getChildren(parentId: String): List<HopNetDocument> = store.findChildren(parentId)

    fun getAllDocuments(): List<HopNetDocument> = store.documents.toList()

    // --- Write operations ---

    /**
     * Create a new document.
     * @return The ID of the created document.
     */
    fun createDocument(parentId: String, name: String, mimeType: String): String {
        val id = UUID.randomUUID().toString()
        val now = System.currentTimeMillis()

        val document = HopNetDocument(
            id = id,
            name = name,
            mimeType = mimeType,
            size = 0,
            lastModified = now,
            parentId = parentId
        )

        store.add(document)
        save()
        notifyChange()
        return id
    }

    /**
     * Delete a document and all its descendants.
     */
    fun deleteDocument(id: String) {
        // First, recursively delete children if this is a folder
        val doc = store.findById(id) ?: return
        if (doc.isDirectory) {
            store.findChildren(id).forEach { child ->
                deleteDocument(child.id)
            }
        }

        // Delete binary content if it exists
        contentStorage.delete(id)

        store.remove(id)
        save()
        notifyChange()
    }

    /**
     * Rename a document.
     * @return The new document ID (same as old for this implementation).
     */
    fun renameDocument(id: String, newName: String): String {
        store.update(id) { doc ->
            doc.copy(name = newName, lastModified = System.currentTimeMillis())
        }
        save()
        notifyChange()
        return id
    }

    /**
     * Update document size (called after binary content is written).
     */
    fun updateSize(id: String, size: Long) {
        store.update(id) { doc ->
            doc.copy(
                size = size,
                lastModified = System.currentTimeMillis()
            )
        }
        save()
        notifyChange()
    }

    /**
     * Move a document to a new parent.
     */
    fun moveDocument(id: String, newParentId: String) {
        store.update(id) { doc ->
            doc.copy(parentId = newParentId, lastModified = System.currentTimeMillis())
        }
        save()
        notifyChange()
    }

    // --- JSON access for UI ---

    /**
     * Get the current store as a pretty-printed JSON string.
     * Useful for displaying in the debug UI.
     */
    fun toJson(): String = json.encodeToString(store)

    /**
     * Get the path to the JSON file for reference.
     */
    fun getFilePath(): String = file.absolutePath
}
