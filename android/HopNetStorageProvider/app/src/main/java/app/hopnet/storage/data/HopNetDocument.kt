package app.hopnet.storage.data

import kotlinx.serialization.Serializable

/**
 * Represents a document (file or folder) in HopNet storage.
 * This structure mirrors what the backend API will need to provide.
 */
@Serializable
data class HopNetDocument(
    /** Unique identifier for this document */
    val id: String,

    /** Display name shown to users */
    val name: String,

    /** MIME type - use "vnd.android.document/directory" for folders */
    val mimeType: String,

    /** File size in bytes (0 for folders) */
    val size: Long,

    /** Last modification timestamp in milliseconds since epoch */
    val lastModified: Long,

    /** Parent document ID, or null for root-level items */
    val parentId: String?
) {
    companion object {
        const val MIME_TYPE_DIR = "vnd.android.document/directory"
        const val ROOT_ID = "hopnet_root"
    }

    val isDirectory: Boolean
        get() = mimeType == MIME_TYPE_DIR
}

/**
 * Container for the full document store, serialized to JSON.
 */
@Serializable
data class DocumentStore(
    val documents: MutableList<HopNetDocument> = mutableListOf()
) {
    fun findById(id: String): HopNetDocument? = documents.find { it.id == id }

    fun findChildren(parentId: String): List<HopNetDocument> =
        documents.filter { it.parentId == parentId }

    fun add(document: HopNetDocument) {
        documents.add(document)
    }

    fun remove(id: String): Boolean {
        return documents.removeIf { it.id == id }
    }

    fun update(id: String, transform: (HopNetDocument) -> HopNetDocument): Boolean {
        val index = documents.indexOfFirst { it.id == id }
        if (index == -1) return false
        documents[index] = transform(documents[index])
        return true
    }
}
