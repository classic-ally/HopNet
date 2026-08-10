package app.hopnet.drive.data

import android.content.Context
import android.util.Log
import java.io.File

/**
 * Binary content storage that stores file data separately from JSON metadata.
 *
 * Files are stored in the app's internal files directory using the document ID
 * as the filename. This avoids corruption issues when storing binary data in JSON.
 */
class ContentStorage private constructor(private val context: Context) {

    companion object {
        private const val TAG = "ContentStorage"
        private const val CONTENT_DIR = "content"

        @Volatile
        private var instance: ContentStorage? = null

        fun getInstance(context: Context): ContentStorage {
            return instance ?: synchronized(this) {
                instance ?: ContentStorage(context.applicationContext).also { instance = it }
            }
        }
    }

    private val contentDir: File by lazy {
        File(context.filesDir, CONTENT_DIR).also { it.mkdirs() }
    }

    /**
     * Get the file for a document's content.
     */
    fun getContentFile(documentId: String): File {
        // Sanitize document ID for use as filename (replace unsafe characters)
        val safeId = documentId.replace(Regex("[^a-zA-Z0-9_-]"), "_")
        return File(contentDir, safeId)
    }

    /**
     * Read binary content for a document.
     * Returns empty ByteArray if no content exists.
     */
    fun read(documentId: String): ByteArray {
        val file = getContentFile(documentId)
        return if (file.exists()) {
            try {
                file.readBytes()
            } catch (e: Exception) {
                Log.e(TAG, "Error reading content for $documentId", e)
                ByteArray(0)
            }
        } else {
            ByteArray(0)
        }
    }

    /**
     * Write binary content for a document.
     */
    fun write(documentId: String, content: ByteArray) {
        val file = getContentFile(documentId)
        try {
            file.writeBytes(content)
            Log.d(TAG, "Wrote ${content.size} bytes for $documentId")
        } catch (e: Exception) {
            Log.e(TAG, "Error writing content for $documentId", e)
        }
    }

    /**
     * Delete content for a document.
     */
    fun delete(documentId: String) {
        val file = getContentFile(documentId)
        if (file.exists()) {
            file.delete()
            Log.d(TAG, "Deleted content for $documentId")
        }
    }

    /**
     * Check if content exists for a document.
     */
    fun exists(documentId: String): Boolean {
        return getContentFile(documentId).exists()
    }

    /**
     * Get the size of content for a document.
     */
    fun getSize(documentId: String): Long {
        val file = getContentFile(documentId)
        return if (file.exists()) file.length() else 0
    }

    /**
     * Get the path to the content directory (for debugging/UI).
     */
    fun getContentDirPath(): String = contentDir.absolutePath

    /**
     * Clear all stored content.
     */
    fun clearAll() {
        contentDir.listFiles()?.forEach { it.delete() }
        Log.d(TAG, "Cleared all content")
    }
}
