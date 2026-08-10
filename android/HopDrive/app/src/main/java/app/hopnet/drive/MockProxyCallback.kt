package app.hopnet.drive

import android.os.ProxyFileDescriptorCallback
import android.system.ErrnoException
import app.hopnet.drive.data.ContentStorage
import app.hopnet.drive.data.DocumentRepository
import java.io.RandomAccessFile

/**
 * ProxyFileDescriptorCallback implementation using RandomAccessFile.
 *
 * This approach:
 * - Writes directly to disk - no in-memory buffering of large files
 * - Supports efficient seeking (random access)
 * - Lets the OS handle buffering
 * - Works the same way production would (just swap data source)
 */
class MockProxyCallback(
    private val documentId: String,
    private val repository: DocumentRepository,
    private val contentStorage: ContentStorage,
    private val mode: Int,
    private val onRelease: (() -> Unit)? = null
) : ProxyFileDescriptorCallback() {

    private val file: RandomAccessFile = RandomAccessFile(
        contentStorage.getContentFile(documentId), "rw"
    )
    private var modified = false

    override fun onGetSize(): Long = file.length()

    @Throws(ErrnoException::class)
    override fun onRead(offset: Long, size: Int, data: ByteArray): Int {
        if (offset >= file.length()) {
            return 0 // EOF
        }
        file.seek(offset)
        val bytesRead = file.read(data, 0, size)
        return if (bytesRead == -1) 0 else bytesRead
    }

    @Throws(ErrnoException::class)
    override fun onWrite(offset: Long, size: Int, data: ByteArray): Int {
        file.seek(offset)
        file.write(data, 0, size)
        modified = true
        return size
    }

    @Throws(ErrnoException::class)
    override fun onFsync() {
        file.fd.sync()
        if (modified) {
            repository.updateSize(documentId, file.length())
        }
    }

    override fun onRelease() {
        if (modified) {
            repository.updateSize(documentId, file.length())
        }
        file.close()
        onRelease?.invoke()
    }
}
