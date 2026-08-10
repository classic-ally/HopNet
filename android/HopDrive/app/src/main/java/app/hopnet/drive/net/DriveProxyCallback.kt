package app.hopnet.drive.net

import android.os.HandlerThread
import android.os.ProxyFileDescriptorCallback
import android.system.ErrnoException
import android.system.OsConstants
import android.util.Log
import app.hopnet.drive.data.LogRepository
import java.io.File
import java.io.IOException
import java.io.InputStream
import java.io.RandomAccessFile

private const val TAG = "HopDriveProxy"

/**
 * Read-only proxy over ranged downloads. Sequential reads ride a single
 * streaming response; a seek closes it and reopens at the new offset via
 * a Range request (server support added alongside this client).
 */
class ReadProxyCallback(
    private val client: ApiClient,
    private val documentId: String,
    private val size: Long,
    private val thread: HandlerThread,
) : ProxyFileDescriptorCallback() {

    private var stream: InputStream? = null
    private var streamPos = -1L

    override fun onGetSize(): Long = size

    @Throws(ErrnoException::class)
    override fun onRead(offset: Long, size: Int, data: ByteArray): Int {
        try {
            if (offset >= this.size) return 0
            val input = streamAt(offset)
            var read = 0
            while (read < size) {
                val n = input.read(data, read, size - read)
                if (n < 0) break
                read += n
            }
            streamPos = offset + read
            return read
        } catch (e: IOException) {
            Log.w(TAG, "read $documentId @$offset failed", e)
            stream?.runCatching { close() }
            stream = null
            streamPos = -1
            throw ErrnoException("onRead", OsConstants.EIO)
        }
    }

    private fun streamAt(offset: Long): InputStream {
        val current = stream
        if (current != null && offset == streamPos) return current
        current?.runCatching { close() }
        val response = client.download(documentId, offset)
        val input = response.body!!.byteStream()
        stream = input
        streamPos = offset
        return input
    }

    override fun onRelease() {
        stream?.runCatching { close() }
        thread.quitSafely()
    }
}

/**
 * Write proxy: edits land in a cache-dir temp file and upload as one
 * content replacement when the writer closes. onFsync only marks dirty —
 * uploading per-fsync would mint a consensus blob per sync. An upload
 * failure at release cannot reach the (already closed) writing app: it is
 * logged loudly and the temp file kept for post-mortem (documented v1
 * limitation).
 */
class WriteProxyCallback(
    private val client: ApiClient,
    private val documentId: String,
    private val tempFile: File,
    private val thread: HandlerThread,
    private val onCommitted: () -> Unit,
) : ProxyFileDescriptorCallback() {

    private val file = RandomAccessFile(tempFile, "rw")
    private var dirty = false

    override fun onGetSize(): Long = file.length()

    @Throws(ErrnoException::class)
    override fun onRead(offset: Long, size: Int, data: ByteArray): Int {
        if (offset >= file.length()) return 0
        file.seek(offset)
        val n = file.read(data, 0, size)
        return if (n < 0) 0 else n
    }

    @Throws(ErrnoException::class)
    override fun onWrite(offset: Long, size: Int, data: ByteArray): Int {
        file.seek(offset)
        file.write(data, 0, size)
        dirty = true
        return size
    }

    @Throws(ErrnoException::class)
    override fun onFsync() {
        file.fd.sync()
    }

    override fun onRelease() {
        try {
            file.close()
            if (dirty) {
                client.putContent(documentId, tempFile)
                onCommitted()
            }
            tempFile.delete()
        } catch (e: Exception) {
            Log.e(TAG, "content upload for $documentId FAILED; edits kept at $tempFile", e)
            LogRepository.log(
                method = "PUT content $documentId",
                error = "upload failed after close; edits kept at ${tempFile.name}: $e",
            )
        } finally {
            thread.quitSafely()
        }
    }
}
