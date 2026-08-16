package app.hopnet.drive.net

import android.content.Context
import android.provider.DocumentsContract
import android.util.Log
import app.hopnet.drive.HopDriveProvider
import java.io.BufferedReader
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

private const val TAG = "HopDriveWatch"

/**
 * Push-driven refresh: one daemon thread holding the node's SSE poke
 * stream while (and only while) someone is actively using the provider.
 * Shape mirrors the FUSE daemon's watcher (hopnet-mount/src/watch.rs):
 * sync on every (re)connect, coalesce poke bursts, anchor on the height
 * each delta reports.
 *
 * Lifecycle: every provider entry point calls [touch]; the loop starts
 * lazily and exits after [IDLE_STOP_MS] without touches. The provider
 * process itself only lives while clients hold cursors, so this bounds
 * the connection's battery cost to active browsing — no service.
 */
object WatchLoop {

    /**
     * First-connect sentinel (mirrors the daemon's ANCHOR_INIT): the
     * server stores heights as an i64 bit-cast, so exactly i64::MAX —
     * never above it — yields an empty delta plus the real height.
     */
    private const val ANCHOR_INIT = Long.MAX_VALUE

    private const val IDLE_STOP_MS = 3L * 60 * 1000
    private const val POKE_COALESCE_MS = 75L
    private const val BACKOFF_INITIAL_MS = 1_000L
    private const val BACKOFF_MAX_MS = 30_000L
    private const val BACKOFF_CLEAN_END_MS = 500L

    private val lastTouchMs = AtomicLong(0)

    @Volatile
    private var thread: Thread? = null

    /**
     * Shared child → parent map (ROOT_ID for root-parented). The provider
     * feeds it from queries; the watch loop feeds it from deltas and uses
     * the cached value to notify the OLD parent on moves and deletes.
     */
    val parentMap = ConcurrentHashMap<String, String>()

    /** Called from every provider entry point; starts the loop if needed. */
    fun touch(context: Context) {
        lastTouchMs.set(System.currentTimeMillis())
        if (thread != null) return
        synchronized(this) {
            if (thread != null) return
            val appContext = context.applicationContext
            thread = Thread({ run(appContext) }, "hopdrive-watch").apply {
                isDaemon = true
                start()
            }
        }
    }

    /** Called on unpair; the loop also dies naturally when idle. */
    fun stop() {
        synchronized(this) {
            thread?.interrupt()
            thread = null
        }
    }

    private fun idle(): Boolean =
        System.currentTimeMillis() - lastTouchMs.get() > IDLE_STOP_MS

    private fun run(context: Context) {
        var anchor = ANCHOR_INIT
        var backoffMs = BACKOFF_INITIAL_MS
        try {
            while (!Thread.interrupted() && !idle()) {
                val client = ApiClient.forContext(context) ?: return
                val cleanEnd = try {
                    val response = client.openWatch()
                    backoffMs = BACKOFF_INITIAL_MS
                    // Post-(re)connect catch-up before waiting for pokes.
                    anchor = sync(context, client, anchor)
                    response.use { open ->
                        streamPokes(open) { anchor = sync(context, client, anchor) }
                    }
                    true
                } catch (e: InterruptedException) {
                    return
                } catch (e: Exception) {
                    Log.d(TAG, "watch connect/stream failed: $e")
                    false
                }
                if (idle()) break
                Thread.sleep(if (cleanEnd) BACKOFF_CLEAN_END_MS else backoffMs)
                if (!cleanEnd) backoffMs = (backoffMs * 2).coerceAtMost(BACKOFF_MAX_MS)
            }
        } catch (e: InterruptedException) {
            // stop() or process teardown — fall through.
        } finally {
            synchronized(this) {
                if (thread === Thread.currentThread()) thread = null
            }
            Log.d(TAG, "watch loop exited")
        }
    }

    /**
     * Read SSE lines until stream end, liveness timeout (the client's 45s
     * read timeout), or idleness. `data:` = poke; `:` = keepalive; other
     * lines ignored. Pokes coalesce over a short drain window — which also
     * skips past the poke-fires-pre-commit window on the node.
     */
    private fun streamPokes(
        response: okhttp3.Response,
        onPoke: () -> Unit,
    ) {
        val reader = response.body!!.byteStream().bufferedReader()
        while (!Thread.interrupted() && !idle()) {
            val line = reader.readLine() ?: return
            when {
                line.startsWith("data:") -> {
                    drainBurst(reader)
                    onPoke()
                }
                line.startsWith(":") -> Unit // keepalive resets the read timeout by arriving
                else -> Unit
            }
        }
    }

    /** Swallow further pokes arriving within the coalescing window. */
    private fun drainBurst(reader: BufferedReader) {
        val deadline = System.currentTimeMillis() + POKE_COALESCE_MS
        while (System.currentTimeMillis() < deadline) {
            Thread.sleep(POKE_COALESCE_MS / 3)
            while (reader.ready()) {
                reader.readLine() ?: return
            }
        }
    }

    /**
     * Fetch the delta and notify every affected parent's children URI.
     * Returns the new anchor; on failure the old anchor is kept so the
     * same window is retried (deltas are idempotent).
     */
    private fun sync(context: Context, client: ApiClient, anchor: Long): Long {
        val delta = try {
            client.changes(anchor)
        } catch (e: Exception) {
            Log.d(TAG, "changes($anchor) failed: $e")
            return anchor
        }

        val affected = mutableSetOf<String>()
        for (item in delta.items) {
            val id = item.id ?: continue
            val newParent = item.parentId ?: ROOT_ID
            val oldParent = parentMap.put(id, newParent)
            affected.add(newParent)
            if (oldParent != null && oldParent != newParent) {
                affected.add(oldParent) // move: old listing changed too
            }
        }
        for (id in delta.deletedIds) {
            affected.add(parentMap.remove(id) ?: ROOT_ID)
        }

        if (affected.isNotEmpty()) {
            val resolver = context.contentResolver
            for (parent in affected) {
                resolver.notifyChange(
                    DocumentsContract.buildChildDocumentsUri(HopDriveProvider.AUTHORITY, parent),
                    null
                )
            }
            Log.d(TAG, "delta @${delta.height}: ${delta.items.size} items, " +
                "${delta.deletedIds.size} deleted → notified ${affected.size} parent(s)")
        }
        return delta.height
    }
}
