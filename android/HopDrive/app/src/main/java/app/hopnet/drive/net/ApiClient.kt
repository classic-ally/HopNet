package app.hopnet.drive.net

import android.content.Context
import android.os.CancellationSignal
import app.hopnet.drive.data.Pairing
import app.hopnet.drive.data.PairingStore
import java.io.File
import java.io.IOException
import kotlinx.serialization.json.Json
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.MultipartBody
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.asRequestBody
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response

/** Non-2xx from the node; SAF-facing callers map codes to UX. */
class NodeHttpException(val code: Int, detail: String) : IOException("HTTP $code: $detail")

/**
 * Synchronous client for the node's device-token surfaces. Reads ride the
 * DocumentProvider surface; mutations ride the mount surface because its
 * responses are strict-wait (decided + applied) and carry the resulting
 * item — no convergence polling.
 */
class ApiClient(private val pairing: Pairing) {

    private val http: OkHttpClient = buildPinnedClient(pairing)
    private val json = Json { ignoreUnknownKeys = true }

    private val octetStream = "application/octet-stream".toMediaType()
    private val jsonMedia = "application/json".toMediaType()

    // --- plumbing ---

    private fun url(path: String, vararg query: Pair<String, String?>): String {
        val builder = (pairing.baseUrl + path).toHttpUrl().newBuilder()
        for ((key, value) in query) {
            if (value != null) builder.addQueryParameter(key, value)
        }
        return builder.build().toString()
    }

    /**
     * Execute with cancellation wiring and one bounded retry on overload
     * shed (503 + Retry-After, capped at 2s). Returns an open Response the
     * caller must close (or consume).
     */
    private fun execute(request: Request, signal: CancellationSignal?): Response {
        var attempt = 0
        while (true) {
            val call = http.newCall(request)
            signal?.setOnCancelListener { call.cancel() }
            val response = try {
                call.execute()
            } finally {
                signal?.setOnCancelListener(null)
            }
            if (response.code == 503 && attempt == 0) {
                val retryAfterMs = (response.header("Retry-After")?.toLongOrNull() ?: 1L)
                    .coerceAtMost(2) * 1000
                response.close()
                attempt++
                Thread.sleep(retryAfterMs)
                continue
            }
            if (!response.isSuccessful) {
                val detail = response.body?.string().orEmpty().take(200)
                response.close()
                throw NodeHttpException(response.code, detail)
            }
            return response
        }
    }

    private inline fun <reified T> getJson(url: String, signal: CancellationSignal?): T {
        execute(Request.Builder().url(url).build(), signal).use { response ->
            return json.decodeFromString(response.body!!.string())
        }
    }

    // --- DocumentProvider read surface ---

    fun enumerate(parentId: String?, signal: CancellationSignal? = null): List<DpItem> =
        getJson<DpEnumerateResponse>(
            url("/api/integrations/documentprovider/enumerate", "parent_id" to parentId),
            signal
        ).items

    fun item(id: String, signal: CancellationSignal? = null): DpItem =
        getJson(url("/api/integrations/documentprovider/item", "id" to id), signal)

    /**
     * Open a download stream; `offset > 0` requests `bytes={offset}-`.
     * Caller owns the returned Response and must close it.
     */
    fun download(id: String, offset: Long = 0, signal: CancellationSignal? = null): Response {
        val builder = Request.Builder()
            .url(url("/api/integrations/documentprovider/download", "id" to id))
        if (offset > 0) builder.header("Range", "bytes=$offset-")
        return execute(builder.build(), signal)
    }

    // --- Mount surfaces ---

    fun statfs(signal: CancellationSignal? = null): StatfsResponse =
        getJson(url("/api/integrations/mount/statfs"), signal)

    fun createFolder(
        parentId: String?,
        name: String,
        signal: CancellationSignal? = null,
    ): MountItem = mountMutation(
        Request.Builder()
            .url(url("/api/integrations/mount/create"))
            .post(
                multipart(parentId)
                    .addFormDataPart("folder_name", name)
                    .build()
            )
            .build(),
        signal
    )

    fun createFile(
        parentId: String?,
        name: String,
        content: File,
        signal: CancellationSignal? = null,
    ): MountItem = mountMutation(
        Request.Builder()
            .url(url("/api/integrations/mount/create"))
            .post(
                multipart(parentId)
                    .addFormDataPart(
                        "file_${content.length()}",
                        name,
                        content.asRequestBody(octetStream)
                    )
                    .build()
            )
            .build(),
        signal
    )

    fun putContent(
        inodeId: String,
        content: File,
        signal: CancellationSignal? = null,
    ): MountItem = mountMutation(
        Request.Builder()
            .url(url("/api/integrations/mount/content"))
            .put(
                MultipartBody.Builder().setType(MultipartBody.FORM)
                    .addFormDataPart("inode_id", inodeId)
                    .addFormDataPart(
                        "file_${content.length()}",
                        content.name,
                        content.asRequestBody(octetStream)
                    )
                    .build()
            )
            .build(),
        signal
    )

    fun rename(id: String, newName: String, signal: CancellationSignal? = null): MountItem =
        modify(MountModifyRequest(id = id, newName = newName), signal)

    fun move(id: String, newParentId: String?, signal: CancellationSignal? = null): MountItem =
        modify(
            MountModifyRequest(
                id = id,
                newParentId = newParentId,
                newParentRoot = newParentId == null,
            ),
            signal
        )

    private fun modify(request: MountModifyRequest, signal: CancellationSignal?): MountItem =
        mountMutation(
            Request.Builder()
                .url(url("/api/integrations/mount/modify"))
                .patch(json.encodeToString(MountModifyRequest.serializer(), request).toRequestBody(jsonMedia))
                .build(),
            signal
        )

    fun delete(id: String, recursive: Boolean, signal: CancellationSignal? = null) {
        execute(
            Request.Builder()
                .url(url("/api/integrations/mount/delete"))
                .delete(
                    json.encodeToString(
                        MountDeleteRequest.serializer(),
                        MountDeleteRequest(id = id, recursive = recursive)
                    ).toRequestBody(jsonMedia)
                )
                .build(),
            signal
        ).close()
    }

    private fun mountMutation(request: Request, signal: CancellationSignal?): MountItem {
        execute(request, signal).use { response ->
            val body: MountMutationResponse = json.decodeFromString(response.body!!.string())
            return body.item ?: throw IOException("mutation response carried no item")
        }
    }

    private fun multipart(parentId: String?): MultipartBody.Builder {
        val builder = MultipartBody.Builder().setType(MultipartBody.FORM)
        if (parentId != null && parentId != ROOT_ID) {
            builder.addFormDataPart("parent_id", parentId)
        }
        return builder
    }

    companion object {
        @Volatile
        private var cachedFor: Pairing? = null

        @Volatile
        private var cachedClient: ApiClient? = null

        /** Client for the current pairing, rebuilt when the pairing changes. */
        fun forContext(context: Context): ApiClient? {
            val pairing = PairingStore.load(context) ?: return null
            val cached = cachedClient
            if (cached != null && cachedFor == pairing) return cached
            synchronized(this) {
                if (cachedClient == null || cachedFor != pairing) {
                    cachedClient = ApiClient(pairing)
                    cachedFor = pairing
                }
                return cachedClient!!
            }
        }
    }
}
