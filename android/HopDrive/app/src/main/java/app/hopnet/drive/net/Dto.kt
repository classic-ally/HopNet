package app.hopnet.drive.net

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** SAF document id of the synthesized root folder. */
const val ROOT_ID = "hopnet_root"

const val MIME_TYPE_DIR = "vnd.android.document/directory"

// --- DocumentProvider read surface (/api/integrations/documentprovider) ---
// camelCase on the wire (serde rename_all; common/src/documentprovider.rs).

@Serializable
data class DpItem(
    val id: String,
    val name: String,
    val mimeType: String,
    val size: Long,
    /** Epoch milliseconds. */
    val lastModified: Long,
    val parentId: String? = null,
)

@Serializable
data class DpEnumerateResponse(val items: List<DpItem>)

// --- Mount mutation surface (/api/integrations/mount) ---
// snake_case on the wire (common/src/mount.rs, no rename_all). Mutations
// respond only after the transaction is decided AND applied on the serving
// node, so responses carry the authoritative post-apply state — no
// convergence polling needed.

@Serializable
data class MountItem(
    /** Null = the root folder itself. */
    val id: String? = null,
    @SerialName("parent_id") val parentId: String? = null,
    val name: String,
    /** "File" | "Folder" (serde unit-variant strings). */
    @SerialName("item_type") val itemType: String,
    val size: Long? = null,
    @SerialName("blob_id") val blobId: String? = null,
    @SerialName("created_ms") val createdMs: Long,
    @SerialName("modified_ms") val modifiedMs: Long? = null,
    val height: Long? = null,
) {
    val isFolder: Boolean get() = itemType == "Folder"
}

@Serializable
data class MountMutationResponse(val item: MountItem? = null, val height: Long)

@Serializable
data class MountModifyRequest(
    val id: String,
    @SerialName("new_parent_id") val newParentId: String? = null,
    @SerialName("new_parent_root") val newParentRoot: Boolean = false,
    @SerialName("new_name") val newName: String? = null,
)

@Serializable
data class MountDeleteRequest(val id: String, val recursive: Boolean = false)

@Serializable
data class StatfsResponse(
    @SerialName("total_bytes") val totalBytes: Long,
    @SerialName("used_bytes") val usedBytes: Long,
)

// --- Pairing (docs/specs/pinned-https.md, QR payload v1) ---

@Serializable
data class QrPayload(
    val v: Int,
    val kind: String,
    /** Absent when the QR was rendered from a loopback browser — prompt. */
    val host: String? = null,
    val port: Int,
    /** 64 lower-hex chars: SHA-256 of the node cert's SPKI DER. */
    val spki: String,
    /** Device token: {device_id}.{secret_hex}. */
    val token: String,
) {
    companion object {
        const val KIND = "hopnet-device"
        const val VERSION = 1
    }
}
