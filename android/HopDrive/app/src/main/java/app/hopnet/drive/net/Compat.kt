package app.hopnet.drive.net

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Client API compatibility wire contract (RFC-023 S3), mirroring
 * common/src/compat.rs. Hand-written: the app's DTOs live here rather than
 * through typeshare (see Dto.kt).
 */

/**
 * Header carrying the client's identity (its CalVer code) on every request
 * to a DeviceToken surface: `x-hopnet-client-version: 20260804`. Identity
 * only — acceptance policy lives at each end.
 */
const val CLIENT_VERSION_HEADER = "x-hopnet-client-version"

/**
 * Body of a `426 Upgrade Required` version rejection. Distinct from 401/403
 * so a version rejection never reads as a credential problem; the fields
 * tell the user exactly what to upgrade to.
 */
@Serializable
data class UpgradeRequiredResponse(
    /** The surface that rejected the request (its mount prefix). */
    val surface: String,
    /** Oldest client version code this surface accepts. */
    @SerialName("min_client") val minClient: Int,
    /** The node's own version code. */
    @SerialName("node_version") val nodeVersion: Int,
)

/** Render a CalVer code back to its display form (20260804 → "2026.8.4"). */
fun formatVersionCode(code: Int): String =
    "${code / 10_000}.${(code / 100) % 100}.${code % 100}"
