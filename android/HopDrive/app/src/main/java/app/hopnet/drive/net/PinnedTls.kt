package app.hopnet.drive.net

import app.hopnet.drive.data.LogRepository
import app.hopnet.drive.data.Pairing
import java.io.IOException
import java.security.MessageDigest
import java.security.cert.CertificateException
import java.security.cert.X509Certificate
import java.util.concurrent.TimeUnit
import javax.net.ssl.SSLContext
import javax.net.ssl.X509TrustManager
import okhttp3.OkHttpClient

/**
 * Trust manager whose ONLY trust decision is the SPKI pin: SHA-256 over the
 * leaf certificate's SubjectPublicKeyInfo DER must equal the fingerprint
 * learned at pairing. Chain, validity window, and hostname deliberately
 * carry no weight — the node's certificate is self-signed and the pin IS
 * the trust (docs/specs/pinned-https.md; mirror of the orchestrator
 * tls_pinning verifier).
 */
class SpkiPinningTrustManager(pinHex: String) : X509TrustManager {

    private val pin: ByteArray = pinHex.lowercase().let { hex ->
        require(hex.length == 64 && hex.all { it in "0123456789abcdef" }) {
            "SPKI pin must be 64 hex chars"
        }
        hex.chunked(2).map { it.toInt(16).toByte() }.toByteArray()
    }

    override fun checkServerTrusted(chain: Array<X509Certificate>, authType: String) {
        val leaf = chain.firstOrNull() ?: throw CertificateException("empty certificate chain")
        // publicKey.encoded IS the SubjectPublicKeyInfo DER — the same bytes
        // the node hashes when advertising spki_sha256.
        val spki = MessageDigest.getInstance("SHA-256").digest(leaf.publicKey.encoded)
        if (!MessageDigest.isEqual(spki, pin)) {
            throw CertificateException("SPKI pin mismatch — node identity changed or wrong node")
        }
    }

    override fun checkClientTrusted(chain: Array<X509Certificate>, authType: String) {
        throw CertificateException("client certificates unsupported")
    }

    override fun getAcceptedIssuers(): Array<X509Certificate> = arrayOf()
}

/**
 * OkHttp client for a pairing: SPKI-pinned TLS, Bearer auth on every
 * request, and LogRepository wiring so the in-app log shows real traffic.
 */
fun buildPinnedClient(pairing: Pairing): OkHttpClient {
    val trustManager = SpkiPinningTrustManager(pairing.spki)
    val sslContext = SSLContext.getInstance("TLS").apply {
        init(null, arrayOf(trustManager), null)
    }
    return OkHttpClient.Builder()
        .sslSocketFactory(sslContext.socketFactory, trustManager)
        // The pin is the trust decision; the self-signed cert carries no
        // meaningful names to verify.
        .hostnameVerifier { _, _ -> true }
        .connectTimeout(10, TimeUnit.SECONDS)
        .readTimeout(30, TimeUnit.SECONDS)
        .writeTimeout(120, TimeUnit.SECONDS)
        .addInterceptor { chain ->
            val request = chain.request().newBuilder()
                .header("Authorization", "Bearer ${pairing.token}")
                .build()
            val started = System.nanoTime()
            val label = "${request.method} ${request.url.encodedPath}"
            try {
                val response = chain.proceed(request)
                LogRepository.log(
                    method = label,
                    parameters = mapOf("query" to request.url.query),
                    result = "${response.code} in ${(System.nanoTime() - started) / 1_000_000}ms",
                )
                response
            } catch (e: IOException) {
                LogRepository.log(method = label, error = e.toString())
                throw e
            }
        }
        .build()
}
