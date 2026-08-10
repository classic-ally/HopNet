package app.hopnet.drive.ui

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.Button
import androidx.compose.material3.ButtonDefaults
import androidx.compose.material3.Card
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.unit.dp
import app.hopnet.drive.data.Pairing
import app.hopnet.drive.data.PairingStore
import app.hopnet.drive.net.ApiClient
import app.hopnet.drive.net.NodeHttpException
import app.hopnet.drive.net.QrPayload
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json

private val json = Json { ignoreUnknownKeys = true }

/**
 * Parse QR payload v1 (docs/specs/pinned-https.md). Returns null with a
 * reason when the payload is not a Hop Drive pairing code.
 */
fun parsePairingPayload(text: String): Result<QrPayload> = runCatching {
    val payload = json.decodeFromString<QrPayload>(text.trim())
    require(payload.v == QrPayload.VERSION) { "unsupported payload version ${payload.v}" }
    require(payload.kind == QrPayload.KIND) { "not a Hop Drive pairing code" }
    require(payload.spki.length == 64 && payload.spki.all { it in "0123456789abcdef" }) {
        "malformed SPKI fingerprint"
    }
    require(payload.port in 1..65535) { "invalid port" }
    require('.' in payload.token) { "malformed device token" }
    payload
}

@Composable
fun PairingTab(onScanQr: (() -> Unit)? = null) {
    val context = LocalContext.current
    var pairing by remember { mutableStateOf(PairingStore.load(context)) }

    DisposableEffect(Unit) {
        val listener = { pairing = PairingStore.load(context) }
        PairingStore.addListener(listener)
        onDispose { PairingStore.removeListener(listener) }
    }

    val current = pairing
    if (current == null) {
        UnpairedContent(onScanQr = onScanQr, onPaired = { newPairing ->
            PairingStore.save(context, newPairing)
        })
    } else {
        PairedContent(current, onUnpair = { PairingStore.clear(context) })
    }
}

@Composable
private fun UnpairedContent(onScanQr: (() -> Unit)?, onPaired: (Pairing) -> Unit) {
    var payloadText by remember { mutableStateOf("") }
    var host by remember { mutableStateOf("") }
    var port by remember { mutableStateOf("34632") }
    var spki by remember { mutableStateOf("") }
    var token by remember { mutableStateOf("") }
    var error by remember { mutableStateOf<String?>(null) }

    Column(
        modifier = Modifier
            .fillMaxSize()
            .padding(16.dp)
            .verticalScroll(rememberScrollState()),
        verticalArrangement = Arrangement.spacedBy(8.dp)
    ) {
        Text("Pair with a node", style = MaterialTheme.typography.titleMedium)
        Text(
            "Register this device on your node (Settings → Devices) and scan " +
                "the QR code, or paste/enter the pairing details.",
            style = MaterialTheme.typography.bodySmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant
        )

        if (onScanQr != null) {
            Button(onClick = onScanQr, modifier = Modifier.fillMaxWidth()) {
                Text("Scan QR code")
            }
        }

        OutlinedTextField(
            value = payloadText,
            onValueChange = { payloadText = it },
            label = { Text("Paste pairing payload (JSON)") },
            modifier = Modifier.fillMaxWidth(),
            minLines = 2
        )
        OutlinedButton(
            onClick = {
                parsePairingPayload(payloadText).fold(
                    onSuccess = { payload ->
                        if (payload.host != null) {
                            onPaired(
                                Pairing(payload.host, payload.port, payload.spki, payload.token)
                            )
                        } else {
                            // Loopback-rendered QR: payload carries no host.
                            host = ""
                            port = payload.port.toString()
                            spki = payload.spki
                            token = payload.token
                            error = "Payload has no host — enter the node's address below"
                        }
                    },
                    onFailure = { error = it.message }
                )
            },
            enabled = payloadText.isNotBlank(),
            modifier = Modifier.fillMaxWidth()
        ) {
            Text("Use payload")
        }

        Text("Manual entry", style = MaterialTheme.typography.titleSmall)
        OutlinedTextField(
            value = host,
            onValueChange = { host = it },
            label = { Text("Host (e.g. 192.168.1.20)") },
            modifier = Modifier.fillMaxWidth(),
            singleLine = true
        )
        OutlinedTextField(
            value = port,
            onValueChange = { port = it },
            label = { Text("Port") },
            modifier = Modifier.fillMaxWidth(),
            singleLine = true
        )
        OutlinedTextField(
            value = spki,
            onValueChange = { spki = it },
            label = { Text("Node fingerprint (SPKI SHA-256, 64 hex)") },
            modifier = Modifier.fillMaxWidth(),
            singleLine = true
        )
        OutlinedTextField(
            value = token,
            onValueChange = { token = it },
            label = { Text("Device token") },
            modifier = Modifier.fillMaxWidth(),
            singleLine = true
        )

        error?.let {
            Text(it, color = MaterialTheme.colorScheme.error, style = MaterialTheme.typography.bodySmall)
        }

        Button(
            onClick = {
                val portNumber = port.toIntOrNull()
                val spkiNorm = spki.trim().lowercase()
                error = when {
                    host.isBlank() -> "Host is required"
                    portNumber == null || portNumber !in 1..65535 -> "Invalid port"
                    spkiNorm.length != 64 || !spkiNorm.all { it in "0123456789abcdef" } ->
                        "Fingerprint must be 64 hex characters"
                    '.' !in token -> "Token must look like {device_id}.{secret}"
                    else -> {
                        onPaired(Pairing(host.trim(), portNumber, spkiNorm, token.trim()))
                        null
                    }
                }
            },
            modifier = Modifier.fillMaxWidth()
        ) {
            Text("Pair")
        }
    }
}

@Composable
private fun PairedContent(pairing: Pairing, onUnpair: () -> Unit) {
    val context = LocalContext.current
    val scope = rememberCoroutineScope()
    var testResult by remember { mutableStateOf<String?>(null) }
    var testing by remember { mutableStateOf(false) }

    Column(
        modifier = Modifier
            .fillMaxSize()
            .padding(16.dp),
        verticalArrangement = Arrangement.spacedBy(8.dp)
    ) {
        Text("Paired node", style = MaterialTheme.typography.titleMedium)
        Card {
            Column(Modifier.padding(12.dp), verticalArrangement = Arrangement.spacedBy(4.dp)) {
                Text("Address: ${pairing.host}:${pairing.port}")
                Text("Device: ${pairing.deviceId}", fontFamily = FontFamily.Monospace)
                Text(
                    "Pin: ${pairing.spki.take(12)}…",
                    fontFamily = FontFamily.Monospace,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
        }

        Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
            Button(
                onClick = {
                    testing = true
                    testResult = null
                    scope.launch {
                        testResult = withContext(Dispatchers.IO) { runConnectionTest(context) }
                        testing = false
                    }
                },
                enabled = !testing
            ) {
                Text(if (testing) "Testing…" else "Test connection")
            }
            Button(
                onClick = onUnpair,
                colors = ButtonDefaults.buttonColors(
                    containerColor = MaterialTheme.colorScheme.error
                )
            ) {
                Text("Unpair")
            }
        }

        testResult?.let {
            Text(it, style = MaterialTheme.typography.bodyMedium)
        }

        Spacer(Modifier.height(8.dp))
        Text(
            "Files live in the system Files app under the Hop Drive root.",
            style = MaterialTheme.typography.bodySmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant
        )
    }
}

private fun runConnectionTest(context: android.content.Context): String {
    val client = ApiClient.forContext(context) ?: return "Not paired"
    return try {
        val items = client.enumerate(null)
        "OK — root has ${items.size} item(s)"
    } catch (e: NodeHttpException) {
        when (e.code) {
            401 -> "Rejected: the device token is invalid or revoked — re-pair"
            428 -> "Node is locked — sign in on the node's web UI once"
            else -> "Node error: HTTP ${e.code}"
        }
    } catch (e: java.security.cert.CertificateException) {
        "TLS pin mismatch — this is not the node you paired with"
    } catch (e: Exception) {
        if (e.cause is java.security.cert.CertificateException) {
            "TLS pin mismatch — this is not the node you paired with"
        } else {
            "Unreachable: ${e.message}"
        }
    }
}
