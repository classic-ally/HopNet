<script lang="ts">
    import Modal from '../../primitives/Modal.svelte';
    import Button from '../../Button.svelte';
    import QrCode from 'svelte-qrcode';
    import { authenticatedFetch, API_BASE_URL } from '../../stores';
    import type { PairingInfoResponse } from '../../types';

    interface DeviceKeyModalProps {
        isOpen: boolean;
        deviceName: string;
        apiKey: string;
        onClose: () => void;
    }

    let {
        isOpen,
        deviceName,
        apiKey,
        onClose
    }: DeviceKeyModalProps = $props();

    let copied = $state(false);
    let qrExpanded = $state(false);
    let pairing = $state<PairingInfoResponse | null>(null);

    $effect(() => {
        if (!isOpen) return;
        authenticatedFetch(`${API_BASE_URL}/devices/pairing-info`)
            .then(async (r) => {
                pairing = r.ok ? await r.json() : null;
            })
            .catch(() => {
                pairing = null;
            });
    });

    // QR payload v1 (docs/specs/pinned-https.md): everything a client
    // needs to reach the TLS surface and pin the node's cert. `host` is
    // what the operator's browser reached this node at — omitted when
    // that's loopback (Tauri webview / local dev), in which case the
    // client prompts for it. Falls back to the bare API key when the
    // node has no TLS listener.
    const qrValue = $derived.by(() => {
        if (!pairing?.tls_enabled || !pairing.https_port || !pairing.spki_sha256) {
            return apiKey;
        }
        const host = window.location.hostname;
        const isLoopback =
            host === 'localhost' || host === '127.0.0.1' || host === 'tauri.localhost';
        return JSON.stringify({
            v: 1,
            kind: 'hopnet-device',
            ...(isLoopback ? {} : { host }),
            port: pairing.https_port,
            spki: pairing.spki_sha256,
            token: apiKey
        });
    });

    async function copyToClipboard() {
        try {
            await navigator.clipboard.writeText(apiKey);
            copied = true;
            setTimeout(() => {
                copied = false;
            }, 2000);
        } catch (err) {
            console.error('Failed to copy:', err);
        }
    }

    function toggleQr() {
        qrExpanded = !qrExpanded;
    }

    function handleClose() {
        qrExpanded = false;
        copied = false;
        onClose();
    }
</script>

{#if isOpen}
    <Modal
        title="Device Registered Successfully"
        size="md"
        onClose={handleClose}
        success={copied ? 'Copied to clipboard!' : undefined}
    >
        {#snippet content()}
            <div class="space-y-4">
                <!-- Warning -->
                <div class="bg-yellow/20 border border-yellow rounded-lg p-3">
                    <div class="flex items-start gap-2">
                        <div class="i-carbon-warning text-yellow flex-shrink-0 mt-0.5"></div>
                        <div>
                            <p class="text-yellow font-medium">Save this API key now!</p>
                            <p class="text-yellow text-sm">It will not be shown again.</p>
                        </div>
                    </div>
                </div>

                <!-- Device name -->
                <div class="flex gap-2 items-center">
                    <p class="text-muted">Device:</p>
                    <p class="text-primary font-medium">{deviceName}</p>
                </div>

                <!-- API Key display -->
                <div>
                    <p class="text-muted mb-2">API Key:</p>
                    <div class="bg-surface1 border border-overlay1 rounded-lg p-3 font-mono text-sm text-primary break-all">
                        {apiKey}
                    </div>
                </div>

                <!-- Node TLS fingerprint (manual pairing counterpart of the QR) -->
                {#if pairing?.tls_enabled && pairing.spki_sha256}
                    <div>
                        <p class="text-muted mb-2">Node fingerprint (SPKI SHA-256):</p>
                        <div class="bg-surface1 border border-overlay1 rounded-lg p-3 font-mono text-sm text-primary break-all">
                            {pairing.spki_sha256}
                        </div>
                    </div>
                {:else}
                    <p class="text-muted text-sm">
                        No TLS listener available — the QR code carries the bare API key only.
                    </p>
                {/if}

                <!-- Copy button -->
                <div class="flex justify-center">
                    <Button
                        icon={copied ? 'i-carbon-checkmark' : 'i-carbon-copy'}
                        text={copied ? 'Copied!' : 'Copy to Clipboard'}
                        onClick={copyToClipboard}
                    />
                </div>

                <!-- QR code section -->
                <div>
                    <button
                        class="flex items-center gap-2 transition-colors cursor-pointer bg-surface0 text-primary p-2 border-overlay1 border border-solid rounded-md hover:bg-surface1 hover:border-mauve w-full"
                        onclick={toggleQr}
                    >
                        <span class="text-sm">
                            {qrExpanded ? '\u25BC' : '\u25B6'}
                        </span>
                        <p>Show QR Code</p>
                    </button>

                    {#if qrExpanded}
                        <div class="mt-3 flex justify-center p-4 bg-white rounded-lg">
                            <QrCode size={200} value={qrValue} />
                        </div>
                    {/if}
                </div>
            </div>
        {/snippet}

        {#snippet footer()}
            <div class="flex justify-end">
                <Button
                    icon="i-carbon-checkmark"
                    text="Done"
                    onClick={handleClose}
                />
            </div>
        {/snippet}
    </Modal>
{/if}
