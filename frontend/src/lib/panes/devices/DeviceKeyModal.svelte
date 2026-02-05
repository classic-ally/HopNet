<script lang="ts">
    import Modal from '../../primitives/Modal.svelte';
    import Button from '../../Button.svelte';
    import QrCode from 'svelte-qrcode';

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
                            <QrCode size={150} value={apiKey} />
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
