<script lang="ts">
    import Modal from '../../primitives/Modal.svelte';
    import TextInput from '../../primitives/TextInput.svelte';
    import Button from '../../Button.svelte';
    import { authenticatedFetch, API_BASE_URL } from '../../stores';
    import type { RegisterDeviceResponse } from '../../types';

    interface AddDeviceModalProps {
        isOpen: boolean;
        onClose: () => void;
        onDeviceAdded: (response: RegisterDeviceResponse, deviceName: string) => void;
    }

    let {
        isOpen,
        onClose,
        onDeviceAdded
    }: AddDeviceModalProps = $props();

    let deviceName = $state('');
    let loading = $state(false);
    let error = $state('');

    function validateDeviceName(name: string): string | null {
        if (!name.trim()) {
            return 'Device name is required';
        }
        if (name.length > 100) {
            return 'Device name must be 100 characters or less';
        }
        return null;
    }

    async function handleSubmit() {
        const validationError = validateDeviceName(deviceName);
        if (validationError) {
            error = validationError;
            return;
        }

        loading = true;
        error = '';

        try {
            const response = await authenticatedFetch(`${API_BASE_URL}/devices/register`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ device_name: deviceName.trim() }),
            });

            if (response.ok) {
                const data: RegisterDeviceResponse = await response.json();
                const savedName = deviceName.trim();
                handleClose();
                onDeviceAdded(data, savedName);
            } else {
                const errorData = await response.text();
                error = `Failed to register device: ${errorData || response.statusText}`;
            }
        } catch (err) {
            error = err instanceof Error ? err.message : 'Network error occurred';
        } finally {
            loading = false;
        }
    }

    function handleClose() {
        deviceName = '';
        error = '';
        loading = false;
        onClose();
    }

    function handleKeydown(event: KeyboardEvent) {
        if (event.key === 'Enter' && !loading) {
            handleSubmit();
        }
    }
</script>

{#if isOpen}
    <Modal
        title="Add a Device"
        size="sm"
        onClose={handleClose}
        {loading}
        {error}
    >
        {#snippet content()}
            <div class="space-y-4">
                <div>
                    <label for="device-name" class="block text-muted text-sm mb-2">
                        Device Name
                    </label>
                    <TextInput
                        id="device-name"
                        value={deviceName}
                        placeholder="e.g., My Android Phone"
                        disabled={loading}
                        oninput={(e) => deviceName = (e.target as HTMLInputElement).value}
                        onkeydown={handleKeydown}
                    />
                </div>

                <p class="text-muted text-sm">
                    This will generate an API key for the device to access your files.
                </p>
            </div>
        {/snippet}

        {#snippet footer()}
            <div class="flex justify-end gap-2">
                <Button
                    icon="i-carbon-close"
                    text="Cancel"
                    onClick={handleClose}
                    disabled={loading}
                />
                <Button
                    icon="i-carbon-add"
                    text="Add Device"
                    onClick={handleSubmit}
                    disabled={loading || !deviceName.trim()}
                />
            </div>
        {/snippet}
    </Modal>
{/if}
