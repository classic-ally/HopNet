<script lang="ts">
    import Button from '../../Button.svelte';
    import Modal from '../../primitives/Modal.svelte';
    import TextInput from '../../primitives/TextInput.svelte';
    import type { IncomingShareResponse } from '../../types';

    interface AcceptShareModalProps {
        isOpen?: boolean;
        share?: IncomingShareResponse | null;
        loading?: boolean;
        error?: string;
        onAccept?: (path: string) => void;
        onClose?: () => void;
    }

    let {
        isOpen = false,
        share = null,
        loading = false,
        error = '',
        onAccept = () => {},
        onClose = () => {},
    }: AcceptShareModalProps = $props();

    let placementPath = $state('');

    // Update default path when share changes
    $effect(() => {
        if (share) {
            placementPath = `/${share.display_name}`;
        }
    });

    function handleAccept() {
        if (placementPath.trim()) {
            onAccept(placementPath.trim());
        }
    }

    function handleKeydown(e: KeyboardEvent) {
        if (e.key === 'Enter' && placementPath.trim() && !loading) {
            handleAccept();
        }
    }

    function handleClose() {
        placementPath = '';
        onClose();
    }
</script>

{#if isOpen && share}
<Modal title="Accept Shared File" onClose={handleClose} size="md">
    {#snippet content()}
        <div class="space-y-4">
            <div class="text-sm text-subtitle">
                <span class="text-primary font-medium">{share.sender_username}</span> shared
                <span class="text-primary font-medium">{share.display_name}</span> with you.
            </div>

            <div class="space-y-2">
                <div class="block text-sm font-medium text-subtitle">
                    Save as
                </div>
                <TextInput
                    value={placementPath}
                    placeholder="/filename.ext"
                    disabled={loading}
                    oninput={(e: Event) => placementPath = (e.target as HTMLInputElement).value}
                    onkeydown={handleKeydown}
                />
                <p class="text-xs text-muted">Path where the file will appear in your file tree</p>
            </div>

            {#if error}
                <div class="bg-red/10 border border-red/30 rounded-lg p-3">
                    <p class="text-red text-sm">{error}</p>
                </div>
            {/if}
        </div>
    {/snippet}

    {#snippet footer()}
        <div class="flex items-center justify-end gap-2">
            <Button
                icon="i-carbon-close"
                text="Cancel"
                onClick={handleClose}
            />
            <Button
                icon="i-carbon-checkmark"
                text={loading ? 'Accepting...' : 'Accept'}
                onClick={handleAccept}
                disabled={!placementPath.trim() || loading}
            />
        </div>
    {/snippet}
</Modal>
{/if}
