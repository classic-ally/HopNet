<script lang="ts">
    import Button from '../../Button.svelte';
    import Modal from '../../primitives/Modal.svelte';
    import TextInput from '../../primitives/TextInput.svelte';
    import type { UserInfo } from '../../api/shares';

    interface ShareFileModalProps {
        isOpen?: boolean;
        users?: UserInfo[];
        fileName?: string;
        loading?: boolean;
        error?: string;
        success?: string;
        onShare?: (username: string) => void;
        onClose?: () => void;
    }

    let {
        isOpen = false,
        users = [],
        fileName = '',
        loading = false,
        error = '',
        success = '',
        onShare = () => {},
        onClose = () => {},
    }: ShareFileModalProps = $props();

    let filterText = $state('');
    let selectedUsername = $state('');

    const filteredUsers = $derived(
        users.filter(u =>
            u.username.toLowerCase().includes(filterText.toLowerCase())
        )
    );

    function handleShare() {
        if (selectedUsername) {
            onShare(selectedUsername);
        }
    }

    function handleKeydown(e: KeyboardEvent) {
        if (e.key === 'Enter' && selectedUsername && !loading) {
            handleShare();
        }
    }

    function handleClose() {
        filterText = '';
        selectedUsername = '';
        onClose();
    }
</script>

{#if isOpen}
<Modal title="Share File" onClose={handleClose} size="md">
    {#snippet content()}
        <div class="space-y-4">
            <div class="text-sm text-subtitle">
                Share <span class="text-primary font-medium">{fileName}</span> with:
            </div>

            <TextInput
                value={filterText}
                placeholder="Search users..."
                disabled={loading}
                oninput={(e: Event) => filterText = (e.target as HTMLInputElement).value}
                onkeydown={handleKeydown}
            />

            <div class="max-h-48 overflow-y-auto space-y-1">
                {#if filteredUsers.length === 0}
                    <div class="text-muted text-sm text-center p-4">
                        {users.length === 0 ? 'No other users on this network' : 'No users match your search'}
                    </div>
                {:else}
                    {#each filteredUsers as user}
                        <button
                            class="w-full flex items-center gap-2 p-2 rounded-md cursor-pointer border-1 border-solid transition-colors {selectedUsername === user.username ? 'bg-mauve/20 border-mauve text-primary' : 'bg-transparent border-transparent hover:bg-surface0 text-subtitle'}"
                            onclick={() => selectedUsername = user.username}
                            disabled={loading}
                        >
                            <div class="i-carbon-user w-4 h-4 text-muted"></div>
                            <span class="text-sm">{user.username}</span>
                        </button>
                    {/each}
                {/if}
            </div>

            {#if error}
                <div class="bg-red/10 border border-red/30 rounded-lg p-3">
                    <p class="text-red text-sm">{error}</p>
                </div>
            {/if}

            {#if success}
                <div class="bg-green/10 border border-green/30 rounded-lg p-3">
                    <p class="text-green text-sm">{success}</p>
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
                icon="i-carbon-share"
                text={loading ? 'Sharing...' : 'Share'}
                onClick={handleShare}
                disabled={!selectedUsername || loading}
            />
        </div>
    {/snippet}
</Modal>
{/if}

<style>
    .overflow-y-auto {
        scrollbar-width: thin;
        scrollbar-color: #45475a #313244;
    }
</style>
