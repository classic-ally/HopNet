<script lang="ts">
    import Button from '../../Button.svelte';
    import Modal from '../../primitives/Modal.svelte';
    import type { ShareParticipant } from '../../types';

    interface ShareDetailsModalProps {
        isOpen?: boolean;
        fileName?: string;
        participants?: ShareParticipant[];
        currentUserId?: number;
        loading?: boolean;
        onUnshare?: () => void;
        onClose?: () => void;
    }

    let {
        isOpen = false,
        fileName = '',
        participants = [],
        currentUserId = 0,
        loading = false,
        onUnshare = () => {},
        onClose = () => {},
    }: ShareDetailsModalProps = $props();

    function statusColor(status: string): string {
        switch (status) {
            case 'accepted': return 'text-green bg-green/15';
            case 'pending': return 'text-yellow bg-yellow/15';
            default: return 'text-muted bg-surface1';
        }
    }
</script>

{#if isOpen}
<Modal title="Share Details" onClose={onClose} size="md">
    {#snippet content()}
        <div class="space-y-4">
            <div class="text-sm text-subtitle">
                Sharing details for <span class="text-primary font-medium">{fileName}</span>
            </div>

            {#if loading}
                <div class="text-muted p-4 text-center">Loading participants...</div>
            {:else if participants.length === 0}
                <div class="text-muted p-4 text-center">This file is not shared with anyone.</div>
            {:else}
                <div class="space-y-1">
                    {#each participants as participant}
                        <div class="flex items-center justify-between p-2 rounded-md bg-surface0 border border-overlay0">
                            <div class="flex items-center gap-2">
                                <div class="i-carbon-user w-4 h-4 text-muted"></div>
                                <span class="text-sm text-primary">{participant.username}</span>
                                {#if participant.user_id === currentUserId}
                                    <span class="text-xs text-muted">(you)</span>
                                {/if}
                            </div>
                            <span class="text-xs px-2 py-0.5 rounded-full {statusColor(participant.status)}">
                                {participant.status}
                            </span>
                        </div>
                    {/each}
                </div>
            {/if}
        </div>
    {/snippet}

    {#snippet footer()}
        <div class="flex items-center justify-between w-full">
            <p class="text-xs text-muted">{participants.length} {participants.length === 1 ? 'participant' : 'participants'}</p>
            <div class="flex items-center gap-2">
                <Button
                    icon="i-carbon-close"
                    text="Close"
                    onClick={onClose}
                />
                <Button
                    icon="i-carbon-unlink"
                    text="Leave Share"
                    onClick={onUnshare}
                    disabled={loading}
                />
            </div>
        </div>
    {/snippet}
</Modal>
{/if}
