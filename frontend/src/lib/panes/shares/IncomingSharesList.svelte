<script lang="ts">
    import Button from '../../Button.svelte';
    import type { IncomingShareResponse } from '../../types';
    import { writesGatedStore, WRITES_GATED_TOOLTIP } from '../../stores';

    interface IncomingSharesListProps {
        shares?: IncomingShareResponse[];
        loading?: boolean;
        onAccept?: (share: IncomingShareResponse) => void;
        onDecline?: (share: IncomingShareResponse) => void;
    }

    let {
        shares = [],
        loading = false,
        onAccept = () => {},
        onDecline = () => {},
    }: IncomingSharesListProps = $props();

    const gated = $derived($writesGatedStore);

    function formatDate(dateStr: string): string {
        try {
            const date = new Date(dateStr);
            return date.toLocaleDateString(undefined, {
                year: 'numeric',
                month: 'short',
                day: 'numeric',
            });
        } catch {
            return dateStr;
        }
    }
</script>

{#if loading}
    <div class="text-muted p-4 text-center">
        Loading shared files...
    </div>
{:else if shares.length === 0}
    <div class="flex flex-col items-center gap-2 p-8 text-muted">
        <div class="i-carbon-share w-8 h-8"></div>
        <p class="text-sm">No pending shares</p>
    </div>
{:else}
    <div class="space-y-2">
        {#each shares as share}
            <div class="bg-surface0 border border-overlay0 rounded-lg p-3 flex items-center justify-between gap-3">
                <div class="flex-1 min-w-0">
                    <div class="flex items-center gap-2">
                        <div class="i-carbon-document w-4 h-4 text-muted flex-shrink-0"></div>
                        <span class="text-primary text-sm font-medium truncate">{share.display_name}</span>
                    </div>
                    <div class="text-xs text-muted mt-1">
                        From <span class="text-subtitle">{share.sender_username}</span> · {formatDate(share.created_at)}
                    </div>
                </div>
                <div class="flex items-center gap-1 flex-shrink-0">
                    <Button
                        icon="i-carbon-checkmark"
                        text="Accept"
                        onClick={() => onAccept(share)}
                        disabled={gated}
                        tooltip={gated ? WRITES_GATED_TOOLTIP : 'Accept share'}
                    />
                    <Button
                        icon="i-carbon-close"
                        text="Decline"
                        onClick={() => onDecline(share)}
                        disabled={gated}
                        tooltip={gated ? WRITES_GATED_TOOLTIP : 'Decline share'}
                    />
                </div>
            </div>
        {/each}
    </div>
{/if}
