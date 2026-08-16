<script lang="ts">
    import Card from '../../primitives/Card.svelte';
    import type { ImportPathCounts } from '../../types';

    interface Props {
        // null on non-owner — render abstract spinner instead of progress bar
        counts: ImportPathCounts | null;
    }

    let { counts }: Props = $props();

    const progressPct = $derived(counts && counts.total > 0
        ? Math.round(((counts.imported + counts.skipped + counts.failed) / counts.total) * 100)
        : 0);

    const processed = $derived(counts ? counts.imported + counts.skipped + counts.failed : 0);

    const subtitle = $derived(counts
        ? `${processed} of ${counts.total} files processed`
        : 'Working on the owner node — counts unavailable from this device.');
</script>

{#snippet progress()}
    {#if counts}
        <div class="space-y-3">
            <div class="w-full bg-surface1 rounded-full h-2">
                <div class="bg-blue h-2 rounded-full transition-all duration-300" style="width: {progressPct}%"></div>
            </div>
            <div class="flex items-center gap-4 text-xs text-muted">
                <span><span class="text-green">{counts.imported}</span> imported</span>
                <span><span class="text-yellow">{counts.skipped}</span> skipped</span>
                <span><span class="text-red">{counts.failed}</span> failed</span>
                <span><span class="text-subtitle">{counts.pending}</span> pending</span>
            </div>
        </div>
    {/if}
{/snippet}

<!-- Non-owner (no counts) is header-only: no snippet, no dangling header margin. -->
<Card
    title="Import in progress"
    {subtitle}
    icon="i-carbon-circle-dash text-blue animate-spin"
    children={counts ? progress : undefined}
/>
