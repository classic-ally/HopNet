<script lang="ts">
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
</script>

<div class="bg-surface0 border border-overlay0 rounded-lg p-4 space-y-3">
    <div class="flex items-center gap-3">
        <div class="i-carbon-circle-dash text-blue text-2xl animate-spin flex-shrink-0"></div>
        <div class="flex-1">
            <h4 class="font-medium text-primary">Import in progress</h4>
            <p class="text-sm text-muted">
                {#if counts}
                    {processed} of {counts.total} files processed
                {:else}
                    Working on the owner node — counts unavailable from this device.
                {/if}
            </p>
        </div>
    </div>
    {#if counts}
        <div class="w-full bg-surface1 rounded-full h-2">
            <div class="bg-blue h-2 rounded-full transition-all duration-300" style="width: {progressPct}%"></div>
        </div>
        <div class="flex items-center gap-4 text-xs text-muted">
            <span><span class="text-green">{counts.imported}</span> imported</span>
            <span><span class="text-yellow">{counts.skipped}</span> skipped</span>
            <span><span class="text-red">{counts.failed}</span> failed</span>
            <span><span class="text-subtitle">{counts.pending}</span> pending</span>
        </div>
    {/if}
</div>
