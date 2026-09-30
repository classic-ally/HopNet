<script lang="ts">
    import { formatStorageCapacity } from '../../utils/formatters';
    import AgeHistogram from './AgeHistogram.svelte';

    // Unplaced data bucketed by age. Age comes from data_blocks.id, which is
    // a UUIDv7 — the creation timestamp is embedded, replicated as part of the
    // primary key, so every node reads the same value and it cannot diverge the
    // way an apply-time now() column would.
    //
    // The chart itself is AgeHistogram (shared with the in-flight series);
    // this wrapper owns the volume formatting and the summary line.
    export let buckets: {
        label: string;
        gb: number;
        severity?: 'warn' | 'stale';
    }[] = [];

    $: total = buckets.reduce((a, b) => a + b.gb, 0);
    $: staleGb = buckets.filter(b => b.severity === 'stale').reduce((a, b) => a + b.gb, 0);
    $: warnGb = buckets.filter(b => b.severity === 'warn').reduce((a, b) => a + b.gb, 0);

    $: shaped = buckets.map(b => ({
        label: b.label,
        value: b.gb,
        display: formatStorageCapacity(b.gb),
        severity: b.severity
    }));
</script>

<AgeHistogram title="Unplaced by age" buckets={shaped} emptyText="All committed data is placed.">
    <svelte:fragment slot="summary">
        {#if staleGb > 0}
            <span class="text-red">{formatStorageCapacity(staleGb)}</span>
            <span class="text-subtitle">stale</span>
            <span class="text-muted">· {formatStorageCapacity(total)} total</span>
        {:else if warnGb > 0}
            <span class="text-yellow">{formatStorageCapacity(warnGb)}</span>
            <span class="text-subtitle">overdue</span>
            <span class="text-muted">· {formatStorageCapacity(total)} total</span>
        {:else}
            <span class="text-subtitle">{formatStorageCapacity(total)} in flight</span>
        {/if}
    </svelte:fragment>
</AgeHistogram>
