<script lang="ts">
    import { formatStorageCapacity } from '../../utils/formatters';

    // Capacity is NOT the raw disk total. It is the volume up to which the
    // ideal curve still yields >= 2 tolerable node failures — beyond that the
    // system keeps accepting writes but is producing fragments with no node
    // redundancy, which is an unsupported configuration rather than a larger
    // one. Reporting raw disk here would call that regime "spare capacity".
    export let consumedGb = 0;
    export let capacityGb = 0;

    export let unrecoverableGb = 0;

    // Severity is read off the buckets rather than recomputed, so this strip
    // and the age chart below can never disagree about which ranges count.
    export let unattestedBuckets: {
        label: string;
        gb: number;
        severity?: 'warn' | 'stale';
    }[] = [];

    $: unattestedGb = unattestedBuckets.reduce((a, b) => a + b.gb, 0);
    $: hasStale = unattestedBuckets.some(b => b.severity === 'stale' && b.gb > 0);
    $: hasWarn = unattestedBuckets.some(b => b.severity === 'warn' && b.gb > 0);

    $: unattestedTone = hasStale ? 'text-red' : hasWarn ? 'text-yellow' : 'text-muted';

    // Any unrecoverable data is loss that already happened — RS cannot rebuild
    // from fewer than K classes even with a perfectly healthy control plane.
    $: unrecoverableTone = unrecoverableGb > 0 ? 'text-red' : 'text-muted';

    $: overCapacity = capacityGb > 0 && consumedGb > capacityGb;
    $: storedTone = overCapacity ? 'text-yellow' : 'text-primary';
</script>

<div class="flex items-end gap-2">
    <div class="flex-1 text-center">
        <div
            class="text-xs text-subtitle mb-1"
            title="Total is the volume at which the ideal curve still tolerates 2 node failures"
        >
            Stored
        </div>
        <div class="font-mono text-2xl font-semibold leading-none {storedTone}">
            {formatStorageCapacity(consumedGb)}<span
                class="text-base font-normal text-muted"
            >&nbsp;of {formatStorageCapacity(capacityGb)}</span>
        </div>
    </div>

    <div class="flex-1 text-center">
        <div class="text-xs text-subtitle mb-1" title="Placed, but no node has attested to holding it">
            Unattested
        </div>
        <div class="font-mono text-2xl font-semibold leading-none {unattestedTone}">
            {formatStorageCapacity(unattestedGb)}
        </div>
    </div>

    <div class="flex-1 text-center">
        <div class="text-xs text-subtitle mb-1" title="Fewer than K classes survive — already lost">
            Unrecoverable
        </div>
        <div class="font-mono text-2xl font-semibold leading-none {unrecoverableTone}">
            {formatStorageCapacity(unrecoverableGb)}
        </div>
    </div>
</div>

{#if overCapacity}
    <div class="mt-3 text-xs text-yellow">
        Beyond supported capacity — writes past this point produce fragments with no node
        redundancy.
    </div>
{/if}
