<script lang="ts">
    import Card from '../../primitives/Card.svelte';
    import StageStrip from './StageStrip.svelte';
    import AgeHistogram from './AgeHistogram.svelte';
    import { formatStorageCapacity } from '../../utils/formatters';

    // Block Lifecycle (RFC-STORAGE-003 S7). Every number is the reconciler's
    // own work-list predicate, read by hopnet_storage::observe: owed is
    // `desired < T`, in flight is an unconfirmed current goal, confirmed is the
    // quiescent rest. Converged is a displayed, checked predicate — not
    // inferred from silence — so a quiet mesh is visibly quiet-because-done.
    export let tip = 0;
    export let transitionHeight: number | null = null;
    export let owed = 0;
    export let inFlight = 0;
    export let confirmed = 0;
    export let converged = true;
    export let inFlightBuckets: {
        label: string;
        blobs: number;
        gb: number;
        severity?: 'warn' | 'stale';
    }[] = [];

    $: total = owed + inFlight + confirmed;

    // The staleness backlog is the one thing that must not persist, so it
    // alone takes a hue; the other two are a lightness ramp.
    $: segments = [
        { name: 'owed', n: owed, fill: 'bg-red', ink: 'text-base' },
        { name: 'in flight', n: inFlight, fill: 'bg-mauve', ink: 'text-base' },
        { name: 'confirmed', n: confirmed, fill: 'bg-overlay0', ink: 'text-text' }
    ];

    $: shaped = inFlightBuckets.map(b => ({
        label: b.label,
        value: b.blobs,
        display: `${b.blobs}`,
        severity: b.severity
    }));
    $: inFlightGb = inFlightBuckets.reduce((a, b) => a + b.gb, 0);
    $: stuck = inFlightBuckets.filter(b => b.severity === 'stale').reduce((a, b) => a + b.blobs, 0);
    $: late = inFlightBuckets.filter(b => b.severity === 'warn').reduce((a, b) => a + b.blobs, 0);
</script>

<Card title="Block Lifecycle">
    {#snippet headerRight()}
        <span class="text-xs font-mono flex items-center gap-3">
            {#if transitionHeight !== null}
                <span class="text-muted">T = height {transitionHeight}</span>
            {/if}
            {#if converged}
                <span class="text-green" title="No blob below T, nothing in flight">● Converged</span>
            {:else}
                <span class="text-mauve" title="Declarations owed or goals unconfirmed">○ Draining</span>
            {/if}
        </span>
    {/snippet}

    <StageStrip label="Blobs by stage" {total} {segments}>
        <svelte:fragment slot="right">
            <span class={owed > 0 ? 'text-red' : 'text-muted'}>{owed} owed</span>
            <span class="text-muted">·</span>
            <span class={inFlight > 0 ? 'text-mauve' : 'text-muted'}>{inFlight} in flight</span>
            <span class="text-muted">· {confirmed} confirmed</span>
        </svelte:fragment>
    </StageStrip>

    <div class="my-4 border-t border-overlay0"></div>

    <AgeHistogram
        title="In flight by age (heights since goal, tip {tip})"
        buckets={shaped}
        emptyText="Every goal is confirmed."
    >
        <svelte:fragment slot="summary">
            {#if stuck > 0}
                <span class="text-red">{stuck}</span>
                <span class="text-subtitle">stalled</span>
                <span class="text-muted">· {formatStorageCapacity(inFlightGb)} in flight</span>
            {:else if late > 0}
                <span class="text-yellow">{late}</span>
                <span class="text-subtitle">late</span>
                <span class="text-muted">· {formatStorageCapacity(inFlightGb)} in flight</span>
            {:else}
                <span class="text-subtitle">{formatStorageCapacity(inFlightGb)} in flight</span>
            {/if}
        </svelte:fragment>
    </AgeHistogram>
</Card>
