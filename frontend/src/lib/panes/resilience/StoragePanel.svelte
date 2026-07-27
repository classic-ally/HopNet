<script lang="ts">
    import FaultToleranceChart from './FaultToleranceChart.svelte';
    import UnplacedByAge from './UnplacedByAge.svelte';
    import StorageSummary from './StorageSummary.svelte';
    import type { FaultToleranceCurvePoint } from '../../types';

    // Named to mirror State Machine Replication: that one replicates the log,
    // this one replicates the bytes.
    export let curve: FaultToleranceCurvePoint[] = [];

    export let observedLevels: { tolerance: number; rawGb: number }[] = [];
    export let unrecoverableGb = 0;
    export let unknownGb = 0;
    export let unreachableMembers = 0;

    export let unplacedBuckets: {
        label: string;
        gb: number;
        severity?: 'warn' | 'stale';
    }[] = [];

    // The minimum supported redundancy. Below 2 tolerable failures the mesh is
    // still writing, but it is producing fragments that buy no node redundancy
    // — an unsupported configuration, not extra capacity.
    const MIN_SUPPORTED_TOLERANCE = 2;

    $: consumedGb = observedLevels.reduce((a, s) => a + s.rawGb, 0);

    // Capacity is where the ideal curve stops clearing the supported bar.
    $: firstUnsupported = curve.find(p => p.nodes_can_fail < MIN_SUPPORTED_TOLERANCE);
    $: capacityGb =
        firstUnsupported?.user_data_gb ?? (curve.length > 0 ? curve[curve.length - 1].user_data_gb : 0);

    // Truncate the plot at capacity so the unsupported tail does not read as
    // headroom — UNLESS data has actually crossed into it, in which case the
    // whole point is to see how far. The last supported point is extended to
    // the boundary so its step keeps its full width.
    $: supported = curve.filter(p => p.nodes_can_fail >= MIN_SUPPORTED_TOLERANCE);
    $: shownCurve =
        supported.length === 0 || consumedGb > capacityGb
            ? curve
            : [...supported, { ...supported[supported.length - 1], user_data_gb: capacityGb }];

    // INV-DURABLE is a min-property — the worst block decides whether data is
    // lost, however healthy the rest are. Computed here rather than read back
    // out of the chart so the panel header owns its own headline.
    $: worstTolerance =
        observedLevels.length > 0
            ? observedLevels.reduce((a, s) => Math.min(a, s.tolerance), Infinity)
            : null;
</script>

<div class="p-4 bg-surface0">
    <div class="flex items-baseline justify-between mb-4 gap-3">
        <h4 class="text-lg font-semibold text-primary">Data Replication</h4>
        {#if worstTolerance !== null}
            <span class="text-xs font-mono">
                <span class="text-subtitle">worst block tolerates</span>
                <span class={worstTolerance === 0 ? 'text-red' : 'text-mauve'}>
                    {worstTolerance}
                </span>
                <span class="text-subtitle">node failures</span>
            </span>
        {/if}
    </div>

    <StorageSummary {consumedGb} {capacityGb} {unrecoverableGb} {unplacedBuckets} />

    <div class="my-4 border-t border-overlay0"></div>

    <FaultToleranceChart
        data={shownCurve}
        {observedLevels}
        {unrecoverableGb}
        {unknownGb}
        {unreachableMembers}
    />

    <div class="my-4 border-t border-overlay0"></div>

    <UnplacedByAge buckets={unplacedBuckets} />
</div>
