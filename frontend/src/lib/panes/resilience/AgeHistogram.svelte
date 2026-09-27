<script lang="ts">
    // A distribution by age bucket, plotted rather than checked against a
    // threshold: a flat bound false-positives on the large or the recent, and
    // here a slow item just shifts right — the diagnosis is the SHAPE. Healthy
    // decays toward nothing; a plateau or bump at the right end is stuck work.
    //
    // `severity` is set by the caller (ultimately the backend), never inferred
    // from position, so where the warn/stale lines fall stays a decision made
    // beside the engine's cadence rather than a number invented here.
    export let title = '';
    export let buckets: {
        label: string;
        value: number;
        display: string;
        severity?: 'warn' | 'stale';
    }[] = [];
    export let emptyText = 'Nothing here.';

    $: total = buckets.reduce((a, b) => a + b.value, 0);
    $: max = buckets.reduce((a, b) => Math.max(a, b.value), 0);

    // Age is already the x-axis, so a ramp would spend the colour channel
    // re-encoding it. Colour marks only what position does not say: which
    // ranges are past the point of being explainable as in flight.
    const TONE = { warn: 'bg-yellow', stale: 'bg-red' } as const;
    const INK = { warn: 'text-yellow', stale: 'text-red' } as const;

    $: ariaLabel = `${title}: ` + buckets.map(b => `${b.label} ${b.display}`).join(', ');
</script>

<div>
    <div class="flex items-baseline justify-between mb-3">
        <div class="text-xs text-subtitle font-medium">{title}</div>
        <div class="text-xs font-mono">
            <slot name="summary" />
        </div>
    </div>

    {#if total === 0}
        <div class="text-xs text-subtitle py-6 text-center">{emptyText}</div>
    {:else}
        <div class="flex items-end gap-2 h-24" role="img" aria-label={ariaLabel}>
            {#each buckets as b}
                <div class="flex-1 flex flex-col items-center justify-end h-full">
                    {#if b.value > 0}
                        <div class="text-[10px] font-mono text-subtitle mb-1">{b.display}</div>
                    {/if}
                    <div
                        class="w-full rounded-t-sm {b.severity ? TONE[b.severity] : 'bg-mauve'}"
                        style="height: {max > 0 ? Math.max((b.value / max) * 100, b.value > 0 ? 2 : 0) : 0}%"
                    ></div>
                </div>
            {/each}
        </div>

        <div class="flex gap-2 mt-1">
            {#each buckets as b}
                <div
                    class="flex-1 text-center text-[10px] font-mono {b.severity
                        ? INK[b.severity]
                        : 'text-muted'}"
                >
                    {b.label}
                </div>
            {/each}
        </div>
    {/if}
</div>
