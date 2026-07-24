<script lang="ts">
    import { formatStorageCapacity } from '../../utils/formatters';

    // Unattested data bucketed by age. Age comes from data_blocks.id, which is
    // a UUIDv7 — the creation timestamp is embedded, replicated as part of the
    // primary key, so every node reads the same value and it cannot diverge the
    // way an apply-time now() column would.
    //
    // Plotted as a distribution rather than checked against a threshold: a flat
    // bound false-positives on large blobs, which legitimately take longer to
    // distribute. Here a large blob just shifts right — the diagnosis is the
    // SHAPE. Healthy decays toward nothing; a plateau or bump at the right end
    // is data that is never going to be attested.
    // `severity` is set by the caller, not inferred from position, so where the
    // warn/stale lines fall stays a backend decision derived from the storage
    // engine's cadence rather than a number invented in a component.
    export let buckets: {
        label: string;
        gb: number;
        severity?: 'warn' | 'stale';
    }[] = [];

    $: total = buckets.reduce((a, b) => a + b.gb, 0);
    $: max = buckets.reduce((a, b) => Math.max(a, b.gb), 0);
    $: staleGb = buckets.filter(b => b.severity === 'stale').reduce((a, b) => a + b.gb, 0);
    $: warnGb = buckets.filter(b => b.severity === 'warn').reduce((a, b) => a + b.gb, 0);

    // Age is already the x-axis, so a ramp would spend the colour channel
    // re-encoding it. Colour marks only what position does not say: which
    // ranges are past the point of being explainable as in-flight.
    const TONE = { warn: 'bg-yellow', stale: 'bg-red' } as const;
    const INK = { warn: 'text-yellow', stale: 'text-red' } as const;

    $: ariaLabel =
        `Unattested data by age: ` +
        buckets.map(b => `${b.label} ${formatStorageCapacity(b.gb)}`).join(', ');
</script>

<div>
    <div class="flex items-baseline justify-between mb-3">
        <div class="text-xs text-subtitle font-medium">Unattested by age</div>
        <div class="text-xs font-mono">
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
        </div>
    </div>

    {#if total === 0}
        <div class="text-xs text-subtitle py-6 text-center">
            All placed data is attested.
        </div>
    {:else}
        <div class="flex items-end gap-2 h-24" role="img" aria-label={ariaLabel}>
            {#each buckets as b}
                <div class="flex-1 flex flex-col items-center justify-end h-full">
                    {#if b.gb > 0}
                        <div class="text-[10px] font-mono text-subtitle mb-1">
                            {formatStorageCapacity(b.gb)}
                        </div>
                    {/if}
                    <div
                        class="w-full rounded-t-sm {b.severity ? TONE[b.severity] : 'bg-mauve'}"
                        style="height: {max > 0 ? Math.max((b.gb / max) * 100, b.gb > 0 ? 2 : 0) : 0}%"
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
