<script lang="ts">
    // A composition as one stacked bar: disjoint segments that sum to the
    // axis, in the Validator Pool's style. Deliberately no thresholds — it
    // states the split and leaves the judgement to the reader. Callers choose
    // fills; ordering is the message, so prefer a one-hue lightness ramp
    // (CVD-safe by construction) and spend a hue only on what must stand out.
    export let label = '';
    export let total = 0;
    export let segments: { name: string; n: number; fill: string; ink: string }[] = [];
    export let height = 'h-9';
    export let ariaLabel = '';

    $: pct = (n: number) => (total > 0 ? (n / total) * 100 : 0);
    $: shown = segments.filter(s => s.n > 0);
    $: aria =
        ariaLabel ||
        `${label}: ` + segments.map(s => `${s.n} ${s.name}`).join(', ') + ` of ${total}`;
</script>

<div>
    {#if label || $$slots.right}
        <div class="flex items-baseline justify-between mb-2">
            <div class="text-xs text-subtitle font-medium">{label}</div>
            <div class="text-xs font-mono"><slot name="right" /></div>
        </div>
    {/if}

    <div
        class="relative {height} flex rounded-md overflow-hidden border border-overlay0 bg-surface0"
        role="img"
        aria-label={aria}
    >
        {#each shown as s}
            <div
                class="relative h-full flex items-center justify-center {s.fill}"
                style="width: {pct(s.n)}%"
                title="{s.n} {s.name}"
            >
                {#if pct(s.n) >= 16}
                    <span class="text-[10px] font-mono {s.ink} select-none">{s.name} {s.n}</span>
                {/if}
            </div>
        {/each}
    </div>
</div>
