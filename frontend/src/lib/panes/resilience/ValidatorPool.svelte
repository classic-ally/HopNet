<script lang="ts">
    // The global range: every registered node, split by how close it is to
    // validating. Current Validators zooms into the `seated` segment — its
    // whole axis is this bar's first block.
    //
    // Deliberately no thresholds. The old Decision Participation banded this
    // ratio at 15%/40%, which implied more-is-better; but seating more
    // validators raises quorum too, so the ratio is not a safety measure. This
    // states the composition and leaves the judgement to the reader.
    export let total = 12;
    export let seated = 7;
    export let reachableUnseated = 3;
    export let unreachableUnseated = 2;

    $: pct = (n: number) => (total > 0 ? (n / total) * 100 : 0);
    $: share = total > 0 ? Math.round((seated / total) * 100) : 0;

    // The candidate set: every node that could be validating right now.
    // Seating cannot reach a node it cannot contact, so the unreachable block
    // is excluded rather than counted as latent capacity.
    $: ceiling = seated + reachableUnseated;

    // A one-hue lightness ramp, not categorical hues: ordering is the message,
    // and lightness separation is CVD-safe by construction. Validated on the
    // dark surface at worst-adjacent dE 22.6 normal / 22.5 simulated. The last
    // step is the track colour itself, so unreachable reads as unfilled —
    // correct, since seating cannot reach a node it cannot contact.
    // "standby" rather than "online": a seated validator can itself be dark,
    // so labelling the middle block online would imply the first block is not.
    // Standby claims only that these are ready and not currently serving.
    $: segments = [
        { name: 'validators', n: seated, fill: 'bg-mauve', ink: 'text-base' },
        { name: 'standby', n: reachableUnseated, fill: 'bg-overlay0', ink: 'text-text' },
        { name: 'unreachable', n: unreachableUnseated, fill: 'bg-surface0', ink: 'text-muted' },
    ].filter((s) => s.n > 0);
</script>

<div>
    <div class="flex items-baseline justify-between mb-3">
        <div class="text-xs text-subtitle font-medium">Validator Pool</div>
        <div class="text-xs font-mono">
            <span class="text-text">{seated}</span>
            <span class="text-subtitle">of {total} nodes</span>
            <span class="text-muted">· {share}%</span>
        </div>
    </div>

    <!-- Above the bar, not below: anything between this bar and the flow that
         follows it breaks the join the flow exists to make. -->
    <div class="relative h-4 text-xs font-mono">
        <div class="absolute left-0 text-muted">0</div>
        {#if ceiling < total && ceiling > 0}
            <div
                class="absolute -translate-x-1/2 whitespace-nowrap text-subtitle"
                style="left: {pct(ceiling)}%"
            >
                candidates {ceiling}
            </div>
        {/if}
        <div class="absolute right-0 text-muted">{total}</div>
    </div>

    <div
        class="relative h-9 flex rounded-t-md overflow-hidden border border-overlay0 bg-surface0"
        role="img"
        aria-label="{seated} of {total} registered nodes are seated validators.
            {reachableUnseated} on standby, {unreachableUnseated} unreachable."
    >
        {#each segments as s}
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
