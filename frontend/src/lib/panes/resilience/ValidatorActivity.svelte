<script lang="ts">
    import { headroomStatus, type Band } from './headroomStatus';

    // Every value comes from GET /consensus/evidence; all of these are
    // already in the summary block today.
    export let v = 5;
    export let live = 5;
    export let quorum = 3;
    export let headroom = 2;
    export let band: Band = 'Lazy';
    export let tProbeMs = 120000;
    export let tOutMs = 245000;

    const clamp = (x: number) => Math.max(0, Math.min(100, x));

    // The spec's band names don't survive contact with a user: "Fast" reads as
    // positive when it means one failure from stalling, and "Cliff" names the
    // situation rather than the quantity. headroomStatus renames them as
    // severity, and is shared with FaultModel so the two cannot disagree.
    $: status = headroomStatus(band, headroom);

    // The axis is windowed to [quorum - 1, v] rather than [0, v]. Every value
    // below quorum is equally stalled, so drawing them all spends most of the
    // bar on an undifferentiated region — at BFT v=10 the stall block took 70%
    // — and squeezes the part that actually varies. One cell of stall keeps the
    // edge tangible; falling into it switches to the banner below.
    $: axisMin = Math.max(0, quorum - 1);
    $: cells = Math.max(1, v - axisMin + 1);
    $: cellW = 100 / cells;
    $: pos = (n: number) => clamp((n - axisMin) * cellW);

    // Cliff at h <= 0, Fast at h == 1, Lazy at h >= 2. Cliff and Fast are each
    // exactly one cell wide because each is a single headroom value.
    $: stallEdge = pos(quorum);
    $: cliffEnd = pos(quorum + 1);
    $: fastEnd = pos(quorum + 2);

    $: stalled = live < quorum;

    // How far under quorum, i.e. how many must come back before anything
    // commits again. -headroom, but stated positively so the banner reads as
    // a distance rather than a negative margin.
    $: shortfall = quorum - live;
    $: probeSeconds = Math.round(tProbeMs / 1000);
    $: outSeconds = Math.round(tOutMs / 1000);

    $: regions = [
        { name: 'stall', from: 0, to: stallEdge },
        { name: 'h=0', from: stallEdge, to: cliffEnd },
        { name: 'h=1', from: cliffEnd, to: fastEnd },
        { name: 'h≥2', from: fastEnd, to: 100 },
    ].filter((r) => r.to - r.from >= 7);

    // Cell boundaries, so the marker's width reads as exactly one validator.
    $: ticks = cells <= 16 ? Array.from({ length: cells - 1 }, (_, i) => axisMin + i + 1) : [];

    // headroom is exactly how many FURTHER failures are absorbable, so phrase
    // it that way rather than "N failures from stalling", which is off by one.
    // The probe cadence rides along here rather than taking a row of its own:
    // it is the one thing the band adds beyond headroom, but it is reference
    // detail, not a number to watch.
    $: markerTitle =
        (headroom === 0
            ? 'headroom 0 — the next failure stalls consensus'
            : headroom === 1
              ? 'headroom 1 — one more failure absorbable'
              : `headroom ${headroom} — ${headroom} more failures absorbable`) +
        `. Probing every ${probeSeconds}s; unreachable validators removable after ${outSeconds}s.`;
</script>

<div>
    <div class="flex items-baseline justify-between mb-3">
        <div class="text-xs text-subtitle font-medium">Current Validators</div>
        <div class="text-xs font-mono">
            <span class={status.tone}>{live}</span>
            <span class="text-subtitle">responding of {v}</span>
        </div>
    </div>

    {#if stalled}
        <!-- Below quorum every value is the same state, so the windowed axis
             has nothing left to distinguish. Say the one thing that matters. -->
        <div
            class="h-9 rounded-md border border-red bg-red/25 flex items-center justify-center gap-3"
            role="img"
            aria-label="Stalled. {live} of {v} validators responding, {shortfall} below quorum."
        >
            <span class="text-sm font-semibold tracking-widest text-red">STALLED</span>
            <span class="text-xs font-mono text-subtitle">
                {shortfall}
                {shortfall === 1 ? 'validator' : 'validators'} below quorum
            </span>
        </div>

        <div class="mt-3 text-xs text-red">
            Below quorum — nothing commits, including the removals that would restore it.
        </div>
    {:else}
        <div class="relative h-4 text-[10px] text-muted select-none">
            {#each regions as r}
                <div class="absolute -translate-x-1/2" style="left: {(r.from + r.to) / 2}%">
                    {r.name}
                </div>
            {/each}
        </div>

        <div
            class="relative h-9"
            role="img"
            aria-label="{live} of {v} validators responding. Stalls below {quorum}."
        >
            <div
                class="absolute inset-0 rounded-md overflow-hidden bg-surface1 border border-overlay0"
            >
                <div class="absolute inset-y-0 left-0 bg-red/30" style="width: {stallEdge}%"></div>

                <div class="absolute top-0 bottom-0 w-px bg-overlay1" style="left: {cliffEnd}%"></div>
                <div class="absolute top-0 bottom-0 w-px bg-overlay1" style="left: {fastEnd}%"></div>

                {#each ticks as t}
                    <div
                        class="absolute bottom-0 h-1.5 w-px bg-overlay1/60"
                        style="left: {pos(t)}%"
                    ></div>
                {/each}

                <!-- One cell wide, so at headroom 0 it fills the h=0 cell
                     exactly. Inset 2px so it never abuts the stall line. -->
                <div
                    class="absolute inset-y-0 rounded-sm cursor-help transition-all duration-300 {status.fill}"
                    style="left: calc({pos(live)}% + 2px); width: calc({cellW}% - 4px)"
                    title={markerTitle}
                ></div>

                <!-- Last, and centred on the boundary: drawn before the marker
                     it was painted over at headroom 0, when it matters most. -->
                <div
                    class="absolute top-0 bottom-0 w-0.5 -translate-x-1/2 bg-red"
                    style="left: {stallEdge}%"
                ></div>
            </div>
        </div>

        <div class="relative h-4 mt-1 text-xs font-mono text-muted">
            <div class="absolute left-0">{axisMin}</div>
            <div class="absolute right-0">{v}</div>
        </div>
    {/if}
</div>
