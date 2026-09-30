<script lang="ts">
    import Card from '../../primitives/Card.svelte';
    import StageStrip from './StageStrip.svelte';

    // Disk Truth (RFC-STORAGE-003 S5/S7). Each holder's inventory rows by
    // how recently the bytes were actually seen on its disk — the confirm
    // evidence rule applied to the whole table. Fresh rows are evidence;
    // stale and never-verified rows are belief; suspect rows are treated as
    // missing until re-verified. Per node, because the operational question
    // is WHOSE belief is lagging or dishonest.
    export let windowHeights = 1024;
    export let mesh = { fresh: 0, stale: 0, never: 0, suspect: 0 };
    export let nodes: {
        nodeId: number;
        name: string | null;
        fresh: number;
        stale: number;
        never: number;
        suspect: number;
    }[] = [];

    type Counts = { fresh: number; stale: number; never: number; suspect: number };
    const rows = (c: Counts) => c.fresh + c.stale + c.never + c.suspect;

    // One-hue ramp for the freshness order; red only for suspect, which is
    // the one state that means something is wrong rather than merely old.
    const segments = (c: Counts) => [
        { name: 'fresh', n: c.fresh, fill: 'bg-mauve', ink: 'text-base' },
        { name: 'stale', n: c.stale, fill: 'bg-overlay0', ink: 'text-text' },
        { name: 'never', n: c.never, fill: 'bg-surface1', ink: 'text-muted' },
        { name: 'suspect', n: c.suspect, fill: 'bg-red', ink: 'text-base' }
    ];

    const LEGEND = [
        ['bg-mauve', 'fresh'],
        ['bg-overlay0', 'stale'],
        ['bg-surface1', 'never verified'],
        ['bg-red', 'suspect']
    ] as const;
</script>

<Card title="Disk Truth">
    {#snippet headerRight()}
        <span class="text-xs font-mono">
            <span class={mesh.suspect > 0 ? 'text-red' : 'text-muted'}>suspect {mesh.suspect}</span>
            <span class="text-muted">· window {windowHeights} heights</span>
        </span>
    {/snippet}

    {#if rows(mesh) === 0}
        <div class="text-xs text-subtitle py-6 text-center">No inventory rows yet.</div>
    {:else}
        <div class="flex flex-col gap-3">
            <StageStrip label="mesh" total={rows(mesh)} segments={segments(mesh)} height="h-6">
                <svelte:fragment slot="right">
                    <span class="text-text">{mesh.fresh}</span>
                    <span class="text-subtitle">of {rows(mesh)} rows fresh</span>
                </svelte:fragment>
            </StageStrip>

            {#each nodes as n (n.nodeId)}
                <StageStrip
                    label={n.name ?? `node ${n.nodeId}`}
                    total={rows(n)}
                    segments={segments(n)}
                    height="h-5"
                >
                    <svelte:fragment slot="right">
                        {#if n.suspect > 0}
                            <span class="text-red" title="rows flagged suspect">⚠ {n.suspect}</span>
                            <span class="text-muted">·</span>
                        {/if}
                        <span class="text-muted">{rows(n)} rows</span>
                    </svelte:fragment>
                </StageStrip>
            {/each}
        </div>

        <div class="flex gap-4 mt-4 text-[10px] font-mono text-muted">
            {#each LEGEND as [fill, name]}
                <span class="flex items-center gap-1">
                    <span class="inline-block w-2.5 h-2.5 rounded-sm {fill}"></span>
                    {name}
                </span>
            {/each}
        </div>
    {/if}
</Card>
