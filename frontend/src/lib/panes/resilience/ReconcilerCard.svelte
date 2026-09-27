<script lang="ts">
    import Card from '../../primitives/Card.svelte';
    import {
        formatBytesPerSec,
        formatMicros,
        formatRelativeAgo,
        formatSeconds
    } from '../../utils/formatters';

    // Reconciler (RFC-STORAGE-003 S7): this node's fetch record since the
    // process started, in the commit-latency shape (count and quantiles),
    // and its time to conformance per worker queue. The worker is ONE
    // serial loop, so a tier's ETA is its owed fetches at the measured
    // median fetch — computed by the backend, only formatted here.
    type Percentiles = { count: number; p50: number; p90: number; p99: number; p999: number; max: number };

    export let fetches = 0;
    export let failures = 0;
    export let latencyUs: Percentiles = { count: 0, p50: 0, p90: 0, p99: 0, p999: 0, max: 0 };
    export let throughputBps: Percentiles = { count: 0, p50: 0, p90: 0, p99: 0, p999: 0, max: 0 };
    export let p50FetchUs: number | null = null;
    export let tiers: { tier: string; owedFetches: number; etaSecs: number | null }[] = [];
    export let partial = false;
    export let tickAt: number | null = null;

    const TIER_LABEL: Record<string, string> = {
        urgent: 'Urgent rebuilds',
        pull: 'Pulls',
        lazy: 'Lazy rebuilds'
    };
    const TIER_HINT: Record<string, string> = {
        urgent: 'Chunks below the watermark this node owes a rebuild of (K fetches each)',
        pull: 'Classes this node owes under its goals and does not hold (one fetch each)',
        lazy: 'Chunks at or above the watermark this node owes a rebuild of (K fetches each)'
    };

    $: hasSamples = fetches > 0;
    $: owedTotal = tiers.reduce((a, t) => a + t.owedFetches, 0);

    // The four quantiles a tile row shows; p999 is in the payload for the
    // curious but reads as noise beside max on a narrow card.
    const quantiles = (p: Percentiles): [string, number][] => [
        ['p50', p.p50],
        ['p90', p.p90],
        ['p99', p.p99],
        ['max', p.max]
    ];
    $: latencyRows = quantiles(latencyUs);
    $: throughputRows = quantiles(throughputBps);

    const etaTone = (t: { owedFetches: number; etaSecs: number | null }) =>
        t.owedFetches === 0 ? 'text-muted' : t.etaSecs === null ? 'text-subtitle' : 'text-primary';
</script>

<Card title="Reconciler" subtitle="This node, since process start">
    {#snippet headerRight()}
        <span class="text-xs font-mono">
            <span class="text-text">{fetches}</span>
            <span class="text-subtitle">fetches</span>
            {#if failures > 0}
                <span class="text-muted">·</span>
                <span class="text-yellow">{failures} failed</span>
            {/if}
        </span>
    {/snippet}

    {#if !hasSamples}
        <div class="text-xs text-subtitle py-4 text-center">
            No fragment fetched yet — timings appear after the first pull.
        </div>
    {:else}
        <div class="grid grid-cols-5 gap-2">
            <div class="text-xs text-subtitle self-end">Fetch time</div>
            {#each latencyRows as [q, v]}
                <div class="text-center">
                    <div class="text-[10px] text-muted font-mono">{q}</div>
                    <div class="font-mono text-lg font-semibold leading-none text-primary">
                        {formatMicros(v)}
                    </div>
                </div>
            {/each}

            <div class="text-xs text-subtitle self-end">Throughput</div>
            {#each throughputRows as [q, v]}
                <div class="text-center">
                    <div class="text-[10px] text-muted font-mono">{q}</div>
                    <div class="font-mono text-lg font-semibold leading-none text-primary">
                        {formatBytesPerSec(v)}
                    </div>
                </div>
            {/each}
        </div>
    {/if}

    <div class="my-4 border-t border-overlay0"></div>

    <div class="flex items-baseline justify-between mb-3">
        <div class="text-xs text-subtitle font-medium">Time to conformance</div>
        <div class="text-xs font-mono text-muted">
            {#if owedTotal === 0}
                nothing owed
            {:else}
                {owedTotal} fetches owed{partial ? ' (first 2,000 blobs)' : ''}
            {/if}
        </div>
    </div>

    <div class="flex items-end gap-2">
        {#each tiers as t (t.tier)}
            <div class="flex-1 text-center" title={TIER_HINT[t.tier] ?? t.tier}>
                <div class="text-xs text-subtitle mb-1">{TIER_LABEL[t.tier] ?? t.tier}</div>
                <div class="font-mono text-2xl font-semibold leading-none {etaTone(t)}">
                    {#if t.owedFetches === 0}
                        0s
                    {:else if t.etaSecs === null}
                        —
                    {:else}
                        {formatSeconds(t.etaSecs)}
                    {/if}
                </div>
                <div class="text-[10px] font-mono text-muted mt-1">
                    {t.owedFetches} {t.owedFetches === 1 ? 'fetch' : 'fetches'}
                </div>
            </div>
        {/each}
    </div>

    <div class="mt-3 text-[10px] font-mono text-muted">
        {#if p50FetchUs !== null}
            @ p50 {formatMicros(p50FetchUs)} per fetch
        {:else}
            no fetch sample yet — ETAs wait for the first pull
        {/if}
        {#if tickAt !== null}
            · rebuild tiers as of the policy tick {formatRelativeAgo(tickAt)}
        {:else}
            · rebuild tiers unknown until the first policy tick
        {/if}
    </div>
</Card>
