<script lang="ts">
    import { onMount, onDestroy } from 'svelte';

    interface Props {
        /// Pool of present-participle verbs to rotate through. Pick a themed
        /// set per call site (e.g. setup vs. login) so the flair stays apt.
        words: string[];
        /// Milliseconds between word swaps.
        intervalMs?: number;
        /// Show "Ns" tail once elapsed crosses this threshold (seconds). Keeps
        /// the line clean for fast ops while still reassuring on slow ones.
        elapsedAfterSecs?: number;
    }

    let {
        words,
        intervalMs = 2500,
        elapsedAfterSecs = 3,
    }: Props = $props();

    function pick(prev?: string): string {
        if (words.length <= 1) return words[0] ?? '';
        let next = prev;
        while (next === prev) {
            next = words[Math.floor(Math.random() * words.length)];
        }
        return next!;
    }

    let current = $state(pick());
    let elapsed = $state(0);
    const start = Date.now();
    let wordTimer: ReturnType<typeof setInterval> | null = null;
    let tickTimer: ReturnType<typeof setInterval> | null = null;

    onMount(() => {
        wordTimer = setInterval(() => { current = pick(current); }, intervalMs);
        tickTimer = setInterval(() => { elapsed = Math.floor((Date.now() - start) / 1000); }, 1000);
    });

    onDestroy(() => {
        if (wordTimer) clearInterval(wordTimer);
        if (tickTimer) clearInterval(tickTimer);
    });
</script>

<div class="flex items-center gap-2 text-sm" aria-live="polite">
    <div class="i-carbon-circle-dash animate-spin text-mauve text-base"></div>
    <span class="shimmer text-primary" data-text={`${current}…`}>{current}…</span>
    {#if elapsed >= elapsedAfterSecs}
        <span class="text-overlay1 text-xs ml-auto">{elapsed}s</span>
    {/if}
</div>

<style>
    /* Base verb always renders in text-primary. A mauve duplicate is overlaid
       via ::after, masked through a narrow band that slides L→R every 1s.
       Word swaps every ~2.5s, so each verb gets two-and-a-bit shimmers. */
    .shimmer {
        position: relative;
        display: inline-block;
    }
    .shimmer::after {
        content: attr(data-text);
        position: absolute;
        inset: 0;
        color: #cba6f7;
        pointer-events: none;
        -webkit-mask-image: linear-gradient(90deg, transparent 0%, #000 50%, transparent 100%);
        mask-image: linear-gradient(90deg, transparent 0%, #000 50%, transparent 100%);
        -webkit-mask-size: 40% 100%;
        mask-size: 40% 100%;
        -webkit-mask-repeat: no-repeat;
        mask-repeat: no-repeat;
        animation: status-shimmer 1s linear infinite;
    }
    @keyframes status-shimmer {
        from {
            -webkit-mask-position: -100% 0;
            mask-position: -100% 0;
        }
        to {
            -webkit-mask-position: 200% 0;
            mask-position: 200% 0;
        }
    }
    @media (prefers-reduced-motion: reduce) {
        .shimmer::after { display: none; }
    }
</style>
