<script lang="ts">
  import type { MonthBucket } from './viewmodel';

  // Vertical timeline rail: one row per month, newest at the top (matching
  // the grid's sort order), bar length ∝ photo count. Collapsed it's a thin
  // strip of bars; hovering expands the rail as an OVERLAY — the slot keeps a
  // constant 28px layout width so the grid never reflows (the parent zooms the
  // grid out via transform instead, see App).
  let {
    buckets,
    current = null,
    onJump,
    onExpand,
  }: {
    buckets: MonthBucket[];
    /** Month ("YYYY-MM") the grid is currently scrolled to — highlighted. */
    current?: string | null;
    /** Click a month to jump the grid to it. */
    onJump?: (month: string) => void;
    /** Fired on hover expand/collapse so the parent can zoom the grid. */
    onExpand?: (expanded: boolean) => void;
  } = $props();

  const max = $derived(Math.max(1, ...buckets.map((b) => b.count)));

  let expanded = $state(false);
  let railEl = $state<HTMLDivElement>();
  let hovered = $state<{ bucket: MonthBucket; y: number } | null>(null);

  const monthFmt = new Intl.DateTimeFormat(undefined, { month: 'long', year: 'numeric' });
  function monthLabel(month: string): string {
    const [y, m] = month.split('-').map(Number);
    if (!y || !m) return month;
    return monthFmt.format(new Date(y, m - 1, 1));
  }

  /** New calendar year starts at this bucket (scanning top-down, newest first). */
  function yearBoundary(i: number): string | null {
    const year = buckets[i].month.slice(0, 4);
    if (i === 0 || buckets[i - 1].month.slice(0, 4) !== year) return year;
    return null;
  }

  function onRowEnter(bucket: MonthBucket, e: MouseEvent) {
    const rail = railEl?.getBoundingClientRect();
    const row = (e.currentTarget as HTMLElement).getBoundingClientRect();
    hovered = { bucket, y: row.top + row.height / 2 - (rail?.top ?? 0) };
  }
</script>

{#if buckets.length > 0}
  <div class="rail-slot">
    <div
      class="rail bg-mantle border-l border-overlay0/40"
      class:expanded
      bind:this={railEl}
      onmouseenter={() => {
        expanded = true;
        onExpand?.(true);
      }}
      onmouseleave={() => {
        expanded = false;
        hovered = null;
        onExpand?.(false);
      }}
      role="presentation"
    >
      {#each buckets as bucket, i (bucket.month)}
        {@const year = yearBoundary(i)}
        <button
          class="month-row"
          class:active={bucket.month === current}
          title={`${monthLabel(bucket.month)} — ${bucket.count}`}
          aria-label={`${monthLabel(bucket.month)}: ${bucket.count} photos`}
          aria-current={bucket.month === current ? 'true' : undefined}
          onmouseenter={(e) => onRowEnter(bucket, e)}
          onclick={() => onJump?.(bucket.month)}
        >
          {#if year && expanded}
            <span class="year text-muted">{year}</span>
          {/if}
          <span
            class={`bar ${bucket.month === current ? 'bg-peach' : 'bg-mauve/70'}`}
            style={`width: ${Math.max(8, (bucket.count / max) * 100)}%`}
          ></span>
        </button>
      {/each}

      {#if expanded && hovered}
        <div class="tip bg-surface0 border border-overlay0/40" style={`top: ${hovered.y}px`}>
          <span class="text-text">{monthLabel(hovered.bucket.month)}</span>
          <span class="text-muted">{hovered.bucket.count}</span>
        </div>
      {/if}
    </div>
  </div>
{/if}

<style>
  /* Constant-width layout slot: the expanding rail is an absolute overlay
     anchored to its right edge, so expansion never changes the flex layout
     (the grid beside it zooms via transform instead of reflowing). */
  .rail-slot {
    position: relative;
    width: 28px;
    flex-shrink: 0;
    height: 100%;
  }

  .rail {
    position: absolute;
    top: 0;
    bottom: 0;
    right: 0;
    width: 28px;
    z-index: 20;
    display: flex;
    flex-direction: column;
    justify-content: stretch;
    overflow: hidden;
    transition: width 0.18s ease;
  }
  .rail.expanded {
    width: 176px;
  }

  .month-row {
    position: relative;
    flex: 1 1 0;
    min-height: 2px;
    display: flex;
    align-items: center;
    justify-content: flex-end;
    padding: 0 6px;
    border: none;
    background: none;
    cursor: pointer;
  }
  .month-row:hover {
    background: rgba(49, 50, 68, 0.6); /* surface0/60 */
  }
  .month-row.active {
    background: rgba(69, 71, 90, 0.7); /* surface1/70 */
  }

  .bar {
    display: block;
    height: max(2px, 55%);
    max-height: 6px;
    border-radius: 2px;
    transition: width 0.18s ease;
    pointer-events: none;
  }

  .year {
    position: absolute;
    left: 8px;
    font-size: 0.625rem;
    line-height: 1;
    pointer-events: none;
  }

  .tip {
    position: absolute;
    left: 0.375rem;
    right: 0.375rem;
    transform: translateY(-50%);
    display: flex;
    justify-content: space-between;
    gap: 0.5rem;
    padding: 0.25rem 0.5rem;
    border-radius: 0.375rem;
    font-size: 0.75rem;
    pointer-events: none;
    white-space: nowrap;
    z-index: 10;
  }
</style>
