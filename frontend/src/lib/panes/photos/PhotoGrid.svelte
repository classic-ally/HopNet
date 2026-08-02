<script lang="ts">
  import { tick } from 'svelte';
  import { fetchPhotoPage } from '../../api/photos';
  import { registerResources, urlFor } from './content.svelte';
  import { edgeCursor, type Filter } from './filters';
  import { toSummary, type PhotoSummary } from './viewmodel';
  import PhotoGridView from './PhotoGridView.svelte';

  // Windowed timeline: `items` is a sliding window over the library, not a
  // grow-only list. Both ends page independently (dir=older / dir=newer) and
  // the window is capped — far items are evicted so a long session or a month
  // jump never accumulates the whole library in the DOM. Continuation cursors
  // are synthesized from the window's edge items (sort_ms is on every
  // PhotoSummary for exactly this), so there is no cursor state to desync.
  let {
    filter = {},
    items = $bindable<PhotoSummary[]>([]),
    sharedLibs = [],
    anchorMs = null,
    paused = false,
    scrollEl = undefined,
    onTopMonth = undefined,
    onOpen,
  }: {
    filter?: Filter;
    items?: PhotoSummary[];
    /** library_ids whose assets get the shared badge (fused view). */
    sharedLibs?: string[];
    /** Start the window at this sort_ms boundary (histogram month jump)
     *  instead of the top of the timeline. Read once — jumps remount via
     *  {#key}. */
    anchorMs?: number | null;
    /** Freeze loading/eviction (lightbox open — its index into `items` must
     *  not shift underneath it). */
    paused?: boolean;
    /** Scrollable ancestor: scroll compensation + month sync need it. */
    scrollEl?: HTMLElement;
    onTopMonth?: (month: string) => void;
    onOpen: (index: number) => void;
  } = $props();

  const PAGE = 100;
  /** Window cap — 6 pages. Eviction keeps the DOM bounded; the evicted side's
   *  `done` flag resets so scrolling back re-fetches it. */
  const CAP = 600;

  const anchorCursor = anchorMs != null ? edgeCursor(anchorMs, '') : null;

  let doneOlder = $state(false);
  // No anchor = window starts at the top of the timeline — nothing newer.
  let doneNewer = $state(anchorMs == null);
  // One shared in-flight flag: both directions mutate `items` and adjust
  // scroll, so they are serialized.
  let loading = $state(false);
  let loadingDir = $state<'older' | 'newer' | null>(null);
  let errorOlder = $state<string | null>(null);
  let errorNewer = $state<string | null>(null);

  function cursorFor(dir: 'older' | 'newer'): string | null {
    const edge = dir === 'older' ? items[items.length - 1] : items[0];
    return edge ? edgeCursor(edge.sort_ms, edge.photo_id) : anchorCursor;
  }

  async function load(dir: 'older' | 'newer') {
    if (loading || (dir === 'older' ? doneOlder : doneNewer)) return;
    loading = true;
    loadingDir = dir;
    if (dir === 'older') errorOlder = null;
    else errorNewer = null;
    try {
      const cursor = cursorFor(dir);
      const page = await fetchPhotoPage({
        cursor: cursor ?? undefined,
        dir: dir === 'newer' ? 'newer' : undefined,
        limit: PAGE,
        filter,
      });
      const exhausted = !page.next_cursor || page.items.length === 0;
      // The container owns cache registration + view-model adaptation, so the
      // grid view stays pure.
      const summaries = page.items.map((item) => {
        registerResources(item.photo_id, item.resources);
        return toSummary(item, item.sort_ms);
      });

      if (dir === 'older') {
        items.push(...summaries);
        if (exhausted) doneOlder = true;
        if (items.length > CAP) {
          // Evict from the head. Content above the viewport disappears, so
          // pin the view by the measured height delta (headers merge/split —
          // arithmetic on row counts would drift).
          await tick();
          const before = scrollEl?.scrollHeight ?? 0;
          items.splice(0, items.length - CAP);
          doneNewer = false;
          await tick();
          if (scrollEl) scrollEl.scrollTop -= before - scrollEl.scrollHeight;
        }
      } else {
        // Prepend grows content above the viewport — same pinning, other sign.
        // done flips BEFORE tick so the vanishing top strip is inside the
        // measured delta. overflow-anchor is off on the container (the pane's
        // scroll div), so this is the only scroll adjustment in play.
        const before = scrollEl?.scrollHeight ?? 0;
        items.unshift(...summaries);
        if (exhausted) doneNewer = true;
        await tick();
        if (scrollEl) scrollEl.scrollTop += scrollEl.scrollHeight - before;
        if (items.length > CAP) {
          // Tail eviction is below the viewport: no scroll adjustment needed.
          items.splice(CAP);
          doneOlder = false;
        }
      }
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      if (dir === 'older') errorOlder = msg;
      else errorNewer = msg;
    } finally {
      loading = false;
      loadingDir = null;
    }
  }

  // Infinite scroll, both ends: observers only report visibility; the effects
  // below own the load loops. An IO callback fires on *transitions* — if one
  // lands while a load is in flight and the sentinel then stays visible, no
  // second callback ever comes ("scroll to end, nothing loads"). Observers
  // are (re)created in $effects tracking the sentinel bind — one pointed at a
  // detached node goes silent forever.
  let topSentinel = $state<HTMLDivElement>();
  let bottomSentinel = $state<HTMLDivElement>();
  let topVisible = $state(false);
  let bottomVisible = $state(false);

  function observe(el: HTMLElement | undefined, set: (v: boolean) => void) {
    if (!el) return;
    const io = new IntersectionObserver(
      (entries) => set(entries[0]?.isIntersecting ?? false),
      { rootMargin: '800px' },
    );
    io.observe(el);
    return () => {
      io.disconnect();
      set(false);
    };
  }

  $effect(() => observe(topSentinel, (v) => (topVisible = v)));
  $effect(() => observe(bottomSentinel, (v) => (bottomVisible = v)));

  // Load loops. `loading` flipping back re-runs them, so short pages keep
  // loading until the sentinel leaves range. An error breaks the loop (no
  // retry storm) — the end's button retries. Older wins when both ends are
  // hungry (fills the viewport downward first).
  $effect(() => {
    if (!paused && bottomVisible && !loading && !doneOlder && !errorOlder) load('older');
  });
  $effect(() => {
    if (!paused && topVisible && !loading && !doneNewer && !errorNewer) load('newer');
  });
</script>

{#if !doneNewer}
  <!-- Constant height whatever the state — this strip sits above the grid,
       so a height change here would shift content mid-prepend-measurement. -->
  <div class="edge-strip">
    {#if loadingDir === 'newer'}
      <span class="i-carbon-circle-dash text-2xl spin text-muted"></span>
    {:else}
      {#if errorNewer}
        <div class="text-red text-sm">{errorNewer}</div>
      {/if}
      <button class="load-more" onclick={() => load('newer')}>
        {errorNewer ? 'Retry' : 'Load newer'}
      </button>
    {/if}
  </div>
{/if}

<PhotoGridView
  {items}
  {onOpen}
  {sharedLibs}
  {scrollEl}
  {onTopMonth}
  thumbUrl={(id) => urlFor(id, 'thumb')}
  displayUrl={(id) => urlFor(id, 'display')}
>
  {#snippet header()}
    <div bind:this={topSentinel} class="col-span-full h-1"></div>
  {/snippet}
  {#snippet footer()}
    <div bind:this={bottomSentinel} class="col-span-full h-4"></div>
  {/snippet}
</PhotoGridView>

{#if loadingDir === 'older'}
  <div class="grid place-items-center py-4 text-muted">
    <span class="i-carbon-circle-dash text-2xl spin"></span>
  </div>
{:else if !doneOlder}
  <!-- Belt-and-suspenders: if observer wiring ever fails, pagination stays
       reachable by hand. Also the retry path after an error. -->
  <div class="flex flex-col items-center gap-2 py-4">
    {#if errorOlder}
      <div class="text-red text-sm">{errorOlder}</div>
    {/if}
    <button class="load-more" onclick={() => load('older')}>
      {errorOlder ? 'Retry' : 'Load more'}
    </button>
  </div>
{/if}
{#if doneOlder && doneNewer && items.length === 0}
  <div class="grid place-items-center py-16 text-muted">No photos match.</div>
{/if}

<style>
  .edge-strip {
    height: 3.5rem;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    gap: 0.25rem;
  }

  .load-more {
    padding: 0.375rem 1rem;
    border: 1px solid #45475a; /* surface1 */
    border-radius: 0.5rem;
    background: #313244; /* surface0 */
    color: #bac2de; /* subtext1 */
    cursor: pointer;
    font-size: 0.875rem;
  }
  .load-more:hover {
    background: #45475a;
    color: #cdd6f4;
  }

  /* presetMini ships no animate-spin utility; scoped equivalent. */
  .spin {
    animation: spin 1s linear infinite;
  }
  @keyframes spin {
    to {
      transform: rotate(360deg);
    }
  }
</style>
