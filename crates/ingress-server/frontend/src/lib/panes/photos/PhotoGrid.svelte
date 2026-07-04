<script lang="ts">
  import { apiJson } from '../../api';
  import type { PhotoPage, PhotoSummary } from '../../types';
  import { filterQuery, type Filter } from './filters';
  import PhotoGridView from './PhotoGridView.svelte';

  let {
    libraries,
    filter = {},
    items = $bindable<PhotoSummary[]>([]),
    sharedLibs = [],
    onOpen,
  }: {
    /** One or more library_ids; multiple fuse into one timeline. */
    libraries: string[];
    filter?: Filter;
    items?: PhotoSummary[];
    /** library_ids whose assets get the shared badge (fused view). */
    sharedLibs?: string[];
    onOpen: (index: number) => void;
  } = $props();

  const PAGE = 100;

  let cursor: string | null = null;
  let done = $state(false);
  let loading = $state(false);
  let error = $state<string | null>(null);
  let sentinel = $state<HTMLDivElement>();
  let sentinelVisible = $state(false);

  async function loadMore() {
    if (loading || done) return;
    loading = true;
    error = null;
    try {
      const q = new URLSearchParams({ library: libraries.join(','), limit: String(PAGE) });
      if (cursor) q.set('cursor', cursor);
      filterQuery(filter, q);
      const page = await apiJson<PhotoPage>(`/api/photos?${q}`);
      items.push(...page.items);
      cursor = page.next_cursor ?? null;
      if (!cursor || page.items.length === 0) done = true;
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
    } finally {
      loading = false;
    }
  }

  // Infinite scroll: the observer only reports visibility; the effect below
  // owns the load loop. An IO callback fires on *transitions* — if one lands
  // while a load is in flight and the sentinel then stays visible, no second
  // callback ever comes, which is exactly the "scroll to bottom, nothing
  // loads" stall. rootMargin prefetches a screenful early.
  //
  // An $effect (not onMount) so the observer re-attaches whenever the sentinel
  // *element* is recreated — an observer pointed at a detached node goes
  // silent forever, the other way this stalls.
  $effect(() => {
    const el = sentinel;
    if (!el) return;
    const io = new IntersectionObserver(
      (entries) => {
        sentinelVisible = entries[0]?.isIntersecting ?? false;
      },
      { rootMargin: '800px' },
    );
    io.observe(el);
    return () => {
      io.disconnect();
      sentinelVisible = false;
    };
  });

  // The load loop: whenever the sentinel is visible and we're idle, load the
  // next page. `loading` flipping back to false re-runs this, so short pages
  // keep loading until the sentinel scrolls out of range or the cursor ends.
  // An error breaks the loop (no retry storm) — the Load More button retries.
  $effect(() => {
    if (sentinelVisible && !loading && !done && !error) loadMore();
  });
</script>

<PhotoGridView {items} {onOpen} {sharedLibs}>
  {#snippet footer()}
    <div bind:this={sentinel} class="col-span-full h-4"></div>
  {/snippet}
</PhotoGridView>

{#if loading}
  <div class="grid place-items-center py-4 text-muted">
    <span class="i-carbon-circle-dash text-2xl animate-spin"></span>
  </div>
{:else if !done}
  <!-- Belt-and-suspenders: if observer wiring ever fails, pagination stays
       reachable by hand. Also the retry path after an error. -->
  <div class="flex flex-col items-center gap-2 py-4">
    {#if error}
      <div class="text-red text-sm">{error}</div>
    {/if}
    <button class="load-more" onclick={() => loadMore()}>
      {error ? 'Retry' : 'Load more'}
    </button>
  </div>
{/if}
{#if done && items.length === 0}
  <div class="grid place-items-center py-16 text-muted">No photos match.</div>
{/if}

<style>
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
</style>
