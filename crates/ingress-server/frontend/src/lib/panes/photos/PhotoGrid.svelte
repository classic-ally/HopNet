<script lang="ts">
  import { onMount } from 'svelte';
  import { apiJson } from '../../api';
  import type { PhotoPage, PhotoSummary } from '../../types';
  import PhotoGridView from './PhotoGridView.svelte';

  interface Filter {
    media_type?: string;
    favorite?: boolean;
  }

  let {
    library,
    filter = {},
    items = $bindable<PhotoSummary[]>([]),
    onOpen,
  }: {
    library: string;
    filter?: Filter;
    items?: PhotoSummary[];
    onOpen: (index: number) => void;
  } = $props();

  const PAGE = 100;

  let cursor: string | null = null;
  let done = $state(false);
  let loading = $state(false);
  let error = $state<string | null>(null);
  let sentinel = $state<HTMLDivElement>();

  async function loadMore() {
    if (loading || done) return;
    loading = true;
    error = null;
    try {
      const q = new URLSearchParams({ library, limit: String(PAGE) });
      if (cursor) q.set('cursor', cursor);
      if (filter.media_type) q.set('media_type', filter.media_type);
      if (filter.favorite) q.set('favorite', 'true');
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

  onMount(() => {
    // Infinite scroll: fire loadMore whenever the bottom sentinel nears view.
    // rootMargin prefetches a screenful early so scrolling stays smooth.
    const io = new IntersectionObserver(
      (entries) => {
        if (entries[0]?.isIntersecting) loadMore();
      },
      { rootMargin: '800px' },
    );
    if (sentinel) io.observe(sentinel);
    return () => io.disconnect();
  });
</script>

<PhotoGridView {items} {onOpen}>
  {#snippet footer()}
    <div bind:this={sentinel} class="col-span-full h-4"></div>
  {/snippet}
</PhotoGridView>

{#if loading}
  <div class="grid place-items-center py-4 text-muted">
    <span class="i-carbon-circle-dash text-2xl animate-spin"></span>
  </div>
{/if}
{#if error}
  <div class="grid place-items-center py-4 text-red">{error}</div>
{/if}
{#if done && items.length === 0}
  <div class="grid place-items-center py-16 text-muted">No photos match.</div>
{/if}
