<script lang="ts">
  import { onMount } from 'svelte';
  import { ApiError, apiJson, login, setUnauthorizedHandler } from './lib/api';
  import type { LibrarySummary, MonthBucket, PhotoSummary } from './lib/types';
  import LoginPane from './lib/LoginPane.svelte';
  import {
    defaultFilterState,
    filterKey,
    filterQuery,
    isEmpty,
    monthBoundaryMs,
    toFilter,
    type FilterState,
  } from './lib/panes/photos/filters';
  import FilterDropdown from './lib/panes/photos/FilterDropdown.svelte';
  import LibraryDropdown from './lib/panes/photos/LibraryDropdown.svelte';
  import MonthHistogram from './lib/panes/photos/MonthHistogram.svelte';
  import PhotoGrid from './lib/panes/photos/PhotoGrid.svelte';
  import PhotoLightbox from './lib/panes/photos/PhotoLightbox.svelte';

  let libraries = $state<LibrarySummary[]>([]);
  // Selected library_ids — one or many; many fuse into a single timeline.
  let selectedLibs = $state<string[]>([]);
  let ready = $state(false);
  let loadError = $state<string | null>(null);
  // Any 401 (boot probe or mid-session expiry) lands here: show the login
  // page instead of silently bouncing into the OIDC provider.
  let needsLogin = $state(false);
  let sessionExpired = $state(false);
  setUnauthorizedHandler(() => {
    sessionExpired = ready; // a 401 after boot means the session lapsed
    needsLogin = true;
  });

  // Badge shared-library assets only when the view mixes libraries.
  const sharedLibs = $derived(
    selectedLibs.length > 1
      ? libraries.filter((l) => l.shared).map((l) => l.library_id)
      : [],
  );

  // One filter source of truth; grid + histogram both derive from it.
  let filterState = $state<FilterState>({ ...defaultFilterState });
  const filter = $derived(toFilter(filterState));
  const filterEmpty = $derived(isEmpty(filterState));

  // The grid publishes its loaded page here so the lightbox can navigate it.
  let items = $state<PhotoSummary[]>([]);
  let lightboxIndex = $state<number | null>(null);

  // --- histogram <-> grid sync ----------------------------------------------
  let scrollEl = $state<HTMLDivElement>();
  // Month the grid is currently scrolled to (reported by the grid view).
  let currentMonth = $state<string | null>(null);
  // Non-null after a histogram jump to a month outside the loaded window:
  // the grid remounts anchored at this sort_ms boundary and pages BOTH
  // directions from there (windowed browse).
  let anchorMs = $state<number | null>(null);

  function jumpToMonth(month: string) {
    // Already in the window → just scroll to its first day header.
    const el = scrollEl?.querySelector(`h2[data-month="${month}"]`);
    if (el) {
      el.scrollIntoView({ behavior: 'smooth', block: 'start' });
      return;
    }
    // Outside the window → re-anchor the window at that month's boundary.
    resetBrowse();
    anchorMs = monthBoundaryMs(month);
    currentMonth = month;
  }

  // --- rail hover: zoom the grid out instead of reflowing it ----------------
  // The rail overlay grows 28px -> 176px; the grid's LAYOUT width never
  // changes (constant column count, no reflow, scroll position intact) — it
  // scales down by k to clear the overlay, with height compensated so the
  // visual viewport stays full.
  const RAIL_STRIP = 28;
  const RAIL_EXPANDED = 176;
  let railExpanded = $state(false);
  let gridAreaW = $state(0);
  const zoomK = $derived(
    railExpanded && gridAreaW > 0
      ? (gridAreaW - (RAIL_EXPANDED - RAIL_STRIP)) / gridAreaW
      : 1,
  );

  // Month histogram for the rail. Absent on the old backend (404) or on any
  // error → the rail simply doesn't render.
  let buckets = $state<MonthBucket[]>([]);
  $effect(() => {
    const libs = selectedLibs;
    const f = filter;
    buckets = [];
    if (libs.length === 0 || filterEmpty) return;
    const q = new URLSearchParams({ library: libs.join(',') });
    filterQuery(f, q);
    let cancelled = false;
    apiJson<MonthBucket[]>(`/api/photos/histogram?${q}`)
      .then((b) => !cancelled && (buckets = b))
      .catch(() => {});
    return () => {
      cancelled = true;
    };
  });

  onMount(async () => {
    try {
      // A 401 here flips needsLogin via the unauthorized handler.
      libraries = await apiJson<LibrarySummary[]>('/api/libraries');
      // Default: everything you can see, fused.
      selectedLibs = libraries.map((l) => l.library_id);
    } catch (e) {
      if (!(e instanceof ApiError && e.status === 401)) {
        loadError = e instanceof Error ? e.message : String(e);
      }
    } finally {
      ready = true;
    }
  });

  function selectLibraries(ids: string[]) {
    selectedLibs = ids;
    resetBrowse();
  }

  // The grid remounts via {#key}, but `bind:items` hands it the same array —
  // without this, the new mount appends after the old library's photos.
  function resetBrowse() {
    items = [];
    lightboxIndex = null;
    anchorMs = null;
    currentMonth = null;
    if (scrollEl) scrollEl.scrollTop = 0;
  }
</script>

<div class="h-full flex flex-col bg-crust text-text">
  <header
    class="flex items-center gap-2 px-3 py-2 bg-mantle border-b border-overlay0/40 shrink-0"
  >
    <span class="i-carbon-image text-xl text-mauve"></span>
    {#if libraries.length > 0}
      <LibraryDropdown {libraries} selected={selectedLibs} onChange={selectLibraries} />
    {/if}

    <div class="flex-1"></div>

    <FilterDropdown
      value={filterState}
      onChange={(next) => {
        filterState = next;
        resetBrowse();
      }}
    />
  </header>

  <main class="flex-1 min-h-0 flex">
    <div class="flex-1 min-w-0" bind:clientWidth={gridAreaW}>
      <div
        class="grid-zoom h-full overflow-y-auto"
        bind:this={scrollEl}
        style={zoomK < 1
          ? `height: calc(100% / ${zoomK}); transform: scale(${zoomK});`
          : ''}
      >
        {#if !ready}
          <div class="h-full grid place-items-center text-muted">
            <span class="i-carbon-circle-dash text-3xl animate-spin"></span>
          </div>
        {:else if loadError}
          <div class="h-full grid place-items-center text-red">{loadError}</div>
        {:else if selectedLibs.length === 0}
          <div class="h-full grid place-items-center text-muted">No libraries available.</div>
        {:else if filterEmpty}
          <div class="h-full grid place-items-center text-muted">
            No media types selected.
          </div>
        {:else}
          {#key `${selectedLibs.join(',')}:${filterKey(filter)}:${anchorMs ?? ''}`}
            <PhotoGrid
              libraries={selectedLibs}
              {filter}
              {sharedLibs}
              {anchorMs}
              paused={lightboxIndex !== null}
              {scrollEl}
              onTopMonth={(m) => (currentMonth = m)}
              bind:items
              onOpen={(i) => (lightboxIndex = i)}
            />
          {/key}
        {/if}
      </div>
    </div>

    {#if ready && !loadError && selectedLibs.length > 0 && !filterEmpty}
      <MonthHistogram
        {buckets}
        current={currentMonth}
        onJump={jumpToMonth}
        onExpand={(e) => (railExpanded = e)}
      />
    {/if}
  </main>
</div>

<style>
  /* Rail-hover zoom: transform only while expanded — at rest there's no
     transform so position:fixed children (hover preview) keep viewport
     coordinates. none <-> scale interpolates as identity.
     overflow-anchor off: the windowed grid pins the scroll position itself
     around prepends/evictions — browser scroll anchoring would double-adjust. */
  .grid-zoom {
    transform-origin: top left;
    overflow-anchor: none;
    transition:
      transform 0.18s ease,
      height 0.18s ease;
  }
</style>

{#if lightboxIndex !== null && items[lightboxIndex]}
  <PhotoLightbox
    {items}
    index={lightboxIndex}
    onIndex={(i) => (lightboxIndex = i)}
    onClose={() => (lightboxIndex = null)}
  />
{/if}

{#if needsLogin}
  <LoginPane
    onLogin={login}
    reason={sessionExpired ? 'Your session has expired — sign in again to continue.' : undefined}
  />
{/if}
