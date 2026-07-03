<script lang="ts">
  import { onMount } from 'svelte';
  import Button from '$ui/Button.svelte';
  import { apiJson } from './lib/api';
  import type { LibrarySummary, PhotoSummary } from './lib/types';
  import PhotoGrid from './lib/panes/photos/PhotoGrid.svelte';
  import PhotoLightbox from './lib/panes/photos/PhotoLightbox.svelte';

  let libraries = $state<LibrarySummary[]>([]);
  let currentLib = $state<string | null>(null);
  let ready = $state(false);
  let loadError = $state<string | null>(null);

  // Filters (applied server-side via the /api/photos query).
  let mediaFilter = $state<'all' | 'image' | 'video'>('all');
  let favOnly = $state(false);
  const filter = $derived({
    media_type: mediaFilter === 'all' ? undefined : mediaFilter,
    favorite: favOnly ? true : undefined,
  });

  // The grid publishes its loaded page here so the lightbox can navigate it.
  let items = $state<PhotoSummary[]>([]);
  let lightboxIndex = $state<number | null>(null);

  onMount(async () => {
    try {
      // A 401 here is turned into an OIDC redirect inside apiJson.
      libraries = await apiJson<LibrarySummary[]>('/api/libraries');
      currentLib = libraries[0]?.library_id ?? null;
    } catch (e) {
      loadError = e instanceof Error ? e.message : String(e);
    } finally {
      ready = true;
    }
  });

  function selectLibrary(id: string) {
    if (id === currentLib) return;
    currentLib = id;
    lightboxIndex = null;
  }
</script>

<div class="h-full flex flex-col bg-crust text-text">
  <header
    class="flex items-center gap-2 px-3 py-2 bg-mantle border-b border-overlay0/40 shrink-0"
  >
    <span class="i-carbon-image text-xl text-mauve"></span>
    <div class="flex gap-1">
      {#each libraries as lib (lib.library_id)}
        <Button
          icon="i-carbon-folder"
          text={`${lib.display_name} (${lib.count})`}
          variant="desktop"
          className={lib.library_id === currentLib
            ? 'bg-surface1 text-text'
            : 'text-subtitle'}
          onClick={() => selectLibrary(lib.library_id)}
        />
      {/each}
    </div>

    <div class="flex-1"></div>

    <Button
      icon={mediaFilter === 'all'
        ? 'i-carbon-apps'
        : mediaFilter === 'image'
          ? 'i-carbon-image'
          : 'i-carbon-video'}
      text={mediaFilter === 'all' ? 'All' : mediaFilter === 'image' ? 'Photos' : 'Videos'}
      variant="desktop"
      onClick={() =>
        (mediaFilter = mediaFilter === 'all' ? 'image' : mediaFilter === 'image' ? 'video' : 'all')}
    />
    <Button
      icon={favOnly ? 'i-carbon-favorite-filled' : 'i-carbon-favorite'}
      text="Favorites"
      variant="desktop"
      className={favOnly ? 'bg-surface1 text-red' : 'text-subtitle'}
      onClick={() => (favOnly = !favOnly)}
    />
  </header>

  <main class="flex-1 min-h-0 overflow-y-auto">
    {#if !ready}
      <div class="h-full grid place-items-center text-muted">
        <span class="i-carbon-circle-dash text-3xl animate-spin"></span>
      </div>
    {:else if loadError}
      <div class="h-full grid place-items-center text-red">{loadError}</div>
    {:else if !currentLib}
      <div class="h-full grid place-items-center text-muted">No libraries available.</div>
    {:else}
      {#key `${currentLib}:${filter.media_type}:${filter.favorite}`}
        <PhotoGrid
          library={currentLib}
          {filter}
          bind:items
          onOpen={(i) => (lightboxIndex = i)}
        />
      {/key}
    {/if}
  </main>
</div>

{#if lightboxIndex !== null && items[lightboxIndex]}
  <PhotoLightbox
    {items}
    index={lightboxIndex}
    onIndex={(i) => (lightboxIndex = i)}
    onClose={() => (lightboxIndex = null)}
  />
{/if}
