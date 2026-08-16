<script lang="ts">
  import { fade, scale } from 'svelte/transition';
  import Button from '../../Button.svelte';
  import type { PhotoDetailVM, PhotoSummary } from './viewmodel';
  import InfoPanel from './InfoPanel.svelte';

  // Presentational: all data access comes through props — the pane supplies
  // cache-backed URL getters ('' while the blob is still fetching) and the
  // detail loader, so the lightbox stays portable and storyable.
  let {
    items,
    index,
    onIndex,
    onClose,
    displayUrl,
    videoUrl,
    loadDetail,
    onDownload,
  }: {
    items: PhotoSummary[];
    index: number;
    onIndex: (i: number) => void;
    onClose: () => void;
    displayUrl: (photoId: string) => string;
    videoUrl: (photoId: string) => string;
    loadDetail: (photoId: string) => Promise<PhotoDetailVM>;
    onDownload: (photoId: string, resourceType: number) => void;
  } = $props();

  const current = $derived(items[index]);
  const hasPrev = $derived(index > 0);
  const hasNext = $derived(index < items.length - 1);

  let detail = $state<PhotoDetailVM | null>(null);
  let showInfo = $state(false);
  // Photo whose display resource failed to DECODE (undecodable fallback,
  // e.g. an HEIC original). Self-resetting: compared against the current id,
  // so navigating away needs no effect to clear it.
  let failedId = $state<string | null>(null);

  // Load full metadata whenever the visible photo changes.
  $effect(() => {
    const id = current?.photo_id;
    detail = null;
    if (!id) return;
    let cancelled = false;
    loadDetail(id)
      .then((d) => !cancelled && (detail = d))
      .catch(() => {});
    return () => {
      cancelled = true;
    };
  });

  function prev() {
    if (hasPrev) onIndex(index - 1);
  }
  function next() {
    if (hasNext) onIndex(index + 1);
  }
  function onKey(e: KeyboardEvent) {
    if (e.key === 'Escape') onClose();
    else if (e.key === 'ArrowLeft') prev();
    else if (e.key === 'ArrowRight') next();
    else if (e.key === 'i') showInfo = !showInfo;
  }

  const isVideo = $derived(current?.media_type === 'video');
</script>

<svelte:window onkeydown={onKey} />

<div
  class="fixed inset-0 z-50 flex bg-crust/95"
  transition:fade={{ duration: 120 }}
>
  <!-- Main stage -->
  <div class="relative flex-1 flex items-center justify-center min-w-0">
    <!-- Close + info controls -->
    <div class="absolute top-3 right-3 z-10 flex gap-1">
      <Button icon="i-carbon-information" text="Info" variant="compact" onClick={() => (showInfo = !showInfo)} />
      <Button icon="i-carbon-close" text="Close" variant="compact" onClick={onClose} />
    </div>

    <!-- Prev / next -->
    {#if hasPrev}
      <div class="absolute left-2 top-1/2 -translate-y-1/2 z-10">
        <Button icon="i-carbon-chevron-left" text="Previous" variant="compact" onClick={prev} />
      </div>
    {/if}
    {#if hasNext}
      <div class="absolute right-2 top-1/2 -translate-y-1/2 z-10">
        <Button icon="i-carbon-chevron-right" text="Next" variant="compact" onClick={next} />
      </div>
    {/if}

    {#key current.photo_id}
      <div class="max-h-full max-w-full p-6 flex items-center justify-center" transition:scale={{ duration: 120, start: 0.97 }}>
        {#if isVideo}
          <!-- HEVC won't decode in Chrome/Firefox; poster + the download button
               below are the fallback. H.264 plays inline. Both URLs come from
               the blob cache: the video is fully buffered before playback (no
               Range streaming through object URLs — documented casualty). -->
          {#if videoUrl(current.photo_id)}
            <!-- svelte-ignore a11y_media_has_caption -->
            <video
              class="max-h-[85vh] max-w-full object-contain rounded-lg"
              controls
              autoplay
              poster={displayUrl(current.photo_id)}
              src={videoUrl(current.photo_id)}
            ></video>
          {:else}
            <div class="grid place-items-center w-48 h-48 text-muted">
              <span class="i-carbon-circle-dash text-3xl animate-spin"></span>
            </div>
          {/if}
        {:else if failedId === current.photo_id}
          <div class="grid place-items-center gap-2 w-48 h-48 text-muted">
            <span class="i-carbon-no-image text-3xl"></span>
            <span class="text-sm">preview unavailable — download below</span>
          </div>
        {:else if displayUrl(current.photo_id)}
          <img
            class="max-h-[85vh] max-w-full object-contain rounded-lg"
            src={displayUrl(current.photo_id)}
            alt=""
            onerror={() => (failedId = current.photo_id)}
          />
        {:else}
          <div class="grid place-items-center w-48 h-48 text-muted">
            <span class="i-carbon-circle-dash text-3xl animate-spin"></span>
          </div>
        {/if}
      </div>
    {/key}
  </div>

  <!-- Info panel -->
  {#if showInfo}
    <div transition:fade={{ duration: 120 }}>
      <InfoPanel {detail} {onDownload} />
    </div>
  {/if}
</div>
