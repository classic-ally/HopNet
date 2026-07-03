<script lang="ts">
  import { fade, scale } from 'svelte/transition';
  import Button from '$ui/Button.svelte';
  import { apiJson } from '../../api';
  import type { PhotoDetail, PhotoSummary } from '../../types';
  import InfoPanel from './InfoPanel.svelte';

  let {
    items,
    index,
    onIndex,
    onClose,
  }: {
    items: PhotoSummary[];
    index: number;
    onIndex: (i: number) => void;
    onClose: () => void;
  } = $props();

  const current = $derived(items[index]);
  const hasPrev = $derived(index > 0);
  const hasNext = $derived(index < items.length - 1);

  let detail = $state<PhotoDetail | null>(null);
  let showInfo = $state(false);

  // Load full metadata whenever the visible photo changes.
  $effect(() => {
    const id = current?.photo_id;
    detail = null;
    if (!id) return;
    let cancelled = false;
    apiJson<PhotoDetail>(`/api/photos/${id}`)
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
  const originalUrl = $derived(`/api/photos/${current?.photo_id}/resource/original`);
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
      <button
        class="absolute left-2 top-1/2 -translate-y-1/2 z-10 p-2 rounded-full bg-surface0/70 hover:bg-surface1 text-text"
        onclick={prev}
        aria-label="Previous"
      >
        <span class="i-carbon-chevron-left text-2xl"></span>
      </button>
    {/if}
    {#if hasNext}
      <button
        class="absolute right-2 top-1/2 -translate-y-1/2 z-10 p-2 rounded-full bg-surface0/70 hover:bg-surface1 text-text"
        onclick={next}
        aria-label="Next"
      >
        <span class="i-carbon-chevron-right text-2xl"></span>
      </button>
    {/if}

    {#key current.photo_id}
      <div class="max-h-full max-w-full p-6 flex items-center justify-center" transition:scale={{ duration: 120, start: 0.97 }}>
        {#if isVideo}
          <!-- HEVC won't decode in Chrome/Firefox; poster + the download button
               below are the fallback. H.264 plays inline. -->
          <!-- svelte-ignore a11y_media_has_caption -->
          <video
            class="stage-media"
            controls
            autoplay
            poster={`/api/photos/${current.photo_id}/display`}
            src={originalUrl}
          ></video>
        {:else}
          <img
            class="stage-media"
            src={`/api/photos/${current.photo_id}/display`}
            alt=""
          />
        {/if}
      </div>
    {/key}
  </div>

  <!-- Info panel -->
  {#if showInfo}
    <div transition:fade={{ duration: 120 }}>
      <InfoPanel {detail} {originalUrl} />
    </div>
  {/if}
</div>

<style>
  /* object-fit + viewport clamp in scoped CSS (presetMini ships neither). */
  .stage-media {
    max-height: 85vh;
    max-width: 100%;
    object-fit: contain;
    border-radius: 0.5rem;
  }
</style>
