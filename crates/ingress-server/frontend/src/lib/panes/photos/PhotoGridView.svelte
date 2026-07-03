<script lang="ts">
  import type { Snippet } from 'svelte';
  import type { PhotoSummary } from '../../types';

  // Presentational grid: pure props, no fetching. The data container
  // (PhotoGrid) supplies items + the real thumbnail URL; Storybook supplies
  // mock items + a placeholder URL. Keeping this pure is also what makes the
  // HopNet fold-in a copy of the *view*, with the fetch wiring re-pointed.
  let {
    items,
    onOpen,
    thumbUrl = (id: string) => `/api/photos/${id}/thumb`,
    footer,
  }: {
    items: PhotoSummary[];
    onOpen: (index: number) => void;
    thumbUrl?: (photoId: string) => string;
    footer?: Snippet;
  } = $props();

  type Row =
    | { kind: 'header'; label: string; key: string }
    | { kind: 'photo'; photo: PhotoSummary; index: number };

  const dateFmt = new Intl.DateTimeFormat(undefined, {
    year: 'numeric',
    month: 'long',
    day: 'numeric',
  });
  function dayLabel(iso: string | undefined): string {
    if (!iso) return 'Undated';
    const d = new Date(iso);
    return isNaN(d.getTime()) ? 'Undated' : dateFmt.format(d);
  }

  // Items arrive already sorted (sort_ms DESC); a linear scan finds day breaks.
  const rows = $derived.by(() => {
    const out: Row[] = [];
    let last = '';
    items.forEach((photo, index) => {
      const label = dayLabel(photo.captured_at);
      if (label !== last) {
        out.push({ kind: 'header', label, key: `${label}-${index}` });
        last = label;
      }
      out.push({ kind: 'photo', photo, index });
    });
    return out;
  });

  function badges(p: PhotoSummary): string[] {
    const b: string[] = [];
    if (p.is_live_photo) b.push('i-carbon-motion');
    if (p.media_type === 'video') b.push('i-carbon-play-filled-alt');
    if (p.media_subtypes?.some((s) => /raw/i.test(s))) b.push('i-carbon-raw');
    return b;
  }

  function fmtDuration(ms: number | undefined | null): string | null {
    if (!ms) return null;
    const s = Math.round(ms / 1000);
    const m = Math.floor(s / 60);
    return `${m}:${String(s % 60).padStart(2, '0')}`;
  }
</script>

<div class="photo-grid p-2">
  {#each rows as row (row.kind === 'header' ? row.key : row.photo.photo_id)}
    {#if row.kind === 'header'}
      <h2 class="col-span-full sticky top-0 z-10 bg-crust/90 px-1 py-2 text-sm text-subtitle">
        {row.label}
      </h2>
    {:else}
      <!-- object-fit / focus / hover live in scoped CSS below: HopNet's
           presetMini ships no object-*/ring/scale utilities, so keeping these
           out of the class list keeps the component portable to HopNet verbatim. -->
      <button
        class="cell"
        onclick={() => onOpen(row.index)}
        aria-label={`Open photo from ${dayLabel(row.photo.captured_at)}`}
      >
        <img class="thumb" src={thumbUrl(row.photo.photo_id)} alt="" loading="lazy" decoding="async" />
        {#if row.photo.favorite}
          <span class="i-carbon-favorite-filled absolute top-1 left-1 text-red drop-shadow"></span>
        {/if}
        <div class="absolute bottom-1 right-1 flex items-center gap-1 text-white drop-shadow">
          {#if row.photo.media_type === 'video' && fmtDuration(row.photo.duration_ms)}
            <span class="text-xs font-mono">{fmtDuration(row.photo.duration_ms)}</span>
          {/if}
          {#each badges(row.photo) as icon}
            <span class={`${icon} text-sm`}></span>
          {/each}
        </div>
      </button>
    {/if}
  {/each}

  {@render footer?.()}
</div>

<style>
  .photo-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(140px, 1fr));
    gap: 4px;
  }

  /* Uniform square cell, transparent — no chrome until keyboard focus. */
  .cell {
    position: relative;
    aspect-ratio: 1 / 1;
    padding: 0;
    border: none;
    background: none;
    border-radius: 0.5rem;
    cursor: pointer;
    outline: none;
  }
  .cell:focus-visible {
    outline: 2px solid #cba6f7; /* mauve */
    outline-offset: 2px;
  }

  /* Fill the square, show the true aspect ratio (no crop, no squash). */
  .thumb {
    position: absolute;
    inset: 0;
    width: 100%;
    height: 100%;
    object-fit: contain;
    border-radius: 0.5rem;
    transition: transform 0.15s ease;
  }
  .cell:hover .thumb {
    transform: scale(1.02);
  }
</style>
