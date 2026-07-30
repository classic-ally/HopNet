<script lang="ts">
  import { SvelteSet } from 'svelte/reactivity';
  import type { Snippet } from 'svelte';
  import type { PhotoSummary } from './viewmodel';

  // Presentational grid: pure props, no fetching. The data container
  // (PhotoGrid) supplies items + the real thumbnail URL; Storybook supplies
  // mock items + a placeholder URL. URL getters return '' while the content
  // cache is still fetching a blob — cells render a placeholder until the
  // reactive getter re-evaluates with a real object URL.
  let {
    items,
    onOpen,
    thumbUrl = () => '',
    displayUrl = () => '',
    hoverPreview = true,
    sharedLibs = [],
    scrollEl = undefined,
    onTopMonth = undefined,
    header,
    footer,
  }: {
    items: PhotoSummary[];
    onOpen: (index: number) => void;
    thumbUrl?: (photoId: string) => string;
    /** Higher-res source the hover preview upgrades to after a linger. */
    displayUrl?: (photoId: string) => string;
    hoverPreview?: boolean;
    /** library_ids whose assets get a shared badge (multi-library fused view;
     *  pass [] in single-library views to keep cells clean). */
    sharedLibs?: string[];
    /** The scrollable ancestor — needed for scroll-synced month reporting. */
    scrollEl?: HTMLElement;
    /** Reports the month ("YYYY-MM") currently at the top of the viewport. */
    onTopMonth?: (month: string) => void;
    /** Rendered inside the grid before the first row (top scroll sentinel). */
    header?: Snippet;
    footer?: Snippet;
  } = $props();

  const isShared = (p: PhotoSummary) => sharedLibs.includes(p.library_id);

  // Photos whose blob fetched but failed to DECODE (e.g. an HEIC original
  // served as the thumb fallback for a photo without rendition resources) —
  // the cell degrades to the placeholder instead of a broken-image glyph.
  const decodeFailed = new SvelteSet<string>();

  type Row =
    | { kind: 'header'; label: string; key: string; month: string | null }
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

  /** UTC "YYYY-MM" — must match the histogram's strftime-over-sort_ms buckets. */
  function monthKey(iso: string | undefined): string | null {
    if (!iso) return null;
    const d = new Date(iso);
    return isNaN(d.getTime()) ? null : d.toISOString().slice(0, 7);
  }

  // Items arrive already sorted (sort_ms DESC); a linear scan finds day breaks.
  const rows = $derived.by(() => {
    const out: Row[] = [];
    let last = '';
    items.forEach((photo, index) => {
      const label = dayLabel(photo.captured_at);
      if (label !== last) {
        out.push({
          kind: 'header',
          label,
          key: `${label}-${index}`,
          month: monthKey(photo.captured_at),
        });
        last = label;
      }
      out.push({ kind: 'photo', photo, index });
    });
    return out;
  });

  // --- scroll-synced month: report which month the viewport top sits in -----
  // Day headers are sticky with a shared containing block, so every header
  // scrolled past stays pinned at the container top — the LAST header at or
  // above the threshold is exactly the section under the viewport. rAF-
  // throttled; also recomputed when a new page lands (items.length).
  let gridEl = $state<HTMLDivElement>();
  $effect(() => {
    const root = scrollEl;
    const grid = gridEl;
    const report = onTopMonth;
    if (!root || !grid || !report) return;
    void items.length;
    let raf = 0;
    const compute = () => {
      raf = 0;
      const rootTop = root.getBoundingClientRect().top;
      let current: string | null = null;
      for (const h of grid.querySelectorAll<HTMLElement>('h2[data-month]')) {
        if (h.getBoundingClientRect().top - rootTop <= 48) current = h.dataset.month ?? null;
        else break;
      }
      if (current) report(current);
    };
    const onScroll = () => {
      if (!raf) raf = requestAnimationFrame(compute);
    };
    root.addEventListener('scroll', onScroll, { passive: true });
    compute();
    return () => {
      root.removeEventListener('scroll', onScroll);
      if (raf) cancelAnimationFrame(raf);
    };
  });

  function badges(p: PhotoSummary): string[] {
    const b: string[] = [];
    if (p.is_live_photo) b.push('i-carbon-renew');
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

  // --- hover preview: scrub the library without opening the lightbox --------
  // The preview follows the pointer (offset + viewport flip). It opens after a
  // short delay on the first cell, then swaps instantly while scrubbing across
  // cells (a brief grace on leave avoids flicker over the grid gaps). The
  // image starts from the thumb (already in browser cache — instant) and
  // upgrades to the display rendition after a linger on one photo.
  const OPEN_DELAY = 150;
  const CLOSE_GRACE = 80;
  const UPGRADE_DELAY = 400;
  const coarsePointer =
    typeof window !== 'undefined' && window.matchMedia('(pointer: coarse)').matches;

  let preview = $state<PhotoSummary | null>(null);
  let previewPos = $state({ x: 0, y: 0 });
  let previewSharp = $state(false);
  let previewDims = $state<{ w: number; h: number } | null>(null);
  let openTimer: ReturnType<typeof setTimeout> | undefined;
  let closeTimer: ReturnType<typeof setTimeout> | undefined;
  let upgradeTimer: ReturnType<typeof setTimeout> | undefined;

  /** Fit the rendition's aspect ratio into the preview box (upscaling small
   *  thumbs — the sharp layer replaces them before it matters). */
  function fitDims(nw: number, nh: number) {
    const scale = Math.min(previewBox / nw, previewBox / nh);
    previewDims = { w: Math.round(nw * scale), h: Math.round(nh * scale) };
  }

  function showPreview(photo: PhotoSummary) {
    const id = photo.photo_id;
    preview = photo;
    previewSharp = false;
    previewDims = null;
    // The grid already decoded this thumb, so its natural size is usually
    // available synchronously from cache; otherwise onload fills it in.
    // A '' url means the content cache hasn't landed the blob yet — skip the
    // probe entirely (probe.src = '' would fetch the page URL); the preview
    // simply doesn't render for that cell until the thumb exists.
    const thumb = thumbUrl(id);
    if (!thumb) return;
    const probe = new Image();
    probe.onload = () => {
      if (preview?.photo_id === id) fitDims(probe.naturalWidth, probe.naturalHeight);
    };
    probe.src = thumb;
    if (probe.complete && probe.naturalWidth > 0) {
      fitDims(probe.naturalWidth, probe.naturalHeight);
    }
    clearTimeout(upgradeTimer);
    // Preload the display rendition off-DOM, swap only once decoded — a bare
    // src change would blank the preview while the big JPEG streams in.
    upgradeTimer = setTimeout(() => {
      const display = displayUrl(id);
      if (!display) return;
      const img = new Image();
      img.onload = () => {
        if (preview?.photo_id === id) previewSharp = true;
      };
      img.src = display;
    }, UPGRADE_DELAY);
  }

  function cellEnter(photo: PhotoSummary) {
    if (!hoverPreview || coarsePointer) return;
    clearTimeout(closeTimer);
    if (preview) {
      showPreview(photo); // already open: swap instantly (scrubbing)
    } else {
      clearTimeout(openTimer);
      openTimer = setTimeout(() => showPreview(photo), OPEN_DELAY);
    }
  }

  function cellLeave() {
    clearTimeout(openTimer);
    clearTimeout(closeTimer);
    closeTimer = setTimeout(() => {
      preview = null;
      clearTimeout(upgradeTimer);
    }, CLOSE_GRACE);
  }

  // Preview box: generous but never crowding the viewport out.
  let previewBox = $state(560);
  function trackPointer(e: MouseEvent) {
    if (!hoverPreview || coarsePointer) return;
    const box = Math.round(
      Math.min(720, window.innerWidth * 0.45, window.innerHeight * 0.7),
    );
    previewBox = box;
    const pad = 18;
    let x = e.clientX + pad;
    let y = e.clientY + pad;
    if (x + box > window.innerWidth) x = e.clientX - box - pad;
    if (y + box > window.innerHeight) y = Math.max(8, window.innerHeight - box - 8);
    previewPos = { x, y };
  }

</script>

<!-- svelte-ignore a11y_no_static_element_interactions -->
<div class="photo-grid p-2" bind:this={gridEl} onmousemove={trackPointer}>
  {@render header?.()}
  {#each rows as row (row.kind === 'header' ? row.key : row.photo.photo_id)}
    {#if row.kind === 'header'}
      <h2
        class="col-span-full sticky top-0 z-10 bg-crust/90 px-1 py-2 text-sm text-subtitle"
        data-month={row.month}
      >
        {row.label}
      </h2>
    {:else}
      <!-- object-fit / focus / hover live in scoped CSS below: HopNet's
           presetMini ships no object-*/ring/scale utilities, so keeping these
           out of the class list keeps the component portable to HopNet verbatim. -->
      <button
        class="cell"
        onclick={() => onOpen(row.index)}
        onmouseenter={() => cellEnter(row.photo)}
        onmouseleave={cellLeave}
        aria-label={`Open photo from ${dayLabel(row.photo.captured_at)}`}
      >
        {#if thumbUrl(row.photo.photo_id) && !decodeFailed.has(row.photo.photo_id)}
          <img
            class="thumb"
            src={thumbUrl(row.photo.photo_id)}
            alt=""
            loading="lazy"
            decoding="async"
            onerror={() => decodeFailed.add(row.photo.photo_id)}
          />
        {:else}
          <div class="thumb thumb-placeholder"></div>
        {/if}
        {#if row.photo.favorite}
          <span class="i-carbon-favorite-filled absolute top-1 left-1 text-red badge-shadow"></span>
        {/if}
        {#if isShared(row.photo)}
          <span class="i-carbon-user-multiple absolute top-1 right-1 text-sm text-white badge-shadow"
          ></span>
        {/if}
        <div class="absolute bottom-1 right-1 flex items-center gap-1 text-white badge-shadow">
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

{#if preview && previewDims}
  <div
    class="preview"
    style={`left: ${previewPos.x}px; top: ${previewPos.y}px; width: ${previewDims.w}px; height: ${previewDims.h}px`}
  >
    {#key preview.photo_id}
      <!-- Two layers: the thumb (already decoded by the grid) is never removed;
           the sharp rendition stacks on top once loaded. Swapping src on one
           img instead flashes the backdrop for a decode frame. -->
      {#if thumbUrl(preview.photo_id) && !decodeFailed.has(preview.photo_id)}
        <img class="preview-img" src={thumbUrl(preview.photo_id)} alt="" />
      {/if}
      {#if previewSharp && displayUrl(preview.photo_id)}
        <!-- A sharp-layer decode failure just leaves the thumb visible. -->
        <img
          class="preview-img preview-sharp"
          src={displayUrl(preview.photo_id)}
          alt=""
          onerror={() => (previewSharp = false)}
        />
      {/if}
      {#if isShared(preview)}
        <span class="preview-shared i-carbon-user-multiple text-white badge-shadow"></span>
      {/if}
    {/key}
  </div>
{/if}

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
  /* Neutral slab while the content cache fetches the blob. */
  .thumb-placeholder {
    background: #181825; /* mantle */
  }
  /* presetMini ships no drop-shadow utility; scoped equivalent. */
  .badge-shadow {
    filter: drop-shadow(0 1px 2px rgba(0, 0, 0, 0.6));
  }
  .cell:hover .thumb {
    transform: scale(1.02);
  }

  /* Pointer-following preview. pointer-events: none so it never steals the
     hover from the cell underneath (it can sit over the grid). */
  /* Sized explicitly from the rendition's aspect ratio fit into the preview
     box, so both layers fill it exactly (a bare <img> won't upscale a 400px
     thumb past its natural size). */
  .preview {
    position: fixed;
    z-index: 45;
    pointer-events: none;
    border-radius: 0.5rem;
    box-shadow: 0 12px 32px rgba(0, 0, 0, 0.55);
    background: #181825; /* mantle */
  }
  .preview-img {
    position: absolute;
    inset: 0;
    width: 100%;
    height: 100%;
    object-fit: contain;
    border-radius: 0.5rem;
  }
  .preview-shared {
    position: absolute;
    top: 0.5rem;
    right: 0.5rem;
    font-size: 1.125rem;
  }
</style>
