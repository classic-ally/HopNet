<script lang="ts">
  import { resourceName } from './content.svelte';
  import type { PhotoDetailVM } from './viewmodel';

  // Presentational metadata panel. Pure props so it stories cleanly with a
  // mock detail. Downloads go through a callback — a bare <a href> can't carry
  // the Bearer header, so the pane wires this to the blob-download helper.
  let {
    detail,
    onDownload,
  }: {
    detail: PhotoDetailVM | null;
    onDownload: (photoId: string, resourceType: number) => void;
  } = $props();

  function fmtDims(w?: number | null, h?: number | null): string | null {
    return w && h ? `${w} × ${h}` : null;
  }
</script>

<aside class="w-80 shrink-0 bg-mantle border-l border-overlay0/40 overflow-y-auto p-4 flex flex-col gap-4">
  <div class="flex items-center justify-between">
    <h3 class="text-sm text-subtitle uppercase tracking-wide">Info</h3>
    {#if detail}
      <button
        class="link-button flex items-center gap-1 text-sm text-blue"
        onclick={() => onDownload(detail!.photo_id, 0)}
      >
        <span class="i-carbon-download"></span> Original
      </button>
    {/if}
  </div>

  {#if detail}
    <dl class="text-sm flex flex-col gap-2">
      {#if detail.captured_at}
        <div><dt class="text-muted">Captured</dt><dd>{new Date(detail.captured_at).toLocaleString()}</dd></div>
      {/if}
      {#if fmtDims(detail.pixel_width, detail.pixel_height)}
        <div><dt class="text-muted">Dimensions</dt><dd>{fmtDims(detail.pixel_width, detail.pixel_height)}</dd></div>
      {/if}
      {#if detail.camera_make || detail.camera_model}
        <div><dt class="text-muted">Camera</dt><dd>{[detail.camera_make, detail.camera_model].filter(Boolean).join(' ')}</dd></div>
      {/if}
      {#if detail.lat != null && detail.lon != null}
        <div>
          <dt class="text-muted">Location</dt>
          <dd>
            <a class="text-blue hover:underline" target="_blank" rel="noreferrer"
               href={`https://www.openstreetmap.org/?mlat=${detail.lat}&mlon=${detail.lon}#map=15/${detail.lat}/${detail.lon}`}>
              {detail.lat.toFixed(5)}, {detail.lon.toFixed(5)}
            </a>
          </dd>
        </div>
      {/if}
      {#if detail.group_type}
        <div><dt class="text-muted">Group</dt><dd>{detail.group_type}</dd></div>
      {/if}
    </dl>

    <div>
      <h4 class="text-xs text-muted uppercase mb-1">Resources</h4>
      <ul class="text-sm flex flex-col gap-1">
        {#each detail.resources as [type, blobId] (blobId)}
          <li class="flex items-center justify-between gap-2">
            <button
              class="link-button text-blue truncate"
              onclick={() => onDownload(detail!.photo_id, type)}
            >
              {resourceName(type)}
            </button>
          </li>
        {/each}
      </ul>
    </div>
  {:else}
    <div class="text-muted">Loading…</div>
  {/if}
</aside>

<style>
  /* Link-styled buttons: downloads need JS (auth header), not anchors. */
  .link-button {
    padding: 0;
    border: none;
    background: none;
    cursor: pointer;
    font-size: inherit;
    text-align: left;
  }
  .link-button:hover {
    text-decoration: underline;
  }
</style>
