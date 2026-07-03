<script lang="ts">
  import type { PhotoDetail } from '../../types';

  // Presentational metadata panel. Pure props so it stories cleanly with a mock
  // PhotoDetail. The container passes URL builders; a story passes stubs.
  let {
    detail,
    originalUrl,
    resourceUrl = (photoId: string, type: string) => `/api/photos/${photoId}/resource/${type}`,
  }: {
    detail: PhotoDetail | null;
    originalUrl: string;
    resourceUrl?: (photoId: string, resourceType: string) => string;
  } = $props();

  function fmtDims(w?: number | null, h?: number | null): string | null {
    return w && h ? `${w} × ${h}` : null;
  }
  function fmtBytes(n: number): string {
    const u = ['B', 'KB', 'MB', 'GB'];
    let i = 0;
    let v = n;
    while (v >= 1024 && i < u.length - 1) {
      v /= 1024;
      i++;
    }
    return `${v.toFixed(v < 10 && i > 0 ? 1 : 0)} ${u[i]}`;
  }
</script>

<aside class="w-80 shrink-0 bg-mantle border-l border-overlay0/40 overflow-y-auto p-4 flex flex-col gap-4">
  <div class="flex items-center justify-between">
    <h3 class="text-sm text-subtitle uppercase tracking-wide">Info</h3>
    <a class="flex items-center gap-1 text-sm text-blue hover:underline" href={originalUrl} download>
      <span class="i-carbon-download"></span> Original
    </a>
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
        {#each detail.resources as r}
          <li class="flex items-center justify-between gap-2">
            <a class="text-blue hover:underline truncate" href={resourceUrl(detail.photo_id, r.resource_type)} download>
              {r.resource_type}.{r.ext}
            </a>
            <span class="text-muted shrink-0">{fmtBytes(r.size_bytes)}</span>
          </li>
        {/each}
      </ul>
    </div>
  {:else}
    <div class="text-muted">Loading…</div>
  {/if}
</aside>
