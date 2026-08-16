<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import PhotoLightbox from './PhotoLightbox.svelte';
  import type { PhotoDetailVM, PhotoSummary } from './viewmodel';

  /**
   * The lightbox is fully presentational — every data access is a prop — so the
   * story supplies fakes rather than a backend. The URL getters return inline
   * SVG data URIs: a real image, no network, and each one visibly different so
   * navigation is observable.
   */
  function swatch(label: string, fill: string): string {
    const svg =
      `<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="800">` +
      `<rect width="1200" height="800" fill="${fill}"/>` +
      `<text x="600" y="420" font-family="sans-serif" font-size="72" fill="#11111b" ` +
      `text-anchor="middle">${label}</text></svg>`;
    return 'data:image/svg+xml;utf8,' + encodeURIComponent(svg);
  }

  function photo(id: string, media_type = 'image'): PhotoSummary {
    return {
      photo_id: id,
      library_id: 'lib',
      sort_ms: 0,
      captured_at: '2026-07-14T11:05:00Z',
      media_type,
      is_live_photo: false,
      pixel_width: 1200,
      pixel_height: 800,
      orientation: 1,
      duration_ms: null,
      favorite: false,
      media_subtypes: [],
      resources: [[1, 'block-' + id]]
    };
  }

  const ITEMS = [photo('one'), photo('two'), photo('three')];
  const FILLS: Record<string, string> = { one: '#cba6f7', two: '#89b4fa', three: '#a6e3a1' };

  const displayUrl = (id: string) => swatch(id, FILLS[id] ?? '#f9e2af');
  const videoUrl = () => '';

  const loadDetail = async (photoId: string): Promise<PhotoDetailVM> => ({
    photo_id: photoId,
    captured_at: '2026-07-14T11:05:00Z',
    pixel_width: 1200,
    pixel_height: 800,
    camera_make: 'Fujifilm',
    camera_model: 'X-T5',
    resources: [[1, 'block-' + photoId]]
  });

  const { Story } = defineMeta({
    title: 'Panes/Photos/PhotoLightbox',
    component: PhotoLightbox,
    argTypes: {
      index: { control: 'number' },
      onIndex: { action: 'onIndex' },
      onClose: { action: 'onClose' },
      onDownload: { action: 'onDownload' }
    },
    parameters: {
      docs: {
        description: {
          component:
            'Full-bleed media viewer — deliberately not built on Modal, whose chrome is a ' +
            'centred panel. It shares only the behaviour: Escape, arrow keys, and an overlay. ' +
            'Close, info, previous and next all use the standard icon Button. Press i to ' +
            'toggle the info panel.'
        }
      }
    }
  });
</script>

<!-- The lightbox is fixed-position; the wrapper is only the page behind it. -->
{#snippet template(args)}
  <div class="min-h-screen bg-crust">
    <PhotoLightbox
      items={ITEMS}
      index={args.index}
      onIndex={(i) => console.log('index', i)}
      onClose={() => console.log('close')}
      {displayUrl}
      {videoUrl}
      {loadDetail}
      onDownload={(id, t) => console.log('download', id, t)}
    />
  </div>
{/snippet}

<!-- First of the set: previous should be absent, next present. -->
<Story name="First" {template} args={{ index: 0 }} />

<Story name="Mid-set (both arrows)" {template} args={{ index: 1 }} />

<Story name="Last" {template} args={{ index: 2 }} />

<!-- No display URL yet — the blob is still fetching, so the spinner shows. -->
{#snippet pending()}
  <div class="min-h-screen bg-crust">
    <PhotoLightbox
      items={ITEMS}
      index={0}
      onIndex={() => {}}
      onClose={() => console.log('close')}
      displayUrl={() => ''}
      videoUrl={() => ''}
      {loadDetail}
      onDownload={() => {}}
    />
  </div>
{/snippet}

<Story name="Loading" template={pending} args={{}} />
