<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import PhotoGridView from './PhotoGridView.svelte';
  import { mockManyPhotos, mockPhotos, placeholderDisplay, placeholderThumb } from './fixtures';

  const { Story } = defineMeta({
    title: 'Photos/PhotoGridView',
    component: PhotoGridView,
  });

  const onOpen = (i: number) => console.log('open', i);
</script>

<!-- Full spread: date-section headers, favorite + live + raw + video badges. -->
<Story name="Mixed">
  {#snippet template()}
    <div style="height: 100vh; overflow-y: auto;">
      <PhotoGridView
        items={mockPhotos}
        {onOpen}
        thumbUrl={placeholderThumb}
        displayUrl={placeholderDisplay}
      />
    </div>
  {/snippet}
</Story>

<!-- Big page: scrub across cells to exercise the pointer-following hover
     preview (thumb first, sharpens after a linger). -->
<Story name="Hover preview">
  {#snippet template()}
    <div style="height: 100vh; overflow-y: auto;">
      <PhotoGridView
        items={mockManyPhotos}
        {onOpen}
        thumbUrl={placeholderThumb}
        displayUrl={placeholderDisplay}
      />
    </div>
  {/snippet}
</Story>

<!-- Fused multi-library view: shared-library assets carry the corner badge
     (every third mock photo belongs to the shared library). -->
<Story name="Fused libraries (shared badges)">
  {#snippet template()}
    <div style="height: 100vh; overflow-y: auto;">
      <PhotoGridView
        items={mockManyPhotos}
        {onOpen}
        thumbUrl={placeholderThumb}
        displayUrl={placeholderDisplay}
        sharedLibs={['vivid_birch']}
      />
    </div>
  {/snippet}
</Story>

<!-- One day only — check header + single-row layout. -->
<Story name="Single day">
  {#snippet template()}
    <PhotoGridView
      items={mockPhotos.filter((p) => p.captured_at?.startsWith('2026-06-21'))}
      {onOpen}
      thumbUrl={placeholderThumb}
    />
  {/snippet}
</Story>

<!-- Empty state. -->
<Story name="Empty">
  {#snippet template()}
    <PhotoGridView items={[]} {onOpen} thumbUrl={placeholderThumb} />
  {/snippet}
</Story>
