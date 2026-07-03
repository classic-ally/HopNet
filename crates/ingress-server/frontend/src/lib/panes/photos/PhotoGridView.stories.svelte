<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import PhotoGridView from './PhotoGridView.svelte';
  import { mockPhotos, placeholderThumb } from './fixtures';

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
      <PhotoGridView items={mockPhotos} {onOpen} thumbUrl={placeholderThumb} />
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
