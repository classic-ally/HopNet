<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import MonthHistogram from './MonthHistogram.svelte';
  import PhotoGridView from './PhotoGridView.svelte';
  import { mockBuckets, mockManyPhotos, placeholderDisplay, placeholderThumb } from './fixtures';

  const { Story } = defineMeta({
    title: 'Panes/Photos/MonthHistogram',
    component: MonthHistogram,
    // These components are dark-only; the main preview offers a light option.
    parameters: { backgrounds: { default: 'dark' } },
  });

  const onOpen = (i: number) => console.log('open', i);
</script>

<!-- Rail alone: hover to expand, year boundaries, floating month tooltip. -->
<Story name="Rail">
  {#snippet template()}
    <div style="height: 100vh; display: flex; justify-content: flex-end;">
      <MonthHistogram buckets={mockBuckets} />
    </div>
  {/snippet}
</Story>

<!-- The real layout: grid + rail side by side. Hovering the rail widens it
     and the grid shrinks (width transition, no remount). -->
<Story name="Beside grid">
  {#snippet template()}
    <div style="height: 100vh; display: flex;">
      <div style="flex: 1; min-width: 0; overflow-y: auto;">
        <PhotoGridView
          items={mockManyPhotos}
          {onOpen}
          thumbUrl={placeholderThumb}
          displayUrl={placeholderDisplay}
        />
      </div>
      <MonthHistogram buckets={mockBuckets} />
    </div>
  {/snippet}
</Story>

<!-- Sparse library: a few months only. -->
<Story name="Sparse">
  {#snippet template()}
    <div style="height: 100vh; display: flex; justify-content: flex-end;">
      <MonthHistogram
        buckets={[
          { month: '2026-06', count: 120 },
          { month: '2026-03', count: 8 },
          { month: '2025-11', count: 340 },
        ]}
      />
    </div>
  {/snippet}
</Story>
