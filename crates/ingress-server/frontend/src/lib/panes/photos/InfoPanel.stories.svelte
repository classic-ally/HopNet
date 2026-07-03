<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import InfoPanel from './InfoPanel.svelte';
  import { mockDetail } from './fixtures';

  const { Story } = defineMeta({
    title: 'Photos/InfoPanel',
    component: InfoPanel,
  });

  const originalUrl = '#';
</script>

<!-- Full metadata: camera, location, group, multiple resources. -->
<Story name="Full">
  {#snippet template()}
    <div style="height: 100vh;">
      <InfoPanel detail={mockDetail} {originalUrl} />
    </div>
  {/snippet}
</Story>

<!-- Sparse: no camera / location (e.g. a screenshot or imported asset). -->
<Story name="Minimal">
  {#snippet template()}
    <div style="height: 100vh;">
      <InfoPanel
        detail={{
          ...mockDetail,
          camera_make: undefined,
          camera_model: undefined,
          lat: undefined,
          lon: undefined,
          media_subtypes: [],
        }}
        {originalUrl}
      />
    </div>
  {/snippet}
</Story>

<!-- Loading (detail not yet fetched). -->
<Story name="Loading">
  {#snippet template()}
    <div style="height: 100vh;">
      <InfoPanel detail={null} {originalUrl} />
    </div>
  {/snippet}
</Story>
