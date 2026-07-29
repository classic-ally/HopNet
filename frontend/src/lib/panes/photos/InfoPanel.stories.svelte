<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import InfoPanel from './InfoPanel.svelte';
  import { mockDetail } from './fixtures';

  const { Story } = defineMeta({
    title: 'Photos/InfoPanel',
    component: InfoPanel,
    // These components are dark-only; the main preview offers a light option.
    parameters: { backgrounds: { default: 'dark' } },
  });

  const onDownload = (id: string, type: number) => console.log('download', id, type);
</script>

<!-- Full metadata: camera, location, multiple resources. -->
<Story name="Full">
  {#snippet template()}
    <div style="height: 100vh;">
      <InfoPanel detail={mockDetail} {onDownload} />
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
        }}
        {onDownload}
      />
    </div>
  {/snippet}
</Story>

<!-- Loading (detail not yet fetched). -->
<Story name="Loading">
  {#snippet template()}
    <div style="height: 100vh;">
      <InfoPanel detail={null} {onDownload} />
    </div>
  {/snippet}
</Story>
