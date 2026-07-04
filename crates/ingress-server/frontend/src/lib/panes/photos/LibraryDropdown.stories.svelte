<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import LibraryDropdown from './LibraryDropdown.svelte';
  import { mockLibraries } from './fixtures';

  const { Story } = defineMeta({
    title: 'Photos/LibraryDropdown',
    component: LibraryDropdown,
  });
</script>

<script lang="ts">
  // Fused default (all libraries), min-one enforced on uncheck.
  let selected = $state(mockLibraries.map((l) => l.library_id));
  let single = $state(['vivid_birch']);
</script>

<Story name="Fused (all)">
  {#snippet template()}
    <div style="height: 20rem; padding: 0.5rem;">
      <LibraryDropdown
        libraries={mockLibraries}
        {selected}
        onChange={(next) => (selected = next)}
      />
      <pre style="margin-top: 1rem; font-size: 11px; color: #a6adc8;">
selected: {JSON.stringify(selected)}</pre>
    </div>
  {/snippet}
</Story>

<Story name="Single library">
  {#snippet template()}
    <div style="height: 20rem; padding: 0.5rem;">
      <LibraryDropdown
        libraries={mockLibraries}
        selected={single}
        onChange={(next) => (single = next)}
      />
    </div>
  {/snippet}
</Story>
