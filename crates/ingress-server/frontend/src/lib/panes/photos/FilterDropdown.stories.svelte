<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import FilterDropdown from './FilterDropdown.svelte';
  import { defaultFilterState, toFilter, type FilterState } from './filters';

  const { Story } = defineMeta({
    title: 'Photos/FilterDropdown',
    component: FilterDropdown,
  });
</script>

<script lang="ts">
  // Interactive harness: holds the state the way App does, and shows the wire
  // flags the API would receive so the tri-state → query mapping is auditable.
  let value = $state<FilterState>({ ...defaultFilterState });
  let inverted = $state<FilterState>({
    photos: true,
    videos: false,
    live: 'exclude',
    raw: 'only',
    favorite: 'any',
  });
</script>

<Story name="Interactive">
  {#snippet template()}
    <div style="height: 24rem; padding: 0.5rem; display: flex; justify-content: flex-end;">
      <div>
        <FilterDropdown {value} onChange={(next) => (value = next)} />
        <pre style="margin-top: 1rem; font-size: 11px; color: #a6adc8; text-align: right;">
wire: {JSON.stringify(toFilter(value))}</pre>
      </div>
    </div>
  {/snippet}
</Story>

<!-- Pre-set inverse filters: photos-only, no live, RAW required. -->
<Story name="Active filters">
  {#snippet template()}
    <div style="height: 24rem; padding: 0.5rem; display: flex; justify-content: flex-end;">
      <div>
        <FilterDropdown value={inverted} onChange={(next) => (inverted = next)} />
        <pre style="margin-top: 1rem; font-size: 11px; color: #a6adc8; text-align: right;">
wire: {JSON.stringify(toFilter(inverted))}</pre>
      </div>
    </div>
  {/snippet}
</Story>
