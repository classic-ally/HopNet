<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import LifecycleCard from './LifecycleCard.svelte';

  const { Story } = defineMeta({
    title: 'Panes/Resilience/LifecycleCard',
    component: LifecycleCard,
    argTypes: {
      inFlightBuckets: {
        control: false,
        description:
          'The in-flight set by heights since its goal, youngest first; severity is set by the backend from the engine cadence.'
      }
    }
  });

  type Bucket = { label: string; blobs: number; gb: number; severity?: 'warn' | 'stale' };
  const ages = (b: [number, number, number, number, number], gbEach = 0.5): Bucket[] => [
    { label: '<8', blobs: b[0], gb: b[0] * gbEach },
    { label: '<256', blobs: b[1], gb: b[1] * gbEach },
    { label: '<1k', blobs: b[2], gb: b[2] * gbEach, severity: 'warn' },
    { label: '<8k', blobs: b[3], gb: b[3] * gbEach, severity: 'warn' },
    { label: '≥8k', blobs: b[4], gb: b[4] * gbEach, severity: 'stale' }
  ];
</script>

{#snippet template(args: Record<string, unknown>)}
  <div class="p-4 bg-base max-w-2xl">
    <LifecycleCard {...args} />
  </div>
{/snippet}

<!-- The quiet mesh, visibly quiet-because-done: the predicate holds and the
     strip is one colour. -->
<Story
  name="Converged"
  {template}
  args={{
    tip: 4812,
    transitionHeight: 4790,
    owed: 0,
    inFlight: 0,
    confirmed: 1204,
    converged: true,
    inFlightBuckets: ages([0, 0, 0, 0, 0])
  }}
/>

<!-- A view transition just landed: the staleness pass owes a few
     declarations and the reconciler is working a young in-flight set. -->
<Story
  name="Draining - young in-flight set"
  {template}
  args={{
    tip: 4812,
    transitionHeight: 4800,
    owed: 3,
    inFlight: 12,
    confirmed: 1189,
    converged: false,
    inFlightBuckets: ages([9, 3, 0, 0, 0])
  }}
/>

<!-- The shape that means stalled handoffs: a bump at the right that does not
     drain, older than the attestation window. -->
<Story
  name="Stuck tail - stalled handoffs"
  {template}
  args={{
    tip: 9000,
    transitionHeight: 8950,
    owed: 0,
    inFlight: 7,
    confirmed: 1197,
    converged: false,
    inFlightBuckets: ages([1, 0, 0, 2, 4], 1.5)
  }}
/>
