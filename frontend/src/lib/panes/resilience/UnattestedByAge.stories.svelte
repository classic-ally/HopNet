<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import UnattestedByAge from './UnattestedByAge.svelte';

  const { Story } = defineMeta({
    title: 'Components/UnattestedByAge',
    component: UnattestedByAge,
    argTypes: {
      buckets: {
        control: false,
        description: 'Age decades youngest first; severity marks warn/stale ranges. Age comes from the UUIDv7 block id.'
      }
    }
  });
</script>

{#snippet template(args: { buckets?: { label: string; gb: number; severity?: 'warn' | 'stale' }[] })}
  <div class="p-4 bg-surface0 max-w-lg">
    <UnattestedByAge buckets={args.buckets ?? []} />
  </div>
{/snippet}

<!--
  The healthy shape: a decay curve. Most unattested data is seconds old and in
  flight, tapering to nothing. No threshold needed to read this as fine.
-->
<Story
  name="Healthy - decays to nothing"
  {template}
  args={{
    buckets: [
      { label: '<1m', gb: 42 },
      { label: '1-10m', gb: 11 },
      { label: '10m-1h', gb: 2 },
      { label: '1h-1d', gb: 0, severity: 'warn' },
      { label: '>1d', gb: 0, severity: 'stale' }
    ]
  }}
/>

<!--
  The failure shape: a tail that does not decay. Those blocks are never going
  to be attested, and no single-threshold check would separate them from a
  large blob still legitimately in transit.
-->
<Story
  name="Stale tail - attestation is stuck"
  {template}
  args={{
    buckets: [
      { label: '<1m', gb: 8 },
      { label: '1-10m', gb: 3 },
      { label: '10m-1h', gb: 4 },
      { label: '1h-1d', gb: 19, severity: 'warn' },
      { label: '>1d', gb: 31, severity: 'stale' }
    ]
  }}
/>

<!-- A large blob mid-distribution: shifted right, but still decaying -->
<Story
  name="Large blob in transit"
  {template}
  args={{
    buckets: [
      { label: '<1m', gb: 5 },
      { label: '1-10m', gb: 120 },
      { label: '10m-1h', gb: 14 },
      { label: '1h-1d', gb: 0, severity: 'warn' },
      { label: '>1d', gb: 0, severity: 'stale' }
    ]
  }}
/>

<Story
  name="Nothing unattested"
  {template}
  args={{
    buckets: [
      { label: '<1m', gb: 0 },
      { label: '1-10m', gb: 0 },
      { label: '10m-1h', gb: 0 },
      { label: '1h-1d', gb: 0, severity: 'warn' },
      { label: '>1d', gb: 0, severity: 'stale' }
    ]
  }}
/>
