<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import StoragePanel from './StoragePanel.svelte';
  import type { FaultToleranceCurvePoint } from '../../types';


  // Ideal curve: tolerance steps down as nodes fill and drop out of the
  // viable set. Capacity is where it stops clearing 2.
  const curve: FaultToleranceCurvePoint[] = [
    { user_data_gb: 0, active_nodes: 7, nodes_can_fail: 3, participating_nodes: [] },
    { user_data_gb: 200, active_nodes: 7, nodes_can_fail: 3, participating_nodes: [] },
    { user_data_gb: 500, active_nodes: 6, nodes_can_fail: 2, participating_nodes: [] },
    { user_data_gb: 800, active_nodes: 4, nodes_can_fail: 1, participating_nodes: [] },
    { user_data_gb: 1000, active_nodes: 3, nodes_can_fail: 0, participating_nodes: [] }
  ];

  const buckets = (warn: number, stale: number) => [
    { label: '<1m', gb: 12 },
    { label: '1-10m', gb: 4 },
    { label: '10m-1h', gb: 1 },
    { label: '1h-1d', gb: warn, severity: 'warn' as const },
    { label: '>1d', gb: stale, severity: 'stale' as const }
  ];

  const { Story } = defineMeta({
    title: 'Panes/Resilience/StoragePanel',
    component: StoragePanel,
    argTypes: { curve: { control: false }, observedLevels: { control: false } }
  });
</script>

{#snippet template(args: Record<string, unknown>)}
  <div class="max-w-2xl">
    <StoragePanel {...args} />
  </div>
{/snippet}

<!--
  Healthy. The plot stops at 800GB — the unsupported <2 tail is hidden so it
  cannot read as headroom.
-->
<Story name="Healthy - within supported capacity" {template}
  args={{
    curve,
    observedLevels: [
      { tolerance: 3, rawGb: 300 },
      { tolerance: 2, rawGb: 220 }
    ],
    unplacedBuckets: buckets(0, 0)
  }} />

<!-- Distribution overdue but not stale: Unplaced goes yellow -->
<Story name="Distribution overdue" {template}
  args={{
    curve,
    observedLevels: [
      { tolerance: 3, rawGb: 300 },
      { tolerance: 2, rawGb: 220 }
    ],
    unplacedBuckets: buckets(22, 0)
  }} />

<!-- Stale tail plus real loss: both stats red -->
<Story name="Stale and unrecoverable" {template}
  args={{
    curve,
    observedLevels: [
      { tolerance: 3, rawGb: 260 },
      { tolerance: 1, rawGb: 180 }
    ],
    unrecoverableGb: 14,
    unplacedBuckets: buckets(19, 33)
  }} />

<!--
  Past capacity: the unsupported tail is revealed precisely because data has
  crossed into it, and Stored turns yellow.
-->
<Story name="Beyond supported capacity" {template}
  args={{
    curve,
    observedLevels: [
      { tolerance: 3, rawGb: 400 },
      { tolerance: 2, rawGb: 300 },
      { tolerance: 1, rawGb: 190 }
    ],
    unreachableMembers: 2,
    unplacedBuckets: buckets(0, 0)
  }} />
