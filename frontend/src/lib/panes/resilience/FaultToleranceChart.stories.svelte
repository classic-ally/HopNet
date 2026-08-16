<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import FaultToleranceChart from './FaultToleranceChart.svelte';
  import type { FaultToleranceCurvePoint, NodeStorageBaseline } from '../../types';

  // Sample node data for testing
  const createNode = (id: number, name: string, totalGb: number, baselineGb: number): NodeStorageBaseline => ({
    node_id: id,
    name,
    display_name: name,
    storage_total_gb: totalGb,
    baseline_storage_gb: baselineGb,
    source: 'System'
  });

  // Sample fault tolerance data sets
  const smallNetworkData: FaultToleranceCurvePoint[] = [
    {
      user_data_gb: 0,
      active_nodes: 3,
      nodes_can_fail: 1,
      participating_nodes: [
        createNode(1, 'Node-1', 100, 80),
        createNode(2, 'Node-2', 100, 80),
        createNode(3, 'Node-3', 100, 80)
      ]
    },
    {
      user_data_gb: 50,
      active_nodes: 3,
      nodes_can_fail: 1,
      participating_nodes: [
        createNode(1, 'Node-1', 100, 80),
        createNode(2, 'Node-2', 100, 80),
        createNode(3, 'Node-3', 100, 80)
      ]
    },
    {
      user_data_gb: 120,
      active_nodes: 3,
      nodes_can_fail: 0,
      participating_nodes: [
        createNode(1, 'Node-1', 100, 80),
        createNode(2, 'Node-2', 100, 80),
        createNode(3, 'Node-3', 100, 80)
      ]
    }
  ];

  const mediumNetworkData: FaultToleranceCurvePoint[] = [
    {
      user_data_gb: 0,
      active_nodes: 7,
      nodes_can_fail: 3,
      participating_nodes: Array.from({ length: 7 }, (_, i) =>
        createNode(i + 1, `Node-${i + 1}`, 200, 150)
      )
    },
    {
      user_data_gb: 200,
      active_nodes: 7,
      nodes_can_fail: 3,
      participating_nodes: Array.from({ length: 7 }, (_, i) =>
        createNode(i + 1, `Node-${i + 1}`, 200, 150)
      )
    },
    {
      user_data_gb: 500,
      active_nodes: 7,
      nodes_can_fail: 2,
      participating_nodes: Array.from({ length: 7 }, (_, i) =>
        createNode(i + 1, `Node-${i + 1}`, 200, 150)
      )
    },
    {
      user_data_gb: 800,
      active_nodes: 7,
      nodes_can_fail: 1,
      participating_nodes: Array.from({ length: 7 }, (_, i) =>
        createNode(i + 1, `Node-${i + 1}`, 200, 150)
      )
    },
    {
      user_data_gb: 1000,
      active_nodes: 7,
      nodes_can_fail: 0,
      participating_nodes: Array.from({ length: 7 }, (_, i) =>
        createNode(i + 1, `Node-${i + 1}`, 200, 150)
      )
    }
  ];

  const largeNetworkData: FaultToleranceCurvePoint[] = [
    {
      user_data_gb: 0,
      active_nodes: 15,
      nodes_can_fail: 7,
      participating_nodes: Array.from({ length: 15 }, (_, i) =>
        createNode(i + 1, `Node-${i + 1}`, 500, 400)
      )
    },
    {
      user_data_gb: 1000,
      active_nodes: 15,
      nodes_can_fail: 7,
      participating_nodes: Array.from({ length: 15 }, (_, i) =>
        createNode(i + 1, `Node-${i + 1}`, 500, 400)
      )
    },
    {
      user_data_gb: 3000,
      active_nodes: 15,
      nodes_can_fail: 5,
      participating_nodes: Array.from({ length: 15 }, (_, i) =>
        createNode(i + 1, `Node-${i + 1}`, 500, 400)
      )
    },
    {
      user_data_gb: 5000,
      active_nodes: 15,
      nodes_can_fail: 2,
      participating_nodes: Array.from({ length: 15 }, (_, i) =>
        createNode(i + 1, `Node-${i + 1}`, 500, 400)
      )
    },
    {
      user_data_gb: 6000,
      active_nodes: 15,
      nodes_can_fail: 0,
      participating_nodes: Array.from({ length: 15 }, (_, i) =>
        createNode(i + 1, `Node-${i + 1}`, 500, 400)
      )
    }
  ];

  const { Story } = defineMeta({
    title: 'Panes/Resilience/FaultToleranceChart',
    component: FaultToleranceChart,
    argTypes: {
      data: {
        control: false,
        description: 'Array of fault tolerance curve points'
      },
      observedLevels: {
        control: false,
        description: 'One entry per distinct tolerance level; GROUP BY from the diagnostics query'
      },
      unrecoverableGb: {
        control: { type: 'number' },
        description: 'Already below K — shown as a stat, not a point on the curve'
      },
      unknownGb: {
        control: { type: 'number' },
        description: 'No attestation data — an observability gap'
      },
      zoomToData: {
        control: { type: 'boolean' },
        description: 'Zoom x-axis to current stored data volume'
      }
    }
  });
</script>

{#snippet template(args)}
  <FaultToleranceChart {...args} />
{/snippet}

<Story
  name="Empty State"
  {template}
  args={{
    data: []
  }}
/>

<Story
  name="Small Network (3 nodes)"
  {template}
  args={{
    data: smallNetworkData
  }}
/>

<Story
  name="Medium Network (7 nodes)"
  {template}
  args={{
    data: mediumNetworkData
  }}
/>

<Story
  name="Large Network (15 nodes)"
  {template}
  args={{
    data: largeNetworkData
  }}
/>

<!--
  Observed frontier tracking the ideal. Sorted best-placed first, so the curve
  reads "this much data is at least this resilient" — and because sorting
  removes all interleaving it is a pure step function, never a slope.
-->
<Story
  name="Observed - tracking the ideal"
  {template}
  args={{
    data: mediumNetworkData,
    observedLevels: [
      { tolerance: 3, rawGb: 460 },
      { tolerance: 2, rawGb: 240 }
    ]
  }}
/>

<!-- Repair backlog: the whole frontier sits below what even spread would give -->
<Story
  name="Observed - drifted below ideal"
  {template}
  args={{
    data: mediumNetworkData,
    observedLevels: [
      { tolerance: 2, rawGb: 180 },
      { tolerance: 1, rawGb: 400 }
    ]
  }}
/>

<!--
  The step at y=0 is the point of this shape: its WIDTH is how many GB are one
  failure from unrecoverable. Averaging into x-buckets is exactly what would
  erase it.
-->
<Story
  name="Observed - a step at zero"
  {template}
  args={{
    data: mediumNetworkData,
    observedLevels: [
      { tolerance: 3, rawGb: 300 },
      { tolerance: 2, rawGb: 260 },
      { tolerance: 0, rawGb: 90 }
    ]
  }}
/>

<!-- Already-lost and unattested data are stats, never points on the curve -->
<Story
  name="Observed - with unrecoverable and never attested"
  {template}
  args={{
    data: mediumNetworkData,
    observedLevels: [
      { tolerance: 3, rawGb: 320 },
      { tolerance: 1, rawGb: 210 }
    ],
    unrecoverableGb: 12,
    unknownGb: 45
  }}
/>

<Story
  name="Curve only - no observed data"
  {template}
  args={{
    data: smallNetworkData
  }}
/>

<!--
  F = holders unreachable right now but still storage members. Their fragments
  still count toward the frontier, so it is optimistic about this instant; the
  hatched band is data whose worst-case tolerance sits below F. An upper bound
  on damage, not a measurement — tolerance is adversarial.
-->
<Story
  name="At risk - two holders unreachable"
  {template}
  args={{
    data: mediumNetworkData,
    observedLevels: [
      { tolerance: 3, rawGb: 300 },
      { tolerance: 2, rawGb: 200 },
      { tolerance: 1, rawGb: 120 }
    ],
    unreachableMembers: 2
  }}
/>

<!-- F above every tolerance: the whole frontier is at risk -->
<Story
  name="At risk - all data below F"
  {template}
  args={{
    data: mediumNetworkData,
    observedLevels: [
      { tolerance: 1, rawGb: 260 },
      { tolerance: 0, rawGb: 140 }
    ],
    unreachableMembers: 3
  }}
/>

<!--
  Zoomed to data: x-axis clamped to consumedGb so the frontier fills the width.
  Stored volume (180 GB) is far below capacity (800 GB) — the dramatic case.
-->
<Story
  name="Zoomed to data - small footprint"
  {template}
  args={{
    data: mediumNetworkData,
    observedLevels: [
      { tolerance: 3, rawGb: 120 },
      { tolerance: 2, rawGb: 60 }
    ],
    zoomToData: true
  }}
/>

<!-- Zoomed with at-risk band at the right edge -->
<Story
  name="Zoomed - at risk band at right edge"
  {template}
  args={{
    data: mediumNetworkData,
    observedLevels: [
      { tolerance: 2, rawGb: 80 },
      { tolerance: 1, rawGb: 40 },
      { tolerance: 0, rawGb: 30 }
    ],
    unreachableMembers: 2,
    zoomToData: true
  }}
/>

<!--
  Zoomed and over capacity: consumedGb (950 GB) exceeds capacity (800 GB).
  The full curve is revealed; zoom frames what is stored, including the
  frontier past the ideal curve's end.
-->
<Story
  name="Zoomed - over capacity"
  {template}
  args={{
    data: mediumNetworkData,
    observedLevels: [
      { tolerance: 3, rawGb: 400 },
      { tolerance: 2, rawGb: 300 },
      { tolerance: 1, rawGb: 250 }
    ],
    zoomToData: true
  }}
/>
