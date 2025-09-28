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
    title: 'Components/FaultToleranceChart',
    component: FaultToleranceChart,
    argTypes: {
      data: {
        control: false,
        description: 'Array of fault tolerance curve points'
      },
      onPlanClick: {
        control: false,
        description: 'Callback function when plan button is clicked'
      },
      planButtonText: {
        control: 'text',
        description: 'Text displayed on the plan button'
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
    data: [],
    planButtonText: "Plan..."
  }}
/>

<Story
  name="Small Network (3 nodes)"
  {template}
  args={{
    data: smallNetworkData,
    planButtonText: "Plan..."
  }}
/>

<Story
  name="Medium Network (7 nodes)"
  {template}
  args={{
    data: mediumNetworkData,
    planButtonText: "Optimize Network"
  }}
/>

<Story
  name="Large Network (15 nodes)"
  {template}
  args={{
    data: largeNetworkData,
    planButtonText: "Scale Network"
  }}
/>

<Story
  name="With Plan Callback"
  {template}
  args={{
    data: mediumNetworkData,
    planButtonText: "Interactive Plan",
    onPlanClick: () => alert('Plan button clicked!')
  }}
/>

<Story
  name="Planning Mode"
  {template}
  args={{
    data: smallNetworkData,
    planButtonText: "Done Planning"
  }}
/>