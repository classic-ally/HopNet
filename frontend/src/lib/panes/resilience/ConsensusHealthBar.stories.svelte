<script module>
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import ConsensusHealthBar from './ConsensusHealthBar.svelte';

  const { Story } = defineMeta({
    title: 'Components/ConsensusHealthBar',
    component: ConsensusHealthBar,
    argTypes: {
      activeValidators: {
        control: { type: 'range', min: 1, max: 40, step: 1 },
        description: 'Number of currently active validators'
      },
      totalValidators: {
        control: { type: 'range', min: 1, max: 40, step: 1 },
        description: 'Total number of validators in the network'
      },
      unavailableValidators: {
        control: { type: 'range', min: 0, max: 20, step: 1 },
        description: 'Number of validators that are currently unavailable'
      },
      voteOutThreshold: {
        control: { type: 'range', min: 1, max: 10, step: 1 },
        description: 'Number of validators that trigger auto vote-out'
      },
      totalNetworkNodes: {
        control: { type: 'range', min: 1, max: 100, step: 1 },
        description: 'Total nodes in the entire network (storage + validators)'
      }
    }
  });
</script>

{#snippet template(args)}
  <ConsensusHealthBar {...args} />
{/snippet}

<!-- Setup Mode: activeValidators <= 2 (Red) -->
<Story
  name="Setup Mode - 1 Validator"
  {template}
  args={{
    activeValidators: 1,
    totalValidators: 1,
    unavailableValidators: 0,
    voteOutThreshold: 2,
    totalNetworkNodes: 3
  }}
/>

<Story
  name="Setup Mode - 2 Validators"
  {template}
  args={{
    activeValidators: 2,
    totalValidators: 2,
    unavailableValidators: 0,
    voteOutThreshold: 2,
    totalNetworkNodes: 5
  }}
/>

<!-- Crash Protection: 3 <= activeValidators <= 6 (Yellow) -->
<Story
  name="Crash Protection - 3 Validators"
  {template}
  args={{
    activeValidators: 3,
    totalValidators: 3,
    unavailableValidators: 0,
    voteOutThreshold: 2,
    totalNetworkNodes: 8
  }}
/>

<Story
  name="Crash Protection - 6 Validators"
  {template}
  args={{
    activeValidators: 6,
    totalValidators: 6,
    unavailableValidators: 0,
    voteOutThreshold: 2,
    totalNetworkNodes: 15
  }}
/>

<!-- Anomaly Protection: activeValidators >= 7 (Green) -->
<Story
  name="Anomaly Protection - 7 Validators"
  {template}
  args={{
    activeValidators: 7,
    totalValidators: 7,
    unavailableValidators: 0,
    voteOutThreshold: 3,
    totalNetworkNodes: 18
  }}
/>

<Story
  name="Anomaly Protection - 9+ Validators"
  {template}
  args={{
    activeValidators: 9,
    totalValidators: 10,
    unavailableValidators: 1,
    voteOutThreshold: 3,
    totalNetworkNodes: 25
  }}
/>

<!-- Edge Cases and Degraded States -->
<Story
  name="Threshold Boundary - 2 to 3 Transition"
  {template}
  args={{
    activeValidators: 3,
    totalValidators: 4,
    unavailableValidators: 1,
    voteOutThreshold: 2,
    totalNetworkNodes: 10
  }}
/>

<Story
  name="Threshold Boundary - 6 to 7 Transition"
  {template}
  args={{
    activeValidators: 7,
    totalValidators: 8,
    unavailableValidators: 1,
    voteOutThreshold: 3,
    totalNetworkNodes: 20
  }}
/>

<Story
  name="Degraded Network - High Unavailability"
  {template}
  args={{
    activeValidators: 5,
    totalValidators: 10,
    unavailableValidators: 5,
    voteOutThreshold: 3,
    totalNetworkNodes: 25
  }}
/>

<Story
  name="Large Scale Network"
  {template}
  args={{
    activeValidators: 15,
    totalValidators: 15,
    unavailableValidators: 0,
    voteOutThreshold: 5,
    totalNetworkNodes: 50
  }}
/>