<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import FaultModel from './FaultModel.svelte';

  // membership.rs: headroom >= 2 Lazy, == 1 Fast, else Cliff. The only fixture
  // needed here — headroom itself follows from the equation the component
  // states, so no quorum arithmetic is required to drive these stories.
  function bandOf(headroom: number): 'Lazy' | 'Fast' | 'Cliff' {
    return headroom >= 2 ? 'Lazy' : headroom === 1 ? 'Fast' : 'Cliff';
  }

  const { Story } = defineMeta({
    title: 'Panes/Resilience/FaultModel',
    component: FaultModel,
    argTypes: {
      v: {
        control: { type: 'range', min: 1, max: 30, step: 1 },
        description: 'Seated validators (summary.v)'
      },
      live: {
        control: { type: 'range', min: 0, max: 30, step: 1 },
        description: 'Responding validators, clamped to v (summary.live)'
      },
      faultBudget: {
        control: { type: 'range', min: 0, max: 15, step: 1 },
        description: 'B(v) = v − quorum(v), from summary.fault_budget'
      }
    }
  });
</script>

<!--
  headroom is derived as budget − faults, which is exactly the identity the
  component renders, so a story can never show an equation that fails to
  balance. Production takes all three from the payload independently.
-->
{#snippet template(args: { v?: number; live?: number; faultBudget?: number })}
  {@const v = args.v ?? 7}
  {@const live = Math.min(args.live ?? v, v)}
  {@const faultBudget = args.faultBudget ?? 2}
  {@const headroom = faultBudget - (v - live)}
  <div class="p-4 bg-surface0">
    <FaultModel {v} {live} {faultBudget} {headroom} band={bandOf(headroom)} />
  </div>
{/snippet}

<Story name="Healthy - full budget" {template} args={{ v: 7, live: 7, faultBudget: 2 }} />

<!-- One fault spent: headroom 1, band tightens to Fast -->
<Story name="One fault spent" {template} args={{ v: 7, live: 6, faultBudget: 2 }} />

<!-- Budget exhausted: headroom 0, the next failure stalls consensus -->
<Story name="Budget exhausted" {template} args={{ v: 7, live: 5, faultBudget: 2 }} />

<!-- Overdrawn: headroom goes negative, which the fault budget itself cannot -->
<Story name="Overdrawn - stalled" {template} args={{ v: 7, live: 4, faultBudget: 2 }} />

<!--
  B = 0 is the one case the fault budget renders red: at v <= 2 quorum equals
  v, so there is no budget to spend and any single failure stalls consensus.
-->
<Story name="No budget at all" {template} args={{ v: 2, live: 2, faultBudget: 0 }} />

<!-- Large mesh: a wide budget mostly unspent -->
<Story name="Large mesh - budget 9" {template} args={{ v: 20, live: 18, faultBudget: 9 }} />
