<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import ValidatorPool from './ValidatorPool.svelte';

  const { Story } = defineMeta({
    title: 'Components/ValidatorPool',
    component: ValidatorPool,
    argTypes: {
      seated: {
        control: { type: 'range', min: 0, max: 30, step: 1 },
        description: 'Seated validators (summary.v)'
      },
      reachableUnseated: {
        control: { type: 'range', min: 0, max: 30, step: 1 },
        description: 'Registered, in contact, not seated — the candidate pool'
      },
      unreachableUnseated: {
        control: { type: 'range', min: 0, max: 30, step: 1 },
        description: 'Registered but out of contact — cannot be seated'
      }
    }
  });
</script>

<!-- total is derived, so the three parts always sum to the axis. -->
{#snippet template(args: {
  seated?: number;
  reachableUnseated?: number;
  unreachableUnseated?: number;
})}
  {@const seated = args.seated ?? 7}
  {@const reachableUnseated = args.reachableUnseated ?? 3}
  {@const unreachableUnseated = args.unreachableUnseated ?? 2}
  <div class="p-4 bg-surface0">
    <ValidatorPool
      {seated}
      {reachableUnseated}
      {unreachableUnseated}
      total={seated + reachableUnseated + unreachableUnseated}
    />
  </div>
{/snippet}

<!-- Every node validates: the whole bar is seated, no ceiling marker -->
<Story name="Fully seated - 5 of 5" {template}
  args={{ seated: 5, reachableUnseated: 0, unreachableUnseated: 0 }} />

<!-- The common case: a pool stands by, and some nodes are out of contact -->
<Story name="Typical - 7 of 12" {template}
  args={{ seated: 7, reachableUnseated: 3, unreachableUnseated: 2 }} />

<!--
  A large pool is not a fault. Seating raises quorum too, so standing by is
  often the correct outcome of the seating rule rather than a backlog.
-->
<Story name="Large standing pool - 3 of 20" {template}
  args={{ seated: 3, reachableUnseated: 15, unreachableUnseated: 2 }} />

<!--
  Why the old ratio decayed misleadingly: dead rows accumulate in the nodes
  table forever. Broken out, they grow their own segment instead of silently
  dragging a percentage down.
-->
<Story name="Dead nodes accumulating - 5 of 18" {template}
  args={{ seated: 5, reachableUnseated: 1, unreachableUnseated: 12 }} />

<!-- Small mesh: segments too narrow for inline labels, so they drop out -->
<Story name="Small mesh - 3 of 4" {template}
  args={{ seated: 3, reachableUnseated: 1, unreachableUnseated: 0 }} />
