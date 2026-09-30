<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import DiskTruthCard from './DiskTruthCard.svelte';

  const { Story } = defineMeta({
    title: 'Panes/Resilience/DiskTruthCard',
    component: DiskTruthCard,
    argTypes: {
      nodes: { control: false, description: 'One row per holder with inventory rows.' },
      mesh: { control: false, description: 'Every holder summed.' }
    }
  });

  type Row = { nodeId: number; name: string | null; fresh: number; stale: number; never: number; suspect: number };
  const sum = (rows: Row[]) =>
    rows.reduce(
      (a, r) => ({ fresh: a.fresh + r.fresh, stale: a.stale + r.stale, never: a.never + r.never, suspect: a.suspect + r.suspect }),
      { fresh: 0, stale: 0, never: 0, suspect: 0 }
    );

  const allFresh: Row[] = [
    { nodeId: 0, name: 'asgard', fresh: 90, stale: 0, never: 0, suspect: 0 },
    { nodeId: 1, name: 'desktop', fresh: 70, stale: 0, never: 0, suspect: 0 },
    { nodeId: 2, name: 'laptop', fresh: 70, stale: 0, never: 0, suspect: 0 }
  ];
  const lagging: Row[] = [
    { nodeId: 0, name: 'asgard', fresh: 90, stale: 0, never: 0, suspect: 0 },
    { nodeId: 1, name: 'desktop', fresh: 44, stale: 26, never: 0, suspect: 0 },
    { nodeId: 2, name: null, fresh: 20, stale: 10, never: 40, suspect: 0 }
  ];
  const suspect: Row[] = [
    { nodeId: 0, name: 'asgard', fresh: 90, stale: 0, never: 0, suspect: 0 },
    { nodeId: 1, name: 'desktop', fresh: 70, stale: 0, never: 0, suspect: 0 },
    { nodeId: 2, name: 'laptop', fresh: 61, stale: 0, never: 0, suspect: 9 }
  ];
</script>

{#snippet template(args: Record<string, unknown>)}
  <div class="p-4 bg-base max-w-2xl">
    <DiskTruthCard {...args} />
  </div>
{/snippet}

<!-- Every row seen on disk within the window: belief is evidence everywhere. -->
<Story name="All fresh" {template} args={{ windowHeights: 1024, mesh: sum(allFresh), nodes: allFresh }} />

<!-- One holder's sweep has not run in a while, another has rows that were
     never disk-verified since the crossing: belief, not evidence. -->
<Story name="One node lagging" {template} args={{ windowHeights: 1024, mesh: sum(lagging), nodes: lagging }} />

<!-- Rows flagged suspect are treated as missing until re-verified: the one
     state that means something is wrong rather than merely old. -->
<Story name="Suspect present" {template} args={{ windowHeights: 1024, mesh: sum(suspect), nodes: suspect }} />

<Story
  name="No inventory yet"
  {template}
  args={{ windowHeights: 1024, mesh: { fresh: 0, stale: 0, never: 0, suspect: 0 }, nodes: [] }}
/>
