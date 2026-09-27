<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import ReconcilerCard from './ReconcilerCard.svelte';

  const { Story } = defineMeta({
    title: 'Panes/Resilience/ReconcilerCard',
    component: ReconcilerCard,
    argTypes: {
      tiers: { control: false, description: 'urgent / pull / lazy owed fetches with backend-computed ETAs.' },
      latencyUs: { control: false },
      throughputBps: { control: false }
    }
  });

  const latency = { count: 1842, p50: 42_000, p90: 180_000, p99: 1_200_000, p999: 4_000_000, max: 9_800_000 };
  const throughput = { count: 1842, p50: 38 * 1024 * 1024, p90: 71 * 1024 * 1024, p99: 112 * 1024 * 1024, p999: 130 * 1024 * 1024, max: 140 * 1024 * 1024 };
  const empty = { count: 0, p50: 0, p90: 0, p99: 0, p999: 0, max: 0 };
  const now = Math.floor(Date.now() / 1000);
</script>

{#snippet template(args: Record<string, unknown>)}
  <div class="p-4 bg-base max-w-3xl">
    <ReconcilerCard {...args} />
  </div>
{/snippet}

<!-- Fresh process: nothing fetched yet, so no timings and no ETA — the tiers
     still show what is owed. -->
<Story
  name="No samples yet"
  {template}
  args={{
    fetches: 0,
    failures: 0,
    latencyUs: empty,
    throughputBps: empty,
    p50FetchUs: null,
    tiers: [
      { tier: 'urgent', owedFetches: 0, etaSecs: null },
      { tier: 'pull', owedFetches: 240, etaSecs: null },
      { tier: 'lazy', owedFetches: 0, etaSecs: null }
    ],
    partial: false,
    tickAt: null
  }}
/>

<!-- A catch-up drain after a view transition: a real pull backlog with a
     measured median, nothing urgent. -->
<Story
  name="Draining pull backlog"
  {template}
  args={{
    fetches: 1842,
    failures: 3,
    latencyUs: latency,
    throughputBps: throughput,
    p50FetchUs: 42_000,
    tiers: [
      { tier: 'urgent', owedFetches: 0, etaSecs: 0 },
      { tier: 'pull', owedFetches: 5952, etaSecs: 250 },
      { tier: 'lazy', owedFetches: 30, etaSecs: 2 }
    ],
    partial: true,
    tickAt: now - 95
  }}
/>

<!-- A departure put chunks below the watermark: urgent rebuilds preempt
     everything else, and the ETA says how long durability is at risk. -->
<Story
  name="Urgent rebuilds"
  {template}
  args={{
    fetches: 220,
    failures: 0,
    latencyUs: { ...latency, count: 220 },
    throughputBps: { ...throughput, count: 220 },
    p50FetchUs: 42_000,
    tiers: [
      { tier: 'urgent', owedFetches: 400, etaSecs: 17 },
      { tier: 'pull', owedFetches: 12, etaSecs: 1 },
      { tier: 'lazy', owedFetches: 90, etaSecs: 4 }
    ],
    partial: false,
    tickAt: now - 20
  }}
/>
