<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import ValidatorActivity from './ValidatorActivity.svelte';

  // ---------------------------------------------------------------------
  // Fixture only. Mirrors hopnet_common::quorum and membership::band so the
  // sliders produce self-consistent states. Production reads all of this
  // from GET /consensus/evidence and derives none of it in TypeScript.
  // ---------------------------------------------------------------------
  const V_BFT = 7;

  function quorumOf(v: number): number {
    return v >= V_BFT ? Math.floor((v * 2) / 3) + 1 : Math.floor(v / 2) + 1;
  }

  // membership.rs: headroom >= 2 Lazy, == 1 Fast, else Cliff.
  function bandOf(headroom: number): 'Lazy' | 'Fast' | 'Cliff' {
    return headroom >= 2 ? 'Lazy' : headroom === 1 ? 'Fast' : 'Cliff';
  }

  // ConsensusPolicy defaults: probe_base 30s, grace 5s, doubling ladder on
  // the band. t_out = t_probe * 2 + grace.
  function tProbeMs(band: 'Lazy' | 'Fast' | 'Cliff'): number {
    return band === 'Cliff' ? 30_000 : band === 'Fast' ? 60_000 : 120_000;
  }

  function tOutMs(band: 'Lazy' | 'Fast' | 'Cliff'): number {
    return tProbeMs(band) * 2 + 5_000;
  }

  const { Story } = defineMeta({
    title: 'Panes/Resilience/ValidatorActivity',
    component: ValidatorActivity,
    argTypes: {
      v: {
        control: { type: 'range', min: 1, max: 20, step: 1 },
        description: 'Seated validators (summary.v)'
      },
      live: {
        control: { type: 'range', min: 0, max: 20, step: 1 },
        description: 'Responding validators, clamped to v (summary.live)'
      }
    }
  });
</script>

<!--
  quorum, headroom and band are derived rather than controlled, so the sliders
  cannot produce a state the backend could never emit. live is clamped to v.
-->
{#snippet template(args: { v?: number; live?: number })}
  {@const v = args.v ?? 5}
  {@const live = Math.min(args.live ?? v, v)}
  {@const quorum = quorumOf(v)}
  {@const headroom = live - quorum}
  {@const band = bandOf(headroom)}
  <div class="p-4 bg-surface0">
    <ValidatorActivity
      {v}
      {live}
      {quorum}
      {headroom}
      {band}
      tProbeMs={tProbeMs(band)}
      tOutMs={tOutMs(band)}
    />
  </div>
{/snippet}

<Story name="Healthy - 5 validators" {template} args={{ v: 5, live: 5 }} />

<!-- Dot still right of the stall line, but the band has tightened to Fast -->
<Story name="One down - Fast band" {template} args={{ v: 5, live: 4 }} />

<!-- Dot lands exactly on the stall boundary: headroom 0, Cliff, 30s probes -->
<Story name="At quorum - Cliff" {template} args={{ v: 7, live: 5 }} />

<!-- Dot inside the red region: nothing commits, repair included -->
<Story name="Stalled - dot inside the stall region" {template} args={{ v: 7, live: 4 }} />

<!-- BFT needs 2v/3+1, so the stall region is proportionally wider than majority -->
<Story name="BFT mesh - wider stall region" {template} args={{ v: 10, live: 9 }} />

<!-- Past 15 the per-validator ticks are dropped rather than crowded -->
<Story name="Large mesh - 20 validators" {template} args={{ v: 20, live: 16 }} />
