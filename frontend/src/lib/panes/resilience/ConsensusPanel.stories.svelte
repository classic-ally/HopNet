<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import ConsensusPanel from './ConsensusPanel.svelte';

  // ---------------------------------------------------------------------
  // Fixture only. Mirrors hopnet_common::quorum and membership::band so the
  // sliders produce self-consistent states. Production reads all of this
  // from GET /consensus/evidence and derives none of it in TypeScript.
  // ---------------------------------------------------------------------
  const V_BFT = 7;

  type Mode = 'auto' | 'bft' | 'majority';
  type Profile = 'bft' | 'majority';

  function resolve(mode: Mode, v: number): Profile {
    return mode === 'auto' ? (v >= V_BFT ? 'bft' : 'majority') : mode;
  }

  function quorumOf(p: Profile, v: number): number {
    return p === 'bft' ? Math.floor((v * 2) / 3) + 1 : Math.floor(v / 2) + 1;
  }

  function bandOf(h: number): 'Lazy' | 'Fast' | 'Cliff' {
    return h >= 2 ? 'Lazy' : h === 1 ? 'Fast' : 'Cliff';
  }

  // ConsensusPolicy defaults: probe_base 30s, grace 5s, doubling on the band.
  function tProbe(b: 'Lazy' | 'Fast' | 'Cliff'): number {
    return b === 'Cliff' ? 30_000 : b === 'Fast' ? 60_000 : 120_000;
  }

  const { Story } = defineMeta({
    title: 'Panes/Resilience/ConsensusPanel',
    component: ConsensusPanel,
    argTypes: {
      v: { control: { type: 'range', min: 1, max: 20, step: 1 }, description: 'Seated validators' },
      live: { control: { type: 'range', min: 0, max: 20, step: 1 }, description: 'Responding' },
      profileMode: {
        control: { type: 'inline-radio' },
        options: ['auto', 'bft', 'majority'],
        description: 'Configured quorum profile'
      },
      reachableUnseated: {
        control: { type: 'range', min: 0, max: 20, step: 1 },
        description: 'Candidate pool'
      },
      unreachableUnseated: {
        control: { type: 'range', min: 0, max: 20, step: 1 },
        description: 'Registered but out of contact'
      }
    }
  });
</script>

{#snippet template(args: {
  v?: number;
  live?: number;
  profileMode?: Mode;
  reachableUnseated?: number;
  unreachableUnseated?: number;
})}
  {@const v = args.v ?? 7}
  {@const live = Math.min(args.live ?? v, v)}
  {@const mode = args.profileMode ?? 'auto'}
  {@const profile = resolve(mode, v)}
  {@const quorum = quorumOf(profile, v)}
  {@const headroom = live - quorum}
  {@const band = bandOf(headroom)}
  {@const pool = args.reachableUnseated ?? 3}
  {@const dead = args.unreachableUnseated ?? 2}
  <div class="max-w-2xl">
    <ConsensusPanel
      {v}
      {live}
      profileMode={mode}
      {profile}
      {quorum}
      faultBudget={v - quorum}
      {headroom}
      {band}
      tProbeMs={tProbe(band)}
      tOutMs={tProbe(band) * 2 + 5_000}
      totalNodes={v + pool + dead}
      reachableUnseated={pool}
      unreachableUnseated={dead}
    />
  </div>
{/snippet}

<Story name="Healthy BFT mesh" {template}
  args={{ v: 7, live: 7, profileMode: 'auto', reachableUnseated: 3, unreachableUnseated: 2 }} />

<!-- Headroom 0: marker fills the h=0 cell, flush against the stall line -->
<Story name="At quorum" {template}
  args={{ v: 7, live: 5, profileMode: 'auto', reachableUnseated: 3, unreachableUnseated: 2 }} />

<!-- Below quorum: nothing commits, including the removals that would fix it -->
<Story name="Stalled" {template}
  args={{ v: 7, live: 4, profileMode: 'auto', reachableUnseated: 3, unreachableUnseated: 2 }} />

<!-- Small majority mesh: narrow validators block, so a steep connector -->
<Story name="Small majority mesh" {template}
  args={{ v: 3, live: 3, profileMode: 'auto', reachableUnseated: 6, unreachableUnseated: 1 }} />

<!--
  The case the old component got wrong: pinned majority at v=9 rendered as
  BFT/green with quorum 6. The truth is majority with quorum 5.
-->
<Story name="Pinned majority - 9 validators" {template}
  args={{ v: 9, live: 9, profileMode: 'majority', reachableUnseated: 1, unreachableUnseated: 0 }} />

<!-- Nearly every node validates: connector is almost a rectangle -->
<Story name="Fully seated" {template}
  args={{ v: 5, live: 5, profileMode: 'auto', reachableUnseated: 0, unreachableUnseated: 0 }} />
