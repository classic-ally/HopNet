<script lang="ts">
    import FaultModel from './FaultModel.svelte';
    import ValidatorPool from './ValidatorPool.svelte';
    import ValidatorActivity from './ValidatorActivity.svelte';
    import { headroomStatus, type Band } from './headroomStatus';

    // Flat props mirroring GET /consensus/evidence's summary block, so wiring
    // is a field-for-field mapping later. Nothing here computes quorum
    // arithmetic — hopnet_common::quorum stays the single source of truth.
    export let v = 7;
    export let profileMode: 'auto' | 'bft' | 'majority' = 'auto';
    export let profile: 'bft' | 'majority' = 'bft';
    export let vBft = 7;
    export let quorum = 5;
    export let faultBudget = 2;

    export let live = 5;
    export let headroom = 0;
    export let band: Band = 'Cliff';
    export let tProbeMs = 30000;
    export let tOutMs = 65000;

    export let totalNodes = 12;
    export let reachableUnseated = 3;
    export let unreachableUnseated = 2;

    $: profileLabel = profile === 'bft' ? 'BFT' : 'Majority';

    // Majority is the weaker fault model — its safety proof assumes no
    // equivocation at all, so f_eq is 0 by definition however large the mesh
    // grows. Worth flagging, not alarming: it is the correct choice below the
    // seam, where BFT's larger quorum would leave less headroom than it buys.
    $: profileTone = profile === 'bft' ? 'text-green' : 'text-yellow';

    $: shortOfSeam = Math.max(0, vBft - v);
    $: profileHint =
        profileMode !== 'auto'
            ? 'Profile is pinned; the V_BFT seam does not apply.'
            : profile === 'majority'
              ? `AUTO holds Majority below ${vBft} validators: BFT needs a 2/3 quorum, ` +
                `which would leave too little headroom at this size. ` +
                `${shortOfSeam} more ${shortOfSeam === 1 ? 'validator' : 'validators'} reaches BFT.`
              : `AUTO resolved to BFT at ${vBft} or more validators.`;

    // The panel's headline state, shared with the sections below so a headroom
    // value cannot be named one thing here and another there.
    $: status = headroomStatus(band, headroom);

    // The seated block of the pool is exactly Current Validators' whole axis,
    // so the connector widens from that block's right edge to full width —
    // the same set, zoomed in.
    $: seatedPct = totalNodes > 0 ? (v / totalNodes) * 100 : 0;
</script>

<div class="p-4 bg-surface0">
    <div class="flex items-baseline justify-between mb-4 gap-3">
        <h4 class="text-lg font-semibold text-primary">State Machine Replication</h4>
        <span class="text-lg font-semibold {status.tone}">{status.label}</span>
    </div>

    <FaultModel {v} {live} {faultBudget} {headroom} {band} />

    <div class="my-4 border-t border-overlay0"></div>

    <ValidatorPool
        seated={v}
        total={totalNodes}
        {reachableUnseated}
        {unreachableUnseated}
    />

    <!--
        The flow out of the validators block into its own full-width axis. SVG
        rather than clip-path so the edge can be a cubic: at this height a
        straight bevel reads as a chamfered corner, not a flow. Fill is flat and
        identical to the panel below, so the two are one continuous region whose
        top edge happens to be a curve — a gradient here just muddied it.
    -->
    <div class="relative h-8">
        <svg
            class="absolute inset-0 w-full h-full"
            viewBox="0 0 100 100"
            preserveAspectRatio="none"
            aria-hidden="true"
        >
            <path
                d="M0,0 L{seatedPct},0 C{seatedPct},50 100,50 100,100 L0,100 Z"
                fill="#cba6f7"
                fill-opacity="0.2"
            />
        </svg>
    </div>

    <!-- Inset horizontally so this section reads as sitting inside the flow
         rather than merely below it: the flow's bottom edge is the container,
         its contents are narrower than it. -->
    <div class="bg-mauve/20 rounded-b-md px-3 py-3">
        <ValidatorActivity {v} {live} {quorum} {headroom} {band} {tProbeMs} {tOutMs} />
    </div>

    <!-- Footer rather than header: the profile is the rule everything above is
         computed under, so it reads as the panel's basis, not its headline. -->
    <div class="mt-3 text-center text-xs font-mono">
        {#if profileMode === 'auto'}
            <span class="text-muted">AUTO →</span>
        {/if}
        <span class="cursor-help {profileTone}" title={profileHint}>{profileLabel}</span>
        {#if profileMode !== 'auto'}
            <span class="text-muted">(pinned)</span>
        {/if}
    </div>
</div>
