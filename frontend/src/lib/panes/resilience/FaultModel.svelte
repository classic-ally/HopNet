<script lang="ts">
    import { headroomStatus, type Band } from './headroomStatus';

    // Every value here comes from GET /consensus/evidence. Nothing in this
    // component recomputes quorum arithmetic: hopnet_common::quorum is the
    // single source of truth for the consensus engine and the storage
    // durability watermark alike, and a third copy in TypeScript is exactly
    // how the two Rust copies drifted before they were collapsed.
    export let v = 7;
    export let live = 5;
    export let faultBudget = 2;
    export let headroom = 0;
    export let band: Band = 'Cliff';

    // The identity this row states:
    //   B(v) - (v - live) = (v - quorum) - (v - live) = live - quorum = headroom
    // so the three figures are an exact equation, not three related numbers.
    // All are reported values; none is derived from the others here, so an
    // equation that fails to balance is a real backend inconsistency.
    $: currentFaults = v - live;

    $: status = headroomStatus(band, headroom);

    function tone(n: number): string {
        if (n <= 0) return 'text-red';
        if (n === 1) return 'text-yellow';
        return 'text-green';
    }
</script>

<div class="flex items-end gap-2">
    <div class="flex-1 text-center">
        <div class="text-xs text-subtitle mb-1" title="B(v) = v − quorum(v)">Fault budget</div>
        <div class="font-mono text-2xl font-semibold leading-none {tone(faultBudget)}">
            {faultBudget}
        </div>
    </div>

    <div class="font-mono text-xl text-muted leading-none pb-0.5 select-none">−</div>

    <div class="flex-1 text-center">
        <div class="text-xs text-subtitle mb-1" title="v − live">Current faults</div>
        <div
            class="font-mono text-2xl font-semibold leading-none {currentFaults > 0
                ? 'text-text'
                : 'text-muted'}"
        >
            {currentFaults}
        </div>
    </div>

    <div class="font-mono text-xl text-muted leading-none pb-0.5 select-none">=</div>

    <div class="flex-1 text-center">
        <div class="text-xs text-subtitle mb-1" title="live − quorum(v)">Headroom</div>
        <!-- Number only: the severity word is the panel's headline and saying
             it twice would spend the loudest slot on a repeat. -->
        <div class="font-mono text-2xl font-semibold leading-none {status.tone}">
            {headroom}
        </div>
    </div>
</div>
