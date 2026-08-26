<script lang="ts">
    import { onMount, onDestroy } from 'svelte';
    import type { ResiliencePaneView } from '../../types';
    import { tokenStore, API_BASE_URL } from '../../stores';
    import StoragePanel from './StoragePanel.svelte';
    import ConsensusPanel from './ConsensusPanel.svelte';
    import Toolbar from '../../primitives/Toolbar.svelte';
    import type { ToolbarItem } from '../../primitives/Toolbar.svelte';
    import PaneHeader from '../../primitives/PaneHeader.svelte';

    export let onToggleSidebar: () => void = () => {};

    let view: ResiliencePaneView | null = null;
    let loading = false;
    let error = '';
    let timer: ReturnType<typeof setInterval> | undefined;

    // One request for both panels. They sit side by side and describe the same
    // mesh, so fetching them separately would let them disagree about a node
    // across two round trips.
    async function load() {
        // One outstanding request at a time. The node-side view is a full-table
        // pass over fragment_hashes, which on a large node takes longer than the
        // poll interval — without this the timer stacks scans until they exhaust
        // the node's DB pool and it sheds its whole /api surface (issue #68).
        if (loading) return;

        try {
            loading = true;

            const token = $tokenStore;
            if (!token) {
                error = 'No authentication token found';
                return;
            }

            const response = await fetch(`${API_BASE_URL}/views/network-resilience`, {
                method: 'GET',
                headers: {
                    Authorization: `Bearer ${token}`,
                    'Content-Type': 'application/json'
                }
            });

            if (response.ok) {
                view = await response.json();
                error = '';
            } else {
                error = `Failed to load: ${response.status} ${response.statusText}`;
            }
        } catch (e) {
            error = `Network error: ${e instanceof Error ? e.message : String(e)}`;
        } finally {
            loading = false;
        }
    }

    onMount(() => {
        load();
        // Headroom and band move on failures rather than on a timer, so poll
        // faster than the probe cadence but nowhere near it.
        timer = setInterval(load, 5000);
    });

    onDestroy(() => clearInterval(timer));

    // The wire format is snake_case; the components take camelCase props. This
    // mapping is the only place the two conventions meet — the components stay
    // unaware of the transport.
    $: consensusProps = view && {
        v: view.consensus.v,
        live: view.consensus.live,
        quorum: view.consensus.quorum,
        headroom: view.consensus.headroom,
        faultBudget: view.consensus.fault_budget,
        profileMode: view.consensus.profile_mode as 'auto' | 'bft' | 'majority',
        profile: view.consensus.profile as 'bft' | 'majority',
        vBft: view.consensus.v_bft,
        band: view.consensus.band as 'Lazy' | 'Fast' | 'Cliff',
        tProbeMs: view.consensus.t_probe_ms,
        tOutMs: view.consensus.t_out_ms,
        totalNodes: view.consensus.total_nodes,
        reachableUnseated: view.consensus.reachable_unseated,
        unreachableUnseated: view.consensus.unreachable_unseated,
        versionSkew: view.consensus.version_skew,
        strandedPeers: view.consensus.stranded_peers,
        localVersion: view.consensus.local_version
    };

    $: storageProps = view && {
        curve: view.storage.curve,
        observedLevels: view.storage.observed_levels.map(l => ({
            tolerance: l.tolerance,
            rawGb: l.raw_gb
        })),
        unrecoverableGb: view.storage.unrecoverable_gb,
        unknownGb: view.storage.unknown_gb,
        unreachableMembers: view.storage.unreachable_members,
        unplacedBuckets: view.storage.unplaced_buckets.map(b => ({
            label: b.label,
            gb: b.gb,
            severity: b.severity
        }))
    };

    $: rightElements = [
        {
            type: 'action' as const,
            icon: 'i-carbon-renew',
            text: 'Refresh',
            onClick: load,
            compactStage: 2,
            tooltip: 'Reload now'
        }
    ] satisfies ToolbarItem[];
</script>

<Toolbar leftElements={[]} centerElements={[]} {rightElements} {onToggleSidebar} />

<PaneHeader title="Network Resilience" subtitle="Observed durability and consensus state" />

{#if error}
    <div class="text-red bg-surface0 border border-red rounded p-3 my-3">{error}</div>
{/if}

{#if consensusProps && storageProps}
    <!-- The panels are Cards; they bring their own surface and border. -->
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <StoragePanel {...storageProps} />
        <ConsensusPanel {...consensusProps} />
    </div>
{:else if loading}
    <div class="text-subtitle text-sm py-8 text-center">Loading…</div>
{/if}
