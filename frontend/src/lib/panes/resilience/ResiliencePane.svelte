<script lang="ts">
    import { onMount } from 'svelte';
    import type { NodeStorageBaseline, FaultToleranceCurvePoint, NodeSource } from '../../types';
    import { tokenStore, API_BASE_URL } from '../../stores';
    import FaultToleranceChart from './FaultToleranceChart.svelte';

    // System nodes and working copy for modifications
    let systemNodes: NodeStorageBaseline[] = []; // Immutable system data
    let workingNodes: NodeStorageBaseline[] = []; // User's working copy

    // Fault tolerance curve data
    let curveData: FaultToleranceCurvePoint[] = [];
    let loading = false;
    let error = '';

    // Load system nodes from backend
    async function loadSystemNodes() {
        try {
            loading = true;
            error = '';

            const token = $tokenStore;
            if (!token) {
                error = 'No authentication token found';
                return;
            }

            const response = await fetch(`${API_BASE_URL}/admin/system-nodes-baseline`, {
                method: 'GET',
                headers: {
                    'Authorization': `Bearer ${token}`,
                    'Content-Type': 'application/json',
                },
            });

            if (response.ok) {
                systemNodes = await response.json();
                // Create working copy with system data
                workingNodes = systemNodes.map(node => ({...node}));

                // Auto-analyze the network with real data
                await analyzeNetwork();
            } else {
                error = `Failed to load system nodes: ${response.status} ${response.statusText}`;
            }
        } catch (e) {
            error = `Network error: ${e.message}`;
        } finally {
            loading = false;
        }
    }

    // Add a new hypothetical node
    export function addHypotheticalNode() {
        const newId = Math.max(...workingNodes.map(n => n.node_id), 0) + 1;
        workingNodes = [...workingNodes, {
            node_id: newId,
            display_name: `Node ${newId}`,
            storage_total_gb: 1000,
            baseline_storage_gb: 100,
            source: 'Added' as NodeSource,
            original_values: undefined
        }];
    }

    // Reset to system data
    export function resetToSystemData() {
        workingNodes = systemNodes.map(node => ({...node}));
        curveData = [];
    }

    // Load system data on component mount
    onMount(async () => {
        await loadSystemNodes();
    });

    // Analyze the network and generate fault tolerance curve
    export async function analyzeNetwork() {
        if (workingNodes.length === 0) {
            error = 'No nodes available for analysis';
            return;
        }

        try {
            loading = true;
            error = '';

            const token = $tokenStore;
            if (!token) {
                error = 'No authentication token found';
                return;
            }

            const response = await fetch(`${API_BASE_URL}/admin/hypothetical-fault-tolerance`, {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${token}`,
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(workingNodes),
            });

            if (response.ok) {
                curveData = await response.json();
            } else {
                error = `Failed to analyze network: ${response.status} ${response.statusText}`;
            }
        } catch (e) {
            error = `Network error: ${e.message}`;
        } finally {
            loading = false;
        }
    }

    // Update a node's configuration
    function updateNode(index: number, field: 'storage_total_gb' | 'baseline_storage_gb', value: number) {
        const node = workingNodes[index];

        // Track modifications from system data
        if (node.source === 'System') {
            node.source = 'Modified';
            node.original_values = {
                storage_total_gb: systemNodes.find(n => n.node_id === node.node_id)?.storage_total_gb || node.storage_total_gb,
                baseline_storage_gb: systemNodes.find(n => n.node_id === node.node_id)?.baseline_storage_gb || node.baseline_storage_gb
            };
        }

        node[field] = value;
        workingNodes = workingNodes; // Trigger reactivity
    }

    // Remove a specific node
    function removeNode(index: number) {
        workingNodes = workingNodes.filter((_, i) => i !== index);
    }

    // Reset a specific node to original values
    function resetNode(index: number) {
        const node = workingNodes[index];
        if (node.source === 'Modified' && node.original_values) {
            node.storage_total_gb = node.original_values.storage_total_gb;
            node.baseline_storage_gb = node.original_values.baseline_storage_gb;
            node.source = 'System';
            node.original_values = undefined;
            workingNodes = workingNodes; // Trigger reactivity
        }
    }
</script>

<div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
    <!-- Left Panel: Node Configuration -->
    <div class="border-solid border-1 rounded-lg p-4 border-overlay1">
        <h4 class="text-lg font-semibold text-primary mb-3">Network Configuration</h4>

        <!-- Node List -->
        <div class="space-y-3 mb-4">
            {#each workingNodes as node, index}
                <div class="bg-surface0 rounded-md p-3">
                    <div class="flex items-center justify-between mb-2">
                        <div class="flex items-center gap-2">
                            <!-- Source indicator -->
                            {#if node.source === 'System'}
                                <div class="i-carbon-server text-blue text-sm" title="System node"></div>
                            {:else if node.source === 'Modified'}
                                <div class="i-carbon-edit text-orange text-sm" title="Modified from system"></div>
                            {:else if node.source === 'Added'}
                                <div class="i-carbon-add text-green text-sm" title="Hypothetical node"></div>
                            {/if}
                            <span class="text-primary font-medium">{node.display_name}</span>
                        </div>
                        <div class="flex items-center gap-1">
                            <!-- Reset button for modified nodes -->
                            {#if node.source === 'Modified'}
                                <button
                                    class="text-subtitle hover:bg-surface1 rounded p-1"
                                    onclick={() => resetNode(index)}
                                    title="Reset to original values"
                                >
                                    <div class="i-carbon-reset text-sm"></div>
                                </button>
                            {/if}
                            <!-- Remove button (only for Added nodes, or allow removal of system nodes) -->
                            <button
                                class="text-red hover:bg-surface1 rounded p-1"
                                onclick={() => removeNode(index)}
                                title={node.source === 'Added' ? 'Remove hypothetical node' : 'Remove from analysis'}
                            >
                                <div class="i-carbon-close text-sm"></div>
                            </button>
                        </div>
                    </div>

                    <div class="grid grid-cols-2 gap-2">
                        <div>
                            <label class="text-sm text-subtitle">Total Storage (GB)</label>
                            <input
                                type="number"
                                min="1"
                                step="100"
                                class="w-full bg-base border border-overlay0 rounded px-2 py-1 text-primary"
                                bind:value={node.storage_total_gb}
                                oninput={(e) => updateNode(index, 'storage_total_gb', Number(e.target.value))}
                            />
                        </div>
                        <div>
                            <label class="text-sm text-subtitle">Baseline Usage (GB)</label>
                            <input
                                type="number"
                                min="0"
                                step="10"
                                class="w-full bg-base border border-overlay0 rounded px-2 py-1 text-primary"
                                bind:value={node.baseline_storage_gb}
                                oninput={(e) => updateNode(index, 'baseline_storage_gb', Number(e.target.value))}
                            />
                        </div>
                    </div>
                </div>
            {/each}

            {#if workingNodes.length === 0}
                <div class="text-muted text-center py-8">
                    <div class="i-carbon-server text-2xl mb-2"></div>
                    <p>No nodes available</p>
                    <p class="text-sm">{loading ? 'Loading system data...' : 'Unable to load system nodes'}</p>
                </div>
            {/if}
        </div>

        <!-- Action Buttons -->
        <div class="flex gap-2 mb-4">
            <button
                class="flex items-center gap-2 bg-green text-base px-3 py-2 rounded hover:bg-green/80"
                onclick={addHypotheticalNode}
            >
                <div class="i-carbon-add text-sm"></div>
                Add Hypothetical Node
            </button>
            <button
                class="flex items-center gap-2 bg-blue text-base px-3 py-2 rounded hover:bg-blue/80"
                onclick={analyzeNetwork}
                disabled={loading}
            >
                <div class="i-carbon-chart-line text-sm"></div>
                Re-analyze
            </button>
            <button
                class="flex items-center gap-2 bg-surface1 text-text px-3 py-2 rounded hover:bg-surface2"
                onclick={resetToSystemData}
                disabled={loading}
            >
                <div class="i-carbon-reset text-sm"></div>
                Reset to System
            </button>
        </div>

        <!-- Analysis Status -->
        {#if error}
            <div class="text-red bg-surface0 border border-red rounded p-3 mb-4">
                {error}
            </div>
        {/if}

        {#if loading}
            <div class="text-blue bg-surface0 border border-blue rounded p-3 mb-4">
                Analyzing network...
            </div>
        {/if}
    </div>

    <!-- Right Panel: Fault Tolerance Curve -->
    <div class="border-solid border-1 rounded-lg p-4 border-overlay1">
        <h4 class="text-lg font-semibold text-primary mb-3">Fault Tolerance Analysis</h4>

        <!-- Fault Tolerance Chart -->
        <FaultToleranceChart data={curveData} />
    </div>
</div>