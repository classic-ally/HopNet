<script lang="ts">
    import SecurityModalBox from './SecurityModalBox.svelte';
    import Tabs from '../../primitives/Tabs.svelte';
    import { SECURITY_OVERVIEW, ALWAYS_PROTECTED_BOX_ITEMS, CONSENSUS_MODES } from './securityConstants';

    export let activeValidators = 1;

    let activeTab: 'setup' | 'crash' | 'anomaly' = 'setup';

    // Set active tab based on current validator count
    $: {
        if (activeValidators <= 2) {
            activeTab = 'setup';
        } else if (activeValidators <= 6) {
            activeTab = 'crash';
        } else {
            activeTab = 'anomaly';
        }
    }

    // Tab configuration for the new Tabs component
    const securityTabs = [
        { key: 'setup', label: 'Setup Mode (1-2 validators)', color: 'red' as const },
        { key: 'crash', label: 'Crash Protection (3-6 validators)', color: 'yellow' as const },
        { key: 'anomaly', label: 'Crash + Anomaly Protection (7+ validators)', color: 'green' as const }
    ];


    // Get current mode configuration from constants
    $: currentMode = CONSENSUS_MODES[activeTab];
    $: protectedItems = currentMode?.protectedItems || [];
    $: notProtectedItems = currentMode?.notProtectedItems || [];
    $: alwaysProtectedBoxItems = ALWAYS_PROTECTED_BOX_ITEMS;
</script>

<!-- Security Properties Page -->
<div class="min-h-screen bg-base p-6">
    <div class="max-w-4xl mx-auto">
        <!-- Header -->
        <div class="mb-6">
            <h1 class="text-2xl font-semibold text-primary">Consensus Security Properties</h1>
        </div>

        <!-- Content -->
        <div class="space-y-6">
                <!-- Overview Boxes Side-by-Side -->
                <div class="grid grid-cols-2 gap-6">
                    <!-- Trade-offs Overview -->
                    <SecurityModalBox {...SECURITY_OVERVIEW} />

                    <!-- Always Protected Section -->
                    <SecurityModalBox
                        title="Always Protected (All Validator Counts)"
                        bgColor="blue/5"
                        borderColor="border-blue/30"
                        textColor="blue"
                        headerIcon="locked"
                        items={alwaysProtectedBoxItems}
                    />
                </div>

                <!-- Tab Navigation -->
                <Tabs tabs={securityTabs} bind:activeTab centered />

                <!-- Tab Content -->
                <div class="p-6">
                    <!-- Mode Description -->
                    {#if currentMode}
                        <div class="mb-6">
                            <div class="bg-{currentMode.bgColor} border {currentMode.borderColor} rounded-lg p-4">
                                <h4 class="font-medium text-{currentMode.color} mb-2 flex items-center gap-2">
                                    <div class="i-carbon-{currentMode.icon}"></div>
                                    {currentMode.name} - {currentMode.subtitle}
                                </h4>
                                <p class="text-text text-sm">
                                    {currentMode.description}
                                </p>
                            </div>
                        </div>
                    {/if}

                    <!-- Two Column Security Layout -->
                    <div class="grid grid-cols-2 gap-6 text-sm">
                        <!-- Protected Against (Green) -->
                        <SecurityModalBox
                            title="Protected Against"
                            bgColor="green/5"
                            borderColor="border-green/30"
                            textColor="green"
                            headerIcon="checkmark"
                            items={protectedItems}
                        />

                        <!-- Not Protected Against -->
                        <SecurityModalBox
                            title="Not Protected Against"
                            bgColor={activeTab === 'anomaly' ? 'green/5' : 'red/5'}
                            borderColor={activeTab === 'anomaly' ? 'border-green/30' : 'border-red/30'}
                            textColor={activeTab === 'crash' ? 'yellow' : activeTab === 'anomaly' ? 'green' : 'red'}
                            headerIcon={activeTab === 'anomaly' ? 'checkmark-filled' : 'warning-filled'}
                            items={notProtectedItems}
                        />
                    </div>

                    <!-- Additional Context -->
                    {#if currentMode?.additionalContext}
                        <div class="mt-6 space-y-4">
                            {#each currentMode.additionalContext as context}
                                <div class="bg-{context.bgColor} {context.borderColor ? `border ${context.borderColor}` : ''} rounded-lg p-4">
                                    <h5 class="font-medium text-{context.textColor} mb-2">{context.title}</h5>
                                    {#if Array.isArray(context.content)}
                                        <ul class="space-y-1 text-text text-sm ml-4">
                                            {#each context.content as item}
                                                <li>• {item}</li>
                                            {/each}
                                        </ul>
                                    {:else}
                                        <p class="text-text text-sm">{context.content}</p>
                                    {/if}
                                </div>
                            {/each}
                        </div>
                    {/if}
                </div>
        </div>
    </div>
</div>