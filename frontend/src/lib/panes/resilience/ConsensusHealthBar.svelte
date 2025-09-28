<script lang="ts">
    import SecurityPropertiesPage from './SecurityPropertiesPage.svelte';
    import Button from '../../Button.svelte';

    export let activeValidators = 1;
    export let totalValidators = 1;

    // Proximity thresholds (these would come from backend in real implementation)
    export let unavailableValidators = 0;
    export let voteOutThreshold = 2; // Number of validators that trigger auto vote-out
    export let totalNetworkNodes = 10; // Total nodes in the entire network (storage + validators)

    // Calculate consensus mode and fault tolerance based on validator count
    $: consensusMode = getConsensusMode(activeValidators);
    $: faultTolerance = getFaultTolerance(activeValidators);

    // Calculate threshold positions based on participation rate
    $: thresholdData = calculateThresholds(activeValidators, unavailableValidators, voteOutThreshold);

    // Validator Activity standardized calculations
    $: validatorActivityMin = Math.max(0, thresholdData.minRequired - 1);
    $: validatorActivityPosition = calculatePosition(thresholdData.availableValidators, validatorActivityMin, thresholdData.totalValidators);
    $: validatorActivityThresholds = thresholdData.voteOutViable ? [thresholdData.minRequired, thresholdData.totalValidators - voteOutThreshold] : [thresholdData.minRequired];
    $: validatorActivityZones = calculateZoneSizes(validatorActivityThresholds, validatorActivityMin, thresholdData.totalValidators);

    // Calculate decision participation (percentage of network that are validators)
    $: decisionParticipation = (totalValidators / totalNetworkNodes) * 100;
    $: decisionParticipationPosition = calculatePosition(decisionParticipation, 0, 100);
    $: decisionParticipationZones = calculateZoneSizes([15, 40], 0, 100);

    // Modal state for security info
    let showSecurityModal = false;

    // Unified scaling utilities
    function calculatePosition(value: number, min: number, max: number, capAt = 98): number {
        return Math.min(((value - min) / (max - min)) * 100, capAt);
    }

    function calculateZoneSizes(thresholds: number[], min: number, max: number): number[] {
        // Convert threshold values to percentages
        const thresholdPercentages = thresholds.map(t => ((t - min) / (max - min)) * 100);

        // Calculate zone widths between thresholds
        const zones = [];
        for (let i = 0; i < thresholdPercentages.length + 1; i++) {
            const start = i === 0 ? 0 : thresholdPercentages[i - 1];
            const end = i === thresholdPercentages.length ? 100 : thresholdPercentages[i];
            zones.push(end - start);
        }
        return zones;
    }


    function getConsensusMode(validators: number) {
        if (validators <= 2) {
            return { mode: 'Setup Mode', color: 'red', description: 'Getting network started' };
        } else if (validators <= 6) {
            return { mode: 'Crash Protection', color: 'yellow', description: 'Handles computer failures' };
        } else {
            return { mode: 'Crash + Anomaly Protection', color: 'green', description: 'Handles failures and anomalies' };
        }
    }

    function getFaultTolerance(validators: number) {
        if (validators <= 2) {
            return { crashes: 0, byzantine: 0 };
        } else if (validators <= 6) {
            // Relaxed mode: can tolerate floor(n/2) crashes but 0 Byzantine
            return { crashes: Math.floor(validators / 2), byzantine: 0 };
        } else {
            // Full BFT: can tolerate f Byzantine where n = 3f+1
            const f = Math.floor((validators - 1) / 3);
            return { crashes: f, byzantine: f };
        }
    }

    function calculateThresholds(active: number, unavailable: number, voteOut: number) {
        const available = active - unavailable;

        // BFT failure point calculation
        let minRequired;
        if (active <= 6) {
            // Relaxed mode: need simple majority
            minRequired = Math.floor(active / 2) + 1;
        } else {
            // Full BFT: need 2/3 majority
            minRequired = Math.ceil((2 * active) / 3);
        }

        // Check if vote-out can actually occur (must be above BFT minimum)
        const voteOutViable = (active - voteOut) >= minRequired;

        return {
            // Core validator counts
            availableValidators: available,
            totalValidators: active,
            minRequired: minRequired,

            // Status calculations
            buffer: available - minRequired,
            voteOutBuffer: unavailable - voteOut,
            voteOutViable: voteOutViable,

            // Status flags
            isVoteOutTriggered: unavailable >= voteOut && voteOutViable,
            isBFTFailed: available < minRequired,
        };
    }

    // Zones for the ladder visualization
    const zones = [
        { min: 1, max: 2, label: 'Setup', color: 'bg-red/20 border-red', textColor: 'text-red' },
        { min: 3, max: 6, label: 'Crash', color: 'bg-yellow/20 border-yellow', textColor: 'text-yellow' },
        { min: 7, max: 15, label: 'Crash + Anomaly', color: 'bg-green/20 border-green', textColor: 'text-green' },
    ];

    // Calculate position and zone sizes for Protection Level
    $: protectionLevelPosition = calculatePosition(activeValidators, 1, 9);
    $: protectionLevelZones = calculateZoneSizes([3, 7], 1, 9);

    // Threshold markers for Protection Level
    $: protectionThresholds = [
        { value: 3, position: calculatePosition(3, 1, 9), label: '3', tooltip: 'Minimum for Crash Protection', color: 'bg-red' },
        { value: 7, position: calculatePosition(7, 1, 9), label: '7', tooltip: 'Minimum for Crash + Anomaly Protection', color: 'bg-yellow' },
    ];
</script>

<div class="p-4 bg-surface0">
    <div class="mb-3">
        <h4 class="text-lg font-semibold text-primary">Decision Resilience</h4>
    </div>

    <!-- Top Ladder: Protection Level -->
    <div class="bg-base rounded-md p-3 mb-3">
        <div class="flex items-center justify-between mb-2">
            <div class="text-xs text-subtitle font-medium">Protection Level</div>
            <div class="flex items-center gap-1 text-xs {consensusMode.color === 'red' ? 'text-red' : consensusMode.color === 'yellow' ? 'text-yellow' : 'text-green'}">
                {#if activeValidators >= 9}
                    <div class="i-carbon-locked text-green"></div>
                {:else}
                    {consensusMode.mode}
                {/if}
            </div>
        </div>

        {#if activeValidators >= 9}
            <!-- Robust mode: Full-width green bar -->
            <div class="relative h-8 mb-2">
                <div class="absolute inset-0 bg-green/20 border border-green rounded-md flex items-center justify-center">
                    <span class="text-sm text-green font-medium">Robust Crash + Anomaly Protection</span>
                </div>
            </div>
        {:else}
            <!-- Normal mode: Ladder with position indicator -->
            <div class="relative h-8 mb-2">
            <!-- Zone backgrounds -->
            <div class="absolute inset-0 flex">
                <div class="{zones[0].color} border-1 border-r-0 rounded-l-md" style="width: {protectionLevelZones[0]}%"></div>
                <div class="{zones[1].color} border-1 border-r-0" style="width: {protectionLevelZones[1]}%"></div>
                <div class="{zones[2].color} border-1 rounded-r-md" style="width: {protectionLevelZones[2]}%"></div>
            </div>

            <!-- Threshold markers -->
            {#each protectionThresholds as threshold}
                <div class="absolute top-0 bottom-0 w-0.5 {threshold.color}" style="left: {threshold.position}%">
                    <div class="absolute -bottom-4 left-1/2 -translate-x-1/2 text-xs text-subtitle" title={threshold.tooltip}>
                        {threshold.label}
                    </div>
                </div>
            {/each}

            <!-- Current position indicator -->
            <div class="absolute top-1/2 -translate-y-1/2 transition-all duration-300" style="left: {protectionLevelPosition}%">
                <div class="relative">
                    <div class="w-3.5 h-3.5 {activeValidators <= 2 ? 'bg-red' : activeValidators <= 6 ? 'bg-yellow' : 'bg-green'} rounded-full shadow-lg"></div>
                    <div class="absolute -top-6 left-1/2 -translate-x-1/2 text-xs font-mono font-bold {activeValidators <= 2 ? 'text-red' : activeValidators <= 6 ? 'text-yellow' : 'text-green'} whitespace-nowrap">
                        {activeValidators}
                    </div>
                </div>
            </div>

            <!-- Zone labels -->
            <div class="absolute inset-0 flex items-center pointer-events-none">
                <div class="text-center" style="width: {protectionLevelZones[0]}%">
                    <span class="text-xs {zones[0].textColor}">Setup</span>
                </div>
                <div class="text-center" style="width: {protectionLevelZones[1]}%">
                    <span class="text-xs {zones[1].textColor}">Crash</span>
                </div>
                <div class="text-center" style="width: {protectionLevelZones[2]}%">
                    <span class="text-xs {zones[2].textColor}">Crash + Anomaly</span>
                </div>
            </div>
            </div>
        {/if}

        <!-- Security Properties Summary -->
        <div class="mt-3 pt-3 border-t border-overlay0">
            <div class="flex items-center justify-between mb-2">
                <div class="text-xs text-subtitle font-medium">Security Properties</div>
                <Button
                    icon="i-carbon-information"
                    text="Details"
                    onClick={() => showSecurityModal = true}
                    className="text-xs text-mauve hover:text-mauve/80"
                />
            </div>

            <div class="text-xs text-text space-y-1">
                {#if activeValidators <= 2}
                    <p><span class="text-green">✓</span> Protected against data tampering and unauthorized access</p>
                    <p><span class="text-red">⚠</span> No protection against node failures or coordinated malicious behavior</p>
                {:else if activeValidators <= 6}
                    <p><span class="text-green">✓</span> Protected against data tampering, unauthorized access, and node failures</p>
                    <p><span class="text-yellow">⚠</span> Limited protection against coordinated malicious behavior by validator majority</p>
                {:else}
                    <p><span class="text-green">✓</span> Protected against data tampering, unauthorized access, and node failures</p>
                    <p><span class="text-green">✓</span> Strong protection against coordinated malicious behavior</p>
                {/if}
            </div>
        </div>
    </div>

    <!-- Second Ladder: Validator Activity -->
    <div class="bg-base rounded-md p-3 mb-3">
        <div class="flex items-center justify-between mb-2">
            <div class="text-xs text-subtitle font-medium">Validator Activity</div>
            <div class="flex items-center gap-1 text-xs">
                {#if activeValidators <= 2}
                    <div class="i-carbon-warning-multiple text-red"></div>
                    <span class="text-red">No Backup</span>
                {:else if thresholdData.isBFTFailed}
                    <div class="i-carbon-error text-red"></div>
                    <span class="text-red">Network Offline</span>
                {:else if thresholdData.isVoteOutTriggered}
                    <div class="i-carbon-warning text-yellow"></div>
                    <span class="text-yellow">Awaiting Removal</span>
                {:else}
                    <div class="i-carbon-checkmark text-green"></div>
                    <span class="text-green">Healthy</span>
                {/if}
            </div>
        </div>

        {#if activeValidators <= 2}
            <!-- Setup mode: Simple red bar with message -->
            <div class="relative h-8 mb-2 mt-6">
                <div class="absolute inset-0 bg-red/20 border border-red rounded-md flex items-center justify-center">
                    <span class="text-xs text-red font-medium">No redundancy</span>
                </div>
            </div>
        {:else}
            <!-- Normal mode: Horizontal threshold ladder -->
        <div class="relative h-8 mb-2 mt-6">
            <!-- Background zones from left (worst) to right (best) -->
            <div class="absolute inset-0 rounded-md overflow-hidden flex">
                <!-- Red zone (below BFT minimum) -->
                <div
                    class="bg-red/20 border-r border-red"
                    style="width: {validatorActivityZones[0]}%"
                ></div>
                <!-- Yellow zone (between BFT and vote-out, if applicable) -->
                {#if thresholdData.voteOutViable}
                <div
                    class="bg-yellow/20 border-r border-yellow"
                    style="width: {validatorActivityZones[1]}%"
                ></div>
                {/if}
                <!-- Green zone (healthy) -->
                <div
                    class="bg-green/20"
                    style="width: {thresholdData.voteOutViable ? validatorActivityZones[2] : validatorActivityZones[1]}%"
                ></div>
            </div>

            <!-- BFT failure threshold marker -->
            <div
                class="absolute top-0 bottom-0 w-0.5 bg-red"
                style="left: {calculatePosition(thresholdData.minRequired, validatorActivityMin, thresholdData.totalValidators)}%"
            >
                <div class="absolute -top-5 left-1/2 -translate-x-1/2 text-xs text-red whitespace-nowrap">
                    System Min
                </div>
                <div class="absolute -bottom-4 left-1/2 -translate-x-1/2 text-xs font-mono text-red">
                    {thresholdData.minRequired}
                </div>
            </div>

            <!-- Vote-out threshold marker (only if viable) -->
            {#if thresholdData.voteOutViable}
                <div
                    class="absolute top-0 bottom-0 w-0.5 bg-yellow"
                    style="left: {calculatePosition(thresholdData.totalValidators - voteOutThreshold, validatorActivityMin, thresholdData.totalValidators)}%"
                >
                    <div class="absolute -top-5 left-1/2 -translate-x-1/2 text-xs text-yellow whitespace-nowrap">
                        Auto-removal
                    </div>
                    <div class="absolute -bottom-4 left-1/2 -translate-x-1/2 text-xs font-mono text-yellow">
                        {thresholdData.totalValidators - voteOutThreshold}
                    </div>
                </div>
            {/if}

            <!-- Current participation indicator -->
            <div
                class="absolute top-1/2 -translate-y-1/2 transition-all duration-300"
                style="left: {validatorActivityPosition}%"
            >
                <div class="relative">
                    <div class="w-3.5 h-3.5 {thresholdData.isBFTFailed ? 'bg-red' : thresholdData.isVoteOutTriggered ? 'bg-yellow' : 'bg-green'} rounded-full shadow-lg"></div>
                    <div class="absolute -top-4 left-1/2 -translate-x-1/2 text-xs font-mono font-bold {thresholdData.isBFTFailed ? 'text-red' : thresholdData.isVoteOutTriggered ? 'text-yellow' : 'text-green'} whitespace-nowrap">
                        {thresholdData.availableValidators}
                    </div>
                </div>
            </div>

            <!-- Scale labels -->
            <div class="absolute -bottom-4 left-0 text-xs font-mono text-muted">{validatorActivityMin}</div>
            <div class="absolute -bottom-4 right-0 text-xs font-mono text-muted">{thresholdData.totalValidators}</div>
        </div>
        {/if}

    </div>

    <!-- Third Ladder: Decision Participation -->
    <div class="bg-base rounded-md p-3 mb-3">
        <div class="flex items-center justify-between mb-2">
            <div class="text-xs text-subtitle font-medium">Decision Participation</div>
            <div class="flex items-center gap-1 text-xs">
                {#if decisionParticipation < 15}
                    <div class="i-carbon-warning text-red"></div>
                    <span class="text-red">Highly Concentrated</span>
                {:else if decisionParticipation < 40}
                    <div class="i-carbon-warning text-yellow"></div>
                    <span class="text-yellow">Moderate</span>
                {:else}
                    <div class="i-carbon-checkmark text-green"></div>
                    <span class="text-green">Broad</span>
                {/if}
            </div>
        </div>

        <!-- Horizontal participation ladder -->
        <div class="relative h-8 mb-2 mt-6">
            <!-- Background gradient showing participation levels -->
            <div class="absolute inset-0 rounded-md overflow-hidden">
                <div class="absolute inset-0 bg-gradient-to-r from-red/10 via-yellow/10 to-green/10"></div>
            </div>

            <!-- Participation zones -->
            <div class="absolute inset-0 rounded-md overflow-hidden flex">
                <!-- Low participation (0-15%) -->
                <div class="bg-red/20 border-r border-red" style="width: {decisionParticipationZones[0]}%"></div>
                <!-- Medium participation (15-40%) -->
                <div class="bg-yellow/20 border-r border-yellow" style="width: {decisionParticipationZones[1]}%"></div>
                <!-- High participation (40%+) -->
                <div class="bg-green/20" style="width: {decisionParticipationZones[2]}%"></div>
            </div>

            <!-- Current participation indicator -->
            <div
                class="absolute top-1/2 -translate-y-1/2 transition-all duration-300"
                style="left: {decisionParticipationPosition}%"
            >
                <div class="relative">
                    <div class="w-3.5 h-3.5 {decisionParticipation < 15 ? 'bg-red' : decisionParticipation < 40 ? 'bg-yellow' : 'bg-green'} rounded-full shadow-lg"></div>
                    <div class="absolute -top-4 left-1/2 -translate-x-1/2 text-xs font-mono font-bold {decisionParticipation < 15 ? 'text-red' : decisionParticipation < 40 ? 'text-yellow' : 'text-green'} whitespace-nowrap">
                        {Math.round(decisionParticipation)}%
                    </div>
                </div>
            </div>

            <!-- Zone markers -->
            <div class="absolute top-0 bottom-0 w-0.5 bg-red" style="left: {calculatePosition(15, 0, 100)}%">
                <div class="absolute -bottom-4 left-1/2 -translate-x-1/2 text-xs text-muted">15%</div>
            </div>
            <div class="absolute top-0 bottom-0 w-0.5 bg-yellow" style="left: {calculatePosition(40, 0, 100)}%">
                <div class="absolute -bottom-4 left-1/2 -translate-x-1/2 text-xs text-muted">40%</div>
            </div>

            <!-- Scale labels -->
            <div class="absolute -bottom-4 left-0 text-xs font-mono text-muted">0%</div>
            <div class="absolute -bottom-4 right-0 text-xs font-mono text-muted">100%</div>
        </div>

    </div>

    <!-- Fault Tolerance Details -->
    <div class="grid grid-cols-2 gap-3 text-sm">
        <div class="bg-base rounded-md p-2">
            <div class="text-subtitle text-xs mb-1">Crash Tolerance</div>
            <div class="font-mono font-semibold {faultTolerance.crashes > 0 ? 'text-primary' : 'text-muted'}">
                {faultTolerance.crashes} {faultTolerance.crashes === 1 ? 'node' : 'nodes'}
            </div>
        </div>
        <div class="bg-base rounded-md p-2">
            <div class="text-subtitle text-xs mb-1">Anomaly Tolerance</div>
            <div class="font-mono font-semibold {faultTolerance.byzantine > 0 ? 'text-primary' : 'text-muted'}">
                {faultTolerance.byzantine} {faultTolerance.byzantine === 1 ? 'node' : 'nodes'}
            </div>
        </div>
    </div>

    <!-- Mode Description -->
    <div class="mt-3 text-xs text-subtitle">
        {#if consensusMode.mode === 'Setup Mode'}
            <div class="text-red">⚠️ Single point of failure - Add more computers for protection</div>
        {:else if consensusMode.mode === 'Crash Protection'}
            <div>Protects against computer failures but vulnerable to data anomalies</div>
        {:else}
            <div class="text-green">✓ Full protection against failures and malicious anomalies</div>
        {/if}
    </div>
</div>

<!-- Security Properties Page -->
{#if showSecurityModal}
    <div class="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
        <div class="bg-surface0 border border-overlay1 rounded-lg shadow-xl max-w-4xl w-full max-h-[85vh] overflow-hidden relative">
            <button
                class="absolute top-4 right-4 text-subtitle hover:text-text transition-colors z-10"
                on:click={() => showSecurityModal = false}
            >
                <div class="i-carbon-close text-lg"></div>
            </button>
            <SecurityPropertiesPage {activeValidators} />
        </div>
    </div>
{/if}