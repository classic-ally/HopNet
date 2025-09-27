<script lang="ts">
    import SecurityModalBox from './SecurityModalBox.svelte';

    export let show = false;
    export let activeValidators = 1;

    let activeTab = 'current'; // 'setup', 'crash', 'anomaly'

    // Set active tab based on current mode when modal opens
    $: if (show) {
        if (activeValidators <= 2) {
            activeTab = 'setup';
        } else if (activeValidators <= 6) {
            activeTab = 'crash';
        } else {
            activeTab = 'anomaly';
        }
    }

    function closeModal() {
        show = false;
    }

    // Define security items with detailed explanations
    const alwaysProtectedItems = [
        {
            title: "Data tampering & unauthorized access",
            subtitle: "Ed25519 signatures prevent transaction forgery, user impersonation, and unauthorized network access",
            icon: "security"
        },
        {
            title: "Accidental data corruption",
            subtitle: "BLAKE3 hashes ensure file contents cannot be silently corrupted or modified",
            icon: "data-check"
        },
        {
            title: "Authentication verification",
            subtitle: "All network communications are authenticated and verified before processing",
            icon: "user-certification"
        }
    ];

    const crashProtectionItems = [
        {
            title: "Individual node failures",
            subtitles: ["Network continues operating when individual computers crash, lose power, or become unreachable"],
            icon: "warning"
        },
        {
            title: "Maintenance availability",
            subtitles: ["System remains online during planned maintenance, updates, or configuration changes"],
            icon: "tools"
        }
    ];

    const anomalyProtectionItems = [
        {
            title: "Coordinated malicious behavior (up to 1/3)",
            subtitles: ["Prevents validator conspiracies from manipulating consensus decisions or approving invalid operations"],
            icon: "security"
        },
        {
            title: "Selective service denial attempts",
            subtitles: ["Malicious validators cannot block legitimate transactions from specific users or organizations"],
            icon: "block-storage"
        },
        {
            title: "Unfair resource allocation attempts",
            subtitles: ["Prevents biased storage quota assignments or preferential treatment in resource distribution"],
            icon: "scales"
        },
        {
            title: "History rewriting attacks",
            subtitles: ["Cryptographically prevents altering previously committed transactions or file operations"],
            icon: "version"
        }
    ];

    // Dynamic items based on active tab (no longer includes always protected items)
    $: protectedItems = (() => {
        let items = [];
        if (activeTab === 'crash' || activeTab === 'anomaly') {
            items = items.concat(crashProtectionItems);
        }
        if (activeTab === 'anomaly') {
            items = items.concat(anomalyProtectionItems);
        }
        return items;
    })();

    $: notProtectedItems = (() => {
        if (activeTab === 'setup') {
            return [
                {
                    title: "Any node failures (single point of failure)",
                    subtitles: ["Entire network goes offline if the single validator computer fails"],
                    icon: "warning-alt"
                },
                {
                    title: "Network partitions or connectivity issues",
                    subtitles: ["Connection problems can make the entire storage system inaccessible"],
                    icon: "network-3"
                },
                {
                    title: "Coordinated malicious behavior",
                    subtitles: ["Single validator controls all consensus decisions and can manipulate the system"],
                    icon: "user-multiple"
                },
                {
                    title: "Selective service denial",
                    subtitles: ["Validator can arbitrarily block operations from specific users"],
                    icon: "block-storage"
                },
                {
                    title: "Unfair resource allocation",
                    subtitles: ["No protection against biased storage quotas or resource distribution"],
                    icon: "scales"
                },
                {
                    title: "History rewriting attacks",
                    subtitles: ["Single validator can alter or delete previously committed operations"],
                    icon: "version"
                }
            ];
        } else if (activeTab === 'crash') {
            return [
                {
                    title: "Coordinated malicious behavior by majority",
                    subtitles: ["Validator majority conspiracy (50%+1) can issue illegitimate operations"],
                    icon: "user-multiple"
                },
                {
                    title: "Selective service denial by validators",
                    subtitles: ["Validator majority conspiracy can block legitimate transactions"],
                    icon: "block-storage"
                },
                {
                    title: "Unfair resource allocation decisions",
                    subtitles: ["Majority can make biased storage quota or resource distribution decisions"],
                    icon: "scales"
                },
                {
                    title: "History rewriting by majority",
                    subtitles: ["Validator majority can potentially alter previously committed transactions"],
                    icon: "version"
                }
            ];
        } else if (activeTab === 'anomaly') {
            return [
                {
                    title: "All major threats are protected against",
                    subtitles: ["Byzantine fault tolerance provides mathematical guarantees against coordination attacks"],
                    icon: "checkmark-filled"
                }
            ];
        }
        return [];
    })();

    $: alwaysProtectedBoxItems = [
        {
            title: "Files are encrypted individually",
            subtitles: [
                "Each file gets its own encryption key, so compromising one file doesn't affect others",
                "Only people you explicitly grant access can decrypt your files"
            ],
            icon: "document-security"
        },
        {
            title: "File and folder names are encrypted",
            subtitles: [
                "Path names are encrypted so network operators can't see your folder structure",
                "Each user has unique encryption keys, keeping file organization private from other users and administrators",
                "The only theoretical information that could leak is whether you have identically-named items within your own files (but not what those names are), since item name encryption is deterministic",
                "This doesn't affect privacy between users"
            ],
            icon: "folder-off"
        },
        {
            title: "Keys only exist on your device",
            subtitles: [
                "Even if a sophisticated coordinated attack achieves network quorum, they cannot read files without each user's keys",
                "Network administrators cannot bypass this encryption",
                "Digital signatures ensure all actions are verifiably attributed to specific users",
                "Account impersonation is cryptographically prevented"
            ],
            icon: "locked"
        },
        {
            title: "File tampering is always detected",
            subtitles: [
                "Cryptographic signatures make it impossible to modify file contents without detection",
                "Any unauthorized changes are immediately visible"
            ],
            icon: "data-check"
        }
    ];
</script>

<!-- Security Properties Modal -->
{#if show}
    <div
        class="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4"
        on:click={closeModal}
    >
        <div
            class="bg-surface0 border border-overlay1 rounded-lg shadow-xl max-w-4xl w-full max-h-[85vh] overflow-hidden flex flex-col"
            on:click|stopPropagation
        >
            <!-- Header (fixed) -->
            <div class="flex items-center justify-between p-6 border-b border-overlay0 flex-shrink-0">
                <h3 class="text-lg font-semibold text-primary">Consensus Security Properties</h3>
                <button
                    class="text-subtitle hover:text-text transition-colors"
                    on:click={closeModal}
                >
                    <div class="i-carbon-close text-lg"></div>
                </button>
            </div>

            <!-- Scrollable Content -->
            <div class="flex-1 overflow-y-auto">
                <!-- Trade-offs Overview -->
                <div class="p-6">
                    <SecurityModalBox
                        title="Understanding HopNet's Security Design"
                        bgColor="surface1/50"
                        headerBgColor="surface1"
                        borderColor="border-surface1/30"
                        textColor="primary"
                        headerIcon="information"
                        paragraphs={[
                            "Distributed storage systems face fundamental trade-offs between maximum uptime and defending against coordination attacks. HopNet prioritizes system continuity by providing strong file encryption regardless of network size, while consensus protections scale with your infrastructure. This reflects the reality that in private business networks, maintaining operations is typically more critical than defending against theoretical scenarios where compromised installations on multiple trusted systems conspire against proper system function.",
                            "HopNet's adaptive security approach means you get meaningful protection immediately, starting with per-file encryption and growing into full Byzantine fault tolerance as you expand your HopNet deployment. Rather than forcing you to deploy complex infrastructure from day one, the system automatically strengthens its consensus guarantees as your deployment grows.",
                            "This security strengthening happens automatically as you deploy HopNet across more computers in your organization. You don't need to manually configure consensus settings or choose between security modes - HopNet automatically selects which machines participate in consensus decisions and adjusts protection levels based on your actual deployment size. This ensures you always get the strongest security guarantees possible for your current infrastructure, without requiring distributed systems expertise to configure properly. The tabs below detail exactly what protections each deployment size provides, allowing you to understand what your current setup offers and plan future expansions."
                        ]}
                    />
                </div>

            <!-- Always Protected Section -->
            <div class="p-6 border-b border-overlay0">
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
            <div class="flex justify-center border-b border-overlay0">
                <button
                    class="px-4 py-3 text-sm font-medium transition-colors border-b-2 {
                        activeTab === 'setup'
                            ? 'text-red border-red bg-red/5'
                            : 'text-subtitle border-transparent hover:text-text hover:border-overlay1'
                    }"
                    on:click={() => activeTab = 'setup'}
                >
                    Setup Mode (1-2 validators)
                </button>
                <button
                    class="px-4 py-3 text-sm font-medium transition-colors border-b-2 {
                        activeTab === 'crash'
                            ? 'text-yellow border-yellow bg-yellow/5'
                            : 'text-subtitle border-transparent hover:text-text hover:border-overlay1'
                    }"
                    on:click={() => activeTab = 'crash'}
                >
                    Crash Protection (3-6 validators)
                </button>
                <button
                    class="px-4 py-3 text-sm font-medium transition-colors border-b-2 {
                        activeTab === 'anomaly'
                            ? 'text-green border-green bg-green/5'
                            : 'text-subtitle border-transparent hover:text-text hover:border-overlay1'
                    }"
                    on:click={() => activeTab = 'anomaly'}
                >
                    Crash + Anomaly Protection (7+ validators)
                </button>
            </div>

            <!-- Tab Content -->
            <div class="p-6">
                <!-- Mode Description -->
                <div class="mb-6">
                    {#if activeTab === 'setup'}
                        <div class="bg-red/10 border border-red/30 rounded-lg p-4">
                            <h4 class="font-medium text-red mb-2 flex items-center gap-2">
                                <div class="i-carbon-warning-multiple"></div>
                                Setup Mode - Development Only
                            </h4>
                            <p class="text-text text-sm">
                                Minimal protection for development, testing, or initial network setup. Add validators immediately for production.
                            </p>
                        </div>
                    {:else if activeTab === 'crash'}
                        <div class="bg-yellow/10 border border-yellow/30 rounded-lg p-4">
                            <h4 class="font-medium text-yellow mb-2 flex items-center gap-2">
                                <div class="i-carbon-checkmark"></div>
                                Crash Protection - Operational Reliability
                            </h4>
                            <p class="text-text text-sm">
                                Prioritizes keeping your network online and responsive. Suitable for private networks with trusted operators.
                            </p>
                        </div>
                    {:else if activeTab === 'anomaly'}
                        <div class="bg-green/10 border border-green/30 rounded-lg p-4">
                            <h4 class="font-medium text-green mb-2 flex items-center gap-2">
                                <div class="i-carbon-locked"></div>
                                Crash + Anomaly Protection - Maximum Security
                            </h4>
                            <p class="text-text text-sm">
                                Mathematical guarantees against failures and malicious behavior. For multi-organization networks or high-security requirements.
                            </p>
                        </div>
                    {/if}
                </div>

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

                    <!-- Not Protected Against (Red/Yellow/Green) -->
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
                <div class="mt-6 space-y-4">
                    {#if activeTab === 'crash'}
                        <div class="bg-surface1 rounded-lg p-4">
                            <h5 class="font-medium text-primary mb-2">Risk Context for Private Networks:</h5>
                            <p class="text-text text-sm">
                                In networks where you control or trust validator operators, coordination attacks are primarily theoretical.
                                Cryptographic protections prevent transaction forgery - malicious validators can only make biased decisions about legitimate operations.
                            </p>
                        </div>

                        <div class="bg-green/10 border border-green/30 rounded-lg p-4">
                            <h5 class="font-medium text-green mb-2">Consider upgrading when:</h5>
                            <ul class="space-y-1 text-text text-sm ml-4">
                                <li>• Multiple organizations with competing interests operate validators</li>
                                <li>• Regulatory compliance requires Byzantine fault tolerance</li>
                                <li>• High-value transactions where coordination attacks become economically motivated</li>
                                <li>• Mathematical guarantees of fairness are required</li>
                            </ul>
                        </div>
                    {:else if activeTab === 'anomaly'}
                        <div class="bg-surface1 rounded-lg p-4">
                            <h5 class="font-medium text-primary mb-2">Byzantine Fault Tolerance:</h5>
                            <p class="text-text text-sm">
                                Uses the proven 2/3+1 consensus threshold that guarantees safety and liveness even when up to 1/3 of
                                validators are compromised, offline, or acting maliciously. Provides the strongest possible guarantees in distributed systems.
                            </p>
                        </div>

                        <div class="bg-blue/10 border border-blue/30 rounded-lg p-4">
                            <h5 class="font-medium text-blue mb-2">Best For:</h5>
                            <ul class="space-y-1 text-text text-sm ml-4">
                                <li>• Multi-organization deployments with competing interests</li>
                                <li>• Regulatory environments requiring provable security guarantees</li>
                                <li>• High-value data where coordination attacks are economically motivated</li>
                                <li>• Long-term production deployments requiring maximum resilience</li>
                            </ul>
                        </div>
                    {:else if activeTab === 'setup'}
                        <div class="bg-yellow/10 border border-yellow/30 rounded-lg p-4">
                            <h5 class="font-medium text-yellow mb-2">Immediate Action Required:</h5>
                            <p class="text-text text-sm">
                                Add more validators immediately for production use. Even 3 validators provides significant resilience improvements over setup mode.
                            </p>
                        </div>
                    {/if}
                </div>
            </div>
            </div> <!-- End of scrollable content -->
        </div>
    </div>
{/if}