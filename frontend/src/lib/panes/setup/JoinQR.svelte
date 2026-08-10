<script lang="ts">
    import { onMount } from 'svelte';
    import SetupPane from "../../SetupPane.svelte";
    import QrCode from "svelte-qrcode";
    import { liveSetupApi, TRANSPORT_FAILURE, type SetupApi } from '../../api/setup';

    // Props from previous setup page
    export let name: string;
    export let api: SetupApi = liveSetupApi;

    let manualInfoExpanded = false;
    let pubkey: string = '';
    let loading = true;
    let error = '';

    // Connection info object for QR code
    $: connectionInfo = {
        name,
        pubkey
    };

    // JSON string for QR code
    $: qrValue = JSON.stringify(connectionInfo);

    function toggleManualInfo() {
        manualInfoExpanded = !manualInfoExpanded;
    }

    async function fetchPubkey() {
        const result = await api.fetchPubkey();
        if (result.ok) {
            pubkey = result.pubkey;
        } else if (result.status === TRANSPORT_FAILURE) {
            error = `Network error: ${result.detail ?? 'Unknown error'}`;
        } else {
            error = 'Failed to fetch public key';
        }
        loading = false;
    }

    onMount(() => {
        fetchPubkey();
    });
</script>

<SetupPane
    title="Pair with network"
    body="Scan the QR with a mobile device in the network."
>
    {#snippet features()}
        {#if loading}
            <div class="text-muted">Loading connection information...</div>
        {:else if error}
            <div class="text-red">{error}</div>
        {:else}
            <div class="flex gap-3 items-center">
                <div class="max-w-[100px]">
                    <QrCode size=100 value={qrValue} />
                </div>
                <div class="flex flex-col gap-3">
                    <h2>Waiting for connections...</h2>
                    <button
                        class="flex items-center gap-2 transition-colors cursor-pointer bg-surface0 text-primary p-2 border-overlay1 border border-solid rounded-md hover:bg-surface1 hover:border-mauve"
                        on:click={toggleManualInfo}
                    >
                        <span class="text-sm">
                            {manualInfoExpanded ? '▼' : '▶'}
                        </span>
                        <p>Show manual connection info</p>
                    </button>
                </div>
            </div>
            {#if manualInfoExpanded}
                <div class="space-y-2 animate-in slide-in-from-top-2 duration-200 gap-2 flex flex-col">
                    <div class="flex gap-2 items-center">
                        <p class="text-muted flex-grow">Name</p>
                        <p class="text-white text-xs font-mono">{name}</p>
                    </div>
                    <div>
                        <p class="text-muted">Public Key</p>
                        <p class="text-white text-xs whitespace-normal break-all font-mono">{pubkey}</p>
                    </div>
                    <p class="text-muted">You can use this information in the Nodes page of another desktop device to manually initiate pairing.</p>
                </div>
            {/if}
        {/if}

    {/snippet}

</SetupPane>
