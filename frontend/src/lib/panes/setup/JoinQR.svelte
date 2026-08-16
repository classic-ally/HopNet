<script lang="ts">
    import { onMount } from 'svelte';
    import SetupPane from "../../SetupPane.svelte";
    import QrCode from "svelte-qrcode";
    import { liveSetupApi, TRANSPORT_FAILURE, type SetupApi } from '../../api/setup';

    export let api: SetupApi = liveSetupApi;
    /// Returns to the create-or-join choice, for a misclick.
    export let onBackButton: () => void;

    let pubkey: string = '';
    let loading = true;
    let error = '';

    /**
     * The pubkey alone, not a JSON envelope. This node has no say in its own
     * name — whoever adds it types one on their end, and `POST /api/nodes`
     * takes the name from that request — so a name carried here would be
     * decoration nothing reads. The pubkey is the one value the other side
     * genuinely needs, and encoding it bare makes it directly pasteable into
     * the Nodes page. Matches DeviceKeyModal, which also encodes a raw string.
     */
    $: qrValue = pubkey;

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
    body="Scan the QR with a device already in the network, or enter the key below on its Nodes page."
    onBack={onBackButton}
>
    {#snippet features()}
        {#if loading}
            <div class="text-muted">Loading connection information...</div>
        {:else if error}
            <div class="text-red">{error}</div>
        {:else}
            <div class="flex flex-col gap-4">
                <!--
                  The code is generated well above its display size and scaled
                  down by CSS: qrious emits a JPEG, and rendering a lossy raster
                  at native size leaves visible artefacts on the module edges.
                  The white plate is a wrapper rather than the component's own
                  `padding`, so the quiet zone and the rounded corners come from
                  the same box and cannot clip each other.
                -->
                <div class="bg-white p-4 rounded-xl w-full max-w-[320px] mx-auto">
                    <QrCode
                        value={qrValue}
                        size="512"
                        padding={0}
                        className="block w-full h-auto"
                    />
                </div>

                <div class="flex flex-col gap-1">
                    <p class="text-muted text-sm">Public key</p>
                    <p class="text-xs whitespace-normal break-all font-mono">{pubkey}</p>
                </div>
            </div>
        {/if}
    {/snippet}

</SetupPane>
