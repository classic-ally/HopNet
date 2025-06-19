<script lang="ts">
    import EntryRow from "../EntryRow.svelte";
    import SetupPane from "../SetupPane.svelte";
    import QrCode from "svelte-qrcode";
    
    let manualInfoExpanded = false;
    
    function toggleManualInfo() {
        manualInfoExpanded = !manualInfoExpanded;
    }
</script>

<SetupPane
    title="Pair with network"
    body="Scan the QR with a mobile device in the network."
>
    {#snippet features()}
        <div class="flex gap-3 items-center">
            <div class="max-w-[100px]">
                <QrCode size=100 value="https://github.com/" />
            </div>
            <div class="flex flex-col gap-3">
                <h2>Waiting for connections...</h2>
                <button
                    class="flex items-center gap-2 transition-colors cursor-pointer bg-blue-950 text-white p-2 border-blue-800 border border-solid rounded-md"
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
                        <p class="text-gray-500 flex-grow">IP</p>
                        <p class="text-white text-xs font-mono">192.168.1.100</p>
                    </div>
                    <div>
                        <p class="text-gray-500">Public key</p>
                        <p class="text-white text-xs whitespace-normal break-all font-mono">ssh-rsa AAAAB3NzaC1yc2EAAAABJQAAAQB/nAmOjTmezNUDKYvEeIRf2YnwM9/uUG1d0BYsc8/tRtx+RGi7N2lUbp728MXGwdnL9od4cItzky/zVdLZE2cycOa18xBK9cOWmcKS0A8FYBxEQWJ/q9YVUgZbFKfYGaGQxsER+A0w/fX8ALuk78ktP31K69LcQgxIsl7rNzxsoOQKJ/CIxOGMMxczYTiEoLvQhapFQMs3FL96didKr/QbrfB1WT6s3838SEaXfgZvLef1YB2xmfhbT9OXFE3FXvh2UPBfN+ffE7iiayQf/2XR+8j4N4bW30DiPtOQLGUrH1y5X/rpNZNlWW2+jGIxqZtgWg7lTy3mXy5x836Sj/6L</p>
                    </div>
                    <p class="text-gray-500">You can use this information in the Nodes page of another desktop device to manually initiate pairing.</p>
                </div>
            {/if}

    {/snippet}

    {#snippet buttons()}
    {/snippet}
</SetupPane>