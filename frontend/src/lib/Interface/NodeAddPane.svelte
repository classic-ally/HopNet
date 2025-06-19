<script lang="ts">
    import Button from "../Button.svelte";
    import EntryRow from "../EntryRow.svelte";
    import SetupPane from "../SetupPane.svelte";
    export let onBackButton: () => void

    let ip: string = '';
    let publickey: string = '';
</script>

<div class="fixed inset-0 bg-black bg-opacity-70 items-center justify-center flex z-20">
    <div class="w-[70vw] min-w-[70vw]">
        <SetupPane
            title="Add a Node"
            body="Enter node details below"
        >
        {#snippet features()}
            <EntryRow
                icon="i-carbon-plug"
                title="IP Address"
                password={false}
                bind:value={ip}
            />
            <EntryRow
                icon="i-carbon-api-key"
                title="Public Key"
                multiline={true}
                password={false}
                bind:value={publickey}
            />
        {/snippet}

        {#snippet buttons()}
            <Button
                icon="i-carbon-chevron-left"
                text="Back"
                onClick={onBackButton}
            />
        {/snippet}
        </SetupPane>
    </div>
</div>

<style>
    :global(.w-\[70vw\] .max-w-\[400px\]) {
        max-width: none !important;
        width: 100% !important;
    }
</style>