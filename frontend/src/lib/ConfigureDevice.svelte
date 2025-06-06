<script lang="ts">
    import Button from "./Button.svelte";
    import EntryRow from "./EntryRow.svelte";
    import NetworkDropdown from "./NetworkDropdown.svelte";
    import SetupPane from "./SetupPane.svelte";

    export let onBackButton: () => void;
    export let computername = '';

</script>

<SetupPane
    title="Configure this Device"
    body="Name this device, and configure the network you want the software to communicate over."
>
    {#snippet features()}
        <EntryRow
            icon="i-carbon-devices"
            title="Device name"
            password={false}
            value={computername}
        />
        <NetworkDropdown
            
        />

    {/snippet}

    {#snippet buttons()}
        <Button
            icon="i-carbon-chevron-left"
            text="Back"
            onClick={() => {onBackButton()}}
        />
    {/snippet}
</SetupPane>