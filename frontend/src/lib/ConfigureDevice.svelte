<script lang="ts">
    import Button from "./Button.svelte";
    import EntryRow from "./EntryRow.svelte";
    import NetworkDropdown from "./NetworkDropdown.svelte";
    import SetupPane from "./SetupPane.svelte";

    export let onBackButton: () => void;
    export let onForwardButton: () => void;
    export let computername = '';

    export let ip: string;

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
            bind:value={computername}
        />
        <NetworkDropdown
            icon="i-carbon-plug"
            title="Select network interface"
            bind:selected={ip}
        />

    {/snippet}

    {#snippet buttons()}
        <Button
            icon="i-carbon-chevron-left"
            text="Back"
            onClick={() => {onBackButton()}}
        />
        <Button
            icon="i-carbon-chevron-right"
            text="Next"
            onClick={() => {onForwardButton()}}
            position="right"
        />
    {/snippet}
</SetupPane>