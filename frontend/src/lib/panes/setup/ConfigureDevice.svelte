<script lang="ts">
    import Button from "../../Button.svelte";
    import EntryRow from "../../EntryRow.svelte";
    import SetupPane from "../../SetupPane.svelte";

    export let onBackButton: () => void;
    export let onForwardButton: () => void;
    export let computername = '';

</script>

<SetupPane
    title="Configure this Device"
    body="Name this device."
>
    {#snippet features()}
        <EntryRow
            icon="i-carbon-devices"
            title="Device name"
            password={false}
            bind:value={computername}
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
