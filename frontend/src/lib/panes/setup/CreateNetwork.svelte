<script lang="ts">
    import Button from "../../Button.svelte";
    import EntryRow from "../../EntryRow.svelte";
    import SetupPane from "../../SetupPane.svelte";

    export let onBackButton: () => void;
    export let onForwardButton: () => void;
    export let username = '';
    export let computername = '';
</script>

<SetupPane
    title="Create a Network"
    body="Identify yourself and this device."
    onBack={onBackButton}
    buttonsClass="flex items-center justify-end gap-3"
>
    {#snippet features()}
        <EntryRow
            icon="i-carbon-user"
            title="Username"
            password={false}
            bind:value={username}
        />
        <EntryRow
            icon="i-carbon-devices"
            title="Device name"
            password={false}
            bind:value={computername}
        />
    {/snippet}

    {#snippet buttons()}
        <Button
            icon="i-carbon-chevron-right"
            text="Next"
            onClick={() => {onForwardButton()}}
            position="right"
        />
    {/snippet}
</SetupPane>
