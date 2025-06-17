<script lang="ts">
    import SetupPane from './SetupPane.svelte';
    import Button from "./Button.svelte";
    import EntryRow from './EntryRow.svelte';

    interface Props {
        username?: string;
        computername?: string;
        ip?: string;
    }

    export let username: string;
    export let password: string;
    export let computername: string;
    export let ip: string;

    export let onBackButton: () => void;

    const onSave = () => {
        console.log("Save button clicked");
    }

    console.log(username)
</script>

<SetupPane
    title="Confirm Selections"
    body="Creating new network as follows:"
>
    {#snippet features()}
        <EntryRow
            icon="i-carbon-user"
            title="Username"
            password={false}
            value={username}
            readonly={true}
        />
        <EntryRow
            icon="i-carbon-devices"
            title="Device name"
            password={false}
            value={computername}
            readonly={true}
        />
        <EntryRow
            icon="i-carbon-plug"
            title="IP Address"
            password={false}
            value={ip}
            readonly={true}
        />
    {/snippet}

    {#snippet buttons()}
        <Button
            icon="i-carbon-chevron-left"
            text="Back"
            onClick={() => {onBackButton()}}
        />
        <Button
            icon="i-carbon-save"
            text="Save"
            onClick={() => {onSave()}}
        />
    {/snippet}
</SetupPane>