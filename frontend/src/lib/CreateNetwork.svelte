<script lang="ts">
    import Button from "./Button.svelte";
    import EntryRow from "./EntryRow.svelte";
    import SetupPane from "./SetupPane.svelte";

    export let onBackButton: () => void;
    export let username = '';
    export let password = '';
</script>

<SetupPane
        title="Create a User Account"
        body="This is your shared identity across the network, used for encrypting and delegating access to your files."
>
    {#snippet features()}
        <EntryRow
            icon="i-carbon-user"
            title="Username"
            password={false}
            value={username}
        />
        <EntryRow
            icon="i-carbon-password"
            title="Password"
            password={true}
            value={password}
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
