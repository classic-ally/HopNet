<script lang="ts">
    import SetupPane from '../../SetupPane.svelte';
    import Button from "../../Button.svelte";
    import EntryRow from '../../EntryRow.svelte';
    import StatusSpinner from '../../primitives/StatusSpinner.svelte';
    import { mergeStatusWords, WHIMSY, GENESIS, AUTH } from '../../primitives/statusWords';
    import { liveSetupApi, TRANSPORT_FAILURE, type SetupApi } from '../../api/setup';

    export let username: string;
    export let computername: string;

    export let onBackButton: () => void;
    export let onSetupComplete: (passphrase: string) => void;
    export let api: SetupApi = liveSetupApi;

    let isLoading = false;
    let errorMessage = '';

    const SETUP_WORDS = mergeStatusWords(WHIMSY, GENESIS, AUTH);

    const onSave = async () => {
        isLoading = true;
        errorMessage = '';

        const result = await api.createNetwork(username, computername);
        if (result.ok) {
            onSetupComplete(result.passphrase);
        } else if (result.status === TRANSPORT_FAILURE) {
            errorMessage = result.detail ?? 'Setup failed. Please try again.';
        } else {
            errorMessage = `Setup failed with status: ${result.status}`;
        }

        isLoading = false;
    }
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
        {#if isLoading}
            <StatusSpinner words={SETUP_WORDS} />
        {/if}
        {#if errorMessage}
            <p class="text-red text-sm">{errorMessage}</p>
        {/if}
    {/snippet}

    {#snippet buttons()}
        <Button
            icon="i-carbon-chevron-left"
            text="Back"
            onClick={() => {onBackButton()}}
            disabled={isLoading}
        />
        <Button
            icon="i-carbon-save"
            text="Save"
            onClick={() => {onSave()}}
            disabled={isLoading}
        />
    {/snippet}
</SetupPane>
