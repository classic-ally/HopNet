<script lang="ts">
    import SetupPane from '../../SetupPane.svelte';
    import Button from "../../Button.svelte";
    import EntryRow from '../../EntryRow.svelte';
    import StatusSpinner from '../../primitives/StatusSpinner.svelte';
    import { mergeStatusWords, WHIMSY, GENESIS, AUTH } from '../../primitives/statusWords';
    import { API_BASE_URL } from '../../stores';

    export let username: string;
    export let computername: string;

    export let onBackButton: () => void;
    export let onSetupComplete: (passphrase: string) => void;

    let isLoading = false;
    let errorMessage = '';

    const SETUP_WORDS = mergeStatusWords(WHIMSY, GENESIS, AUTH);

    const onSave = async () => {
        isLoading = true;
        errorMessage = '';

        try {
            const setupData = {
                username: username,
                node_name: computername,
            };

            const response = await fetch(`${API_BASE_URL}/setup`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(setupData)
            });

            if (response.ok) {
                const data = await response.json();
                console.log('Setup completed successfully');
                onSetupComplete(data.passphrase);
            } else {
                throw new Error(`Setup failed with status: ${response.status}`);
            }
        } catch (error) {
            console.error('Setup error:', error);
            errorMessage = error instanceof Error ? error.message : 'Setup failed. Please try again.';
        } finally {
            isLoading = false;
        }
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
