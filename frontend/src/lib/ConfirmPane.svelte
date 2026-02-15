<script lang="ts">
    import SetupPane from './SetupPane.svelte';
    import Button from "./Button.svelte";
    import EntryRow from './EntryRow.svelte';
    import { API_BASE_URL } from './stores';

    export let username: string;
    export let password: string;
    export let computername: string;

    export let onBackButton: () => void;
    export let onSetupComplete: () => void;

    let isLoading = false;
    let errorMessage = '';

    const onSave = async () => {
        isLoading = true;
        errorMessage = '';
        
        try {
            // Create the setup payload matching the backend InitialSetupPayload structure
            const setupData = {
                username: username,
                password: password, // Will be hashed on backend
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
                console.log('Setup completed successfully');
                onSetupComplete();
                // Reload the page to trigger the setup check in App.svelte
                window.location.reload();
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