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
    export let onSetupComplete: () => void;

    let isLoading = false;
    let errorMessage = '';

    const onSave = async () => {
        isLoading = true;
        errorMessage = '';
        
        try {
            // Create the setup object matching the backend SetupObject structure
            const setupData = {
                user: {
                    user_id: 0, // Will be assigned by backend
                    username: username,
                    password: password // hashed on backend
                },
                node: {
                    node_id: 0, // Will be assigned by backend
                    name: computername,
                    ip_address: ip,
                    port: 34632, // Default port from main.rs
                    owner: 0 // Will be set by backend to the created user's ID
                }
            };

            const response = await fetch('http://localhost:34632/setup', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(setupData)
            });

            if (response.ok) {
                console.log('Setup completed successfully');
                onSetupComplete();
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