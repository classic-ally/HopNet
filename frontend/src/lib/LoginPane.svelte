<script lang="ts">
    import Button from './Button.svelte';
    import EntryRow from './EntryRow.svelte';
    import SetupPane from './SetupPane.svelte';
    import { tokenStore, API_BASE_URL } from './stores';

    export let username = '';
    export let password = '';

    async function handleLogin() {
        try {
            const response = await fetch(`${API_BASE_URL}/login`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    username,
                    password
                }),
            });

            if (response.ok) {
                const data = await response.json();
                tokenStore.set(data);
            } else {
                console.error('Login failed:', response.status);
                // Could add error handling here
            }
        } catch (error) {
            console.error('Login error:', error);
        }
    }
</script>

<SetupPane
    title="Log in"
    body=""
>
    {#snippet features()}
        <EntryRow
            icon="i-carbon-user"
            title="Username"
            password={false}
            bind:value={username}
        />
        <EntryRow
            icon="i-carbon-password"
            title="Password"
            password={true}
            bind:value={password}
        />
    {/snippet}

    {#snippet buttons()}
        <Button
            icon="i-carbon-checkmark"
            text="Log in"
            onClick={handleLogin}
        />
    {/snippet}
</SetupPane>