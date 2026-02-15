<script lang="ts">
    import Button from './Button.svelte';
    import EntryRow from './EntryRow.svelte';
    import SetupPane from './SetupPane.svelte';
    import { tokenStore, API_BASE_URL } from './stores';

    export let username = '';
    export let password = '';
    let rememberMe = false;
    let loading = false;
    let errorMessage = '';

    async function handleLogin() {
        loading = true;
        errorMessage = '';
        try {
            const response = await fetch(`${API_BASE_URL}/login`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    username,
                    password,
                    remember_me: rememberMe,
                }),
            });

            if (response.ok) {
                const data = await response.json();
                tokenStore.set(data.token);
            } else if (response.status === 401) {
                errorMessage = 'Invalid username or password';
            } else if (response.status === 503) {
                errorMessage = 'Node not initialized';
            } else {
                errorMessage = 'Login failed. Please try again.';
            }
        } catch (error) {
            errorMessage = 'Connection error. Is the server running?';
        } finally {
            loading = false;
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
        <label class="flex items-center gap-2 text-sm text-overlay1 mt-2 cursor-pointer select-none">
            <input type="checkbox" bind:checked={rememberMe} class="accent-mauve" />
            Remember me for 24 hours
        </label>
        {#if errorMessage}
            <p class="text-red text-sm mt-2">{errorMessage}</p>
        {/if}
    {/snippet}

    {#snippet buttons()}
        <Button
            icon={loading ? 'i-carbon-rotate-360' : 'i-carbon-checkmark'}
            text={loading ? 'Signing in...' : 'Log in'}
            onClick={handleLogin}
            disabled={loading}
        />
    {/snippet}
</SetupPane>
