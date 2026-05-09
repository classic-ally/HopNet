<script lang="ts">
    import Button from '../../Button.svelte';
    import EntryRow from '../../EntryRow.svelte';
    import PassphraseInput from './PassphraseInput.svelte';
    import SetupPane from '../../SetupPane.svelte';
    import StatusSpinner from '../../primitives/StatusSpinner.svelte';
    import { mergeStatusWords, AUTH } from '../../primitives/statusWords';
    import { tokenStore, API_BASE_URL } from '../../stores';

    export let username = '';
    export let passphrase = '';
    let rememberMe = false;
    let loading = false;
    let errorMessage = '';

    const LOGIN_WORDS = mergeStatusWords(AUTH);

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
                    passphrase,
                    remember_me: rememberMe,
                }),
            });

            if (response.ok) {
                const data = await response.json();
                tokenStore.set(data.token);
                // Drop the raw passphrase from component state once we have a
                // token. Binding propagates the clear back to App.svelte and
                // resets PassphraseInput's word fields. Doesn't guarantee V8
                // releases the buffer immediately, but removes the live ref.
                passphrase = '';
                username = '';
            } else if (response.status === 401) {
                errorMessage = 'Invalid username or passphrase';
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
    logoSrc="/hopnet-logo.png"
    title="Log in"
    body=""
    buttonsClass="flex items-center gap-3"
>
    {#snippet features()}
        <EntryRow
            icon="i-carbon-user"
            title="Username"
            password={false}
            bind:value={username}
        />
        <PassphraseInput bind:value={passphrase} />
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
            icon="i-carbon-checkmark"
            text="Log in"
            onClick={handleLogin}
            disabled={loading}
        />
        {#if loading}
            <StatusSpinner words={LOGIN_WORDS} />
        {/if}
    {/snippet}
</SetupPane>
