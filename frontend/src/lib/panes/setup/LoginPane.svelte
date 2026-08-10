<script lang="ts">
    import Button from '../../Button.svelte';
    import EntryRow from '../../EntryRow.svelte';
    import PassphraseInput from './PassphraseInput.svelte';
    import Checkbox from '../../primitives/Checkbox.svelte';
    import SetupPane from '../../SetupPane.svelte';
    import StatusSpinner from '../../primitives/StatusSpinner.svelte';
    import { mergeStatusWords, AUTH } from '../../primitives/statusWords';
    import { tokenStore } from '../../stores';
    import { router } from '../../router.svelte';
    import { liveSetupApi, TRANSPORT_FAILURE, type SetupApi } from '../../api/setup';

    export let username = '';
    export let passphrase = '';
    export let api: SetupApi = liveSetupApi;
    let rememberMe = false;
    let loading = false;
    let errorMessage = '';

    const LOGIN_WORDS = mergeStatusWords(AUTH);

    async function handleLogin() {
        loading = true;
        errorMessage = '';

        const result = await api.login(username, passphrase, rememberMe);
        if (result.ok) {
            tokenStore.set(result.token);
            router.redirectToIntended();
            // Drop the raw passphrase from component state once we have a
            // token. Binding propagates the clear back to App.svelte and
            // resets PassphraseInput's word fields. Doesn't guarantee V8
            // releases the buffer immediately, but removes the live ref.
            passphrase = '';
            username = '';
        } else if (result.status === TRANSPORT_FAILURE) {
            errorMessage = 'Connection error. Is the server running?';
        } else if (result.status === 401) {
            errorMessage = 'Invalid username or passphrase';
        } else if (result.status === 503) {
            errorMessage = 'Node not initialized';
        } else {
            errorMessage = 'Login failed. Please try again.';
        }

        loading = false;
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
        <Checkbox
            bind:checked={rememberMe}
            label="Remember me for 24 hours"
            className="text-overlay1 mt-2"
        />
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
