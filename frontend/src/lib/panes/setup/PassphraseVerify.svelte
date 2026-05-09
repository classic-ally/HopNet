<script lang="ts">
    import Button from "../../Button.svelte";
    import SetupPane from "../../SetupPane.svelte";
    import PassphraseVerifyContent from "../../primitives/PassphraseVerifyContent.svelte";
    import StatusSpinner from "../../primitives/StatusSpinner.svelte";
    import { mergeStatusWords, WHIMSY, AUTH } from "../../primitives/statusWords";

    export let passphrase: string;
    export let onVerified: () => void | Promise<void>;

    let verifyRef: PassphraseVerifyContent;
    let loading = false;

    // Verify itself is instant — the wait is the auto-login that follows
    // (Argon2 derive on the backend). Friendly + auth-accurate.
    const SESSION_WORDS = mergeStatusWords(WHIMSY, AUTH);

    async function handleVerify() {
        if (loading) return;
        if (!verifyRef.verify()) return;
        loading = true;
        try {
            await onVerified();
        } finally {
            // Caller reloads the page on success; if we land here it failed
            // and the user should be able to retry.
            loading = false;
        }
    }
</script>

<SetupPane
    title="Verify Passphrase"
    body="Enter the following words from your passphrase to confirm you've written it down."
    buttonsClass="flex items-center justify-end gap-3"
>
    {#snippet features()}
        <PassphraseVerifyContent {passphrase} bind:this={verifyRef} />
    {/snippet}

    {#snippet buttons()}
        {#if loading}
            <StatusSpinner words={SESSION_WORDS} />
        {/if}
        <Button
            icon="i-carbon-checkmark"
            text="Verify"
            onClick={handleVerify}
            position="right"
            disabled={loading}
        />
    {/snippet}
</SetupPane>
